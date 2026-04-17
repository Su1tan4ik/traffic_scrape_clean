"""
Almaty traffic scraper — city-wide coverage via Yandex Maps tiles.
No API key required for traffic data.
OpenWeatherMap key optional for weather columns.

Collected fields per tile per run:
  timestamp, segment_id, lat, lon, street_name,
  traffic_score, congestion_level, speed_avg,
  pixels_free/slow/moderate/heavy/jam, total_traffic_pixels,
  pct_free/slow/moderate/heavy/jam,
  weather_temp_c, weather_feels_c, weather_humidity, weather_wind_ms,
  weather_precip_1h, weather_visibility_m, weather_condition, weather_description,
  tile_x, tile_y, tile_z

Run once:  python scraper.py
Cron:      */30 * * * * /path/to/run_scraper.sh
Output:    data/traffic_data.csv   ← append-only log (for ML)
           data/traffic_latest.csv ← current city snapshot
"""

import asyncio
import csv
import io
import logging
import time
from datetime import datetime, timezone
from pathlib import Path

import aiohttp
from PIL import Image

from config import (
    ALMATY_BOUNDS,
    COLOR_RULES,
    LATEST_FILE,
    MAX_CONCURRENT_REQUESTS,
    MIN_TRAFFIC_PIXEL_RATIO,
    OUTPUT_DIR,
    OUTPUT_FILE,
    REQUEST_HEADERS,
    REQUEST_TIMEOUT,
    RETRY_ATTEMPTS,
    RETRY_BACKOFF,
    TILE_URL,
    ZOOM,
    score_to_label,
    score_to_speed,
)
from geocoder import build_geocode_cache, load_cache
from tiles import Tile, tiles_for_bbox
from weather import fetch_weather

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("scraper.log"),
        logging.StreamHandler(),
    ],
)
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# CSV schema
# ---------------------------------------------------------------------------
CSV_FIELDS = [
    # Identity
    "timestamp",
    "segment_id",
    "lat",
    "lon",
    "street_name",
    # Traffic
    "traffic_score",
    "congestion_level",
    "speed_avg",
    # Raw pixel counts
    "pixels_free",
    "pixels_slow",
    "pixels_moderate",
    "pixels_heavy",
    "pixels_jam",
    "total_traffic_pixels",
    # Normalised percentages (comparable across tiles of different sizes)
    "pct_free",
    "pct_slow",
    "pct_moderate",
    "pct_heavy",
    "pct_jam",
    # Weather (optional — None when OWM key not set)
    "weather_temp_c",
    "weather_feels_c",
    "weather_humidity",
    "weather_wind_ms",
    "weather_precip_1h",
    "weather_visibility_m",
    "weather_condition",
    "weather_description",
    # Tile metadata
    "tile_x",
    "tile_y",
    "tile_z",
]


# ---------------------------------------------------------------------------
# Pixel colour analysis
# ---------------------------------------------------------------------------

def classify_pixel(r: int, g: int, b: int) -> str | None:
    for label, _s_min, _s_max, r_min, r_max, g_min, g_max, b_min, b_max in COLOR_RULES:
        if r_min <= r <= r_max and g_min <= g <= g_max and b_min <= b <= b_max:
            return label
    return None


def analyse_tile_image(image_bytes: bytes) -> dict:
    """
    Parse a 256×256 RGBA PNG tile.
    Returns raw pixel counts, percentage share per category, and traffic_score.
    """
    counts = {label: 0 for label, *_ in COLOR_RULES}

    img = Image.open(io.BytesIO(image_bytes)).convert("RGBA")
    pixels = img.load()
    width, height = img.size

    total_pixels = 0
    for py in range(height):
        for px in range(width):
            r, g, b, a = pixels[px, py]
            if a < 60:
                continue
            total_pixels += 1
            label = classify_pixel(r, g, b)
            if label:
                counts[label] += 1

    total_traffic = sum(counts.values())

    # Weighted average score
    if total_traffic == 0:
        traffic_score = 0.0
    else:
        weighted = sum(
            counts[label] * (s_min + s_max) / 2
            for label, s_min, s_max, *_ in COLOR_RULES
        )
        traffic_score = round(weighted / total_traffic, 2)

    # Reject near-empty tiles
    if total_pixels > 0 and total_traffic / total_pixels < MIN_TRAFFIC_PIXEL_RATIO:
        traffic_score = 0.0

    # Normalised percentages (0.0–100.0, relative to traffic pixels only)
    def pct(n: int) -> float:
        return round(n / total_traffic * 100, 2) if total_traffic > 0 else 0.0

    return {
        "traffic_score":    traffic_score,
        "pixels_free":      counts["free"],
        "pixels_slow":      counts["slow"],
        "pixels_moderate":  counts["moderate"],
        "pixels_heavy":     counts["heavy"],
        "pixels_jam":       counts["jam"],
        "total_traffic_pixels": total_traffic,
        "pct_free":      pct(counts["free"]),
        "pct_slow":      pct(counts["slow"]),
        "pct_moderate":  pct(counts["moderate"]),
        "pct_heavy":     pct(counts["heavy"]),
        "pct_jam":       pct(counts["jam"]),
    }


# ---------------------------------------------------------------------------
# Fetch one tile
# ---------------------------------------------------------------------------

async def fetch_tile(
    session: aiohttp.ClientSession,
    sem: asyncio.Semaphore,
    tile: Tile,
    timestamp: str,
    street_name: str,
    weather: dict,
) -> dict | None:
    url = TILE_URL.format(x=tile.x, y=tile.y, z=tile.z)
    lat, lon = tile.center_latlon()

    for attempt in range(1, RETRY_ATTEMPTS + 1):
        try:
            async with sem:
                async with session.get(
                    url,
                    headers=REQUEST_HEADERS,
                    timeout=aiohttp.ClientTimeout(total=REQUEST_TIMEOUT),
                ) as resp:
                    if resp.status == 204:
                        return None              # empty tile (park/water) — skip
                    if resp.status == 429:
                        await asyncio.sleep(RETRY_BACKOFF * attempt)
                        continue
                    if resp.status != 200:
                        log.debug("HTTP %s for tile %s", resp.status, tile.segment_id)
                        break
                    image_bytes = await resp.read()

            analysis = analyse_tile_image(image_bytes)
            score = analysis["traffic_score"]

            return {
                "timestamp":        timestamp,
                "segment_id":       tile.segment_id,
                "lat":              lat,
                "lon":              lon,
                "street_name":      street_name,
                "traffic_score":    score,
                "congestion_level": score_to_label(score),
                "speed_avg":        score_to_speed(score),
                **{k: analysis[k] for k in analysis if k != "traffic_score"},
                **weather,
                "tile_x": tile.x,
                "tile_y": tile.y,
                "tile_z": tile.z,
            }

        except asyncio.TimeoutError:
            log.warning("Timeout — %s (attempt %d/%d)", tile.segment_id, attempt, RETRY_ATTEMPTS)
        except Exception as exc:
            log.warning("Error — %s: %s (attempt %d/%d)", tile.segment_id, exc, attempt, RETRY_ATTEMPTS)

        if attempt < RETRY_ATTEMPTS:
            await asyncio.sleep(RETRY_BACKOFF * attempt)

    # All retries failed — null row (preserves the gap in the log)
    return {
        "timestamp":        timestamp,
        "segment_id":       tile.segment_id,
        "lat":              lat,
        "lon":              lon,
        "street_name":      street_name,
        "traffic_score":    None,
        "congestion_level": "Unknown",
        "speed_avg":        None,
        "pixels_free": 0, "pixels_slow": 0, "pixels_moderate": 0,
        "pixels_heavy": 0, "pixels_jam": 0, "total_traffic_pixels": 0,
        "pct_free": 0.0, "pct_slow": 0.0, "pct_moderate": 0.0,
        "pct_heavy": 0.0, "pct_jam": 0.0,
        **weather,
        "tile_x": tile.x, "tile_y": tile.y, "tile_z": tile.z,
    }


# ---------------------------------------------------------------------------
# CSV output
# ---------------------------------------------------------------------------

def ensure_output_dir():
    Path(OUTPUT_DIR).mkdir(parents=True, exist_ok=True)


def append_to_csv(rows: list[dict]):
    path = Path(OUTPUT_DIR) / OUTPUT_FILE
    file_exists = path.exists()
    with open(path, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_FIELDS)
        if not file_exists:
            writer.writeheader()
        writer.writerows(rows)
    log.info("Appended %d rows → %s", len(rows), path)


def write_latest_csv(rows: list[dict]):
    path = Path(OUTPUT_DIR) / LATEST_FILE
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_FIELDS)
        writer.writeheader()
        writer.writerows(rows)
    log.info("Latest snapshot → %s", path)


# ---------------------------------------------------------------------------
# Main collection
# ---------------------------------------------------------------------------

async def run_collection():
    timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    log.info("=== Collection started at %s ===", timestamp)

    all_tiles = tiles_for_bbox(
        ALMATY_BOUNDS["lat_min"],
        ALMATY_BOUNDS["lat_max"],
        ALMATY_BOUNDS["lon_min"],
        ALMATY_BOUNDS["lon_max"],
        ZOOM,
    )
    log.info("Tiles to process: %d (zoom=%d)", len(all_tiles), ZOOM)

    # --- Step 1: load geocode cache (run build_geocache.py once first) ---
    geocode_cache = load_cache()
    if not geocode_cache:
        log.warning("Geocode cache is empty! Run: python build_geocache.py")
        log.warning("Continuing without street names...")

    sem = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)
    connector = aiohttp.TCPConnector(limit=MAX_CONCURRENT_REQUESTS)

    rows: list[dict] = []
    skipped = 0

    async with aiohttp.ClientSession(connector=connector) as session:
        # --- Step 2: fetch weather once for the whole city ---
        weather = await fetch_weather(session)
        if weather.get("weather_condition"):
            log.info("Weather: %s %.1f°C", weather["weather_condition"], weather["weather_temp_c"])
        else:
            log.info("Weather: not configured (set OWM_API_KEY in config.py to enable)")

        # --- Step 3: fetch all traffic tiles ---
        tasks = [
            fetch_tile(
                session, sem, tile, timestamp,
                geocode_cache.get(tile.segment_id, ""),
                weather,
            )
            for tile in all_tiles
        ]

        chunk_size = 50
        for i in range(0, len(tasks), chunk_size):
            results = await asyncio.gather(*tasks[i : i + chunk_size])
            for r in results:
                if r is None:
                    skipped += 1
                else:
                    rows.append(r)
            done = min(i + chunk_size, len(tasks))
            log.info("Progress: %d / %d tiles  (%d rows collected)", done, len(tasks), len(rows))

    ensure_output_dir()
    append_to_csv(rows)
    write_latest_csv(rows)

    success = sum(1 for r in rows if r["traffic_score"] is not None)
    log.info(
        "=== Done: %d rows saved | %d empty tiles skipped | %s ===",
        success, skipped, timestamp,
    )
    return rows


def main():
    t0 = time.time()
    asyncio.run(run_collection())
    log.info("Total elapsed: %.1fs", time.time() - t0)


if __name__ == "__main__":
    main()

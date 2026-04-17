"""
Configuration for the Almaty traffic scraper.
Uses Yandex Maps traffic tiles (public, no API key needed).
Yandex covers Kazakhstan including Almaty with real-time jam data.

Env vars (set in .env locally, or in Render dashboard for production):
  SUPABASE_URL   — from Supabase project settings
  SUPABASE_KEY   — anon/public key from Supabase project settings
  OWM_API_KEY    — optional, from openweathermap.org
"""

import os

# ---------------------------------------------------------------------------
# Almaty city bounding box
# ---------------------------------------------------------------------------
ALMATY_BOUNDS = {
    "lat_min": 43.17,
    "lat_max": 43.38,
    "lon_min": 76.82,
    "lon_max": 77.07,
}

# Tile zoom level.
# 13 → fewer tiles, faster, ~4 km per tile
# 14 → recommended: ~2 km per tile, 289 tiles cover all Almaty
# 15 → more detail, ~1 km per tile, ~1000+ tiles
ZOOM = 14

# ---------------------------------------------------------------------------
# Yandex Maps traffic tile endpoint (no API key required)
# Returns 256×256 RGBA PNG tiles with coloured traffic overlay.
# 204 = empty tile (no roads / no data) — skip silently.
# ---------------------------------------------------------------------------
TILE_URL = (
    "https://core-jams-rdr-cache.maps.yandex.net/1.1/tiles"
    "?trf&l=trf&x={x}&y={y}&z={z}&scale=1&lang=ru_RU"
)

REQUEST_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/123.0.0.0 Safari/537.36"
    ),
    "Referer": "https://yandex.kz/maps/",
    "Accept": "image/png,image/*,*/*;q=0.8",
}

# ---------------------------------------------------------------------------
# Traffic colour rules (Yandex Maps colour palette, empirically verified).
# Each tuple: (label, score_min, score_max, R_min, R_max, G_min, G_max, B_min, B_max)
#
# Verified from real tile pixel analysis:
#   Free flow  → #5ecd4c family  RGB ~(80-120, 190-215, 60-100)
#   Slow       → #f9c600 family  RGB ~(220-255, 180-220,   0- 50)
#   Moderate   → #ff8800 family  RGB ~(220-255, 100-160,   0- 40)
#   Heavy      → #ff3300 family  RGB ~(220-255,  20- 80,   0- 40)
#   Standstill → #990000 family  RGB ~(120-200,   0- 30,   0- 30)
# ---------------------------------------------------------------------------
COLOR_RULES = [
    ("free",      1, 2,   60, 140,  170, 230,  40, 120),   # green
    ("slow",      3, 4,  210, 255,  170, 225,   0,  60),   # yellow
    ("moderate",  5, 6,  210, 255,   90, 170,   0,  50),   # orange
    ("heavy",     7, 8,  210, 255,   10,  90,   0,  50),   # red
    ("jam",       9, 10, 110, 210,    0,  35,   0,  35),   # dark red
]

# Skip tiles where traffic pixels are < this fraction of total pixels
MIN_TRAFFIC_PIXEL_RATIO = 0.003

# ---------------------------------------------------------------------------
# HTTP settings
# ---------------------------------------------------------------------------
REQUEST_TIMEOUT = 15
MAX_CONCURRENT_REQUESTS = 16
RETRY_ATTEMPTS = 3
RETRY_BACKOFF = 2

# ---------------------------------------------------------------------------
# Output
# ---------------------------------------------------------------------------
OUTPUT_DIR = "data"
OUTPUT_FILE = "traffic_data.csv"    # append — full historical log
LATEST_FILE = "traffic_latest.csv"  # overwritten each run — current snapshot

# ---------------------------------------------------------------------------
# Reverse geocoding (Nominatim — free, no key)
# First run geocodes all tiles and caches forever; subsequent runs instant.
# ---------------------------------------------------------------------------
GEOCODE_CACHE_FILE = "data/geocode_cache.json"
NOMINATIM_RATE_LIMIT = 1.1          # seconds between requests (ToS: max 1 req/s)

# ---------------------------------------------------------------------------
# Weather (OpenWeatherMap free tier — optional)
# Leave empty string to skip weather collection.
# Get free key at https://openweathermap.org/api
# ---------------------------------------------------------------------------
OWM_API_KEY = os.environ.get("OWM_API_KEY", "")
OWM_CITY = "Almaty,KZ"

# ---------------------------------------------------------------------------
# Speed estimate from traffic score
# Based on typical Almaty arterial road speeds + congestion degradation.
# score 1 → ~65 km/h (free flow), score 10 → ~3 km/h (standstill)
# ---------------------------------------------------------------------------
SPEED_FREE_KMH = 65.0
SPEED_JAM_KMH  = 3.0


def score_to_speed(score: float) -> float:
    """Estimate average speed (km/h) from traffic score 1-10."""
    if score <= 0:
        return 0.0
    s = max(1.0, min(10.0, score))
    speed = SPEED_FREE_KMH - (s - 1) * (SPEED_FREE_KMH - SPEED_JAM_KMH) / 9.0
    return round(speed, 1)


# ---------------------------------------------------------------------------
# Score → human label
# ---------------------------------------------------------------------------
def score_to_label(score: float) -> str:
    if score <= 0:
        return "Unknown"
    if score <= 3:
        return "Low"
    if score <= 6:
        return "Medium"
    if score <= 8:
        return "High"
    return "Critical"

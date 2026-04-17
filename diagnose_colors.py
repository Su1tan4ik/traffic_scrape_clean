"""
DIAGNOSTIC TOOL v2 — picks tiles from CENTRAL Almaty (not outskirts).

Usage:
  python diagnose_colors.py

IMPORTANT: Run during PEAK HOURS (8-9 AM or 6-7 PM Almaty time)
           to see yellow/orange/red pixels.
"""

import asyncio
import io
from collections import Counter

import aiohttp
from PIL import Image

from config import ALMATY_BOUNDS, REQUEST_HEADERS, REQUEST_TIMEOUT, TILE_URL, ZOOM
from tiles import tiles_for_bbox


def rgb_to_hsv(r, g, b):
    r, g, b = r / 255.0, g / 255.0, b / 255.0
    mx, mn = max(r, g, b), min(r, g, b)
    diff = mx - mn
    v = mx * 100
    s = (diff / mx * 100) if mx > 0 else 0
    if diff == 0:
        h = 0
    elif mx == r:
        h = (60 * ((g - b) / diff) + 360) % 360
    elif mx == g:
        h = (60 * ((b - r) / diff) + 120) % 360
    else:
        h = (60 * ((r - g) / diff) + 240) % 360
    return round(h), round(s), round(v)


async def diagnose():
    tiles = tiles_for_bbox(
        ALMATY_BOUNDS["lat_min"], ALMATY_BOUNDS["lat_max"],
        ALMATY_BOUNDS["lon_min"], ALMATY_BOUNDS["lon_max"],
        ZOOM,
    )

    # ================================================================
    # FIX: Pick tiles from CENTRAL Almaty (not western outskirts!)
    # Central Almaty: lat 43.24-43.30, lon 76.90-77.00
    # This covers: Abay Ave, Dostyk Ave, Al-Farabi Ave, Tole Bi St
    # ================================================================
    central_tiles = [
        t for t in tiles
        if 43.24 < t.center_latlon()[0] < 43.30
        and 76.90 < t.center_latlon()[1] < 77.00
    ]

    if not central_tiles:
        print("ERROR: No central tiles found! Using all tiles.")
        central_tiles = tiles[:10]

    print(f"Found {len(central_tiles)} central Almaty tiles.")
    print(f"Testing {min(10, len(central_tiles))} of them...\n")

    # Track global color stats
    global_colors = Counter()
    global_classified = Counter()

    async with aiohttp.ClientSession() as session:
        for tile in central_tiles[:10]:
            url = TILE_URL.format(x=tile.x, y=tile.y, z=tile.z)
            try:
                async with session.get(
                    url,
                    headers=REQUEST_HEADERS,
                    timeout=aiohttp.ClientTimeout(total=REQUEST_TIMEOUT),
                ) as resp:
                    if resp.status == 204:
                        continue
                    if resp.status != 200:
                        print(f"  {tile.segment_id}: HTTP {resp.status}")
                        continue

                    data = await resp.read()
                    img = Image.open(io.BytesIO(data)).convert("RGBA")
                    pixels = img.load()
                    w, h = img.size

                    color_counter = Counter()
                    total_visible = 0

                    for py in range(h):
                        for px in range(w):
                            r, g, b, a = pixels[px, py]
                            if a < 30:
                                continue
                            total_visible += 1
                            rb, gb, bb = (r // 10) * 10, (g // 10) * 10, (b // 10) * 10
                            color_counter[(rb, gb, bb)] += 1

                    if total_visible < 100:
                        continue

                    lat, lon = tile.center_latlon()
                    print(f"=== {tile.segment_id} ({lat}, {lon}) ===")
                    print(f"  Visible pixels: {total_visible}")
                    print(f"  Top 20 colors (RGB bucketed to nearest 10):")

                    for (r, g, b), count in color_counter.most_common(20):
                        pct = count / total_visible * 100
                        h_val, s_val, v_val = rgb_to_hsv(r + 5, g + 5, b + 5)

                        # Classification logic
                        if s_val < 5 and v_val > 90:
                            label = "BACKGROUND (white/gray)"
                        elif 80 <= h_val <= 160 and s_val >= 10:
                            label = "FREE (green)"
                        elif 35 <= h_val < 80 and s_val > 25:
                            label = "SLOW (yellow)"
                        elif 10 <= h_val < 35 and s_val > 25:
                            label = "MODERATE (orange)"
                        elif (h_val < 10 or h_val > 350) and s_val > 25 and v_val > 40:
                            label = "HEAVY (red)"
                        elif (h_val < 15 or h_val > 345) and s_val > 25 and v_val <= 40:
                            label = "JAM (dark red)"
                        else:
                            label = "??? (unclassified)"

                        global_colors[(r, g, b)] += count
                        global_classified[label] += count

                        print(f"    RGB({r:3d},{g:3d},{b:3d})  "
                              f"HSV({h_val:3d},{s_val:2d},{v_val:2d})  "
                              f"{count:5d} px ({pct:5.1f}%)  -> {label}")
                    print()

            except Exception as e:
                print(f"  {tile.segment_id}: Error — {e}")

            await asyncio.sleep(0.3)

    # Global summary
    print("=" * 70)
    print("  GLOBAL SUMMARY (all tiles combined)")
    print("=" * 70)
    total = sum(global_classified.values())
    if total > 0:
        for label in ["FREE (green)", "SLOW (yellow)", "MODERATE (orange)",
                       "HEAVY (red)", "JAM (dark red)", "BACKGROUND (white/gray)",
                       "??? (unclassified)"]:
            count = global_classified.get(label, 0)
            pct = count / total * 100
            marker = " <-- GOOD!" if label in ["SLOW (yellow)", "MODERATE (orange)",
                                                 "HEAVY (red)", "JAM (dark red)"] and count > 0 else ""
            print(f"  {label:30s}: {count:8d} px ({pct:5.1f}%){marker}")

    if global_classified.get("SLOW (yellow)", 0) == 0:
        print("\n  ⚠️  NO yellow/orange/red pixels detected!")
        print("  → If it's NOT peak hour (8-9 AM / 6-7 PM) — this is normal.")
        print("  → If it IS peak hour — there may be a color detection issue.")
        print("  → Copy-paste this ENTIRE output and send it for analysis.")


if __name__ == "__main__":
    from datetime import datetime, timezone, timedelta

    almaty_tz = timezone(timedelta(hours=5))
    now = datetime.now(almaty_tz)
    print("=" * 70)
    print("  YANDEX TRAFFIC TILE COLOR DIAGNOSTIC v2")
    print(f"  Current time in Almaty: {now.strftime('%H:%M')}")
    print("=" * 70)

    if 7 <= now.hour <= 9 or 17 <= now.hour <= 20:
        print("  ✅ Good — this is peak hour, should see traffic colors.")
    else:
        print(f"  ⚠️  Current hour: {now.hour}:00 — NOT peak hour!")
        print("  Best times to run: 08:00-09:00 or 18:00-19:00 Almaty time.")
        print("  You may not see yellow/orange/red pixels now.")

    print()
    asyncio.run(diagnose())

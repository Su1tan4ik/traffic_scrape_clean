#!/bin/bash
# Wrapper script for cron — ensures correct Python env and working directory.
# Cron entry (runs every hour):
#   0 * * * * /Users/demessinovrakhymzhan/Desktop/2gis_Script/run_scraper.sh

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# Activate virtual environment if it exists
if [ -f "$SCRIPT_DIR/.venv/bin/activate" ]; then
    source "$SCRIPT_DIR/.venv/bin/activate"
fi

python scraper.py >> "$SCRIPT_DIR/cron.log" 2>&1

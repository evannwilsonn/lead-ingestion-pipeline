#!/usr/bin/env bash
# Cron wrapper: loads .env, prevents overlapping runs, runs the pipeline.
set -euo pipefail
cd "$(dirname "$0")"
set -a; source ./.env; set +a
exec /usr/bin/flock -n /tmp/lead-pipeline.lock ./venv/bin/python lead_pipeline.py "$@"

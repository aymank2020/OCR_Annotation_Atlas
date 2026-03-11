#!/usr/bin/env bash
set -euo pipefail

APP_DIR="${1:-/root/OCR_annotation_Atlas}"
SERVICE_NAME="${2:-atlas-autopilot.service}"
INTERVAL_MIN="${3:-5}"

if ! command -v crontab >/dev/null 2>&1; then
  echo "[watchdog-install] crontab not found" >&2
  exit 1
fi

PY_BIN="${PY_BIN:-python3}"
LINE="*/${INTERVAL_MIN} * * * * cd ${APP_DIR} && ${PY_BIN} atlas_watchdog.py --service ${SERVICE_NAME} --outputs-dir outputs --events-file outputs/watchdog_events.jsonl >> outputs/watchdog_cron.log 2>&1"
MARKER="# atlas_watchdog_cron"

TMP="$(mktemp)"
crontab -l 2>/dev/null | grep -v "${MARKER}" > "${TMP}" || true
{
  cat "${TMP}"
  echo "${LINE} ${MARKER}"
} | crontab -
rm -f "${TMP}"

echo "[watchdog-install] installed cron:"
echo "${LINE}"

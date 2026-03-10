#!/usr/bin/env bash
set -euo pipefail

# End-to-end re-audit workflow directly from Google Drive via rclone.
# 1) Pull metadata from Drive to local temp snapshot (no videos by default)
# 2) Build episodes_review_index.json
# 3) Export chat review packages
# 4) Upload generated review artifacts back to the same Drive folder

APP_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REMOTE_NAME="${RCLONE_REMOTE:-gdrive}"
WORK_DIR="${WORK_DIR:-/tmp/atlas_drive_review}"
ONLY_STATUS="${ONLY_STATUS:-disputed,policy_fail,error,labeled_not_submitted}"
EXPORT_LIMIT="${EXPORT_LIMIT:-0}"
INCLUDE_VIDEO="${INCLUDE_VIDEO:-0}"
UPLOAD_RESULTS="${UPLOAD_RESULTS:-1}"

DRIVE_LINK=""
DRIVE_PATH=""

usage() {
  cat <<EOF
Usage:
  bash atlas_drive_review_workflow.sh --drive-link "<google-folder-link>"
  bash atlas_drive_review_workflow.sh --drive-path "OCR_annotation_Atlas/vps_outputs"

Options:
  --drive-link <url>      Google Drive folder link (extracts folder id)
  --drive-path <path>     rclone path under remote (alternative to link)
  --work-dir <dir>        Local temp work dir (default: /tmp/atlas_drive_review)
  --include-video 0|1     Copy videos too (default: 0)
  --upload-results 0|1    Upload index/chat_reviews back to Drive (default: 1)
  --only-status <csv>     Export statuses for chat packages
  --limit <n>             Limit exported episodes (0 = all)
  --remote <name>         rclone remote (default: gdrive)

Env overrides:
  RCLONE_REMOTE, WORK_DIR, INCLUDE_VIDEO, UPLOAD_RESULTS, ONLY_STATUS, EXPORT_LIMIT
EOF
}

extract_folder_id() {
  local link="$1"
  if [[ "$link" =~ /folders/([a-zA-Z0-9_-]+) ]]; then
    echo "${BASH_REMATCH[1]}"
    return 0
  fi
  if [[ "$link" =~ [\?\&]id=([a-zA-Z0-9_-]+) ]]; then
    echo "${BASH_REMATCH[1]}"
    return 0
  fi
  return 1
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --drive-link) DRIVE_LINK="${2:-}"; shift 2 ;;
    --drive-path) DRIVE_PATH="${2:-}"; shift 2 ;;
    --work-dir) WORK_DIR="${2:-}"; shift 2 ;;
    --include-video) INCLUDE_VIDEO="${2:-0}"; shift 2 ;;
    --upload-results) UPLOAD_RESULTS="${2:-1}"; shift 2 ;;
    --only-status) ONLY_STATUS="${2:-}"; shift 2 ;;
    --limit) EXPORT_LIMIT="${2:-0}"; shift 2 ;;
    --remote) REMOTE_NAME="${2:-gdrive}"; shift 2 ;;
    -h|--help) usage; exit 0 ;;
    *) echo "[drive-review] Unknown argument: $1" >&2; usage; exit 1 ;;
  esac
done

if ! command -v rclone >/dev/null 2>&1; then
  echo "[drive-review] rclone is required." >&2
  exit 1
fi

if ! command -v python3 >/dev/null 2>&1; then
  echo "[drive-review] python3 is required." >&2
  exit 1
fi

if [[ -z "$DRIVE_LINK" && -z "$DRIVE_PATH" ]]; then
  echo "[drive-review] Provide either --drive-link or --drive-path." >&2
  exit 1
fi

SRC="${REMOTE_NAME}:${DRIVE_PATH}"
DEST_BASE="${REMOTE_NAME}:${DRIVE_PATH}"
ROOT_ID=""
DRIVE_ARGS=()
if [[ -n "$DRIVE_LINK" ]]; then
  ROOT_ID="$(extract_folder_id "$DRIVE_LINK" || true)"
  if [[ -z "$ROOT_ID" ]]; then
    echo "[drive-review] Could not extract folder id from link." >&2
    exit 1
  fi
  SRC="${REMOTE_NAME}:"
  DEST_BASE="${REMOTE_NAME}:"
  DRIVE_ARGS=(--drive-root-folder-id "$ROOT_ID")
fi

SNAPSHOT_DIR="$WORK_DIR/snapshot"
rm -rf "$SNAPSHOT_DIR"
mkdir -p "$SNAPSHOT_DIR"

unset HTTP_PROXY HTTPS_PROXY ALL_PROXY http_proxy https_proxy all_proxy

echo "[drive-review] pulling metadata from ${SRC} ..."
rclone copy "$SRC" "$SNAPSHOT_DIR" \
  "${DRIVE_ARGS[@]}" \
  --exclude "*.mp4" --exclude "*.mov" --exclude "*.webm" \
  --exclude "*.mkv" --exclude "*.avi" \
  --progress --checkers 8 --transfers 4 --create-empty-src-dirs

if [[ "$INCLUDE_VIDEO" == "1" ]]; then
  echo "[drive-review] pulling videos (this may consume storage/time) ..."
  rclone copy "$SRC" "$SNAPSHOT_DIR" \
    "${DRIVE_ARGS[@]}" \
    --include "video_*.mp4" --include "**/video_*.mp4" \
    --progress --checkers 4 --transfers 2 --create-empty-src-dirs
fi

EFFECTIVE_OUTPUTS="$SNAPSHOT_DIR"
if [[ -d "$SNAPSHOT_DIR/outputs" ]]; then
  EFFECTIVE_OUTPUTS="$SNAPSHOT_DIR/outputs"
fi

echo "[drive-review] effective outputs dir: $EFFECTIVE_OUTPUTS"

INDEX_PATH="$EFFECTIVE_OUTPUTS/episodes_review_index.json"
CHAT_DIR="$EFFECTIVE_OUTPUTS/chat_reviews"

python3 "$APP_DIR/atlas_review_builder.py" \
  --outputs-dir "$EFFECTIVE_OUTPUTS" \
  --out "$INDEX_PATH"

python3 "$APP_DIR/atlas_chat_exporter.py" \
  --index "$INDEX_PATH" \
  --out-dir "$CHAT_DIR" \
  --only-status "$ONLY_STATUS" \
  --limit "$EXPORT_LIMIT"

if [[ "$UPLOAD_RESULTS" == "1" ]]; then
  echo "[drive-review] uploading results back to Drive ..."
  rclone copy "$INDEX_PATH" "$DEST_BASE" "${DRIVE_ARGS[@]}" --progress
  rclone copy "$CHAT_DIR" "$DEST_BASE/chat_reviews" "${DRIVE_ARGS[@]}" --progress
fi

echo "[drive-review] done."
echo "  index: $INDEX_PATH"
echo "  chat packages: $CHAT_DIR"

#!/usr/bin/env bash
set -euo pipefail

# Safe update helper:
# - pulls latest code from origin/<branch>
# - preserves local runtime secrets/settings (e.g. .env and server YAML)
# - avoids destructive git reset/hard operations

BRANCH="${1:-main}"
APP_DIR="${2:-$(pwd)}"
REMOTE_URL="${3:-https://github.com/aymank2020/OCR_Annotation_Atlas.git}"

cd "$APP_DIR"
if [ ! -d .git ]; then
  echo "[safe-update] .git is missing. Bootstrapping git metadata from remote..."
  TMP="$(mktemp -d)"
  git clone --no-checkout "$REMOTE_URL" "$TMP/repo"
  cp -a "$TMP/repo/.git" .
  rm -rf "$TMP"
  echo "[safe-update] git metadata restored."
fi

TS="$(date +%Y%m%d_%H%M%S)"
BACKUP_DIR="${APP_DIR}/.safe_update_backups/${TS}"
mkdir -p "$BACKUP_DIR"
DID_STASH=0
STASH_NAME="safe-update-autostash-${TS}"
AUTO_STASH="${SAFE_UPDATE_AUTOSTASH:-1}"

preserve_files=(
  ".env"
  "sample_web_auto_solver_vps.yaml"
  "sample_web_auto_solver.yaml"
  "sample_web_auto_solver_no_complete.yaml"
  ".state/atlas_auth.json"
)

echo "[safe-update] backup dir: $BACKUP_DIR"
for rel in "${preserve_files[@]}"; do
  if [ -f "$rel" ]; then
    mkdir -p "$BACKUP_DIR/$(dirname "$rel")"
    cp -f "$rel" "$BACKUP_DIR/$rel"
    echo "[safe-update] backed up: $rel"
  fi
done

if [ "$AUTO_STASH" = "1" ]; then
  if ! git diff --quiet || ! git diff --cached --quiet || [ -n "$(git ls-files --others --exclude-standard)" ]; then
    if git stash push --include-untracked -m "$STASH_NAME" >/dev/null; then
      DID_STASH=1
      echo "[safe-update] autostash created: $STASH_NAME"
    fi
  fi
fi

git fetch origin
git pull --ff-only origin "$BRANCH"

if [ "$DID_STASH" -eq 1 ]; then
  if git stash list | grep -q "$STASH_NAME"; then
    if git stash pop --index >/dev/null; then
      echo "[safe-update] autostash restored."
    else
      echo "[safe-update] warning: autostash restore had conflicts. Resolve manually with: git stash list / git stash show -p"
    fi
  fi
fi

# Restore preserved local runtime files (if they existed before update)
for rel in "${preserve_files[@]}"; do
  if [ -f "$BACKUP_DIR/$rel" ]; then
    mkdir -p "$(dirname "$rel")"
    cp -f "$BACKUP_DIR/$rel" "$rel"
    echo "[safe-update] restored: $rel"
  fi
done

if [ -f ".env" ]; then
  chmod 600 .env || true
fi

echo "[safe-update] done. branch=origin/$BRANCH"

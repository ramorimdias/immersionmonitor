#!/usr/bin/env bash
set -euo pipefail

REPO_DIR="${IMMERSIONMONITOR_REPO_DIR:-/opt/immersionmonitor}"
REMOTE="${IMMERSIONMONITOR_REMOTE:-origin}"
BRANCH="${IMMERSIONMONITOR_BRANCH:-main}"
RESTART_SERVICE="${IMMERSIONMONITOR_RESTART_SERVICE:-}"
LOCK_FILE="${IMMERSIONMONITOR_LOCK_FILE:-/var/lock/immersionmonitor-update.lock}"

if [[ "${EUID}" -ne 0 ]]; then
  SUDO="sudo"
else
  SUDO=""
fi

log() {
  echo "[$(date --iso-8601=seconds)] $*"
}

mkdir -p "$(dirname "${LOCK_FILE}")"
exec 9>"${LOCK_FILE}"
if ! flock -n 9; then
  log "Another update is already running. Exiting."
  exit 0
fi

if [[ ! -d "${REPO_DIR}/.git" ]]; then
  log "ERROR: ${REPO_DIR} is not a git repository."
  exit 1
fi

cd "${REPO_DIR}"

current_sha="$(git rev-parse HEAD)"
log "Current revision: ${current_sha}"
log "Fetching ${REMOTE}/${BRANCH}..."
git fetch --prune "${REMOTE}" "${BRANCH}"
new_sha="$(git rev-parse "${REMOTE}/${BRANCH}")"

if [[ "${current_sha}" == "${new_sha}" ]]; then
  log "Already up to date."
  exit 0
fi

log "Updating to ${new_sha}..."
git reset --hard "${REMOTE}/${BRANCH}"

for file in dual_monitor.py worker_agent.py monitor_ui.py monitor_app.py readable_monitor.py; do
  if [[ -f "${file}" ]]; then
    python3 -m py_compile "${file}"
  fi
done

log "Update installed successfully."

if [[ -n "${RESTART_SERVICE}" ]]; then
  if systemctl list-unit-files "${RESTART_SERVICE}" >/dev/null 2>&1; then
    log "Restarting ${RESTART_SERVICE}..."
    ${SUDO} systemctl restart "${RESTART_SERVICE}"
  else
    log "Restart service ${RESTART_SERVICE} not found. Skipping restart."
  fi
else
  log "No restart service configured. New code will be used next time the app starts."
fi

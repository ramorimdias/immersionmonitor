#!/usr/bin/env bash
set -euo pipefail

REPO_OWNER="${IMMERSIONMONITOR_REPO_OWNER:-ramorimdias}"
REPO_NAME="${IMMERSIONMONITOR_REPO_NAME:-immersionmonitor}"
BRANCH="${IMMERSIONMONITOR_BRANCH:-main}"
BASE_URL="${IMMERSIONMONITOR_RAW_BASE:-https://raw.githubusercontent.com/${REPO_OWNER}/${REPO_NAME}/${BRANCH}}"
INSTALL_DIR="${IMMERSIONMONITOR_INSTALL_DIR:-/opt/immersionmonitor}"
AGENT_PORT="${BENCH_AGENT_PORT:-8765}"
SERVICE_NAME="bench-worker-agent.service"

if [[ "${EUID}" -ne 0 ]]; then
  SUDO="sudo"
else
  SUDO=""
fi

require_cmd() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "Missing required command: $1" >&2
    exit 1
  fi
}

require_cmd curl

echo "Installing bench worker dependencies..."
${SUDO} apt-get update
${SUDO} apt-get install -y python3 stress-ng curl ca-certificates

echo "Installing worker agent into ${INSTALL_DIR}..."
${SUDO} mkdir -p "${INSTALL_DIR}"
${SUDO} curl -fsSL "${BASE_URL}/worker_agent.py" -o "${INSTALL_DIR}/worker_agent.py"
${SUDO} chmod 755 "${INSTALL_DIR}/worker_agent.py"

echo "Installing systemd service..."
TMP_SERVICE="$(mktemp)"
curl -fsSL "${BASE_URL}/systemd/${SERVICE_NAME}" -o "${TMP_SERVICE}"
sed -i "s/--port 8765/--port ${AGENT_PORT}/" "${TMP_SERVICE}"
${SUDO} install -m 0644 "${TMP_SERVICE}" "/etc/systemd/system/${SERVICE_NAME}"
rm -f "${TMP_SERVICE}"

${SUDO} systemctl daemon-reload
${SUDO} systemctl enable --now "${SERVICE_NAME}"

echo
${SUDO} systemctl --no-pager --lines=8 status "${SERVICE_NAME}" || true

echo
echo "Bench worker agent installed."
echo "Local check: curl http://127.0.0.1:${AGENT_PORT}/status"
echo "From head Pi: discover this node on port ${AGENT_PORT}."

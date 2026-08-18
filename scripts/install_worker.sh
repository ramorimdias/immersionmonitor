#!/usr/bin/env bash
set -euo pipefail

REPO_OWNER="${IMMERSIONMONITOR_REPO_OWNER:-ramorimdias}"
REPO_NAME="${IMMERSIONMONITOR_REPO_NAME:-immersionmonitor}"
BRANCH="${IMMERSIONMONITOR_BRANCH:-main}"
BASE_URL="${IMMERSIONMONITOR_RAW_BASE:-https://raw.githubusercontent.com/${REPO_OWNER}/${REPO_NAME}/${BRANCH}}"
INSTALL_DIR="${IMMERSIONMONITOR_INSTALL_DIR:-/opt/immersionmonitor}"
AGENT_PORT="${BENCH_AGENT_PORT:-8765}"
SERVICE_NAME="bench-worker-agent.service"
WORKER_IFACE="${BENCH_WORKER_IFACE:-eth0}"

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

ensure_ethernet_dhcp() {
  if ! command -v nmcli >/dev/null 2>&1; then
    echo "NetworkManager not detected; leaving ${WORKER_IFACE} network settings unchanged."
    echo "Fresh Raspberry Pi OS images use DHCP on Ethernet by default."
    return 0
  fi

  if ! ip link show "${WORKER_IFACE}" >/dev/null 2>&1; then
    echo "Interface ${WORKER_IFACE} not found; skipping Ethernet DHCP check."
    return 0
  fi

  local connection
  connection="$(nmcli -g GENERAL.CONNECTION device show "${WORKER_IFACE}" 2>/dev/null | head -n1 || true)"
  if [[ -z "${connection}" || "${connection}" == "--" ]]; then
    echo "No active NetworkManager profile found on ${WORKER_IFACE}; leaving configuration unchanged."
    return 0
  fi

  echo "Ensuring ${WORKER_IFACE} uses DHCP and reconnects automatically after moving to the bench switch..."
  ${SUDO} nmcli connection modify "${connection}" \
    connection.autoconnect yes \
    ipv4.method auto \
    ipv4.never-default no
}

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

ensure_ethernet_dhcp

${SUDO} systemctl daemon-reload
${SUDO} systemctl enable --now "${SERVICE_NAME}"

echo
${SUDO} systemctl --no-pager --lines=8 status "${SERVICE_NAME}" || true

echo
echo "Bench worker agent installed."
echo "Local check: curl http://127.0.0.1:${AGENT_PORT}/status"
echo "After moving this Pi to the isolated bench switch, the head DHCP server will assign its Ethernet address."
echo "From the head Pi: discover this node on ${AGENT_PORT}/tcp in 192.168.50.0/24."

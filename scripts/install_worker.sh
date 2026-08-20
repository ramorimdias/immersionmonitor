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
WORKER_CONNECTION="${BENCH_WORKER_CONNECTION:-immersion-worker-dhcp}"

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
    echo "WARNING: NetworkManager/nmcli not detected."
    echo "Cannot create the dedicated bench DHCP profile automatically."
    echo "Fresh Raspberry Pi OS Bookworm images normally include NetworkManager."
    return 0
  fi

  if ! ip link show "${WORKER_IFACE}" >/dev/null 2>&1; then
    echo "ERROR: interface ${WORKER_IFACE} not found." >&2
    return 1
  fi

  echo "Preparing dedicated DHCP profile ${WORKER_CONNECTION} on ${WORKER_IFACE}..."

  if nmcli -t -f NAME connection show | grep -Fxq "${WORKER_CONNECTION}"; then
    ${SUDO} nmcli connection modify "${WORKER_CONNECTION}" \
      connection.interface-name "${WORKER_IFACE}" \
      connection.autoconnect yes \
      connection.autoconnect-priority 200 \
      ipv4.method auto \
      ipv4.addresses "" \
      ipv4.gateway "" \
      ipv4.dns "" \
      ipv4.never-default no \
      ipv6.method auto
  else
    ${SUDO} nmcli connection add \
      type ethernet \
      ifname "${WORKER_IFACE}" \
      con-name "${WORKER_CONNECTION}" \
      connection.autoconnect yes \
      connection.autoconnect-priority 200 \
      ipv4.method auto \
      ipv4.never-default no \
      ipv6.method auto
  fi

  # Keep the current company connection alive for the rest of this install,
  # but make the dedicated DHCP profile win automatically on the next boot.
  # Competing profiles are left present as fallbacks, with lower priority.
  while IFS=: read -r name iface; do
    [[ -z "${name}" ]] && continue
    if [[ "${iface}" == "${WORKER_IFACE}" && "${name}" != "${WORKER_CONNECTION}" ]]; then
      ${SUDO} nmcli connection modify "${name}" connection.autoconnect yes connection.autoconnect-priority 0 || true
    fi
  done < <(nmcli -t -f NAME,connection.interface-name connection show)

  echo "Dedicated worker Ethernet profile ready."
  echo "It will request DHCP automatically on ${WORKER_IFACE} after the next boot."
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
echo "Ethernet profile: ${WORKER_CONNECTION} (${WORKER_IFACE}, DHCP, autoconnect priority 200)"
echo "After moving this Pi to the isolated bench switch, the head DHCP server will assign its Ethernet address."
echo "From the head Pi: discover this node on ${AGENT_PORT}/tcp in 192.168.50.0/24."

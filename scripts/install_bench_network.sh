#!/usr/bin/env bash
set -euo pipefail

BENCH_IFACE="${BENCH_IFACE:-eth0}"
BENCH_CONNECTION="${BENCH_CONNECTION:-immersion-bench}"
BENCH_HEAD_CIDR="${BENCH_HEAD_CIDR:-192.168.50.5/24}"
BENCH_DHCP_START="${BENCH_DHCP_START:-192.168.50.100}"
BENCH_DHCP_END="${BENCH_DHCP_END:-192.168.50.199}"
BENCH_DHCP_MASK="${BENCH_DHCP_MASK:-255.255.255.0}"
BENCH_DHCP_LEASE="${BENCH_DHCP_LEASE:-12h}"
DHCP_SERVICE="immersion-bench-dhcp.service"
DHCP_CONFIG="/etc/immersionmonitor/bench-dnsmasq.conf"
LEASE_FILE="/var/lib/misc/immersion-bench.leases"
TARGET_IP="${BENCH_HEAD_CIDR%/*}"

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

require_cmd ip
require_cmd systemctl
require_cmd nmcli

if ! ip link show "${BENCH_IFACE}" >/dev/null 2>&1; then
  echo "ERROR: interface ${BENCH_IFACE} does not exist." >&2
  exit 1
fi

# Safety guard: never start the bench DHCP server while eth0 still appears to
# be attached to a different network. This avoids accidentally serving DHCP on
# the company LAN.
CURRENT_IPV4="$(ip -4 -o addr show dev "${BENCH_IFACE}" scope global 2>/dev/null | awk '{print $4}' | head -n1 || true)"
if [[ -n "${CURRENT_IPV4}" && "${CURRENT_IPV4%/*}" != "${TARGET_IP}" ]]; then
  echo "ERROR: ${BENCH_IFACE} currently has ${CURRENT_IPV4}." >&2
  echo "Disconnect ${BENCH_IFACE} from the company network and connect it to the isolated bench switch before running this installer." >&2
  exit 1
fi

echo "Installing DHCP dependency..."
${SUDO} apt-get update
${SUDO} DEBIAN_FRONTEND=noninteractive apt-get install -y dnsmasq-base

DEFAULT_DEVS="$(ip -4 route show default | awk '{print $5}' | sort -u | tr '\n' ' ')"
if [[ " ${DEFAULT_DEVS} " == *" ${BENCH_IFACE} "* ]] && [[ "${DEFAULT_DEVS}" == "${BENCH_IFACE} " ]]; then
  echo "WARNING: ${BENCH_IFACE} is currently the only IPv4 default-route interface."
  echo "The bench configuration intentionally removes its default route."
  echo "Make sure the head has another company/internet connection before relying on remote access."
fi

echo "Configuring ${BENCH_IFACE} as the isolated bench interface (${BENCH_HEAD_CIDR})..."
if nmcli -t -f NAME connection show | grep -Fxq "${BENCH_CONNECTION}"; then
  ${SUDO} nmcli connection modify "${BENCH_CONNECTION}" \
    connection.interface-name "${BENCH_IFACE}" \
    connection.autoconnect yes \
    connection.autoconnect-priority 100 \
    ipv4.method manual \
    ipv4.addresses "${BENCH_HEAD_CIDR}" \
    ipv4.gateway "" \
    ipv4.dns "" \
    ipv4.never-default yes \
    ipv6.method disabled
else
  ${SUDO} nmcli connection add \
    type ethernet \
    ifname "${BENCH_IFACE}" \
    con-name "${BENCH_CONNECTION}" \
    connection.autoconnect yes \
    connection.autoconnect-priority 100 \
    ipv4.method manual \
    ipv4.addresses "${BENCH_HEAD_CIDR}" \
    ipv4.never-default yes \
    ipv6.method disabled
fi

${SUDO} mkdir -p /etc/immersionmonitor /var/lib/misc

TMP_CONFIG="$(mktemp)"
cat > "${TMP_CONFIG}" <<EOF
# DHCP only. DNS and routing are deliberately not provided on the isolated bench LAN.
port=0
interface=${BENCH_IFACE}
bind-dynamic
dhcp-authoritative
dhcp-range=${BENCH_DHCP_START},${BENCH_DHCP_END},${BENCH_DHCP_MASK},${BENCH_DHCP_LEASE}
dhcp-option=3
dhcp-option=6
dhcp-leasefile=${LEASE_FILE}
log-dhcp
log-facility=-
EOF
${SUDO} install -m 0644 "${TMP_CONFIG}" "${DHCP_CONFIG}"
rm -f "${TMP_CONFIG}"

TMP_SERVICE="$(mktemp)"
cat > "${TMP_SERVICE}" <<EOF
[Unit]
Description=Immersion bench DHCP server
After=network.target
Wants=network.target

[Service]
Type=simple
ExecStart=/usr/sbin/dnsmasq --keep-in-foreground --conf-file=${DHCP_CONFIG}
Restart=on-failure
RestartSec=2

[Install]
WantedBy=multi-user.target
EOF
${SUDO} install -m 0644 "${TMP_SERVICE}" "/etc/systemd/system/${DHCP_SERVICE}"
rm -f "${TMP_SERVICE}"

echo "Activating bench Ethernet profile..."
${SUDO} nmcli connection up "${BENCH_CONNECTION}"

${SUDO} systemctl daemon-reload
${SUDO} systemctl enable --now "${DHCP_SERVICE}"

echo
if ! ip -4 addr show dev "${BENCH_IFACE}" | grep -Fq "${TARGET_IP}/"; then
  echo "ERROR: ${BENCH_IFACE} did not receive ${BENCH_HEAD_CIDR}." >&2
  ip -br addr show "${BENCH_IFACE}" || true
  exit 1
fi

${SUDO} systemctl --no-pager --lines=12 status "${DHCP_SERVICE}" || true

echo
echo "Bench network ready."
echo "Head: ${BENCH_HEAD_CIDR} on ${BENCH_IFACE}"
echo "Worker DHCP pool: ${BENCH_DHCP_START} - ${BENCH_DHCP_END}"
echo "Worker agent port: 8765"
echo "Discovery CIDR: 192.168.50.0/24"
echo "DHCP leases: cat ${LEASE_FILE}"

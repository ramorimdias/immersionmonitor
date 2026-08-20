#!/usr/bin/env bash
set -euo pipefail

STATE_DIR="/var/lib/immersionmonitor"
PENDING_MARKER="${STATE_DIR}/clone-firstboot.pending"
WORKER_IFACE="${BENCH_WORKER_IFACE:-eth0}"
AGENT_SERVICE="bench-worker-agent.service"
FIRSTBOOT_SERVICE="immersion-worker-clone-firstboot.service"

if [[ "${EUID}" -ne 0 ]]; then
  echo "ERROR: this script must run as root." >&2
  exit 1
fi

log() {
  echo "[clone-firstboot] $*"
}

if [[ ! -e "${PENDING_MARKER}" ]]; then
  log "No pending clone initialization marker. Nothing to do."
  exit 0
fi

if [[ ! -e "/sys/class/net/${WORKER_IFACE}/address" ]]; then
  log "ERROR: interface ${WORKER_IFACE} not found." >&2
  exit 1
fi

mac="$(cat "/sys/class/net/${WORKER_IFACE}/address")"
mac_compact="${mac//:/}"
if [[ ${#mac_compact} -lt 6 ]]; then
  log "ERROR: invalid MAC address for ${WORKER_IFACE}: ${mac}" >&2
  exit 1
fi

suffix="${mac_compact: -6}"
new_hostname="worker-${suffix,,}"

log "Initializing clone identity for ${mac}."
log "Setting hostname to ${new_hostname}."
printf '%s\n' "${new_hostname}" > /etc/hostname
if grep -qE '^127\.0\.1\.1([[:space:]]|$)' /etc/hosts; then
  sed -i -E "s/^127\.0\.1\.1.*/127.0.1.1\t${new_hostname}/" /etc/hosts
else
  printf '127.0.1.1\t%s\n' "${new_hostname}" >> /etc/hosts
fi
hostname "${new_hostname}" || true

log "Ensuring a unique machine-id exists."
if [[ ! -s /etc/machine-id ]]; then
  systemd-machine-id-setup
fi
mkdir -p /var/lib/dbus
ln -sfn /etc/machine-id /var/lib/dbus/machine-id

log "Generating unique SSH host keys."
rm -f /etc/ssh/ssh_host_*
ssh-keygen -A

log "Enabling worker agent."
systemctl enable "${AGENT_SERVICE}" >/dev/null 2>&1 || true

rm -f "${PENDING_MARKER}"
systemctl disable "${FIRSTBOOT_SERVICE}" >/dev/null 2>&1 || true

log "Clone initialization complete."
log "Hostname: ${new_hostname}"
log "Ethernet: DHCP profile prepared on ${WORKER_IFACE}"

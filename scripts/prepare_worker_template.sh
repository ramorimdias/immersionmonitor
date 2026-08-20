#!/usr/bin/env bash
set -euo pipefail

WORKER_IFACE="${BENCH_WORKER_IFACE:-eth0}"
WORKER_CONNECTION="${BENCH_WORKER_CONNECTION:-immersion-worker-dhcp}"
AGENT_SERVICE="bench-worker-agent.service"
FIRSTBOOT_SERVICE="immersion-worker-clone-firstboot.service"
FIRSTBOOT_SCRIPT="/usr/local/sbin/immersion-worker-clone-firstboot.sh"
STATE_DIR="/var/lib/immersionmonitor"
PENDING_MARKER="${STATE_DIR}/clone-firstboot.pending"
AUTO_POWEROFF="${IMMERSION_TEMPLATE_AUTO_POWEROFF:-1}"

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

require_cmd systemctl
require_cmd ssh-keygen
require_cmd ip

if ! ip link show "${WORKER_IFACE}" >/dev/null 2>&1; then
  echo "ERROR: interface ${WORKER_IFACE} does not exist." >&2
  exit 1
fi

if [[ ! -f /opt/immersionmonitor/worker_agent.py ]]; then
  echo "ERROR: worker agent is not installed at /opt/immersionmonitor/worker_agent.py." >&2
  echo "Run scripts/install_worker.sh first, then prepare the template." >&2
  exit 1
fi

if ! systemctl list-unit-files "${AGENT_SERVICE}" >/dev/null 2>&1; then
  echo "ERROR: ${AGENT_SERVICE} is not installed." >&2
  exit 1
fi

echo "Preparing this worker SD card as a reusable golden image."
echo "After preparation the Pi must be powered off and the SD card imaged before it is booted again."

if command -v nmcli >/dev/null 2>&1; then
  echo "Ensuring the reusable Ethernet DHCP profile exists..."
  if nmcli -t -f NAME connection show | grep -Fxq "${WORKER_CONNECTION}"; then
    ${SUDO} nmcli connection modify "${WORKER_CONNECTION}" \
      connection.interface-name "${WORKER_IFACE}" \
      connection.autoconnect yes \
      connection.autoconnect-priority 200 \
      ipv4.method auto \
      ipv4.never-default no
  else
    ${SUDO} nmcli connection add \
      type ethernet \
      ifname "${WORKER_IFACE}" \
      con-name "${WORKER_CONNECTION}" \
      connection.autoconnect yes \
      connection.autoconnect-priority 200 \
      ipv4.method auto
  fi
fi

${SUDO} install -d -m 0755 "${STATE_DIR}"

TMP_FIRSTBOOT="$(mktemp)"
cat > "${TMP_FIRSTBOOT}" <<'FIRSTBOOT_EOF'
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

log() { echo "[clone-firstboot] $*"; }

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

systemctl enable "${AGENT_SERVICE}" >/dev/null 2>&1 || true
rm -f "${PENDING_MARKER}"
systemctl disable "${FIRSTBOOT_SERVICE}" >/dev/null 2>&1 || true

log "Clone initialization complete."
log "Hostname: ${new_hostname}"
log "Ethernet: DHCP profile prepared on ${WORKER_IFACE}"
FIRSTBOOT_EOF
${SUDO} install -m 0755 "${TMP_FIRSTBOOT}" "${FIRSTBOOT_SCRIPT}"
rm -f "${TMP_FIRSTBOOT}"

TMP_SERVICE="$(mktemp)"
cat > "${TMP_SERVICE}" <<'SERVICE_EOF'
[Unit]
Description=Initialize cloned immersion worker identity
ConditionPathExists=/var/lib/immersionmonitor/clone-firstboot.pending
After=local-fs.target
Before=NetworkManager.service network.target ssh.service sshd.service bench-worker-agent.service

[Service]
Type=oneshot
ExecStart=/usr/local/sbin/immersion-worker-clone-firstboot.sh

[Install]
WantedBy=multi-user.target
SERVICE_EOF
${SUDO} install -m 0644 "${TMP_SERVICE}" "/etc/systemd/system/${FIRSTBOOT_SERVICE}"
rm -f "${TMP_SERVICE}"

${SUDO} touch "${PENDING_MARKER}"
${SUDO} systemctl daemon-reload
${SUDO} systemctl enable "${FIRSTBOOT_SERVICE}" >/dev/null
${SUDO} systemctl enable "${AGENT_SERVICE}" >/dev/null 2>&1 || true

# Stop the random-seed service before deleting the seed so shutdown does not
# write the same seed back into the golden image.
${SUDO} systemctl stop systemd-random-seed.service >/dev/null 2>&1 || true
${SUDO} rm -f /var/lib/systemd/random-seed

# Remove identities that must not be duplicated across cloned systems.
${SUDO} rm -f /etc/ssh/ssh_host_*
${SUDO} sh -c ': > /etc/machine-id'
${SUDO} rm -f /var/lib/dbus/machine-id

sync

echo
echo "Golden-image preparation complete."
echo "On first boot, each clone will:"
echo "  - generate a unique machine-id"
echo "  - generate unique SSH host keys"
echo "  - set hostname to worker-<last-6-MAC-hex>"
echo "  - use DHCP on ${WORKER_IFACE}"
echo "  - enable ${AGENT_SERVICE}"
echo

if [[ "${AUTO_POWEROFF}" == "1" ]]; then
  echo "Powering off now. Do not boot this card again before creating the master image."
  sleep 3
  ${SUDO} systemctl poweroff
else
  echo "AUTO_POWEROFF disabled. Run 'sudo poweroff' now and do not reboot before imaging."
fi

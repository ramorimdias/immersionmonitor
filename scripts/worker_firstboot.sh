#!/usr/bin/env bash
set -euo pipefail

LOG_FILE="/boot/firmware/immersionmonitor-firstboot.log"
STATUS_FILE="/boot/firmware/immersionmonitor-status.txt"
BOOT_CMDLINE="/boot/firmware/cmdline.txt"
PROVISION_ENV="/boot/firmware/immersionmonitor-worker.env"
LEGACY_WIFI_ENV="/boot/firmware/immersionmonitor-wifi.env"
INSTALL_DIR="${IMMERSIONMONITOR_INSTALL_DIR:-/opt/immersionmonitor}"
REPO_OWNER="${IMMERSIONMONITOR_REPO_OWNER:-ramorimdias}"
REPO_NAME="${IMMERSIONMONITOR_REPO_NAME:-immersionmonitor}"
BRANCH="${IMMERSIONMONITOR_BRANCH:-main}"
BASE_URL="${IMMERSIONMONITOR_RAW_BASE:-https://raw.githubusercontent.com/${REPO_OWNER}/${REPO_NAME}/${BRANCH}}"
AGENT_PORT="${BENCH_AGENT_PORT:-8765}"
SERVICE_NAME="bench-worker-agent.service"
PROVISION_SERVICE="immersionmonitor-worker-provision.service"

exec > >(tee -a "${LOG_FILE}" /dev/console) 2>&1

status() {
  local message="$*"
  echo "[immersionmonitor] ${message}"
  printf '%s %s\n' "$(date --iso-8601=seconds 2>/dev/null || date)" "${message}" >> "${STATUS_FILE}" || true
}

status "worker provisioning started"

load_boot_config() {
  if [[ -f "${PROVISION_ENV}" ]]; then
    # shellcheck disable=SC1090
    source "${PROVISION_ENV}"
  elif [[ -f "${LEGACY_WIFI_ENV}" ]]; then
    # shellcheck disable=SC1090
    source "${LEGACY_WIFI_ENV}"
  fi
  STATUS_FILE="${PROVISION_STATUS_FILE:-${STATUS_FILE}}"
}

cleanup_cmdline() {
  if [[ -f "${BOOT_CMDLINE}" ]]; then
    sed -i \
      -e 's# *systemd.run=/boot/firmware/worker_firstboot.sh##g' \
      -e 's# *systemd.run_success_action=reboot##g' \
      -e 's# *systemd.unit=kernel-command-line.target##g' \
      "${BOOT_CMDLINE}"
  fi
}

configure_linux_user() {
  if [[ -z "${LINUX_USER_B64:-}" || -z "${LINUX_PASSWORD_B64:-}" ]]; then
    status "no Linux user fallback configured; skipping user creation"
    return 0
  fi

  username="$(printf '%s' "${LINUX_USER_B64}" | base64 -d)"
  password="$(printf '%s' "${LINUX_PASSWORD_B64}" | base64 -d)"

  if id "${username}" >/dev/null 2>&1; then
    status "Linux user ${username} already exists"
  else
    status "creating Linux user ${username}"
    groups="sudo,adm,dialout,cdrom,audio,video,plugdev,games,users,input,render,netdev"
    for optional_group in gpio i2c spi; do
      if getent group "${optional_group}" >/dev/null 2>&1; then
        groups="${groups},${optional_group}"
      fi
    done
    useradd -m -s /bin/bash -G "${groups}" "${username}"
  fi

  echo "${username}:${password}" | chpasswd
  status "password set for ${username}"

  cat > "/etc/sudoers.d/010_${username}-bench" <<EOF
${username} ALL=(ALL) NOPASSWD:ALL
EOF
  chmod 440 "/etc/sudoers.d/010_${username}-bench"

  systemctl disable userconfig.service >/dev/null 2>&1 || true
  systemctl mask userconfig.service >/dev/null 2>&1 || true
  status "interactive first-user prompt disabled if present"
}

configure_wifi() {
  if [[ -z "${WIFI_SSID_B64:-}" || -z "${WIFI_PASSWORD_B64:-}" ]]; then
    status "no Wi-Fi config found; skipping Wi-Fi setup"
    return 0
  fi

  ssid="$(printf '%s' "${WIFI_SSID_B64}" | base64 -d)"
  password="$(printf '%s' "${WIFI_PASSWORD_B64}" | base64 -d)"
  country="${WIFI_COUNTRY:-FR}"

  status "configuring Wi-Fi SSID: ${ssid}"
  mkdir -p /etc/NetworkManager/system-connections
  cat > /etc/NetworkManager/system-connections/immersionmonitor-worker-wifi.nmconnection <<EOF
[connection]
id=immersionmonitor-worker-wifi
type=wifi
interface-name=wlan0
autoconnect=true

[wifi]
mode=infrastructure
ssid=${ssid}

[wifi-security]
key-mgmt=wpa-psk
psk=${password}

[ipv4]
method=auto

[ipv6]
method=auto
addr-gen-mode=default
EOF
  chmod 600 /etc/NetworkManager/system-connections/immersionmonitor-worker-wifi.nmconnection

  mkdir -p /etc/wpa_supplicant
  cat > /etc/wpa_supplicant/wpa_supplicant.conf <<EOF
country=${country}
ctrl_interface=DIR=/var/run/wpa_supplicant GROUP=netdev
update_config=1
network={
    ssid="${ssid}"
    psk="${password}"
}
EOF
  chmod 600 /etc/wpa_supplicant/wpa_supplicant.conf
  status "Wi-Fi configuration written"
}

install_second_stage_service() {
  status "installing second-stage provisioning service"
  cat > "/etc/systemd/system/${PROVISION_SERVICE}" <<EOF
[Unit]
Description=Provision immersionmonitor worker after network is online
Wants=network-online.target
After=network-online.target

[Service]
Type=oneshot
ExecStart=/boot/firmware/worker_firstboot.sh --install
RemainAfterExit=yes
StandardOutput=journal+console
StandardError=journal+console

[Install]
WantedBy=multi-user.target
EOF
  systemctl daemon-reload
  systemctl enable "${PROVISION_SERVICE}"
}

wait_for_network() {
  status "waiting for usable network: DHCP + DNS + GitHub access"
  for attempt in $(seq 1 120); do
    ip -4 addr show scope global || true
    if getent hosts raw.githubusercontent.com >/dev/null 2>&1 && \
       curl -fsI --connect-timeout 5 --max-time 10 "${BASE_URL}/worker_agent.py" >/dev/null 2>&1; then
      status "network ready on attempt ${attempt}"
      return 0
    fi
    if (( attempt % 10 == 0 )); then
      status "still waiting for network, attempt ${attempt}/120"
    fi
    sleep 3
  done
  status "ERROR: network is not ready or GitHub is blocked"
  status "check DHCP, DNS, proxy, firewall, and raw.githubusercontent.com access"
  return 1
}

wait_for_apt() {
  status "waiting for apt locks"
  for _ in $(seq 1 60); do
    if ! fuser /var/lib/dpkg/lock-frontend >/dev/null 2>&1 && \
       ! fuser /var/lib/dpkg/lock >/dev/null 2>&1 && \
       ! fuser /var/cache/apt/archives/lock >/dev/null 2>&1; then
      return 0
    fi
    sleep 2
  done
  status "APT lock wait timed out; continuing anyway"
}

install_worker() {
  wait_for_network
  wait_for_apt

  status "running apt-get update"
  apt-get update

  status "installing dependencies: python3 stress-ng curl ca-certificates"
  DEBIAN_FRONTEND=noninteractive apt-get install -y python3 stress-ng curl ca-certificates

  status "installing worker agent in ${INSTALL_DIR}"
  mkdir -p "${INSTALL_DIR}"
  curl -fsSL "${BASE_URL}/worker_agent.py" -o "${INSTALL_DIR}/worker_agent.py"
  chmod 755 "${INSTALL_DIR}/worker_agent.py"

  status "installing worker systemd service"
  tmp_service="$(mktemp)"
  curl -fsSL "${BASE_URL}/systemd/${SERVICE_NAME}" -o "${tmp_service}"
  sed -i "s/--port 8765/--port ${AGENT_PORT}/" "${tmp_service}"
  install -m 0644 "${tmp_service}" "/etc/systemd/system/${SERVICE_NAME}"
  rm -f "${tmp_service}"

  systemctl daemon-reload
  systemctl enable "${SERVICE_NAME}"

  status "disabling provisioning service"
  systemctl disable "${PROVISION_SERVICE}" || true
  rm -f "/etc/systemd/system/${PROVISION_SERVICE}"
  systemctl daemon-reload

  status "installation complete; worker agent will start after reboot"
}

main() {
  load_boot_config
  case "${1:-}" in
    --install)
      if install_worker; then
        sync
        status "second-stage provisioning succeeded; rebooting"
        reboot
      else
        status "second-stage provisioning failed; it will retry on next normal boot"
        exit 1
      fi
      ;;
    *)
      configure_linux_user
      configure_wifi
      install_second_stage_service
      cleanup_cmdline
      sync
      status "first-stage setup complete; rebooting into normal boot"
      reboot
      ;;
  esac
}

main "$@"

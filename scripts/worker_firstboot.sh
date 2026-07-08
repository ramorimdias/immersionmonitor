#!/usr/bin/env bash
set -euo pipefail

LOG_FILE="/boot/firmware/immersionmonitor-firstboot.log"
BOOT_CMDLINE="/boot/firmware/cmdline.txt"
INSTALL_DIR="${IMMERSIONMONITOR_INSTALL_DIR:-/opt/immersionmonitor}"
REPO_OWNER="${IMMERSIONMONITOR_REPO_OWNER:-ramorimdias}"
REPO_NAME="${IMMERSIONMONITOR_REPO_NAME:-immersionmonitor}"
BRANCH="${IMMERSIONMONITOR_BRANCH:-main}"
BASE_URL="${IMMERSIONMONITOR_RAW_BASE:-https://raw.githubusercontent.com/${REPO_OWNER}/${REPO_NAME}/${BRANCH}}"
AGENT_PORT="${BENCH_AGENT_PORT:-8765}"
SERVICE_NAME="bench-worker-agent.service"
PROVISION_SERVICE="immersionmonitor-worker-provision.service"

exec > >(tee -a "${LOG_FILE}") 2>&1

echo "===== immersionmonitor worker provisioning: $(date --iso-8601=seconds) ====="

cleanup_cmdline() {
  if [[ -f "${BOOT_CMDLINE}" ]]; then
    sed -i \
      -e 's# *systemd.run=/boot/firmware/worker_firstboot.sh##g' \
      -e 's# *systemd.run_success_action=reboot##g' \
      -e 's# *systemd.unit=kernel-command-line.target##g' \
      "${BOOT_CMDLINE}"
  fi
}

install_second_stage_service() {
  echo "Installing second-stage provisioning service."
  cat > "/etc/systemd/system/${PROVISION_SERVICE}" <<EOF
[Unit]
Description=Provision immersionmonitor worker after network is online
Wants=network-online.target
After=network-online.target

[Service]
Type=oneshot
ExecStart=/boot/firmware/worker_firstboot.sh --install
RemainAfterExit=yes

[Install]
WantedBy=multi-user.target
EOF
  systemctl daemon-reload
  systemctl enable "${PROVISION_SERVICE}"
}

wait_for_network() {
  echo "Waiting for usable network: DHCP + DNS + GitHub access..."
  for _ in $(seq 1 120); do
    ip -4 addr show scope global || true
    if getent hosts raw.githubusercontent.com >/dev/null 2>&1 && \
       curl -fsI --connect-timeout 5 --max-time 10 "${BASE_URL}/worker_agent.py" >/dev/null 2>&1; then
      echo "Network is ready."
      return 0
    fi
    sleep 3
  done
  echo "ERROR: Network is not ready or GitHub is blocked."
  echo "Check DHCP, DNS, proxy, firewall, and whether raw.githubusercontent.com is allowed."
  return 1
}

wait_for_apt() {
  echo "Waiting for apt locks..."
  for _ in $(seq 1 60); do
    if ! fuser /var/lib/dpkg/lock-frontend >/dev/null 2>&1 && \
       ! fuser /var/lib/dpkg/lock >/dev/null 2>&1 && \
       ! fuser /var/cache/apt/archives/lock >/dev/null 2>&1; then
      return 0
    fi
    sleep 2
  done
  echo "APT lock wait timed out; continuing anyway."
}

install_worker() {
  wait_for_network
  wait_for_apt

  echo "Updating package index..."
  apt-get update

  echo "Installing dependencies..."
  DEBIAN_FRONTEND=noninteractive apt-get install -y python3 stress-ng curl ca-certificates

  echo "Installing worker agent in ${INSTALL_DIR}..."
  mkdir -p "${INSTALL_DIR}"
  curl -fsSL "${BASE_URL}/worker_agent.py" -o "${INSTALL_DIR}/worker_agent.py"
  chmod 755 "${INSTALL_DIR}/worker_agent.py"

  echo "Installing worker systemd service..."
  tmp_service="$(mktemp)"
  curl -fsSL "${BASE_URL}/systemd/${SERVICE_NAME}" -o "${tmp_service}"
  sed -i "s/--port 8765/--port ${AGENT_PORT}/" "${tmp_service}"
  install -m 0644 "${tmp_service}" "/etc/systemd/system/${SERVICE_NAME}"
  rm -f "${tmp_service}"

  systemctl daemon-reload
  systemctl enable "${SERVICE_NAME}"

  echo "Disabling provisioning service."
  systemctl disable "${PROVISION_SERVICE}" || true
  rm -f "/etc/systemd/system/${PROVISION_SERVICE}"
  systemctl daemon-reload

  echo "Installation complete. Worker agent will start after reboot."
}

main() {
  case "${1:-}" in
    --install)
      if install_worker; then
        sync
        echo "Second-stage provisioning succeeded. Rebooting."
        reboot
      else
        echo "Second-stage provisioning failed. It will retry on next normal boot."
        exit 1
      fi
      ;;
    *)
      install_second_stage_service
      cleanup_cmdline
      sync
      echo "First-stage setup complete. Rebooting into normal boot so networking can start."
      reboot
      ;;
  esac
}

main "$@"

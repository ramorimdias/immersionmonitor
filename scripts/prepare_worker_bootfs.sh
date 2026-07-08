#!/usr/bin/env bash
set -euo pipefail

BOOTFS="${1:-}"
BRANCH="${IMMERSIONMONITOR_BRANCH:-main}"
REPO_OWNER="${IMMERSIONMONITOR_REPO_OWNER:-ramorimdias}"
REPO_NAME="${IMMERSIONMONITOR_REPO_NAME:-immersionmonitor}"
RAW_BASE="${IMMERSIONMONITOR_RAW_BASE:-https://raw.githubusercontent.com/${REPO_OWNER}/${REPO_NAME}/${BRANCH}}"
FIRSTBOOT_NAME="worker_firstboot.sh"

usage() {
  cat <<EOF
Usage:
  $0 /path/to/bootfs

Example:
  $0 /media/$USER/bootfs

This prepares a freshly flashed Raspberry Pi OS SD card so a worker Pi installs
immersionmonitor automatically on first boot. The boot partition must be mounted.
EOF
}

if [[ -z "${BOOTFS}" || "${BOOTFS}" == "-h" || "${BOOTFS}" == "--help" ]]; then
  usage
  exit 0
fi

if [[ ! -d "${BOOTFS}" ]]; then
  echo "ERROR: bootfs path does not exist: ${BOOTFS}" >&2
  exit 1
fi

CMDLINE="${BOOTFS}/cmdline.txt"
if [[ ! -f "${CMDLINE}" ]]; then
  echo "ERROR: ${CMDLINE} not found. Did you select the FAT boot partition?" >&2
  exit 1
fi

if command -v curl >/dev/null 2>&1; then
  curl -fsSL "${RAW_BASE}/scripts/${FIRSTBOOT_NAME}" -o "${BOOTFS}/${FIRSTBOOT_NAME}"
else
  echo "ERROR: curl is required." >&2
  exit 1
fi
chmod 755 "${BOOTFS}/${FIRSTBOOT_NAME}"

HOOK="systemd.run=/boot/firmware/${FIRSTBOOT_NAME} systemd.run_success_action=reboot systemd.unit=kernel-command-line.target"

if grep -q "systemd.run=/boot/firmware/${FIRSTBOOT_NAME}" "${CMDLINE}"; then
  echo "First-boot hook already present in cmdline.txt."
else
  tmp="$(mktemp)"
  tr -d '\n' < "${CMDLINE}" > "${tmp}"
  printf ' %s\n' "${HOOK}" >> "${tmp}"
  cp "${tmp}" "${CMDLINE}"
  rm -f "${tmp}"
fi

if [[ ! -e "${BOOTFS}/ssh" && ! -e "${BOOTFS}/ssh.txt" ]]; then
  touch "${BOOTFS}/ssh"
fi

sync

echo "Worker SD card prepared."
echo "Next steps:"
echo "1. Eject the SD card safely."
echo "2. Insert it into the worker Pi."
echo "3. Connect Ethernet to a network with internet access for first boot."
echo "4. Power on the Pi and wait until it installs, cleans the first-boot hook, and reboots."
echo "5. Move it to the bench switch."

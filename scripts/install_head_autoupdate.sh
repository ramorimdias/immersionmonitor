#!/usr/bin/env bash
set -euo pipefail

REPO_OWNER="${IMMERSIONMONITOR_REPO_OWNER:-ramorimdias}"
REPO_NAME="${IMMERSIONMONITOR_REPO_NAME:-immersionmonitor}"
BRANCH="${IMMERSIONMONITOR_BRANCH:-main}"
REPO_URL="${IMMERSIONMONITOR_REPO_URL:-https://github.com/${REPO_OWNER}/${REPO_NAME}.git}"
RAW_BASE="${IMMERSIONMONITOR_RAW_BASE:-https://raw.githubusercontent.com/${REPO_OWNER}/${REPO_NAME}/${BRANCH}}"
INSTALL_DIR="${IMMERSIONMONITOR_REPO_DIR:-/opt/immersionmonitor}"
UPDATE_SERVICE="bench-head-update.service"
UPDATE_TIMER="bench-head-update.timer"
RESTART_SERVICE="${IMMERSIONMONITOR_RESTART_SERVICE:-}"

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

echo "Installing update dependencies..."
${SUDO} apt-get update
${SUDO} apt-get install -y git curl ca-certificates python3 util-linux

if [[ ! -d "${INSTALL_DIR}/.git" ]]; then
  if [[ -e "${INSTALL_DIR}" ]]; then
    echo "ERROR: ${INSTALL_DIR} exists but is not a git repository." >&2
    exit 1
  fi
  echo "Cloning ${REPO_URL} into ${INSTALL_DIR}..."
  ${SUDO} git clone --branch "${BRANCH}" "${REPO_URL}" "${INSTALL_DIR}"
else
  echo "Repository already present at ${INSTALL_DIR}."
fi

echo "Installing updater script..."
TMP_UPDATE="$(mktemp)"
curl -fsSL "${RAW_BASE}/scripts/update_head.sh" -o "${TMP_UPDATE}"
${SUDO} install -m 0755 "${TMP_UPDATE}" /usr/local/sbin/immersionmonitor-update-head.sh
rm -f "${TMP_UPDATE}"

echo "Writing updater configuration..."
TMP_ENV="$(mktemp)"
cat > "${TMP_ENV}" <<EOF
IMMERSIONMONITOR_REPO_DIR=${INSTALL_DIR}
IMMERSIONMONITOR_REMOTE=origin
IMMERSIONMONITOR_BRANCH=${BRANCH}
IMMERSIONMONITOR_RESTART_SERVICE=${RESTART_SERVICE}
EOF
${SUDO} install -m 0644 "${TMP_ENV}" /etc/default/immersionmonitor-update
rm -f "${TMP_ENV}"

echo "Installing systemd update service and timer..."
TMP_SERVICE="$(mktemp)"
TMP_TIMER="$(mktemp)"
curl -fsSL "${RAW_BASE}/systemd/${UPDATE_SERVICE}" -o "${TMP_SERVICE}"
curl -fsSL "${RAW_BASE}/systemd/${UPDATE_TIMER}" -o "${TMP_TIMER}"
${SUDO} install -m 0644 "${TMP_SERVICE}" "/etc/systemd/system/${UPDATE_SERVICE}"
${SUDO} install -m 0644 "${TMP_TIMER}" "/etc/systemd/system/${UPDATE_TIMER}"
rm -f "${TMP_SERVICE}" "${TMP_TIMER}"

${SUDO} systemctl daemon-reload
${SUDO} systemctl enable --now "${UPDATE_TIMER}"

echo
echo "Running one immediate update check..."
${SUDO} systemctl start "${UPDATE_SERVICE}"

echo
echo "Head Pi auto-update installed."
echo "Timer status: systemctl status ${UPDATE_TIMER}"
echo "Update logs: journalctl -u ${UPDATE_SERVICE} -n 50 --no-pager"

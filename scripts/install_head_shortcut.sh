#!/usr/bin/env bash
set -euo pipefail

APP_DIR="${IMMERSIONMONITOR_REPO_DIR:-/opt/immersionmonitor}"
APP_NAME="Immersion Monitor"
DESKTOP_FILE_NAME="immersion-monitor.desktop"
APPLICATIONS_DIR="${HOME}/.local/share/applications"
STATE_DIR="${HOME}/.local/state/immersionmonitor"
LAUNCHER_DIR="${HOME}/.local/bin"
LAUNCHER="${LAUNCHER_DIR}/immersion-monitor"

if [[ ! -f "${APP_DIR}/readable_monitor.py" ]]; then
  echo "ERROR: readable_monitor.py not found in ${APP_DIR}." >&2
  exit 1
fi

if [[ ! -f "${APP_DIR}/scripts/run_readable_monitor_auto.sh" ]]; then
  echo "ERROR: run_readable_monitor_auto.sh not found in ${APP_DIR}/scripts." >&2
  exit 1
fi

mkdir -p "${APPLICATIONS_DIR}" "${STATE_DIR}" "${LAUNCHER_DIR}"

cat > "${LAUNCHER}" <<EOF
#!/usr/bin/env bash
set -euo pipefail
mkdir -p "${STATE_DIR}"
exec /bin/bash "${APP_DIR}/scripts/run_readable_monitor_auto.sh" >> "${STATE_DIR}/monitor.log" 2>&1
EOF
chmod 755 "${LAUNCHER}"

APP_ENTRY="${APPLICATIONS_DIR}/${DESKTOP_FILE_NAME}"
cat > "${APP_ENTRY}" <<EOF
[Desktop Entry]
Type=Application
Version=1.0
Name=${APP_NAME}
Comment=Launch the immersion bench readable monitor
Exec=${LAUNCHER}
Icon=utilities-system-monitor
Terminal=false
Categories=Utility;System;
StartupNotify=true
EOF
chmod 755 "${APP_ENTRY}"

DESKTOP_DIR=""
if command -v xdg-user-dir >/dev/null 2>&1; then
  DESKTOP_DIR="$(xdg-user-dir DESKTOP 2>/dev/null || true)"
fi
if [[ -z "${DESKTOP_DIR}" || "${DESKTOP_DIR}" == "${HOME}" ]]; then
  if [[ -d "${HOME}/Bureau" ]]; then
    DESKTOP_DIR="${HOME}/Bureau"
  else
    DESKTOP_DIR="${HOME}/Desktop"
  fi
fi
mkdir -p "${DESKTOP_DIR}"

DESKTOP_ENTRY="${DESKTOP_DIR}/${APP_NAME}.desktop"
cp "${APP_ENTRY}" "${DESKTOP_ENTRY}"
chmod 755 "${DESKTOP_ENTRY}"

if command -v gio >/dev/null 2>&1; then
  gio set "${DESKTOP_ENTRY}" metadata::trusted true >/dev/null 2>&1 || true
fi

if command -v update-desktop-database >/dev/null 2>&1; then
  update-desktop-database "${APPLICATIONS_DIR}" >/dev/null 2>&1 || true
fi

echo "Immersion Monitor shortcut installed."
echo "Desktop shortcut: ${DESKTOP_ENTRY}"
echo "Applications entry: ${APP_ENTRY}"
echo "Log file: ${STATE_DIR}/monitor.log"
echo
echo "Double-click '${APP_NAME}' on the desktop to launch the monitor."

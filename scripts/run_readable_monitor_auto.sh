#!/usr/bin/env bash
set -euo pipefail

APP_DIR="${IMMERSIONMONITOR_REPO_DIR:-/opt/immersionmonitor}"
AGENT_PORT="${BENCH_AGENT_PORT:-8765}"
PYTHON="${PYTHON:-python3}"

choose_cidr() {
  local ips ip prefix
  ips="$(hostname -I 2>/dev/null || true)"

  # Prefer the two known bench/company ranges seen during setup.
  for ip in ${ips}; do
    case "${ip}" in
      192.168.50.*)
        echo "192.168.50.0/24"
        return 0
        ;;
      10.50.0.*)
        echo "10.50.0.0/24"
        return 0
        ;;
    esac
  done

  # Generic private IPv4 fallback: scan the /24 of the first private address.
  for ip in ${ips}; do
    case "${ip}" in
      10.*.*.*|192.168.*.*|172.1[6-9].*.*|172.2[0-9].*.*|172.3[0-1].*.*)
        prefix="${ip%.*}"
        echo "${prefix}.0/24"
        return 0
        ;;
    esac
  done

  # Last resort: original default bench network.
  echo "10.50.0.0/24"
}

CIDR="${DISCOVERY_CIDR:-$(choose_cidr)}"

echo "Starting readable monitor"
echo "Discovery CIDR: ${CIDR}"
echo "Worker agent port: ${AGENT_PORT}"

cd "${APP_DIR}"
exec "${PYTHON}" readable_monitor.py --discovery-cidr "${CIDR}" --agent-port "${AGENT_PORT}" "$@"

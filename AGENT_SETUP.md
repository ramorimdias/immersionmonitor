# Worker agent setup

This repository now supports a worker-agent mode. The head Pi can still use `/home/motul/nodes_ips` and SSH, but it can also discover worker Pis running `worker_agent.py`.

## Target network

Recommended bench network:

```text
Head Pi bench IP: 10.50.0.1
Worker DHCP range: 10.50.0.100 to 10.50.0.200
Worker agent port: 8765
Discovery CIDR: 10.50.0.0/24
```

## Fresh worker Pi workflow

Install **Raspberry Pi OS Lite 64-bit**, boot the worker, give it temporary internet access, then run this one-command installer:

```bash
curl -fsSL https://raw.githubusercontent.com/ramorimdias/immersionmonitor/main/scripts/install_worker.sh | bash
```

Until this PR is merged, test the installer from this branch with:

```bash
IMMERSIONMONITOR_BRANCH=codex/worker-agent-discovery \
  bash -c "$(curl -fsSL https://raw.githubusercontent.com/ramorimdias/immersionmonitor/codex/worker-agent-discovery/scripts/install_worker.sh)"
```

The installer performs these steps automatically:

```text
apt update
install python3, stress-ng, curl, ca-certificates
copy worker_agent.py to /opt/immersionmonitor
install bench-worker-agent.service
start and enable the service at boot
```

After installation, connect the worker Pi to the bench switch. The head Pi should discover it if the dashboard is started with the correct discovery CIDR.

Check from the worker itself:

```bash
curl http://127.0.0.1:8765/status
```

## Manual worker installation

```bash
sudo apt update
sudo apt install -y python3 stress-ng
sudo mkdir -p /opt/immersionmonitor
sudo cp worker_agent.py /opt/immersionmonitor/worker_agent.py
sudo cp systemd/bench-worker-agent.service /etc/systemd/system/bench-worker-agent.service
sudo systemctl daemon-reload
sudo systemctl enable --now bench-worker-agent.service
```

## Head Pi usage

Run the dashboard with worker discovery enabled:

```bash
python3 dual_monitor.py --discovery-cidr 10.50.0.0/24 --agent-port 8765
```

If `/home/motul/nodes_ips` exists, those SSH nodes are still used as fallback nodes. If the file is missing but `--discovery-cidr` is supplied, the dashboard relies on discovered worker agents instead.

## Worker API

```text
GET  /status
POST /stress/start
POST /stress/stop
POST /reboot
```

`/status` returns serial number, hostname, CPU temperature, CPU clock, CPU usage, stress state, and uptime.

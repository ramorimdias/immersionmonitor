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

## Worker Pi image

Install the runtime tools:

```bash
sudo apt update
sudo apt install -y python3 stress-ng
sudo mkdir -p /opt/immersionmonitor
sudo cp worker_agent.py /opt/immersionmonitor/worker_agent.py
sudo cp systemd/bench-worker-agent.service /etc/systemd/system/bench-worker-agent.service
sudo systemctl daemon-reload
sudo systemctl enable --now bench-worker-agent.service
```

Check from the worker itself:

```bash
curl http://127.0.0.1:8765/status
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

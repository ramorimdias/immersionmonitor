# Readable dashboard

`readable_monitor.py` is an operator-friendly dashboard entrypoint built on top of the existing `dual_monitor.py` logic.

It keeps the same core functions:

```text
MCC-134 thermocouple acquisition
worker-agent discovery
optional SSH fallback nodes
stress start/stop
CSV logging
Excel export
plotting
```

It changes the screen layout so the graph remains readable and the user can understand worker discovery.

## What is improved

```text
Graph and configuration are separated into two tabs
The Graph tab gives the plot almost the full window
The Configuration / Workers tab contains controls, discovery status, and the worker table
The new dashboard defaults to worker-agent discovery only
It does not read /home/motul/nodes_ips unless --nodes-file is explicitly provided
Bench status cards show MCC-134 and worker count
Discovery status shows CIDR, port, scan result, and last scan time
Worker table shows state, hostname, IP, source, temperature, clock, CPU, and last seen
Operator help text explains what to check when workers are missing
```

## Run it

Default discovery-only mode:

```bash
cd /opt/immersionmonitor
python3 readable_monitor.py
```

This scans the default bench subnet:

```text
10.50.0.0/24
```

Use another subnet if needed:

```bash
python3 readable_monitor.py --discovery-cidr 10.50.1.0/24 --agent-port 8765
```

The old dashboard still works and keeps its previous behavior:

```bash
python3 dual_monitor.py --discovery-cidr 10.50.0.0/24 --agent-port 8765
```

## Optional SSH fallback

Static SSH fallback nodes are disabled by default in `readable_monitor.py`.

To enable them explicitly:

```bash
python3 readable_monitor.py --nodes-file /home/motul/nodes_ips --discovery-cidr 10.50.0.0/24
```

## When a worker is missing

Check, in this order:

```text
1. Worker Pi powered on
2. Ethernet cable connected to the bench switch
3. Worker got an IP on the same bench subnet
4. Worker agent service is running
5. Head Pi launched with the correct discovery CIDR
```

Useful worker commands:

```bash
systemctl status bench-worker-agent.service
curl http://127.0.0.1:8765/status
hostname -I
```

Useful head Pi command:

```bash
python3 readable_monitor.py --discovery-cidr 10.50.0.0/24 --agent-port 8765
```

# Readable dashboard

`readable_monitor.py` is an operator-friendly dashboard entrypoint built on top of the existing `dual_monitor.py` logic.

It keeps the same core functions:

```text
MCC-134 thermocouple acquisition
worker-agent discovery
SSH fallback nodes
stress start/stop
CSV logging
Excel export
plotting
```

It changes the screen layout so the user can understand what is happening.

## What is improved

```text
Clear title and test state
Separate logging, test sequence, and node action sections
Bench status cards for MCC-134 and worker count
Discovery status with CIDR, port, last scan, and result
Worker table with state, hostname, IP, source, temperature, clock, CPU, and last seen
Operator help text when no nodes are found or when workers are offline
```

## Run it

```bash
cd /opt/immersionmonitor
python3 readable_monitor.py --discovery-cidr 10.50.0.0/24 --agent-port 8765
```

The old dashboard still works:

```bash
python3 dual_monitor.py --discovery-cidr 10.50.0.0/24 --agent-port 8765
```

## When a worker is missing

Check, in this order:

```text
1. Worker Pi powered on
2. Ethernet cable connected to the bench switch
3. Worker got an IP on the same bench subnet
4. Worker agent service is running
5. Head Pi launched with the correct --discovery-cidr
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

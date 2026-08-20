# Clean dashboard architecture

The operator application has been rebuilt so the UI is no longer an incremental variation of the original dashboard.

The launch command remains unchanged:

```bash
cd /opt/immersionmonitor
bash scripts/run_readable_monitor_auto.sh
```

`readable_monitor.py` is now only a stable compatibility entrypoint. The application is split into:

```text
readable_monitor.py   stable launcher entrypoint
monitor_app.py        application layout, UI state, worker presentation, plot rendering
monitor_ui.py         reusable cards, buttons, duration controls, palette and ttk styles
dual_monitor.py       proven acquisition/control backend kept for hardware compatibility
worker_agent.py       worker HTTP agent
```

## Operator layout

The new interface has two purpose-specific screens instead of a growing set of mixed controls.

### Live monitor

The main screen contains:

- test-state, worker, MCC-134 and recording summary cards
- a large temperature-history plot
- Wait, Stress and Cooling duration controls
- Start sequence, Stop stress and Skip cooling controls
- Start log, Stop log and Save XLSX controls
- Auto/Tighter/Wider temperature scaling
- recent event messages

### Workers & setup

The setup screen contains:

- current worker availability
- discovery status and subnet information
- MCC-134 status
- worker table with hostname, IP, CPU temperature, clock, CPU load and last-seen state
- Reboot all workers
- Clear collected data

## Preserved behavior

The refactor intentionally keeps the existing bench behavior:

```text
MCC-134 thermocouple acquisition
worker-agent auto discovery
optional SSH fallback nodes
stress start/stop
wait -> stress -> cooling sequence
manual logging
raw CSV buffering
Excel export
worker reboot
full-screen operation
headless mode
```

The default worker discovery network is:

```text
192.168.50.0/24
```

and the default worker-agent port is:

```text
8765
```

## Recommended launch

The automatic launcher detects the head bench subnet and starts the clean UI:

```bash
cd /opt/immersionmonitor
bash scripts/run_readable_monitor_auto.sh
```

The desktop shortcut uses the same launcher, so no shortcut change is required after the UI refactor.

## Manual launch

```bash
cd /opt/immersionmonitor
python3 readable_monitor.py --discovery-cidr 192.168.50.0/24 --agent-port 8765
```

Static SSH fallback remains opt-in:

```bash
python3 readable_monitor.py \
  --nodes-file /home/motul/nodes_ips \
  --discovery-cidr 192.168.50.0/24 \
  --agent-port 8765
```

## Troubleshooting

If a worker is missing, check:

```text
worker powered on
Ethernet connected to the isolated bench switch
head DHCP lease exists
worker responds on TCP 8765
bench-worker-agent.service is running
```

Useful head commands:

```bash
cat /var/lib/misc/immersion-bench.leases
curl http://<worker-ip>:8765/status
journalctl -u bench-head-update.service -n 30 --no-pager
```

## Recommended Raspberry Pi 5 bench setup

The bench uses an isolated Ethernet network managed by the head Raspberry Pi.

- Head `eth0`: `192.168.50.5/24`
- Worker addresses: assigned automatically by DHCP
- DHCP pool: `192.168.50.100` to `192.168.50.199`
- Worker agent: HTTP on TCP port `8765`
- Worker discovery: `192.168.50.0/24`

Workers do not need Wi-Fi and do not need a manually assigned static IP.

## 1. Configure the head Pi once

The head should have its company/internet connection on an interface other than the isolated bench `eth0` connection, for example Wi-Fi. Connect `eth0` to the bench switch.

Run on the head while it has internet access:

```bash
curl -fsSL https://raw.githubusercontent.com/ramorimdias/immersionmonitor/main/scripts/install_bench_network.sh | bash
```

This configures `eth0` as `192.168.50.5/24` and starts a DHCP server bound only to the bench Ethernet interface.

Verify:

```bash
ip -4 -br addr show eth0
```

Expected head address:

```text
eth0    UP    192.168.50.5/24
```

Check the DHCP service:

```bash
systemctl status immersion-bench-dhcp.service
```

## 2. Flash a worker SD card

In Raspberry Pi Imager:

- OS: Raspberry Pi OS Lite 64-bit
- Username: `<worker-username>`
- Password: `<worker-password>`
- Enable SSH: yes
- SSH authentication: password authentication
- Wi-Fi: not required
- Wi-Fi country: `FR`
- Timezone: `Europe/Paris`
- Keyboard: `fr`

Do not use the first-boot bootfs provisioning flow for this setup.

## 3. Temporarily connect the worker to the company network

For initial installation only, connect the worker Ethernet port to a company wall/network port that provides DHCP and internet access.

Boot the worker and log in with the username and password configured in Raspberry Pi Imager.

Confirm internet access before continuing.

## 4. Install the worker agent and bench Ethernet profile

Run exactly:

```bash
curl -fsSL https://raw.githubusercontent.com/ramorimdias/immersionmonitor/main/scripts/install_worker.sh | bash
```

The installer:

- installs the required packages
- downloads `worker_agent.py`
- installs and enables `bench-worker-agent.service`
- creates a dedicated NetworkManager Ethernet profile named `immersion-worker-dhcp`
- binds that profile to `eth0`
- configures it for automatic DHCP at boot with high autoconnect priority
- leaves the current company Ethernet connection alive until shutdown

Verify the agent locally:

```bash
curl http://127.0.0.1:8765/status
```

Verify the prepared Ethernet profile:

```bash
nmcli -f NAME,TYPE,DEVICE,AUTOCONNECT connection show immersion-worker-dhcp
```

The response should show an Ethernet profile with autoconnect enabled. It does not need to be the active profile while the worker is still connected to the company network.

## 5. Move the worker to the bench

Shut the worker down cleanly:

```bash
sudo poweroff
```

Then:

1. Disconnect its Ethernet cable from the company network.
2. Connect its Ethernet cable to the isolated bench switch.
3. Power the worker on.

No screen, keyboard, Wi-Fi, or further worker-side configuration is required.

On the next boot, `immersion-worker-dhcp` requests an address from the head DHCP server. The head assigns an address in the `192.168.50.100` to `192.168.50.199` range.

## 6. Verify the worker from the head

On the head, inspect DHCP leases:

```bash
cat /var/lib/misc/immersion-bench.leases
```

To query every currently leased worker address on port `8765`:

```bash
for ip in $(awk '{print $3}' /var/lib/misc/immersion-bench.leases); do
    echo "=== $ip ==="
    curl -fsS --connect-timeout 1 "http://$ip:8765/status" || echo "agent not responding"
done
```

If the lease file is empty, check live DHCP traffic on the head while power-cycling the worker:

```bash
sudo journalctl -u immersion-bench-dhcp.service -f
```

A healthy worker boot should produce `DHCPDISCOVER`, `DHCPOFFER`, `DHCPREQUEST`, and `DHCPACK` messages.

## 7. Start the readable monitor

Use the automatic launcher so the head scans the bench Ethernet subnet:

```bash
cd /opt/immersionmonitor
bash scripts/run_readable_monitor_auto.sh
```

On a head with `192.168.50.5/24` on Ethernet, the launcher scans `192.168.50.0/24` on port `8765` and adds responding worker agents automatically.

## 8. Create a reusable golden worker SD image

Once one worker is fully working on the bench, it can be converted into a golden image and cloned to additional Raspberry Pis.

Do not make a raw clone before this preparation step. A normal clone would duplicate the machine ID, SSH host keys, and hostname.

The template preparation installs a one-time first-boot service and then removes identities that must be unique. Each cloned Raspberry Pi creates its own identity on first boot.

### Prepare the working worker while it remains on the bench

The worker does not need internet for this step. Download the preparation script on the head and copy it to the worker over the bench network.

Replace `<worker-ip>` with the worker DHCP address shown in the head lease file.

On the head:

```bash
curl -fsSL https://raw.githubusercontent.com/ramorimdias/immersionmonitor/main/scripts/prepare_worker_template.sh -o /tmp/prepare_worker_template.sh
scp /tmp/prepare_worker_template.sh <worker-username>@<worker-ip>:/tmp/prepare_worker_template.sh
ssh -t <worker-username>@<worker-ip> 'bash /tmp/prepare_worker_template.sh'
```

The script automatically powers the worker off when preparation is complete.

Do not boot that SD card again before creating the master image. Remove the powered-off SD card and read it into an image file using an SD-card imaging tool.

### What each clone does on its first boot

Each flashed clone automatically:

- generates a new `/etc/machine-id`
- generates new SSH host keys
- sets a unique hostname from the Raspberry Pi Ethernet MAC address, for example `worker-79a196`
- keeps `eth0` on the `immersion-worker-dhcp` DHCP profile
- enables `bench-worker-agent.service`
- removes the one-time initialization marker so the identity step does not repeat

After flashing a clone, insert the SD card into another Raspberry Pi, connect it to the bench switch, and power it on. No company-network installation step is needed for cloned workers.

The head DHCP server assigns each clone a different `192.168.50.100` to `192.168.50.199` address, and `readable_monitor.py` discovers each agent automatically.

## 9. Keep the head application updated automatically

Install the head updater once:

```bash
curl -fsSL https://raw.githubusercontent.com/ramorimdias/immersionmonitor/main/scripts/install_head_autoupdate.sh | bash
```

The systemd timer checks GitHub shortly after every boot and once per day. The boot check starts about 20 seconds after startup. If GitHub is temporarily unreachable, the update service retries every 30 seconds for several minutes.

The updater resets `/opt/immersionmonitor` to the latest `origin/main`, so the desktop shortcut uses the latest code the next time the monitor is launched.

Check the timer with:

```bash
systemctl status bench-head-update.timer
```

Check recent update logs with:

```bash
journalctl -u bench-head-update.service -n 50 --no-pager
```

# Worker first-boot provisioning

This is the no-screen, no-keyboard worker setup path.

Goal:

```text
Flash Raspberry Pi OS Lite
Prepare the SD card boot partition once
Insert SD card into worker Pi
Connect Ethernet and power
Worker installs itself on first boot
Move worker to the bench switch
Head Pi discovers it automatically
```

## Requirements

During first boot, the worker must have:

```text
Ethernet connection
Internet access
DHCP
```

Internet is needed only for first installation because the worker downloads packages and the latest repository files. After installation, the worker can run on the isolated bench network.

## Prepare the SD card from Linux

1. Flash **Raspberry Pi OS Lite 64-bit** with Raspberry Pi Imager.
2. Remove and reinsert the SD card so the `bootfs` partition is mounted.
3. Run:

```bash
curl -fsSL https://raw.githubusercontent.com/ramorimdias/immersionmonitor/main/scripts/prepare_worker_bootfs.sh -o /tmp/prepare_worker_bootfs.sh
bash /tmp/prepare_worker_bootfs.sh /media/$USER/bootfs
```

If your mount path is different, replace `/media/$USER/bootfs` with the mounted boot partition path.

## Test before this PR is merged

```bash
curl -fsSL https://raw.githubusercontent.com/ramorimdias/immersionmonitor/worker-firstboot-provisioning/scripts/prepare_worker_bootfs.sh -o /tmp/prepare_worker_bootfs.sh
IMMERSIONMONITOR_BRANCH=worker-firstboot-provisioning bash /tmp/prepare_worker_bootfs.sh /media/$USER/bootfs
```

## What the preparation script does

```text
Copies worker_firstboot.sh to the FAT boot partition
Adds a systemd.run first-boot hook to cmdline.txt
Enables SSH by creating the bootfs ssh marker file
```

## What happens on first boot

The worker Pi:

```text
waits for network
runs apt update
installs python3, stress-ng, curl, ca-certificates
copies worker_agent.py to /opt/immersionmonitor
installs bench-worker-agent.service
removes the first-boot hook from cmdline.txt
reboots
starts the worker agent automatically after reboot
```

Log file on the SD boot partition:

```text
/boot/firmware/immersionmonitor-firstboot.log
```

If provisioning fails, the first-boot hook is left in place so the worker retries on the next boot.

## After installation

Move the worker Pi to the bench switch.

On the head Pi, run:

```bash
cd /opt/immersionmonitor
python3 readable_monitor.py
```

The readable dashboard scans the default bench subnet:

```text
10.50.0.0/24
```

## Important limitation

This is not a fully offline image. It is a first-boot installer. For a fully offline worker SD card, build a custom Raspberry Pi OS image with all packages and files already embedded.

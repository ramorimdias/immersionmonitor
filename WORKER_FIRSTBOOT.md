# Worker first-boot provisioning

This is the no-screen, no-keyboard worker setup path.

Goal:

```text
Flash Raspberry Pi OS Lite
Prepare the SD card boot partition once
Insert SD card into worker Pi
Connect Ethernet or Wi-Fi
Worker installs itself on first boot
Move worker to the bench switch
Head Pi discovers it automatically
```

## Requirements

During first installation, the worker must have:

```text
Internet access
DHCP
DNS
Access to raw.githubusercontent.com and apt repositories
```

Internet is needed only for first installation because the worker downloads packages and the latest repository files. After installation, the worker can run on the isolated bench network.

## Important Raspberry Pi Imager settings

When flashing the SD card, open Raspberry Pi Imager advanced settings and set at least:

```text
Hostname: bench-worker
Username and password: set them explicitly
SSH: enabled
Wi-Fi: optional here, because this repo also provides a bootfs Wi-Fi setup path
```

Modern Raspberry Pi OS images do not have the old default `pi` user unless you configure a user. Set the user in Raspberry Pi Imager even if you do not plan to use keyboard/screen.

## What goes on the SD card

After flashing, Windows shows only the small FAT boot partition. It usually appears as a drive such as:

```text
D:\
E:\
F:\
```

This is the partition that contains files like:

```text
cmdline.txt
config.txt
kernel8.img
```

That partition is the place where the preparation script writes:

```text
worker_firstboot.sh             at the root of the boot partition
ssh                             empty marker file at the root of the boot partition
immersionmonitor-wifi.env       optional Wi-Fi config at the root of the boot partition
cmdline.txt                     modified by adding a first-boot systemd.run hook
```

Example if Windows mounts the SD card as `D:`:

```text
D:\worker_firstboot.sh
D:\ssh
D:\immersionmonitor-wifi.env
D:\cmdline.txt   modified, not replaced
```

Do not put these files inside a folder. They must be at the root of the visible Raspberry Pi boot drive.

## Prepare the SD card from Windows with Wi-Fi

1. Flash **Raspberry Pi OS Lite 64-bit** with Raspberry Pi Imager.
2. Eject and reinsert the SD card if Windows does not show the boot drive.
3. Open PowerShell.
4. Replace `D:` below if the SD card uses another boot drive letter.
5. Run:

```powershell
Invoke-WebRequest -Uri "https://raw.githubusercontent.com/ramorimdias/immersionmonitor/main/scripts/prepare_worker_bootfs.ps1" -OutFile "$env:TEMP\prepare_worker_bootfs.ps1"
powershell -ExecutionPolicy Bypass -File "$env:TEMP\prepare_worker_bootfs.ps1" -BootDrive D: -WifiSsid "Motul-Guest" -WifiPassword "Welcome-2-Motul!" -WifiCountry FR
```

## Test from Windows before this PR is merged

```powershell
Invoke-WebRequest -Uri "https://raw.githubusercontent.com/ramorimdias/immersionmonitor/worker-firstboot-provisioning/scripts/prepare_worker_bootfs.ps1" -OutFile "$env:TEMP\prepare_worker_bootfs.ps1"
powershell -ExecutionPolicy Bypass -File "$env:TEMP\prepare_worker_bootfs.ps1" -BootDrive D: -Branch worker-firstboot-provisioning -WifiSsid "Motul-Guest" -WifiPassword "Welcome-2-Motul!" -WifiCountry FR
```

## Prepare the SD card from Windows without Wi-Fi

Use this only if the worker first boot will use Ethernet with internet access:

```powershell
Invoke-WebRequest -Uri "https://raw.githubusercontent.com/ramorimdias/immersionmonitor/main/scripts/prepare_worker_bootfs.ps1" -OutFile "$env:TEMP\prepare_worker_bootfs.ps1"
powershell -ExecutionPolicy Bypass -File "$env:TEMP\prepare_worker_bootfs.ps1" -BootDrive D:
```

## Prepare the SD card from Linux

1. Flash **Raspberry Pi OS Lite 64-bit** with Raspberry Pi Imager.
2. Remove and reinsert the SD card so the `bootfs` partition is mounted.
3. Run:

```bash
curl -fsSL https://raw.githubusercontent.com/ramorimdias/immersionmonitor/main/scripts/prepare_worker_bootfs.sh -o /tmp/prepare_worker_bootfs.sh
bash /tmp/prepare_worker_bootfs.sh /media/$USER/bootfs
```

If your mount path is different, replace `/media/$USER/bootfs` with the mounted boot partition path.

## Test from Linux before this PR is merged

```bash
curl -fsSL https://raw.githubusercontent.com/ramorimdias/immersionmonitor/worker-firstboot-provisioning/scripts/prepare_worker_bootfs.sh -o /tmp/prepare_worker_bootfs.sh
IMMERSIONMONITOR_BRANCH=worker-firstboot-provisioning bash /tmp/prepare_worker_bootfs.sh /media/$USER/bootfs
```

## What the preparation script does

```text
Copies worker_firstboot.sh to the FAT boot partition
Optionally writes immersionmonitor-wifi.env to the FAT boot partition
Adds a systemd.run first-boot hook to cmdline.txt
Enables SSH by creating the bootfs ssh marker file
```

## What happens on first boot

The worker Pi:

```text
creates a Wi-Fi connection if immersionmonitor-wifi.env exists
installs a second-stage provisioning service
removes the early first-boot hook from cmdline.txt
reboots into normal boot
waits for network-online.target
checks DNS and GitHub access
runs apt update
installs python3, stress-ng, curl, ca-certificates
copies worker_agent.py to /opt/immersionmonitor
installs bench-worker-agent.service
disables the provisioning service
reboots
starts the worker agent automatically after reboot
```

Log file on the SD boot partition:

```text
/boot/firmware/immersionmonitor-firstboot.log
```

If provisioning fails, the second-stage provisioning service is left enabled so the worker retries on the next boot.

## After installation

Move the worker Pi to the bench switch.

On the head Pi, run the auto launcher:

```bash
cd /opt/immersionmonitor
bash scripts/run_readable_monitor_auto.sh
```

The auto launcher selects the discovery subnet from the head Pi IP:

```text
Head IP 192.168.50.x  -> scans 192.168.50.0/24
Head IP 10.50.0.x     -> scans 10.50.0.0/24
Other private IP      -> scans that /24 subnet
```

You can still override it manually:

```bash
DISCOVERY_CIDR=192.168.50.0/24 bash scripts/run_readable_monitor_auto.sh
DISCOVERY_CIDR=10.50.0.0/24 bash scripts/run_readable_monitor_auto.sh
```

## Important limitation

This is not a fully offline image. It is a first-boot installer. For a fully offline worker SD card, build a custom Raspberry Pi OS image with all packages and files already embedded.

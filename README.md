## Recommended worker setup for Raspberry Pi 5

For Raspberry Pi 5 workers, use the manual installer flow. Do not use the first-boot bootfs provisioning script as the primary method, because it can be opaque during boot and may leave the Pi on a black screen without useful feedback.

### 1. Flash the worker SD card

In Raspberry Pi Imager:

- OS: Raspberry Pi OS Lite 64-bit
- Username: `<worker-username>`
- Password: `<worker-password>`
- Enable SSH: yes
- SSH authentication: password authentication
- Wi-Fi SSID: `<worker-wifi-ssid>`
- Wi-Fi password: `<worker-wifi-password>`
- Wi-Fi country: `FR`
- Timezone: `Europe/Paris`
- Keyboard: `fr`

Do not run the Windows bootfs preparation script for this flow.

### 2. Boot and log into the worker

Boot the worker normally, then log in with the username and password configured in Raspberry Pi Imager.

Make sure the worker has internet access before continuing.

### 3. Install the worker agent

Run exactly:

```bash
curl -fsSL https://raw.githubusercontent.com/ramorimdias/immersionmonitor/main/scripts/install_worker.sh | bash
```

The installer installs the required packages, downloads `worker_agent.py`, installs the `bench-worker-agent.service` systemd service, and starts it automatically.

### 4. Verify the worker agent

Check the agent locally:

```bash
curl http://127.0.0.1:8765/status
```

Then check the worker network addresses:

```bash
ip -br addr
```

The head Raspberry Pi should then be able to reach the worker at:

```text
http://<worker-ip>:8765/status
```

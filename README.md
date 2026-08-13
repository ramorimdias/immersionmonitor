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

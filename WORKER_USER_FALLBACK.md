# Worker Linux user fallback

Use this if Raspberry Pi OS still prompts for interactive user creation even though Raspberry Pi Imager was configured with a username.

The Windows boot preparation script can now write a fallback Linux user into the boot provisioning config. On first boot, `worker_firstboot.sh` creates that user before the OS can drop into the interactive user prompt.

## Windows command before this PR is merged

Assuming the SD boot partition is `D:`:

```powershell
Invoke-WebRequest -Uri "https://raw.githubusercontent.com/ramorimdias/immersionmonitor/worker-user-fallback/scripts/prepare_worker_bootfs.ps1" -OutFile "$env:TEMP\prepare_worker_bootfs.ps1"

powershell -ExecutionPolicy Bypass -File "$env:TEMP\prepare_worker_bootfs.ps1" `
  -BootDrive D: `
  -Branch worker-user-fallback `
  -WifiSsid "Motul-Guest" `
  -WifiPassword "Welcome-2-Motul!" `
  -WifiCountry FR `
  -LinuxUser "motul" `
  -LinuxPassword "motul"
```

## Windows command after merge

```powershell
Invoke-WebRequest -Uri "https://raw.githubusercontent.com/ramorimdias/immersionmonitor/main/scripts/prepare_worker_bootfs.ps1" -OutFile "$env:TEMP\prepare_worker_bootfs.ps1"

powershell -ExecutionPolicy Bypass -File "$env:TEMP\prepare_worker_bootfs.ps1" `
  -BootDrive D: `
  -WifiSsid "Motul-Guest" `
  -WifiPassword "Welcome-2-Motul!" `
  -WifiCountry FR `
  -LinuxUser "motul" `
  -LinuxPassword "motul"
```

## Security note

The fallback password is stored temporarily on the SD boot partition in `immersionmonitor-wifi.env` using base64 encoding. Base64 is not encryption. Use this only for disposable worker provisioning, and reflash or delete the provisioning file after installation if the SD card leaves the bench environment.

param(
    [Parameter(Mandatory = $true)]
    [string]$BootDrive,

    [string]$Branch = "main",
    [string]$RepoOwner = "ramorimdias",
    [string]$RepoName = "immersionmonitor",
    [string]$WifiSsid = "",
    [string]$WifiPassword = "",
    [string]$WifiCountry = "FR"
)

$ErrorActionPreference = "Stop"

if ($BootDrive -notmatch "^[A-Za-z]:\\?$") {
    throw "BootDrive must be a Windows drive letter, for example E:"
}

$BootRoot = $BootDrive.TrimEnd("\") + "\"
$CmdlinePath = Join-Path $BootRoot "cmdline.txt"
$FirstBootPath = Join-Path $BootRoot "worker_firstboot.sh"
$SshPath = Join-Path $BootRoot "ssh"
$WifiEnvPath = Join-Path $BootRoot "immersionmonitor-wifi.env"
$FirstrunPath = Join-Path $BootRoot "firstrun.sh"
$ImagerFirstrunPath = Join-Path $BootRoot "firstrun-imager.sh"

if (-not (Test-Path $CmdlinePath)) {
    throw "cmdline.txt not found on $BootRoot. Select the Raspberry Pi OS bootfs drive, not the Windows recovery prompt."
}

$RawBase = "https://raw.githubusercontent.com/$RepoOwner/$RepoName/$Branch"
$FirstBootUrl = "$RawBase/scripts/worker_firstboot.sh"

Write-Host "Downloading worker provisioning script..."
Invoke-WebRequest -Uri $FirstBootUrl -OutFile $FirstBootPath -UseBasicParsing

if ($WifiSsid -ne "") {
    if ($WifiPassword -eq "") {
        throw "WifiPassword is required when WifiSsid is provided."
    }
    Write-Host "Writing Wi-Fi configuration for first boot..."
    $SsidB64 = [Convert]::ToBase64String([Text.Encoding]::UTF8.GetBytes($WifiSsid))
    $PasswordB64 = [Convert]::ToBase64String([Text.Encoding]::UTF8.GetBytes($WifiPassword))
    $WifiEnv = @"
WIFI_SSID_B64=$SsidB64
WIFI_PASSWORD_B64=$PasswordB64
WIFI_COUNTRY=$WifiCountry
"@
    Set-Content -Path $WifiEnvPath -Value $WifiEnv -Encoding ascii
}

$Cmdline = Get-Content -Raw -Path $CmdlinePath
$Cmdline = $Cmdline -replace "`r", "" -replace "`n", ""
$WorkerHook = "systemd.run=/boot/firmware/worker_firstboot.sh systemd.run_success_action=reboot systemd.unit=kernel-command-line.target"
$HasSystemdRun = $Cmdline -match "systemd\.run="
$HasWorkerHook = $Cmdline -match "systemd\.run=/boot/firmware/worker_firstboot\.sh"

if ($HasSystemdRun -and -not $HasWorkerHook -and (Test-Path $FirstrunPath)) {
    Write-Host "Raspberry Pi Imager first-run script detected. Wrapping it instead of adding a second systemd.run hook..."
    if (-not (Test-Path $ImagerFirstrunPath)) {
        Move-Item -Path $FirstrunPath -Destination $ImagerFirstrunPath
    }
    $Wrapper = @'
#!/usr/bin/env bash
set -e
LOG=/boot/firmware/immersionmonitor-firstboot.log
exec >> "$LOG" 2>&1
printf '\n===== combined Raspberry Pi Imager + immersionmonitor first run: %s =====\n' "$(date --iso-8601=seconds)"
if [ -x /boot/firmware/firstrun-imager.sh ]; then
  /boot/firmware/firstrun-imager.sh
elif [ -f /boot/firmware/firstrun-imager.sh ]; then
  bash /boot/firmware/firstrun-imager.sh
fi
bash /boot/firmware/worker_firstboot.sh
'@
    Set-Content -Path $FirstrunPath -Value $Wrapper -Encoding ascii
} elseif (-not $HasWorkerHook) {
    Write-Host "Adding worker first-boot hook to cmdline.txt..."
    $Cmdline = ($Cmdline.Trim() + " " + $WorkerHook).Trim()
    Set-Content -Path $CmdlinePath -Value $Cmdline -NoNewline -Encoding ascii
} else {
    Write-Host "Worker first-boot hook already present."
}

if (-not (Test-Path $SshPath)) {
    Write-Host "Creating ssh marker file..."
    New-Item -Path $SshPath -ItemType File | Out-Null
}

Write-Host ""
Write-Host "Worker SD card prepared successfully."
Write-Host "Next steps:"
Write-Host "1. Eject the SD card safely from Windows."
Write-Host "2. Insert it into the worker Pi."
Write-Host "3. Connect Ethernet or make sure the configured Wi-Fi is available."
Write-Host "4. Power on the Pi and wait until it installs and reboots."
Write-Host "5. Move it to the bench switch."
Write-Host ""
Write-Host "First-boot log will be written on the Pi at /boot/firmware/immersionmonitor-firstboot.log"

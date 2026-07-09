param(
    [Parameter(Mandatory = $true)]
    [string]$BootDrive,

    [string]$Branch = "main",
    [string]$RepoOwner = "ramorimdias",
    [string]$RepoName = "immersionmonitor",
    [string]$WifiSsid = "",
    [string]$WifiPassword = "",
    [string]$WifiCountry = "FR",
    [string]$LinuxUser = "",
    [string]$LinuxPassword = ""
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

if (-not (Test-Path $CmdlinePath)) {
    throw "cmdline.txt not found on $BootRoot. Select the Raspberry Pi OS bootfs drive, not the Windows recovery prompt."
}

$RawBase = "https://raw.githubusercontent.com/$RepoOwner/$RepoName/$Branch"
$FirstBootUrl = "$RawBase/scripts/worker_firstboot.sh"

Write-Host "Downloading first-boot script..."
Invoke-WebRequest -Uri $FirstBootUrl -OutFile $FirstBootPath -UseBasicParsing

$EnvLines = @()

if ($WifiSsid -ne "") {
    if ($WifiPassword -eq "") {
        throw "WifiPassword is required when WifiSsid is provided."
    }
    Write-Host "Adding Wi-Fi configuration for first boot..."
    $SsidB64 = [Convert]::ToBase64String([Text.Encoding]::UTF8.GetBytes($WifiSsid))
    $PasswordB64 = [Convert]::ToBase64String([Text.Encoding]::UTF8.GetBytes($WifiPassword))
    $EnvLines += "WIFI_SSID_B64=$SsidB64"
    $EnvLines += "WIFI_PASSWORD_B64=$PasswordB64"
    $EnvLines += "WIFI_COUNTRY=$WifiCountry"
}

if ($LinuxUser -ne "") {
    if ($LinuxPassword -eq "") {
        throw "LinuxPassword is required when LinuxUser is provided."
    }
    Write-Host "Adding Linux user fallback for first boot..."
    $LinuxUserB64 = [Convert]::ToBase64String([Text.Encoding]::UTF8.GetBytes($LinuxUser))
    $LinuxPasswordB64 = [Convert]::ToBase64String([Text.Encoding]::UTF8.GetBytes($LinuxPassword))
    $EnvLines += "LINUX_USER_B64=$LinuxUserB64"
    $EnvLines += "LINUX_PASSWORD_B64=$LinuxPasswordB64"
}

if ($EnvLines.Count -gt 0) {
    Set-Content -Path $WifiEnvPath -Value ($EnvLines -join "`n") -Encoding ascii
}

Write-Host "Adding first-boot hook to cmdline.txt..."
$Cmdline = Get-Content -Raw -Path $CmdlinePath
$Cmdline = $Cmdline -replace "`r", "" -replace "`n", ""
$Hook = "systemd.run=/boot/firmware/worker_firstboot.sh systemd.run_success_action=reboot systemd.unit=kernel-command-line.target"

if ($Cmdline -notmatch "systemd\.run=/boot/firmware/worker_firstboot\.sh") {
    $Cmdline = ($Cmdline.Trim() + " " + $Hook).Trim()
    Set-Content -Path $CmdlinePath -Value $Cmdline -NoNewline -Encoding ascii
} else {
    Write-Host "First-boot hook already present."
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

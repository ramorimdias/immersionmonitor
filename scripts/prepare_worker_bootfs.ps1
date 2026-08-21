param(
    [string]$BootDrive = "D:",
    [string]$Branch = "main",
    [string]$RepoOwner = "ramorimdias",
    [string]$RepoName = "immersionmonitor",
    [string]$WifiSsid = "Motul-Guest",
    [string]$WifiPassword = "Welcome-2-Motul!",
    [string]$WifiCountry = "FR",
    [string]$LinuxUser = "motul",
    [string]$LinuxPassword = "motul"
)

$ErrorActionPreference = "Stop"

if ($BootDrive -notmatch "^[A-Za-z]:\\?$") {
    throw "BootDrive must be a Windows drive letter, for example D:"
}

$BootRoot = $BootDrive.TrimEnd("\") + "\"
$CmdlinePath = Join-Path $BootRoot "cmdline.txt"
$FirstBootPath = Join-Path $BootRoot "worker_firstboot.sh"
$SshPath = Join-Path $BootRoot "ssh"
$ProvisionEnvPath = Join-Path $BootRoot "immersionmonitor-worker.env"
$LegacyEnvPath = Join-Path $BootRoot "immersionmonitor-wifi.env"
$StatusPath = Join-Path $BootRoot "immersionmonitor-status.txt"

if (-not (Test-Path $CmdlinePath)) {
    throw "cmdline.txt not found on $BootRoot. Select the Raspberry Pi OS bootfs drive. It should contain cmdline.txt and config.txt."
}

$RawBase = "https://raw.githubusercontent.com/$RepoOwner/$RepoName/$Branch"
$FirstBootUrl = "$RawBase/scripts/worker_firstboot.sh"

Write-Host "Preparing worker SD boot partition: $BootRoot"
Write-Host "Downloading worker first-boot script from $FirstBootUrl"
Invoke-WebRequest -Uri $FirstBootUrl -OutFile $FirstBootPath -UseBasicParsing

$EnvLines = @()
$EnvLines += "PROVISION_STATUS_FILE=/boot/firmware/immersionmonitor-status.txt"

if ($WifiSsid -ne "") {
    if ($WifiPassword -eq "") {
        throw "WifiPassword is required when WifiSsid is provided."
    }
    Write-Host "Adding Wi-Fi configuration for first boot: $WifiSsid"
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
    Write-Host "Adding Linux user fallback: $LinuxUser"
    $LinuxUserB64 = [Convert]::ToBase64String([Text.Encoding]::UTF8.GetBytes($LinuxUser))
    $LinuxPasswordB64 = [Convert]::ToBase64String([Text.Encoding]::UTF8.GetBytes($LinuxPassword))
    $EnvLines += "LINUX_USER_B64=$LinuxUserB64"
    $EnvLines += "LINUX_PASSWORD_B64=$LinuxPasswordB64"
}

Set-Content -Path $ProvisionEnvPath -Value ($EnvLines -join "`n") -Encoding ascii
if (Test-Path $LegacyEnvPath) {
    Remove-Item -Path $LegacyEnvPath -Force
}

$Status = @"
immersionmonitor worker SD prepared from Windows
Prepared at: $(Get-Date -Format s)
Boot drive: $BootRoot
Branch: $Branch
Wi-Fi SSID: $WifiSsid
Linux user fallback: $LinuxUser

On first boot the Pi should show immersionmonitor provisioning messages on screen.
The detailed log is /boot/firmware/immersionmonitor-firstboot.log.
"@
Set-Content -Path $StatusPath -Value $Status -Encoding ascii

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
Write-Host "Files written:"
Write-Host "- $FirstBootPath"
Write-Host "- $ProvisionEnvPath"
Write-Host "- $StatusPath"
Write-Host "- $SshPath"
Write-Host ""
Write-Host "Next steps:"
Write-Host "1. Eject the SD card safely from Windows."
Write-Host "2. Insert it into the worker Pi."
Write-Host "3. Boot it where Motul-Guest Wi-Fi is available, or connect Ethernet with internet."
Write-Host "4. Wait for the worker to create the user, install the agent, and reboot."
Write-Host "5. Move it to the bench switch."

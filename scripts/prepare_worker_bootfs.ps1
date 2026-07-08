param(
    [Parameter(Mandatory = $true)]
    [string]$BootDrive,

    [string]$Branch = "main",
    [string]$RepoOwner = "ramorimdias",
    [string]$RepoName = "immersionmonitor"
)

$ErrorActionPreference = "Stop"

if ($BootDrive -notmatch "^[A-Za-z]:\\?$") {
    throw "BootDrive must be a Windows drive letter, for example E:"
}

$BootRoot = $BootDrive.TrimEnd("\") + "\"
$CmdlinePath = Join-Path $BootRoot "cmdline.txt"
$FirstBootPath = Join-Path $BootRoot "worker_firstboot.sh"
$SshPath = Join-Path $BootRoot "ssh"

if (-not (Test-Path $CmdlinePath)) {
    throw "cmdline.txt not found on $BootRoot. Select the Raspberry Pi OS bootfs drive, not the Windows recovery prompt."
}

$RawBase = "https://raw.githubusercontent.com/$RepoOwner/$RepoName/$Branch"
$FirstBootUrl = "$RawBase/scripts/worker_firstboot.sh"

Write-Host "Downloading first-boot script..."
Invoke-WebRequest -Uri $FirstBootUrl -OutFile $FirstBootPath -UseBasicParsing

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
Write-Host "3. Connect Ethernet to a network with internet access for first boot."
Write-Host "4. Power on the Pi and wait until it installs and reboots."
Write-Host "5. Move it to the bench switch."
Write-Host ""
Write-Host "First-boot log will be written on the Pi at /boot/firmware/immersionmonitor-firstboot.log"

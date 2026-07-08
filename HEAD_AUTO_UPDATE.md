# Head Pi auto-update

The head Pi can automatically fetch the latest repository version with a systemd timer.

## Recommended behavior

The updater:

```text
runs 2 minutes after boot
runs every day around 06:00
uses a lock so two updates cannot run at the same time
fetches the selected GitHub branch
resets /opt/immersionmonitor to origin/<branch>
validates Python syntax with py_compile
optionally restarts a configured systemd service
```

By default, it does **not** restart the dashboard because this repository does not yet define a head dashboard service. New code is used the next time you start the app.

## Install on a fresh head Pi

After installing Raspberry Pi OS and giving the head Pi internet access, run:

```bash
curl -fsSL https://raw.githubusercontent.com/ramorimdias/immersionmonitor/main/scripts/install_head_autoupdate.sh | bash
```

Until this PR is merged, test from this branch with:

```bash
IMMERSIONMONITOR_BRANCH=head-auto-update \
  bash -c "$(curl -fsSL https://raw.githubusercontent.com/ramorimdias/immersionmonitor/head-auto-update/scripts/install_head_autoupdate.sh)"
```

## Configuration

The installer writes:

```text
/etc/default/immersionmonitor-update
```

Default values:

```text
IMMERSIONMONITOR_REPO_DIR=/opt/immersionmonitor
IMMERSIONMONITOR_REMOTE=origin
IMMERSIONMONITOR_BRANCH=main
IMMERSIONMONITOR_RESTART_SERVICE=
```

If you later create a systemd service for the head dashboard, set:

```text
IMMERSIONMONITOR_RESTART_SERVICE=bench-head.service
```

Then reload systemd:

```bash
sudo systemctl daemon-reload
```

## Manual update check

```bash
sudo systemctl start bench-head-update.service
```

## Timer status

```bash
systemctl status bench-head-update.timer
systemctl list-timers bench-head-update.timer
```

## Logs

```bash
journalctl -u bench-head-update.service -n 100 --no-pager
```

## Disable auto-update

```bash
sudo systemctl disable --now bench-head-update.timer
```

## Safety note

Automatic updates are convenient, but avoid restarting the dashboard during an active test. Keep `IMMERSIONMONITOR_RESTART_SERVICE` empty unless the head dashboard service is designed to restart safely.

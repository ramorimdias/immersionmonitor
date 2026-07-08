#!/usr/bin/env python3
"""HTTP worker agent for Raspberry Pi bench nodes.

Run this on each worker Pi. The head Pi discovers it over the bench network,
reads metrics from /status, and controls stress-ng through /stress/start and
/stress/stop.
"""

from __future__ import annotations

import argparse
import json
import os
import socket
import subprocess
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any

STRESS_ARGS = [
    "stress-ng",
    "--cpu",
    "0",
    "--cpu-load",
    "100",
    "--io",
    "2",
    "--matrix",
    "0",
    "--vm",
    "4",
    "--vm-bytes",
    "95%",
    "--memcpy",
    "2",
]

stress_proc: subprocess.Popen | None = None
last_cpu_sample: tuple[float, int, int] | None = None


def read_text(path: str) -> str | None:
    try:
        return Path(path).read_text().strip()
    except OSError:
        return None


def read_serial() -> str:
    cpuinfo = read_text("/proc/cpuinfo") or ""
    for line in cpuinfo.splitlines():
        if line.startswith("Serial"):
            return line.split(":", 1)[1].strip()
    machine_id = read_text("/etc/machine-id")
    return machine_id or socket.gethostname()


def read_temp_c() -> float | None:
    raw = read_text("/sys/class/thermal/thermal_zone0/temp")
    if raw is None:
        return None
    try:
        return round(float(raw) / 1000.0, 2)
    except ValueError:
        return None


def read_clock_mhz() -> float | None:
    raw = read_text("/sys/devices/system/cpu/cpu0/cpufreq/scaling_cur_freq")
    if raw is None:
        return None
    try:
        return round(float(raw) / 1000.0, 1)
    except ValueError:
        return None


def _read_proc_stat() -> tuple[int, int] | None:
    stat = read_text("/proc/stat")
    if not stat:
        return None
    first = stat.splitlines()[0].split()[1:]
    try:
        values = [int(v) for v in first]
    except ValueError:
        return None
    idle = values[3] + (values[4] if len(values) > 4 else 0)
    total = sum(values)
    return total, idle


def cpu_usage_percent() -> float | None:
    global last_cpu_sample
    current = _read_proc_stat()
    if current is None:
        return None
    now = time.monotonic()
    if last_cpu_sample is None:
        last_cpu_sample = (now, current[0], current[1])
        return None
    _, prev_total, prev_idle = last_cpu_sample
    last_cpu_sample = (now, current[0], current[1])
    total_delta = current[0] - prev_total
    idle_delta = current[1] - prev_idle
    if total_delta <= 0:
        return None
    return round(100.0 * (1.0 - idle_delta / total_delta), 1)


def stress_running() -> bool:
    global stress_proc
    if stress_proc is not None and stress_proc.poll() is None:
        return True
    stress_proc = None
    result = subprocess.run(["pgrep", "-f", "stress-ng"], capture_output=True)
    return result.returncode == 0


def set_performance_governor() -> None:
    command = (
        "for g in /sys/devices/system/cpu/cpu*/cpufreq/scaling_governor; do "
        "echo performance | sudo -n tee \"$g\" >/dev/null; done; "
        "[ -e /sys/devices/system/cpu/cpufreq/boost ] && "
        "echo 1 | sudo -n tee /sys/devices/system/cpu/cpufreq/boost >/dev/null || true"
    )
    subprocess.run(command, shell=True, check=False)


def start_stress(seconds: int | None = None) -> None:
    global stress_proc
    if stress_running():
        return
    set_performance_governor()
    cmd = STRESS_ARGS.copy()
    if seconds and seconds > 0:
        cmd += ["--timeout", f"{int(seconds)}s"]
    stress_proc = subprocess.Popen(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)


def stop_stress() -> None:
    global stress_proc
    subprocess.run(["pkill", "-9", "-f", "stress-ng"], check=False)
    stress_proc = None


def status_payload() -> dict[str, Any]:
    return {
        "id": read_serial(),
        "hostname": socket.gethostname(),
        "cpu_temp_c": read_temp_c(),
        "cpu_clock_mhz": read_clock_mhz(),
        "cpu_usage_percent": cpu_usage_percent(),
        "stress_running": stress_running(),
        "uptime_s": round(time.monotonic()),
    }


class Handler(BaseHTTPRequestHandler):
    server_version = "BenchWorkerAgent/1.0"

    def _read_json(self) -> dict[str, Any]:
        length = int(self.headers.get("Content-Length", "0") or "0")
        if length <= 0:
            return {}
        try:
            return json.loads(self.rfile.read(length).decode("utf-8"))
        except json.JSONDecodeError:
            return {}

    def _send_json(self, payload: dict[str, Any], status: int = 200) -> None:
        data = json.dumps(payload).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def log_message(self, fmt: str, *args: Any) -> None:
        return

    def do_GET(self) -> None:  # noqa: N802
        if self.path == "/status":
            self._send_json(status_payload())
        else:
            self._send_json({"error": "not found"}, 404)

    def do_POST(self) -> None:  # noqa: N802
        payload = self._read_json()
        if self.path == "/stress/start":
            seconds = payload.get("seconds")
            start_stress(int(seconds) if seconds is not None else None)
            self._send_json({"ok": True, "stress_running": stress_running()})
        elif self.path == "/stress/stop":
            stop_stress()
            self._send_json({"ok": True, "stress_running": False})
        elif self.path == "/reboot":
            self._send_json({"ok": True, "rebooting": True})
            subprocess.Popen(["sudo", "reboot"])
        else:
            self._send_json({"error": "not found"}, 404)


def main() -> None:
    parser = argparse.ArgumentParser(description="Bench worker HTTP agent")
    parser.add_argument("--host", default="0.0.0.0")
    parser.add_argument("--port", type=int, default=int(os.environ.get("BENCH_AGENT_PORT", "8765")))
    args = parser.parse_args()

    server = ThreadingHTTPServer((args.host, args.port), Handler)
    print(f"worker agent listening on {args.host}:{args.port}", flush=True)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        stop_stress()
        server.server_close()


if __name__ == "__main__":
    main()

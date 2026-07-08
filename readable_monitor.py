#!/usr/bin/env python3
"""Operator-friendly dashboard entrypoint for the immersion bench.

This module reuses the data acquisition, worker discovery, stress control,
MCC-134 reading, CSV logging, and Excel export logic from ``dual_monitor.py``.
It only changes the Tkinter dashboard layout so operators can understand what
has been discovered, which nodes are online, and what to check when nodes are
missing.
"""

from __future__ import annotations

import argparse
import logging
import sys
from datetime import datetime
from pathlib import Path
from threading import Lock

import pandas as pd
import tkinter as tk
from tkinter import ttk

import dual_monitor as core


class ReadableMonitor(core.UnifiedMonitor):
    """A clearer dashboard wrapped around ``dual_monitor.UnifiedMonitor``."""

    ONLINE_AFTER_SECONDS = 6

    def __init__(self, master: tk.Tk, headless: bool = False) -> None:
        self.metrics_lock = Lock()
        self.last_metrics: dict[str, dict] = {}
        self.discovery_text = "Discovery not started"
        self.node_refresh_id = None
        super().__init__(master, headless=headless)

    def _build_ui(self) -> None:
        """Build a more readable operator dashboard."""
        self.pack(fill=tk.BOTH, expand=True)
        style = ttk.Style(self.master)
        style.configure(".", font=core.BIG_FONT, padding=5)
        style.configure("Title.TLabel", font=("Helvetica", 18, "bold"))
        style.configure("Section.TLabelframe.Label", font=("Helvetica", 14, "bold"))
        style.configure("Status.TLabel", font=("Helvetica", 14, "bold"), padding=8)
        style.configure("Node.Treeview", rowheight=30, font=("Helvetica", 12))
        style.configure("Node.Treeview.Heading", font=("Helvetica", 12, "bold"))

        # ---------- header ----------
        header = ttk.Frame(self)
        header.pack(fill=tk.X, padx=8, pady=(8, 4))
        ttk.Label(header, text="Immersion Bench Monitor", style="Title.TLabel").pack(side=tk.LEFT)
        self.status_lbl = ttk.Label(
            header,
            text="Idle",
            width=22,
            anchor="center",
            background="white",
            style="Status.TLabel",
        )
        self.status_lbl.pack(side=tk.LEFT, padx=16)
        self.full_btn = ttk.Button(header, text="Full Screen", command=self._toggle_full)
        self.full_btn.pack(side=tk.RIGHT, padx=4)

        # ---------- controls ----------
        controls = ttk.Frame(self)
        controls.pack(fill=tk.X, padx=8, pady=4)

        log_box = ttk.LabelFrame(controls, text="Logging", style="Section.TLabelframe")
        log_box.pack(side=tk.LEFT, fill=tk.Y, padx=(0, 6))
        ttk.Button(log_box, text="Start Log", command=self._start_log).pack(side=tk.LEFT, padx=2)
        ttk.Button(log_box, text="Stop Log", command=self._stop_log).pack(side=tk.LEFT, padx=2)
        ttk.Button(log_box, text="Save XLSX", command=self._ask_write_excel).pack(side=tk.LEFT, padx=2)
        ttk.Button(log_box, text="Clear ALL", command=self._clear_all).pack(side=tk.LEFT, padx=2)

        test_box = ttk.LabelFrame(controls, text="Test sequence", style="Section.TLabelframe")
        test_box.pack(side=tk.LEFT, fill=tk.Y, padx=6)
        self._add_spinner(test_box, "Stress", 30, attr="stress_min")
        self._add_spinner(test_box, "Cooling", 30, attr="cool_min")
        self._add_spinner(test_box, "Wait", 0, attr="wait_min")
        self.start_btn = ttk.Button(test_box, text="Start Stress", command=self._start_sequence)
        self.start_btn.pack(side=tk.LEFT, padx=6)
        self.stop_btn = ttk.Button(test_box, text="Stop", state=tk.DISABLED, command=self._ask_stop_stress)
        self.stop_btn.pack(side=tk.LEFT, padx=2)
        self.skip_btn = ttk.Button(
            test_box,
            text="Skip Cooling",
            state=tk.DISABLED,
            command=self._ask_skip_cooling,
        )
        self.skip_btn.pack(side=tk.LEFT, padx=2)

        node_actions = ttk.LabelFrame(controls, text="Node actions", style="Section.TLabelframe")
        node_actions.pack(side=tk.LEFT, fill=tk.Y, padx=6)
        self.reboot_btn = ttk.Button(node_actions, text="Reboot Nodes", command=self._ask_reboot_nodes)
        self.reboot_btn.pack(side=tk.LEFT, padx=2)

        # ---------- status and discovery ----------
        status_row = ttk.Frame(self)
        status_row.pack(fill=tk.X, padx=8, pady=4)

        cards = ttk.LabelFrame(status_row, text="Bench status", style="Section.TLabelframe")
        cards.pack(side=tk.LEFT, fill=tk.BOTH, expand=True, padx=(0, 6))
        self.hat_banner = ttk.Label(cards, text="MCC-134 HAT: checking", width=26, anchor="center")
        self.node_banner = ttk.Label(cards, text="Workers: scanning", width=34, anchor="center")
        self.discovery_lbl = ttk.Label(cards, text="Discovery: waiting", anchor="w")
        self.hat_banner.pack(side=tk.LEFT, padx=4, pady=4)
        self.node_banner.pack(side=tk.LEFT, padx=4, pady=4)
        self.discovery_lbl.pack(side=tk.LEFT, fill=tk.X, expand=True, padx=8, pady=4)

        events = ttk.LabelFrame(status_row, text="Last events", style="Section.TLabelframe")
        events.pack(side=tk.RIGHT, fill=tk.BOTH, expand=True)
        self.log_labels = [ttk.Label(events, anchor="w") for _ in range(2)]
        for lb in self.log_labels:
            lb.pack(fill=tk.X)

        # ---------- node table ----------
        nodes_box = ttk.LabelFrame(self, text="Discovered workers", style="Section.TLabelframe")
        nodes_box.pack(fill=tk.X, padx=8, pady=4)
        self.node_help_lbl = ttk.Label(
            nodes_box,
            text="If a worker is missing: check power, Ethernet, same subnet, and that bench-worker-agent.service is running.",
            anchor="w",
        )
        self.node_help_lbl.pack(fill=tk.X, padx=4, pady=(0, 4))

        columns = ("state", "node", "ip", "source", "temp", "clock", "cpu", "last_seen")
        self.node_tree = ttk.Treeview(
            nodes_box,
            columns=columns,
            show="headings",
            height=5,
            style="Node.Treeview",
        )
        headings = {
            "state": "State",
            "node": "Node",
            "ip": "IP",
            "source": "Source",
            "temp": "Temp",
            "clock": "Clock",
            "cpu": "CPU",
            "last_seen": "Last seen",
        }
        widths = {
            "state": 90,
            "node": 190,
            "ip": 130,
            "source": 90,
            "temp": 90,
            "clock": 100,
            "cpu": 90,
            "last_seen": 150,
        }
        for col in columns:
            self.node_tree.heading(col, text=headings[col])
            self.node_tree.column(col, width=widths[col], anchor="center")
        self.node_tree.tag_configure("online", background="#dff4df")
        self.node_tree.tag_configure("offline", background="#f5d8d8")
        self.node_tree.tag_configure("waiting", background="#fff1c4")
        self.node_tree.pack(fill=tk.X, padx=4, pady=(0, 4))

        # ---------- plot ----------
        self.fig, self.ax = core.plt.subplots(1, 1, figsize=core.FIGSIZE, dpi=core.PLOT_DPI)
        self.fig.subplots_adjust(top=0.94, left=0.06, right=core.RIGHT_MARGIN, bottom=0.145)
        self.canvas = core.FigureCanvasTkAgg(self.fig, master=self)
        self.canvas.get_tk_widget().pack(fill=tk.BOTH, expand=True, padx=8, pady=4)

        # ---------- zoom ----------
        zoom = ttk.Frame(self)
        zoom.pack(fill=tk.X, padx=8, pady=(0, 6))
        ttk.Label(zoom, text="Temperature scale").pack(side=tk.LEFT, padx=(0, 4))
        ttk.Button(zoom, text="Zoom in", command=lambda: self._zoom(-10)).pack(side=tk.LEFT, padx=2)
        ttk.Button(zoom, text="Zoom out", command=lambda: self._zoom(10)).pack(side=tk.LEFT, padx=2)

        self.node_refresh_id = self.after(1000, self._refresh_node_table)

    def _show_connection_status(self) -> None:
        """Show clear high-level status without blocking the UI on SSH probes."""
        if self.headless:
            return

        if core.hat_list(core.HatIDs.MCC_134):
            self.hat_banner.configure(text="MCC-134 HAT: OK", background="green", foreground="white")
        else:
            self.hat_banner.configure(text="MCC-134 HAT: not found", background="red", foreground="white")

        nodes = sorted(set(getattr(self, "node_ips", [])))
        online = self._online_node_count()
        if not nodes:
            if core.DISCOVERY_CIDR:
                text = f"Workers: scanning {core.DISCOVERY_CIDR}"
            else:
                text = "Workers: no discovery CIDR"
            self.node_banner.configure(text=text, background="orange", foreground="black")
            return

        if online == len(nodes):
            self.node_banner.configure(
                text=f"Workers: {online}/{len(nodes)} online",
                background="green",
                foreground="white",
            )
        elif online == 0:
            self.node_banner.configure(
                text=f"Workers: 0/{len(nodes)} online",
                background="red",
                foreground="white",
            )
        else:
            self.node_banner.configure(
                text=f"Workers: {online}/{len(nodes)} online",
                background="orange",
                foreground="black",
            )

    def _online_node_count(self) -> int:
        now = datetime.now()
        with self.metrics_lock:
            return sum(
                1
                for ip in set(getattr(self, "node_ips", []))
                if self.last_metrics.get(ip, {}).get("online")
                and (now - self.last_metrics[ip]["last_seen"]).total_seconds() <= self.ONLINE_AFTER_SECONDS
            )

    def _set_discovery_text(self, text: str) -> None:
        self.discovery_text = text
        if self.headless or not hasattr(self, "discovery_lbl"):
            return
        self.discovery_lbl.configure(text=f"Discovery: {text}")

    def _schedule_discovery_text(self, text: str) -> None:
        self.discovery_text = text
        if not self.headless and hasattr(self, "discovery_lbl"):
            self.after(0, lambda t=text: self._set_discovery_text(t))

    def _discover_agent_nodes(self) -> dict[str, core.WorkerInfo]:
        """Wrap discovery with operator-visible status messages."""
        if not core.DISCOVERY_CIDR:
            self._schedule_discovery_text("disabled. Start with --discovery-cidr 10.50.0.0/24")
            return {}
        self._schedule_discovery_text(f"scanning {core.DISCOVERY_CIDR} on port {core.AGENT_PORT}")
        started = datetime.now()
        found = super()._discover_agent_nodes()
        elapsed = (datetime.now() - started).total_seconds()
        if found:
            self._schedule_discovery_text(
                f"found {len(found)} worker agent(s) in {elapsed:.1f}s. Last scan {datetime.now():%H:%M:%S}"
            )
        else:
            self._schedule_discovery_text(
                f"no worker agents found in {core.DISCOVERY_CIDR}. Last scan {datetime.now():%H:%M:%S}"
            )
        return found

    def _discover_and_add_nodes(self) -> None:
        before = set(getattr(self, "node_ips", []))
        super()._discover_and_add_nodes()
        after = set(getattr(self, "node_ips", []))
        added = after - before
        if added:
            self._schedule_discovery_text(
                f"added {len(added)} new worker(s): {', '.join(sorted(added))}"
            )

    def _poll_node(self, ip: str) -> None:
        """Poll one worker and retain operator-facing node status."""
        while not self.stop.is_set():
            temp, clock, usage = self._read_stats(ip)
            now = datetime.now()
            info = self.worker_info.get(ip)
            label = info.label if info else ip

            if temp == 0:
                with self.metrics_lock:
                    previous = self.last_metrics.get(ip, {})
                    self.last_metrics[ip] = {
                        **previous,
                        "ip": ip,
                        "label": label,
                        "online": False,
                        "source": info.source if info else "unknown",
                    }
                self.stop.wait(1)
                continue

            with self.metrics_lock:
                self.last_metrics[ip] = {
                    "ip": ip,
                    "label": label,
                    "temp": temp,
                    "clock": clock,
                    "usage": usage,
                    "last_seen": now,
                    "online": True,
                    "source": info.source if info else "unknown",
                }

            row = pd.Series({"Time": now, "Node": label, "Temp": temp, "Clock": clock, "Usage": usage})
            self._save_raw_soc(row)
            with self.cl_lock:
                self.cl_df = pd.concat([self.cl_df, row.to_frame().T], ignore_index=True)
                self._trim(self.cl_df)
            self.stop.wait(1)

    def _refresh_node_table(self) -> None:
        """Refresh the node table from cached metrics in the UI thread."""
        if self.stop.is_set() or self.headless:
            return

        self._show_connection_status()
        nodes = sorted(set(getattr(self, "node_ips", [])))
        rows = []
        now = datetime.now()
        with self.metrics_lock:
            metrics = {ip: dict(v) for ip, v in self.last_metrics.items()}

        for ip in nodes:
            info = self.worker_info.get(ip)
            m = metrics.get(ip, {})
            label = m.get("label") or (info.label if info else ip)
            source = m.get("source") or (info.source if info else "unknown")
            last_seen = m.get("last_seen")
            online = bool(m.get("online")) and last_seen and (now - last_seen).total_seconds() <= self.ONLINE_AFTER_SECONDS
            if online:
                state = "Online"
                tag = "online"
                age = f"{int((now - last_seen).total_seconds())}s ago"
            elif last_seen:
                state = "Offline"
                tag = "offline"
                age = last_seen.strftime("%H:%M:%S")
            else:
                state = "Waiting"
                tag = "waiting"
                age = "no data yet"

            temp = f"{float(m['temp']):.1f} °C" if "temp" in m and m.get("temp") is not None else "-"
            clock = f"{float(m['clock'])/1000:.2f} GHz" if "clock" in m and m.get("clock") else "-"
            usage = f"{float(m['usage']):.0f} %" if m.get("usage") is not None else "-"
            rows.append((ip, tag, (state, label, ip, source, temp, clock, usage, age)))

        existing = set(self.node_tree.get_children(""))
        wanted = {ip for ip, _, _ in rows}
        for item in existing - wanted:
            self.node_tree.delete(item)
        for ip, tag, values in rows:
            if ip in existing:
                self.node_tree.item(ip, values=values, tags=(tag,))
            else:
                self.node_tree.insert("", tk.END, iid=ip, values=values, tags=(tag,))

        if not rows:
            if core.DISCOVERY_CIDR:
                self.node_help_lbl.configure(
                    text=f"No workers yet. Scanning {core.DISCOVERY_CIDR}:{core.AGENT_PORT}. Check worker power, Ethernet, and bench-worker-agent.service."
                )
            else:
                self.node_help_lbl.configure(
                    text="No workers configured. Start with --discovery-cidr 10.50.0.0/24 or provide /home/motul/nodes_ips."
                )
        else:
            online = self._online_node_count()
            self.node_help_lbl.configure(
                text=f"{online}/{len(rows)} workers online. Missing workers usually mean power, cable, subnet, or agent service issue."
            )

        self.node_refresh_id = self.after(1000, self._refresh_node_table)

    def _refresh_plot(self):
        super()._refresh_plot()
        if not self.headless:
            try:
                self.ax.set_title("Temperature history")
            except Exception:
                pass

    def close(self) -> None:
        if hasattr(self, "node_refresh_id") and self.node_refresh_id:
            try:
                self.after_cancel(self.node_refresh_id)
            except tk.TclError:
                pass
        super().close()


def main() -> None:
    parser = argparse.ArgumentParser(description="Readable immersion bench dashboard")
    parser.add_argument("--nodes-file", default=core.DEFAULT_NODES_FILE, help="File with SSH fallback node IPs")
    parser.add_argument("--csv-dir", default=core.DEFAULT_CSV_DIR, help="Directory for CSV output")
    parser.add_argument("--headless", action="store_true", help="Run without showing the GUI")
    parser.add_argument("--agent-port", type=int, default=core.DEFAULT_AGENT_PORT, help="HTTP worker-agent port")
    parser.add_argument(
        "--discovery-cidr",
        default=core.DEFAULT_DISCOVERY_CIDR,
        help="CIDR range to scan for worker agents, e.g. 10.50.0.0/24",
    )
    args = parser.parse_args()

    core.NODES_FILE = args.nodes_file
    core.AGENT_PORT = args.agent_port
    core.DISCOVERY_CIDR = args.discovery_cidr
    core.CSV_DIR = Path(args.csv_dir)
    core.CSV_DIR.mkdir(parents=True, exist_ok=True)
    core.RAW_SOC = core.CSV_DIR / "raw_soc.csv"
    core.RAW_FLUID = core.CSV_DIR / "raw_fluid.csv"

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s: %(message)s",
        handlers=[
            logging.FileHandler(core.CSV_DIR / "monitor.log"),
            logging.StreamHandler(sys.stdout),
        ],
    )

    root = tk.Tk()
    root.title("Immersion Bench Monitor")
    root.geometry("1280x900")
    if args.headless:
        root.withdraw()
    ui = ReadableMonitor(root, headless=args.headless)

    def _close() -> None:
        ui.close()
        root.destroy()

    root.protocol("WM_DELETE_WINDOW", _close)
    try:
        root.mainloop()
    finally:
        ui.close()


if __name__ == "__main__":
    main()

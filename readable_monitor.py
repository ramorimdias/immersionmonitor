#!/usr/bin/env python3
"""Operator-friendly dashboard entrypoint for the immersion bench.

This module reuses the acquisition and control logic from ``dual_monitor.py``
but separates the UI into a full-size graph tab and a configuration/workers
tab. Unlike ``dual_monitor.py``, this dashboard is discovery-only by default:
it does not load ``/home/motul/nodes_ips`` unless ``--nodes-file`` is explicitly
provided.
"""

from __future__ import annotations

import argparse
import logging
import sys
from datetime import datetime
from pathlib import Path
from threading import Lock

import tkinter as tk
from tkinter import ttk

import dual_monitor as core

DEFAULT_READABLE_DISCOVERY_CIDR = "10.50.0.0/24"


class ReadableMonitor(core.UnifiedMonitor):
    """A clearer tabbed dashboard wrapped around ``dual_monitor.UnifiedMonitor``."""

    ONLINE_AFTER_SECONDS = 6

    def __init__(self, master: tk.Tk, headless: bool = False) -> None:
        self.metrics_lock = Lock()
        self.last_metrics: dict[str, dict] = {}
        self.discovery_text = "Discovery not started"
        self.node_refresh_id = None
        self.summary_vars: dict[str, tk.StringVar] = {}
        super().__init__(master, headless=headless)

    def _set_summary(self, key: str, text: str) -> None:
        var = self.summary_vars.get(key)
        if var is not None:
            var.set(text)

    @staticmethod
    def _card(parent: tk.Widget, title: str, value_var: tk.StringVar, accent: str = "") -> ttk.Frame:
        card = ttk.Frame(parent, style="Card.TFrame", padding=(12, 10))
        if accent:
            tk.Frame(card, width=4, bg=accent, highlightthickness=0).pack(
                side=tk.LEFT, fill=tk.Y, padx=(0, 10)
            )
        body = ttk.Frame(card, style="Card.TFrame")
        body.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        ttk.Label(body, text=title, style="CardTitle.TLabel").pack(anchor="w")
        ttk.Label(
            body,
            textvariable=value_var,
            style="CardValue.TLabel",
            wraplength=280,
            justify="left",
        ).pack(anchor="w", pady=(4, 0))
        return card

    def _build_ui(self) -> None:
        """Build a tabbed dashboard so the graph is not compressed by controls."""
        self.pack(fill=tk.BOTH, expand=True)
        style = ttk.Style(self.master)
        try:
            style.theme_use("clam")
        except tk.TclError:
            pass
        style.configure(".", font=("Helvetica", 11), padding=4)
        style.configure("Page.TFrame", background="#f5f7fb")
        style.configure("Hero.TFrame", background="#0f172a")
        style.configure("HeroTitle.TLabel", background="#0f172a", foreground="#f8fafc", font=("Helvetica", 21, "bold"))
        style.configure("HeroSub.TLabel", background="#0f172a", foreground="#cbd5e1", font=("Helvetica", 11))
        style.configure("HeroChip.TLabel", background="#1e293b", foreground="#e2e8f0", font=("Helvetica", 10, "bold"), padding=(10, 4))
        style.configure("Card.TFrame", background="#ffffff")
        style.configure("CardTitle.TLabel", background="#ffffff", foreground="#64748b", font=("Helvetica", 10, "bold"))
        style.configure("CardValue.TLabel", background="#ffffff", foreground="#0f172a", font=("Helvetica", 13, "bold"))
        style.configure("Section.TLabelframe", background="#ffffff", relief="solid", borderwidth=1, padding=8)
        style.configure("Section.TLabelframe.Label", font=("Helvetica", 13, "bold"), foreground="#334155")
        style.configure(
            "Status.TLabel",
            background="#ffffff",
            foreground="#0f172a",
            font=("Helvetica", 13, "bold"),
            padding=10,
        )
        style.configure("Title.TLabel", background="#f5f7fb", foreground="#0f172a", font=("Helvetica", 18, "bold"))
        style.configure("Node.Treeview", rowheight=30, font=("Helvetica", 12))
        style.configure("Node.Treeview.Heading", font=("Helvetica", 12, "bold"))
        style.configure("Action.TButton", font=("Helvetica", 10, "bold"), padding=(10, 6))
        self.master.configure(background="#f5f7fb")
        self.configure(style="Page.TFrame")

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

        self.notebook = ttk.Notebook(self)
        self.notebook.pack(fill=tk.BOTH, expand=True, padx=8, pady=(0, 8))

        self.graph_tab = ttk.Frame(self.notebook)
        self.config_tab = ttk.Frame(self.notebook)
        self.graph_tab.configure(style="Page.TFrame")
        self.config_tab.configure(style="Page.TFrame")
        self.notebook.add(self.graph_tab, text="Graph")
        self.notebook.add(self.config_tab, text="Configuration / Workers")

        self.summary_vars = {
            "hat": tk.StringVar(master=self.master, value="Checking hardware"),
            "workers": tk.StringVar(master=self.master, value="Scanning workers"),
            "discovery": tk.StringVar(master=self.master, value=self.discovery_text),
            "activity": tk.StringVar(master=self.master, value="Idle"),
        }
        self._build_graph_tab()
        self._build_config_tab()
        self.node_refresh_id = self.after(2000, self._refresh_node_table)

    def _build_graph_tab(self) -> None:
        """Graph tab gets almost the full window."""
        hero = tk.Frame(self.graph_tab, bg="#0f172a", highlightthickness=0)
        hero.pack(fill=tk.X, padx=8, pady=(8, 6))
        hero_left = tk.Frame(hero, bg="#0f172a", highlightthickness=0)
        hero_left.pack(side=tk.LEFT, fill=tk.BOTH, expand=True, padx=18, pady=16)
        tk.Label(
            hero_left,
            text="Immersion Bench Monitor",
            bg="#0f172a",
            fg="#f8fafc",
            font=("Helvetica", 22, "bold"),
        ).pack(anchor="w")
        tk.Label(
            hero_left,
            text="A polished live view for discovery, stress runs, and temperature history.",
            bg="#0f172a",
            fg="#cbd5e1",
            font=("Helvetica", 11),
        ).pack(anchor="w", pady=(6, 0))
        chip_row = tk.Frame(hero_left, bg="#0f172a")
        chip_row.pack(anchor="w", pady=(12, 0))
        for label, key in (
            ("Hardware", "hat"),
            ("Workers", "workers"),
            ("Discovery", "discovery"),
            ("Activity", "activity"),
        ):
            chip = tk.Label(
                chip_row,
                textvariable=self.summary_vars[key],
                bg="#1e293b",
                fg="#e2e8f0",
                font=("Helvetica", 10, "bold"),
                padx=12,
                pady=5,
                relief="flat",
            )
            chip.pack(side=tk.LEFT, padx=(0, 8))

        hero_right = tk.Frame(hero, bg="#0f172a", highlightthickness=0)
        hero_right.pack(side=tk.RIGHT, padx=18, pady=16)
        ttk.Label(
            hero_right,
            text="Quick actions",
            style="HeroChip.TLabel",
        ).pack(anchor="e")
        ttk.Label(
            hero_right,
            text="Keep the plot clean, keep the controls close.",
            style="HeroSub.TLabel",
        ).pack(anchor="e", pady=(8, 0))

        quick = ttk.Frame(self.graph_tab, style="Page.TFrame")
        quick.pack(fill=tk.X, padx=8, pady=(0, 6))
        ttk.Button(quick, text="Start Log", style="Action.TButton", command=self._start_log).pack(
            side=tk.LEFT, padx=(0, 6)
        )
        ttk.Button(quick, text="Stop Log", style="Action.TButton", command=self._stop_log).pack(
            side=tk.LEFT, padx=(0, 12)
        )
        self.start_btn = ttk.Button(quick, text="Start Stress", style="Action.TButton", command=self._start_sequence)
        self.start_btn.pack(side=tk.LEFT, padx=(0, 6))
        self.stop_btn = ttk.Button(quick, text="Stop", state=tk.DISABLED, style="Action.TButton", command=self._ask_stop_stress)
        self.stop_btn.pack(side=tk.LEFT)
        tk.Label(
            quick,
            text="Advanced timing, discovery, and worker management live on the second tab.",
            bg="#f5f7fb",
            fg="#64748b",
        ).pack(side=tk.RIGHT, fill=tk.X, expand=True, padx=12)

        cards = ttk.Frame(self.graph_tab, style="Page.TFrame")
        cards.pack(fill=tk.X, padx=8, pady=(0, 6))
        self.graph_card_1 = self._card(cards, "MCC-134", self.summary_vars["hat"], "#06b6d4")
        self.graph_card_2 = self._card(cards, "Workers", self.summary_vars["workers"], "#22c55e")
        self.graph_card_3 = self._card(cards, "Discovery", self.summary_vars["discovery"], "#f59e0b")
        self.graph_card_4 = self._card(cards, "Run state", self.summary_vars["activity"], "#8b5cf6")
        for card in (self.graph_card_1, self.graph_card_2, self.graph_card_3, self.graph_card_4):
            card.pack(side=tk.LEFT, fill=tk.X, expand=True, padx=(0, 6))
        self.graph_card_4.pack_configure(padx=0)

        self.fig, self.ax = core.plt.subplots(1, 1, figsize=core.FIGSIZE, dpi=core.PLOT_DPI)
        self.fig.patch.set_facecolor("#ffffff")
        self.fig.subplots_adjust(top=0.96, left=0.06, right=core.RIGHT_MARGIN, bottom=0.13)
        self.canvas = core.FigureCanvasTkAgg(self.fig, master=self.graph_tab)
        self.canvas.get_tk_widget().pack(fill=tk.BOTH, expand=True, padx=6, pady=4)

        zoom = ttk.Frame(self.graph_tab)
        zoom.pack(fill=tk.X, padx=6, pady=(0, 6))
        ttk.Label(zoom, text="Temperature scale").pack(side=tk.LEFT, padx=(0, 4))
        ttk.Button(zoom, text="Zoom in", command=lambda: self._zoom(-10)).pack(side=tk.LEFT, padx=2)
        ttk.Button(zoom, text="Zoom out", command=lambda: self._zoom(10)).pack(side=tk.LEFT, padx=2)

    def _build_config_tab(self) -> None:
        """Configuration tab contains controls, discovery state, and worker details."""
        main = ttk.Frame(self.config_tab, style="Page.TFrame")
        main.pack(fill=tk.BOTH, expand=True, padx=8, pady=8)
        main.columnconfigure(0, weight=0)
        main.columnconfigure(1, weight=1)
        main.rowconfigure(1, weight=1)

        sidebar = ttk.Frame(main, style="Page.TFrame")
        sidebar.grid(row=0, column=0, rowspan=2, sticky="nsw", padx=(0, 8))

        workspace = ttk.Frame(main, style="Page.TFrame")
        workspace.grid(row=0, column=1, rowspan=2, sticky="nsew")
        workspace.rowconfigure(2, weight=1)
        workspace.columnconfigure(0, weight=1)

        maintenance = ttk.LabelFrame(sidebar, text="Maintenance", style="Section.TLabelframe")
        maintenance.pack(fill=tk.X, pady=(0, 8))
        ttk.Button(maintenance, text="Save XLSX", style="Action.TButton", command=self._ask_write_excel).pack(
            fill=tk.X, pady=(0, 6)
        )
        ttk.Button(maintenance, text="Clear ALL", style="Action.TButton", command=self._clear_all).pack(fill=tk.X)

        sequence_box = ttk.LabelFrame(sidebar, text="Sequence settings", style="Section.TLabelframe")
        sequence_box.pack(fill=tk.X, pady=(0, 8))
        self._add_spinner(sequence_box, "Stress", 30, attr="stress_min")
        self._add_spinner(sequence_box, "Cooling", 30, attr="cool_min")
        self._add_spinner(sequence_box, "Wait", 0, attr="wait_min")

        node_actions = ttk.LabelFrame(sidebar, text="Node actions", style="Section.TLabelframe")
        node_actions.pack(fill=tk.X)
        self.reboot_btn = ttk.Button(node_actions, text="Reboot Nodes", style="Action.TButton", command=self._ask_reboot_nodes)
        self.reboot_btn.pack(fill=tk.X)

        cards_row = ttk.Frame(workspace, style="Page.TFrame")
        cards_row.grid(row=0, column=0, sticky="ew", pady=(0, 8))
        for idx in range(3):
            cards_row.columnconfigure(idx, weight=1)

        self.hat_banner = ttk.Label(cards_row, text="MCC-134 HAT: checking", anchor="center", style="Status.TLabel")
        self.node_banner = ttk.Label(cards_row, text="Workers: scanning", anchor="center", style="Status.TLabel")
        self.discovery_lbl = ttk.Label(cards_row, text="Discovery: waiting", anchor="center", style="Status.TLabel")
        self.hat_banner.grid(row=0, column=0, sticky="ew", padx=(0, 6))
        self.node_banner.grid(row=0, column=1, sticky="ew", padx=(0, 6))
        self.discovery_lbl.grid(row=0, column=2, sticky="ew")

        events = ttk.LabelFrame(workspace, text="Last events", style="Section.TLabelframe")
        events.grid(row=1, column=0, sticky="ew", pady=(0, 8))
        self.log_labels = [ttk.Label(events, anchor="w") for _ in range(2)]
        for lb in self.log_labels:
            lb.pack(fill=tk.X)

        nodes_box = ttk.LabelFrame(workspace, text="Discovered workers", style="Section.TLabelframe")
        nodes_box.grid(row=2, column=0, sticky="nsew")
        workspace.rowconfigure(2, weight=1)
        self.node_help_lbl = ttk.Label(
            nodes_box,
            text="If a worker is missing: check power, Ethernet, same subnet, and that bench-worker-agent.service is running.",
            anchor="w",
            style="CardTitle.TLabel",
        )
        self.node_help_lbl.pack(fill=tk.X, padx=4, pady=(0, 4))

        columns = ("state", "node", "ip", "source", "temp", "clock", "cpu", "last_seen")
        self.node_tree = ttk.Treeview(
            nodes_box,
            columns=columns,
            show="headings",
            height=12,
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
            "node": 220,
            "ip": 140,
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
        self.node_tree.pack(fill=tk.BOTH, expand=True, padx=4, pady=(0, 4))

    def _load_nodes(self):
        """Discovery-only by default; static SSH nodes are opt-in for this dashboard."""
        if not getattr(core, "NODES_FILE", ""):
            logging.info("No SSH fallback nodes file configured; using worker-agent discovery only")
            return []
        return super()._load_nodes()

    def _show_connection_status(self) -> None:
        """Show clear high-level status without blocking the UI on SSH probes."""
        if self.headless:
            return

        if core.hat_list(core.HatIDs.MCC_134):
            self.hat_banner.configure(text="MCC-134 HAT: OK", background="green", foreground="white")
            self._set_summary("hat", "MCC-134: ready")
        else:
            self.hat_banner.configure(text="MCC-134 HAT: not found", background="red", foreground="white")
            self._set_summary("hat", "MCC-134: not found")

        nodes = sorted(set(getattr(self, "node_ips", [])))
        online = self._online_node_count()
        if not nodes:
            if core.DISCOVERY_CIDR:
                text = f"Workers: scanning {core.DISCOVERY_CIDR}"
                self._set_summary("discovery", f"Scanning {core.DISCOVERY_CIDR} on port {core.AGENT_PORT}")
            else:
                text = "Workers: discovery disabled"
                self._set_summary("discovery", "Discovery disabled")
            self.node_banner.configure(text=text, background="orange", foreground="black")
            self._set_summary("workers", "No workers yet")
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

        self._set_summary("workers", f"{online}/{len(nodes)} online")
        if core.DISCOVERY_CIDR:
            self._set_summary("discovery", f"Scanning {core.DISCOVERY_CIDR} on port {core.AGENT_PORT}")
        else:
            self._set_summary("discovery", "Discovery disabled")

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
        self._set_summary("discovery", f"Discovery: {text}")
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

            row = {"Time": now, "Node": label, "Temp": temp, "Clock": clock, "Usage": usage}
            self._save_raw_soc(row)
            with self.cl_lock:
                self._append_live_row(self.cl_df, row)
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

        online_count = 0
        for ip in nodes:
            info = self.worker_info.get(ip)
            m = metrics.get(ip, {})
            label = m.get("label") or (info.label if info else ip)
            source = m.get("source") or (info.source if info else "unknown")
            last_seen = m.get("last_seen")
            online = bool(m.get("online")) and last_seen and (now - last_seen).total_seconds() <= self.ONLINE_AFTER_SECONDS
            if online:
                online_count += 1
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
                    text="No workers configured. Start with --discovery-cidr 10.50.0.0/24. Static SSH nodes are disabled unless --nodes-file is explicitly provided."
                )
        else:
            self.node_help_lbl.configure(
                text=f"{online_count}/{len(rows)} workers online. Missing workers usually mean power, cable, subnet, or agent service issue."
            )

        self.node_refresh_id = self.after(2000, self._refresh_node_table)

    def _refresh_plot(self):
        super()._refresh_plot()
        if not self.headless:
            try:
                self.ax.set_title("Temperature history")
            except Exception:
                pass

    def _tick(self) -> None:
        super()._tick()
        if hasattr(self, "status_lbl"):
            self._set_summary("activity", self.status_lbl.cget("text"))

    def close(self) -> None:
        if hasattr(self, "node_refresh_id") and self.node_refresh_id:
            try:
                self.after_cancel(self.node_refresh_id)
            except tk.TclError:
                pass
        super().close()


def main() -> None:
    parser = argparse.ArgumentParser(description="Readable immersion bench dashboard")
    parser.add_argument(
        "--nodes-file",
        default="",
        help="Optional SSH fallback node file. Disabled by default in this dashboard.",
    )
    parser.add_argument("--csv-dir", default=core.DEFAULT_CSV_DIR, help="Directory for CSV output")
    parser.add_argument("--headless", action="store_true", help="Run without showing the GUI")
    parser.add_argument("--agent-port", type=int, default=core.DEFAULT_AGENT_PORT, help="HTTP worker-agent port")
    parser.add_argument(
        "--discovery-cidr",
        default=DEFAULT_READABLE_DISCOVERY_CIDR,
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

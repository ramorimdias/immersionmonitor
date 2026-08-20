#!/usr/bin/env python3
"""Clean operator UI for the immersion bench.

Acquisition, worker discovery, stress control, logging, MCC-134 access, and
export behavior stay in ``dual_monitor.py``. This module is a new presentation
and controller layer built from scratch around that proven bench backend.
"""

from __future__ import annotations

import argparse
import logging
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path
from threading import Lock

import pandas as pd
import tkinter as tk
from tkinter import ttk

import dual_monitor as core
from monitor_ui import Card, DurationControl, FONT, Palette, PillLabel, StatusCard, action_button, configure_ttk

DEFAULT_READABLE_DISCOVERY_CIDR = "192.168.50.0/24"
ONLINE_AFTER_SECONDS = 6


class CleanMonitor(core.UnifiedMonitor):
    """Modern operator shell using the existing bench backend."""

    def __init__(self, master: tk.Tk, headless: bool = False) -> None:
        self.metrics_lock = Lock()
        self.last_metrics: dict[str, dict] = {}
        self.discovery_text = "Waiting for first scan"
        self.node_refresh_id = None
        self.active_page = "live"
        super().__init__(master, headless=headless)

    # ------------------------------------------------------------------
    # UI construction
    # ------------------------------------------------------------------
    def _build_ui(self) -> None:
        configure_ttk(self.master)
        self.master.configure(bg=Palette.BG)
        self.master.option_add("*Font", (FONT, 10))

        shell = tk.Frame(self, bg=Palette.BG)
        shell.pack(fill=tk.BOTH, expand=True)

        self._build_header(shell)
        self._build_navigation(shell)

        self.page_host = tk.Frame(shell, bg=Palette.BG)
        self.page_host.pack(fill=tk.BOTH, expand=True, padx=18, pady=(0, 18))
        self.page_host.grid_rowconfigure(0, weight=1)
        self.page_host.grid_columnconfigure(0, weight=1)

        self.live_page = tk.Frame(self.page_host, bg=Palette.BG)
        self.workers_page = tk.Frame(self.page_host, bg=Palette.BG)
        for page in (self.live_page, self.workers_page):
            page.grid(row=0, column=0, sticky="nsew")

        self._build_live_page()
        self._build_workers_page()
        self._show_page("live")

        self.node_refresh_id = self.after(1200, self._refresh_node_table)

    def _build_header(self, master: tk.Misc) -> None:
        header = tk.Frame(master, bg=Palette.BG)
        header.pack(fill=tk.X, padx=18, pady=(14, 8))

        titles = tk.Frame(header, bg=Palette.BG)
        titles.pack(side=tk.LEFT)
        tk.Label(
            titles,
            text="IMMERSION BENCH",
            bg=Palette.BG,
            fg=Palette.ACCENT,
            font=(FONT, 9, "bold"),
        ).pack(anchor="w")
        tk.Label(
            titles,
            text="Thermal Monitor",
            bg=Palette.BG,
            fg=Palette.TEXT,
            font=(FONT, 21, "bold"),
        ).pack(anchor="w")

        actions = tk.Frame(header, bg=Palette.BG)
        actions.pack(side=tk.RIGHT)
        self.status_lbl = PillLabel(actions, "Idle")
        self.status_lbl.pack(side=tk.LEFT, padx=(0, 8))
        self.full_btn = action_button(actions, "Full screen", self._toggle_full)
        self.full_btn.pack(side=tk.LEFT)

    def _build_navigation(self, master: tk.Misc) -> None:
        nav = tk.Frame(master, bg=Palette.BG)
        nav.pack(fill=tk.X, padx=18, pady=(0, 10))

        self.live_nav = action_button(nav, "Live monitor", lambda: self._show_page("live"))
        self.live_nav.pack(side=tk.LEFT, padx=(0, 6))
        self.workers_nav = action_button(nav, "Workers & setup", lambda: self._show_page("workers"))
        self.workers_nav.pack(side=tk.LEFT)

        self.network_lbl = tk.Label(
            nav,
            text=f"Discovery {core.DISCOVERY_CIDR or DEFAULT_READABLE_DISCOVERY_CIDR}  •  agent :{core.AGENT_PORT}",
            bg=Palette.BG,
            fg=Palette.MUTED,
            font=(FONT, 9),
        )
        self.network_lbl.pack(side=tk.RIGHT, pady=7)

    def _show_page(self, name: str) -> None:
        self.active_page = name
        page = self.live_page if name == "live" else self.workers_page
        page.tkraise()
        selected = (Palette.TEXT, Palette.SURFACE)
        normal = (Palette.MUTED, Palette.BG)
        self.live_nav.configure(
            bg=selected[1] if name == "live" else normal[1],
            fg=selected[0] if name == "live" else normal[0],
        )
        self.workers_nav.configure(
            bg=selected[1] if name == "workers" else normal[1],
            fg=selected[0] if name == "workers" else normal[0],
        )

    def _build_live_page(self) -> None:
        self.live_page.grid_columnconfigure(0, weight=1)
        self.live_page.grid_rowconfigure(1, weight=1)

        summary = tk.Frame(self.live_page, bg=Palette.BG)
        summary.grid(row=0, column=0, sticky="ew", pady=(0, 10))
        for i in range(4):
            summary.grid_columnconfigure(i, weight=1, uniform="summary")

        self.phase_card = StatusCard(summary, "TEST STATE", "Idle", "Ready")
        self.worker_card = StatusCard(summary, "WORKERS", "Scanning", "Auto discovery")
        self.hat_card = StatusCard(summary, "THERMOCOUPLES", "Checking", "MCC-134")
        self.record_card = StatusCard(summary, "RECORDING", "Off", "No active log")
        for i, card in enumerate((self.phase_card, self.worker_card, self.hat_card, self.record_card)):
            card.grid(row=0, column=i, sticky="ew", padx=(0 if i == 0 else 5, 0 if i == 3 else 5))

        body = tk.Frame(self.live_page, bg=Palette.BG)
        body.grid(row=1, column=0, sticky="nsew")
        body.grid_rowconfigure(0, weight=1)
        body.grid_columnconfigure(0, weight=1)

        plot_card = Card(body)
        plot_card.grid(row=0, column=0, sticky="nsew", padx=(0, 10))
        plot_card.grid_rowconfigure(1, weight=1)
        plot_card.grid_columnconfigure(0, weight=1)

        plot_head = tk.Frame(plot_card, bg=Palette.SURFACE)
        plot_head.grid(row=0, column=0, sticky="ew", padx=16, pady=(12, 4))
        tk.Label(plot_head, text="Temperature history", bg=Palette.SURFACE, fg=Palette.TEXT, font=(FONT, 12, "bold")).pack(side=tk.LEFT)
        tk.Label(plot_head, text="Workers + MCC-134 channels", bg=Palette.SURFACE, fg=Palette.MUTED, font=(FONT, 9)).pack(side=tk.LEFT, padx=(10, 0), pady=2)

        view_actions = tk.Frame(plot_head, bg=Palette.SURFACE)
        view_actions.pack(side=tk.RIGHT)
        action_button(view_actions, "Tighter", lambda: self._zoom(-10)).pack(side=tk.LEFT, padx=(0, 5))
        action_button(view_actions, "Wider", lambda: self._zoom(10)).pack(side=tk.LEFT, padx=(0, 5))
        action_button(view_actions, "Auto", self._reset_zoom).pack(side=tk.LEFT)

        self.fig, self.ax = core.plt.subplots(1, 1, figsize=(12, 7), dpi=core.PLOT_DPI)
        self.fig.patch.set_facecolor(Palette.SURFACE)
        self.fig.subplots_adjust(top=0.96, left=0.075, right=0.78, bottom=0.13)
        self.canvas = core.FigureCanvasTkAgg(self.fig, master=plot_card)
        self.canvas.get_tk_widget().grid(row=1, column=0, sticky="nsew", padx=8, pady=(0, 8))

        controls = tk.Frame(body, bg=Palette.BG, width=310)
        controls.grid(row=0, column=1, sticky="ns")
        controls.grid_propagate(False)
        self._build_sequence_panel(controls)
        self._build_recording_panel(controls)
        self._build_events_panel(controls)

    def _build_sequence_panel(self, master: tk.Misc) -> None:
        card = Card(master, title="Test sequence")
        card.pack(fill=tk.X, pady=(0, 10))

        tk.Label(
            card,
            text="WAIT → STRESS → COOLING",
            bg=Palette.SURFACE,
            fg=Palette.MUTED,
            font=(FONT, 9),
        ).pack(anchor="w", padx=16, pady=(0, 10))

        duration_row = tk.Frame(card, bg=Palette.SURFACE)
        duration_row.pack(fill=tk.X, padx=16)
        for col in range(3):
            duration_row.grid_columnconfigure(col, weight=1, uniform="duration")

        wait = DurationControl(duration_row, "Wait min", 0)
        stress = DurationControl(duration_row, "Stress min", 30)
        cool = DurationControl(duration_row, "Cooling min", 30)
        wait.grid(row=0, column=0, sticky="ew", padx=(0, 5))
        stress.grid(row=0, column=1, sticky="ew", padx=5)
        cool.grid(row=0, column=2, sticky="ew", padx=(5, 0))
        self.wait_min = wait.var
        self.stress_min = stress.var
        self.cool_min = cool.var

        self.start_btn = action_button(card, "Start sequence", self._start_sequence, kind="primary")
        self.start_btn.pack(fill=tk.X, padx=16, pady=(14, 6))
        self.stop_btn = action_button(card, "Stop stress", self._ask_stop_stress, kind="danger")
        self.stop_btn.configure(state=tk.DISABLED)
        self.stop_btn.pack(fill=tk.X, padx=16, pady=(0, 6))
        self.skip_btn = action_button(card, "Skip cooling", self._ask_skip_cooling)
        self.skip_btn.configure(state=tk.DISABLED)
        self.skip_btn.pack(fill=tk.X, padx=16, pady=(0, 16))

    def _build_recording_panel(self, master: tk.Misc) -> None:
        card = Card(master, title="Data")
        card.pack(fill=tk.X, pady=(0, 10))

        row = tk.Frame(card, bg=Palette.SURFACE)
        row.pack(fill=tk.X, padx=16, pady=(0, 8))
        action_button(row, "Start log", self._start_log, kind="success").pack(side=tk.LEFT, fill=tk.X, expand=True, padx=(0, 4))
        action_button(row, "Stop log", self._stop_log).pack(side=tk.LEFT, fill=tk.X, expand=True, padx=(4, 0))
        action_button(card, "Save XLSX", self._ask_write_excel).pack(fill=tk.X, padx=16, pady=(0, 16))

    def _build_events_panel(self, master: tk.Misc) -> None:
        card = Card(master, title="Recent events")
        card.pack(fill=tk.X)
        self.log_labels = []
        for _ in range(2):
            label = tk.Label(
                card,
                text="",
                anchor="w",
                justify="left",
                wraplength=270,
                bg=Palette.SURFACE,
                fg=Palette.MUTED,
                font=(FONT, 9),
            )
            label.pack(fill=tk.X, padx=16, pady=(0, 8))
            self.log_labels.append(label)
        tk.Frame(card, bg=Palette.SURFACE, height=6).pack()

    def _build_workers_page(self) -> None:
        self.workers_page.grid_columnconfigure(0, weight=1)
        self.workers_page.grid_rowconfigure(1, weight=1)

        top = tk.Frame(self.workers_page, bg=Palette.BG)
        top.grid(row=0, column=0, sticky="ew", pady=(0, 10))
        for i in range(3):
            top.grid_columnconfigure(i, weight=1, uniform="workerstatus")

        self.workers_status_card = StatusCard(top, "WORKERS", "Scanning", "No results yet")
        self.discovery_card = StatusCard(top, "DISCOVERY", "Waiting", f"{core.DISCOVERY_CIDR}:{core.AGENT_PORT}")
        self.hat_status_card = StatusCard(top, "MCC-134", "Checking", "4 thermocouple channels")
        self.workers_status_card.grid(row=0, column=0, sticky="ew", padx=(0, 5))
        self.discovery_card.grid(row=0, column=1, sticky="ew", padx=5)
        self.hat_status_card.grid(row=0, column=2, sticky="ew", padx=(5, 0))

        table_card = Card(self.workers_page)
        table_card.grid(row=1, column=0, sticky="nsew", pady=(0, 10))
        table_card.grid_rowconfigure(2, weight=1)
        table_card.grid_columnconfigure(0, weight=1)

        heading = tk.Frame(table_card, bg=Palette.SURFACE)
        heading.grid(row=0, column=0, sticky="ew", padx=16, pady=(14, 3))
        tk.Label(heading, text="Workers", bg=Palette.SURFACE, fg=Palette.TEXT, font=(FONT, 12, "bold")).pack(side=tk.LEFT)
        self.node_help_lbl = tk.Label(
            table_card,
            text="Discovery is running automatically.",
            anchor="w",
            bg=Palette.SURFACE,
            fg=Palette.MUTED,
            font=(FONT, 9),
        )
        self.node_help_lbl.grid(row=1, column=0, sticky="ew", padx=16, pady=(0, 8))

        table_wrap = tk.Frame(table_card, bg=Palette.SURFACE)
        table_wrap.grid(row=2, column=0, sticky="nsew", padx=12, pady=(0, 12))
        table_wrap.grid_rowconfigure(0, weight=1)
        table_wrap.grid_columnconfigure(0, weight=1)

        columns = ("state", "node", "ip", "temp", "clock", "cpu", "last_seen")
        self.node_tree = ttk.Treeview(table_wrap, columns=columns, show="headings", style="Workers.Treeview", selectmode="browse")
        headings = {
            "state": "STATE",
            "node": "WORKER",
            "ip": "IP ADDRESS",
            "temp": "CPU TEMP",
            "clock": "CLOCK",
            "cpu": "CPU LOAD",
            "last_seen": "LAST SEEN",
        }
        widths = {"state": 95, "node": 190, "ip": 135, "temp": 105, "clock": 105, "cpu": 100, "last_seen": 130}
        for col in columns:
            self.node_tree.heading(col, text=headings[col])
            self.node_tree.column(col, width=widths[col], minwidth=70, anchor="center", stretch=col in {"node", "last_seen"})
        self.node_tree.tag_configure("online", background=Palette.SUCCESS_BG, foreground=Palette.TEXT)
        self.node_tree.tag_configure("offline", background=Palette.DANGER_BG, foreground=Palette.TEXT)
        self.node_tree.tag_configure("waiting", background=Palette.WARNING_BG, foreground=Palette.TEXT)
        scroll = ttk.Scrollbar(table_wrap, orient="vertical", command=self.node_tree.yview, style="Clean.Vertical.TScrollbar")
        self.node_tree.configure(yscrollcommand=scroll.set)
        self.node_tree.grid(row=0, column=0, sticky="nsew")
        scroll.grid(row=0, column=1, sticky="ns")

        bottom = tk.Frame(self.workers_page, bg=Palette.BG)
        bottom.grid(row=2, column=0, sticky="ew")
        bottom.grid_columnconfigure(0, weight=1)
        bottom.grid_columnconfigure(1, weight=1)

        maintenance = Card(bottom, title="Maintenance")
        maintenance.grid(row=0, column=0, sticky="nsew", padx=(0, 5))
        self.reboot_btn = action_button(maintenance, "Reboot all workers", self._ask_reboot_nodes)
        self.reboot_btn.pack(fill=tk.X, padx=16, pady=(0, 8))
        action_button(maintenance, "Clear collected data", self._clear_all, kind="danger").pack(fill=tk.X, padx=16, pady=(0, 16))

        network = Card(bottom, title="Bench network")
        network.grid(row=0, column=1, sticky="nsew", padx=(5, 0))
        self.discovery_detail_lbl = tk.Label(
            network,
            text="",
            justify="left",
            anchor="nw",
            bg=Palette.SURFACE,
            fg=Palette.MUTED,
            font=(FONT, 9),
        )
        self.discovery_detail_lbl.pack(fill=tk.BOTH, expand=True, padx=16, pady=(0, 16))

    # ------------------------------------------------------------------
    # Thread-safe UI/event helpers
    # ------------------------------------------------------------------
    def log_msg(self, msg: str) -> None:
        logging.info(msg)
        line = f"{datetime.now():%H:%M:%S}  {msg}"
        self.log.appendleft(line)
        if self.headless:
            return
        snapshot = list(self.log)

        def render() -> None:
            for idx, label in enumerate(self.log_labels):
                label.configure(text=snapshot[idx] if idx < len(snapshot) else "")

        self.after(0, render)

    def _show_connection_status(self) -> None:
        if self.headless:
            return
        self.after(0, self._render_connection_status)

    def _render_connection_status(self) -> None:
        try:
            hat_ok = bool(core.hat_list(core.HatIDs.MCC_134))
        except Exception:
            hat_ok = False
        if hat_ok:
            self.hat_card.set("Connected", "MCC-134 available", "success")
            self.hat_status_card.set("Connected", "4 thermocouple channels", "success")
        else:
            self.hat_card.set("Not found", "MCC-134 unavailable", "danger")
            self.hat_status_card.set("Not found", "Check HAT / GPIO", "danger")

        total = len(set(getattr(self, "node_ips", [])))
        online = self._online_node_count()
        if total == 0:
            self.worker_card.set("Scanning", core.DISCOVERY_CIDR or "Discovery disabled", "warning")
            self.workers_status_card.set("Scanning", "No workers discovered yet", "warning")
        elif online == total:
            self.worker_card.set(f"{online}/{total} online", "All workers responding", "success")
            self.workers_status_card.set(f"{online}/{total} online", "All workers responding", "success")
        elif online == 0:
            self.worker_card.set(f"0/{total} online", "Workers not responding", "danger")
            self.workers_status_card.set(f"0/{total} online", "Check power / Ethernet", "danger")
        else:
            self.worker_card.set(f"{online}/{total} online", "Partial bench availability", "warning")
            self.workers_status_card.set(f"{online}/{total} online", "Some workers are missing", "warning")

    def _online_node_count(self) -> int:
        now = datetime.now()
        with self.metrics_lock:
            return sum(
                1
                for ip in set(getattr(self, "node_ips", []))
                if self.last_metrics.get(ip, {}).get("online")
                and self.last_metrics[ip].get("last_seen")
                and (now - self.last_metrics[ip]["last_seen"]).total_seconds() <= ONLINE_AFTER_SECONDS
            )

    # ------------------------------------------------------------------
    # Discovery and polling status
    # ------------------------------------------------------------------
    def _load_nodes(self):
        """Use discovery only unless a nodes file is explicitly requested."""
        if not getattr(core, "NODES_FILE", ""):
            logging.info("Static SSH fallback disabled; using worker-agent discovery")
            return []
        return super()._load_nodes()

    def _set_discovery_text(self, text: str) -> None:
        self.discovery_text = text
        if self.headless:
            return
        short = text
        if len(short) > 42:
            short = short[:39] + "..."
        tone = "success" if ("Found" in text or "Added" in text) else "warning"
        if "disabled" in text.lower():
            tone = "danger"
        self.discovery_card.set(short, f"{core.DISCOVERY_CIDR}:{core.AGENT_PORT}", tone)
        self.discovery_detail_lbl.configure(
            text=(
                f"Subnet: {core.DISCOVERY_CIDR or 'disabled'}\n"
                f"Worker agent port: {core.AGENT_PORT}\n\n"
                f"{text}"
            )
        )

    def _schedule_discovery_text(self, text: str) -> None:
        self.discovery_text = text
        if not self.headless:
            self.after(0, lambda value=text: self._set_discovery_text(value))

    def _discover_agent_nodes(self) -> dict[str, core.WorkerInfo]:
        if not core.DISCOVERY_CIDR:
            self._schedule_discovery_text("Discovery disabled")
            return {}
        self._schedule_discovery_text(f"Scanning {core.DISCOVERY_CIDR}")
        started = time.monotonic()
        found = super()._discover_agent_nodes()
        elapsed = time.monotonic() - started
        if found:
            self._schedule_discovery_text(f"Found {len(found)} worker(s) in {elapsed:.1f}s • {datetime.now():%H:%M:%S}")
        else:
            self._schedule_discovery_text(f"No workers found • last scan {datetime.now():%H:%M:%S}")
        return found

    def _discover_and_add_nodes(self) -> None:
        before = set(getattr(self, "node_ips", []))
        super()._discover_and_add_nodes()
        after = set(getattr(self, "node_ips", []))
        added = after - before
        if added:
            self._schedule_discovery_text(f"Added {len(added)} new worker(s): {', '.join(sorted(added))}")

    def _poll_node(self, ip: str) -> None:
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
        if self.stop.is_set() or self.headless:
            return

        self._render_connection_status()
        nodes = sorted(set(getattr(self, "node_ips", [])))
        now = datetime.now()
        with self.metrics_lock:
            metrics = {ip: dict(value) for ip, value in self.last_metrics.items()}

        rows: list[tuple[str, str, tuple[str, ...]]] = []
        online_count = 0
        for ip in nodes:
            info = self.worker_info.get(ip)
            metric = metrics.get(ip, {})
            label = str(metric.get("label") or (info.label if info else ip))
            last_seen = metric.get("last_seen")
            online = bool(metric.get("online")) and last_seen and (now - last_seen).total_seconds() <= ONLINE_AFTER_SECONDS

            if online:
                online_count += 1
                state, tag = "Online", "online"
                age = f"{int((now - last_seen).total_seconds())} s ago"
            elif last_seen:
                state, tag = "Offline", "offline"
                age = last_seen.strftime("%H:%M:%S")
            else:
                state, tag = "Waiting", "waiting"
                age = "No data yet"

            temp = f"{float(metric['temp']):.1f} °C" if metric.get("temp") is not None else "-"
            clock = f"{float(metric['clock']) / 1000:.2f} GHz" if metric.get("clock") else "-"
            usage = f"{float(metric['usage']):.0f} %" if metric.get("usage") is not None else "-"
            rows.append((ip, tag, (state, label, ip, temp, clock, usage, age)))

        existing = set(self.node_tree.get_children(""))
        wanted = {ip for ip, _, _ in rows}
        for iid in existing - wanted:
            self.node_tree.delete(iid)
        for ip, tag, values in rows:
            if ip in existing:
                self.node_tree.item(ip, values=values, tags=(tag,))
            else:
                self.node_tree.insert("", tk.END, iid=ip, values=values, tags=(tag,))

        if rows:
            self.node_help_lbl.configure(
                text=f"{online_count}/{len(rows)} workers online. Discovery refreshes automatically every 15 seconds."
            )
        else:
            self.node_help_lbl.configure(
                text=f"No workers discovered yet. Scanning {core.DISCOVERY_CIDR}:{core.AGENT_PORT}."
            )

        self.node_refresh_id = self.after(1500, self._refresh_node_table)

    # ------------------------------------------------------------------
    # Clean plot renderer
    # ------------------------------------------------------------------
    def _refresh_plot(self) -> None:
        if self.stop.is_set():
            return

        self.ax.clear()
        self.ax.set_facecolor(Palette.SURFACE)
        with self.cl_lock:
            soc = self.cl_df.copy()
        with self.tc_lock:
            fluid = self.tc_df.copy()
        with self.nodes_lock:
            worker_labels = sorted({self.worker_info[ip].label if ip in self.worker_info else ip for ip in self.node_ips})

        series_lines = []
        series_labels = []
        color_map = core.plt.get_cmap("tab10")
        color_index = 0

        for worker in worker_labels:
            data = soc[(soc.Node == worker) & (soc.Temp > 0)]
            if data.empty:
                continue
            line, = self.ax.plot(data.Time, data.Temp, linewidth=2.0, color=color_map(color_index % 10))
            color_index += 1
            latest = data.iloc[-1]
            usage = f" • CPU {float(latest.Usage):.0f}%" if pd.notna(latest.Usage) else ""
            series_lines.append(line)
            series_labels.append(f"{worker}\n{float(latest.Temp):.1f} °C • {float(latest.Clock) / 1000:.2f} GHz{usage}")

        for channel in self.FLUID_ORDER:
            data = fluid[(fluid.Channel == channel) & (fluid.Temp > 0)]
            if data.empty:
                continue
            line, = self.ax.plot(data.Time, data.Temp, linewidth=2.0, linestyle="--", color=color_map(color_index % 10))
            color_index += 1
            series_lines.append(line)
            series_labels.append(f"{self.NAMES[channel]}\n{float(data.Temp.iloc[-1]):.1f} °C")

        mins = [series.min() for series in (soc["Temp"], fluid["Temp"]) if not series.empty]
        maxs = [series.max() for series in (soc["Temp"], fluid["Temp"]) if not series.empty]
        if self.manual_ylim:
            self.ax.set_ylim(self.temp_ylim)
        elif mins and maxs:
            ymin, ymax = min(mins), max(maxs)
            pad = max(3.0, (ymax - ymin) * 0.12)
            self.ax.set_ylim(ymin - pad, ymax + pad)

        time_mins = [series.min() for series in (soc["Time"], fluid["Time"]) if not series.empty]
        time_maxs = [series.max() for series in (soc["Time"], fluid["Time"]) if not series.empty]
        if time_mins and time_maxs:
            xmin, xmax = min(time_mins), max(time_maxs)
            if xmin == xmax:
                xmax = xmin + timedelta(seconds=1)
            self.ax.set_xlim(xmin, xmax)
            locator = core.AutoDateLocator(minticks=4, maxticks=8, interval_multiples=True)
            formatter = core.ConciseDateFormatter(locator)
            self.ax.xaxis.set_major_locator(locator)
            self.ax.xaxis.set_major_formatter(formatter)

        self.ax.set_ylabel("Temperature (°C)", color=Palette.MUTED)
        self.ax.set_xlabel("Time", color=Palette.MUTED)
        self.ax.tick_params(colors=Palette.MUTED, labelsize=9)
        self.ax.grid(axis="y", color=Palette.PLOT_GRID, linewidth=0.8)
        for spine in self.ax.spines.values():
            spine.set_color(Palette.BORDER)
        if series_lines:
            self.ax.legend(
                series_lines,
                series_labels,
                loc="upper left",
                bbox_to_anchor=(1.01, 1.0),
                borderaxespad=0,
                frameon=False,
                fontsize=9,
            )
        else:
            self.ax.text(
                0.5,
                0.5,
                "Waiting for temperature data",
                transform=self.ax.transAxes,
                ha="center",
                va="center",
                color=Palette.MUTED,
                fontsize=12,
            )
        self.canvas.draw_idle()
        self.plot_id = self.after(1000, self._refresh_plot)

    def _reset_zoom(self) -> None:
        self.manual_ylim = False

    # ------------------------------------------------------------------
    # Phase/status rendering
    # ------------------------------------------------------------------
    def _tick(self) -> None:
        if self.stop.is_set():
            return
        now = datetime.now()
        text = "Idle"
        bg, fg = Palette.NEUTRAL_BG, Palette.TEXT
        card_value, card_note, tone = "Idle", "Ready", "neutral"
        enable_skip = False

        if self.waiting:
            remaining = self.wait_end - now
            if remaining.total_seconds() <= 0:
                self.waiting = False
                self._run_stress()
                self.tick_id = self.after(1000, self._tick)
                return
            text = f"Waiting {remaining.seconds // 60:02}:{remaining.seconds % 60:02}"
            bg, fg = Palette.WARNING_BG, Palette.WARNING
            card_value, card_note, tone = "Waiting", text.replace("Waiting ", "Remaining "), "warning"
        elif self.stress_running:
            remaining = self.stress_end - now
            if remaining.total_seconds() <= 0:
                self._stop_stress()
            else:
                text = f"Stress {remaining.seconds // 60:02}:{remaining.seconds % 60:02}"
                bg, fg = Palette.DANGER_BG, Palette.DANGER
                card_value, card_note, tone = "Stress", text.replace("Stress ", "Remaining "), "danger"
        elif self.logging and self.log_stress:
            remaining = self.cool_end - now
            if remaining.total_seconds() <= 0:
                self._write_excel("stress")
                self.logging = False
                self.log_stress = False
                self.log_msg("Cooling finished")
            else:
                text = f"Cooling {remaining.seconds // 60:02}:{remaining.seconds % 60:02}"
                bg, fg = Palette.INFO_BG, Palette.INFO
                card_value, card_note, tone = "Cooling", text.replace("Cooling ", "Remaining "), "info"
                enable_skip = True
        elif self.logging:
            text = "Logging"
            bg, fg = Palette.SUCCESS_BG, Palette.SUCCESS
            card_value, card_note, tone = "Manual log", "Recording temperature data", "success"

        self.status_lbl.configure(text=text, bg=bg, fg=fg)
        self.phase_card.set(card_value, card_note, tone)
        self.skip_btn.configure(state=tk.NORMAL if enable_skip else tk.DISABLED)
        if self.logging:
            self.record_card.set("Recording", "Raw CSV buffer active", "success")
        else:
            self.record_card.set("Off", "No active log", "neutral")

        self.tick_id = self.after(1000, self._tick)

    def _toggle_full(self) -> None:
        fullscreen = bool(self.master.attributes("-fullscreen"))
        self.master.attributes("-fullscreen", not fullscreen)
        self.full_btn.configure(text="Windowed" if not fullscreen else "Full screen")

    # ------------------------------------------------------------------
    # Safer threaded reboot status updates
    # ------------------------------------------------------------------
    def _do_reboot_nodes(self) -> None:
        self.log_msg("Rebooting workers")
        workers = [ip for ip in set(self.node_ips) if ip != self.local_ip]
        for ip in workers:
            if ip in self.worker_info and self.worker_info[ip].source == "agent":
                self._agent_request(ip, "/reboot", method="POST", timeout=1)
            else:
                core.subprocess.Popen(f"ssh {core.SSH_OPTS} pi@{ip} 'sudo reboot &'", shell=True)

        time.sleep(5)
        for ip in workers:
            while not self.stop.is_set():
                if ip in self.worker_info and self.worker_info[ip].source == "agent":
                    if self._agent_status(ip, timeout=2) is not None:
                        break
                else:
                    try:
                        core.subprocess.check_output(f"ssh {core.SSH_OPTS} pi@{ip} 'echo ok'", shell=True, timeout=5)
                        break
                    except core.subprocess.SubprocessError:
                        pass
                time.sleep(2)

        self.log_msg("All workers back online")
        if not self.headless:
            self.after(0, lambda: self.reboot_btn.configure(state=tk.NORMAL))
            self._show_connection_status()

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------
    def close(self) -> None:
        if self.node_refresh_id:
            try:
                self.after_cancel(self.node_refresh_id)
            except tk.TclError:
                pass
            self.node_refresh_id = None
        super().close()


def main() -> None:
    parser = argparse.ArgumentParser(description="Immersion bench thermal monitor")
    parser.add_argument("--nodes-file", default="", help="Optional SSH fallback node file")
    parser.add_argument("--csv-dir", default=core.DEFAULT_CSV_DIR, help="Directory for CSV/XLSX output")
    parser.add_argument("--headless", action="store_true", help="Run acquisition without showing the GUI")
    parser.add_argument("--agent-port", type=int, default=core.DEFAULT_AGENT_PORT, help="Worker agent HTTP port")
    parser.add_argument(
        "--discovery-cidr",
        default=DEFAULT_READABLE_DISCOVERY_CIDR,
        help="CIDR scanned for worker agents",
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
        handlers=[logging.FileHandler(core.CSV_DIR / "monitor.log"), logging.StreamHandler(sys.stdout)],
    )

    root = tk.Tk()
    root.title("Immersion Bench Thermal Monitor")
    root.geometry("1440x900")
    root.minsize(1120, 720)
    if args.headless:
        root.withdraw()

    app = CleanMonitor(root, headless=args.headless)

    def close_app() -> None:
        app.close()
        root.destroy()

    root.protocol("WM_DELETE_WINDOW", close_app)
    try:
        root.mainloop()
    finally:
        app.close()


if __name__ == "__main__":
    main()

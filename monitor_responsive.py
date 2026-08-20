#!/usr/bin/env python3
"""Responsive operator shell for the immersion bench monitor.

This layer keeps the acquisition/control backend and worker protocol from
``monitor_app.py`` while optimizing the live screen for the 1152x680 bench
display. Long XLSX exports are dispatched off the Tk UI thread.
"""

from __future__ import annotations

import argparse
import logging
import sys
from datetime import datetime
from pathlib import Path
from threading import Thread

import tkinter as tk
from tkinter import messagebox

import dual_monitor as core
from monitor_app import CleanMonitor, DEFAULT_READABLE_DISCOVERY_CIDR
from monitor_ui import Card, FONT, Palette, PillLabel, action_button, configure_ttk, small_button


class CompactStatus(tk.Frame):
    """One-line status tile compatible with ``StatusCard.set``."""

    COLORS = {
        "neutral": Palette.TEXT,
        "success": Palette.SUCCESS,
        "warning": Palette.WARNING,
        "danger": Palette.DANGER,
        "info": Palette.INFO,
        "accent": Palette.ACCENT,
    }

    def __init__(self, master: tk.Misc, title: str, value: str = "-") -> None:
        super().__init__(
            master,
            bg=Palette.SURFACE,
            highlightbackground=Palette.BORDER,
            highlightthickness=1,
            bd=0,
        )
        self.accent = tk.Frame(self, bg=Palette.BORDER, width=4)
        self.accent.pack(side=tk.LEFT, fill=tk.Y)
        body = tk.Frame(self, bg=Palette.SURFACE)
        body.pack(side=tk.LEFT, fill=tk.BOTH, expand=True, padx=9, pady=6)
        self.title_lbl = tk.Label(
            body,
            text=title,
            bg=Palette.SURFACE,
            fg=Palette.MUTED,
            font=(FONT, 8, "bold"),
        )
        self.title_lbl.pack(anchor="w")
        self.value_lbl = tk.Label(
            body,
            text=value,
            bg=Palette.SURFACE,
            fg=Palette.TEXT,
            font=(FONT, 11, "bold"),
        )
        self.value_lbl.pack(anchor="w")
        self.note = ""

    def set(self, value: str, note: str = "", tone: str = "neutral") -> None:
        color = self.COLORS.get(tone, Palette.TEXT)
        self.accent.configure(bg=color)
        self.value_lbl.configure(text=value, fg=color)
        self.note = note


class ResponsiveMonitor(CleanMonitor):
    """Live-screen-first monitor with non-blocking export/finalization."""

    CONTROL_WIDTH = 238

    def __init__(self, master: tk.Tk, headless: bool = False) -> None:
        self.export_in_progress = False
        self.export_finalizing = False
        self.controls_visible = True
        super().__init__(master, headless=headless)

    # ------------------------------------------------------------------
    # Compact responsive layout
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
        self.page_host.pack(fill=tk.BOTH, expand=True, padx=10, pady=(0, 10))
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
        header.pack(fill=tk.X, padx=10, pady=(7, 3))

        title = tk.Frame(header, bg=Palette.BG)
        title.pack(side=tk.LEFT)
        tk.Label(
            title,
            text="IMMERSION BENCH",
            bg=Palette.BG,
            fg=Palette.ACCENT,
            font=(FONT, 8, "bold"),
        ).pack(anchor="w")
        tk.Label(
            title,
            text="Thermal Monitor",
            bg=Palette.BG,
            fg=Palette.TEXT,
            font=(FONT, 18, "bold"),
        ).pack(anchor="w")

        actions = tk.Frame(header, bg=Palette.BG)
        actions.pack(side=tk.RIGHT, pady=2)
        self.status_lbl = PillLabel(actions, "Idle")
        self.status_lbl.configure(font=(FONT, 9, "bold"), padx=11, pady=6)
        self.status_lbl.pack(side=tk.LEFT, padx=(0, 6))
        self.full_btn = action_button(actions, "Full screen", self._toggle_full)
        self.full_btn.configure(font=(FONT, 9, "bold"), padx=11, pady=6)
        self.full_btn.pack(side=tk.LEFT)

    def _build_navigation(self, master: tk.Misc) -> None:
        nav = tk.Frame(master, bg=Palette.BG)
        nav.pack(fill=tk.X, padx=10, pady=(0, 6))

        self.live_nav = action_button(nav, "Live monitor", lambda: self._show_page("live"))
        self.live_nav.configure(font=(FONT, 9, "bold"), padx=11, pady=5)
        self.live_nav.pack(side=tk.LEFT, padx=(0, 4))
        self.workers_nav = action_button(nav, "Workers & setup", lambda: self._show_page("workers"))
        self.workers_nav.configure(font=(FONT, 9, "bold"), padx=11, pady=5)
        self.workers_nav.pack(side=tk.LEFT)

        self.network_lbl = tk.Label(
            nav,
            text=f"{core.DISCOVERY_CIDR or DEFAULT_READABLE_DISCOVERY_CIDR}  •  :{core.AGENT_PORT}",
            bg=Palette.BG,
            fg=Palette.MUTED,
            font=(FONT, 8),
        )
        self.network_lbl.pack(side=tk.RIGHT, pady=5)

    def _build_live_page(self) -> None:
        self.live_page.grid_columnconfigure(0, weight=1)
        self.live_page.grid_rowconfigure(1, weight=1)

        summary = tk.Frame(self.live_page, bg=Palette.BG)
        summary.grid(row=0, column=0, sticky="ew", pady=(0, 6))
        for i in range(4):
            summary.grid_columnconfigure(i, weight=1, uniform="summary")

        self.phase_card = CompactStatus(summary, "TEST", "Idle")
        self.worker_card = CompactStatus(summary, "WORKERS", "Scanning")
        self.hat_card = CompactStatus(summary, "MCC-134", "Checking")
        self.record_card = CompactStatus(summary, "DATA", "Off")
        for i, card in enumerate((self.phase_card, self.worker_card, self.hat_card, self.record_card)):
            card.grid(
                row=0,
                column=i,
                sticky="ew",
                padx=(0 if i == 0 else 3, 0 if i == 3 else 3),
            )

        body = tk.Frame(self.live_page, bg=Palette.BG)
        body.grid(row=1, column=0, sticky="nsew")
        body.grid_rowconfigure(0, weight=1)
        body.grid_columnconfigure(0, weight=1)

        self.plot_card = Card(body)
        self.plot_card.grid(row=0, column=0, sticky="nsew", padx=(0, 7))
        self.plot_card.grid_rowconfigure(1, weight=1)
        self.plot_card.grid_columnconfigure(0, weight=1)

        plot_head = tk.Frame(self.plot_card, bg=Palette.SURFACE)
        plot_head.grid(row=0, column=0, sticky="ew", padx=10, pady=(7, 2))
        tk.Label(
            plot_head,
            text="Temperature history",
            bg=Palette.SURFACE,
            fg=Palette.TEXT,
            font=(FONT, 11, "bold"),
        ).pack(side=tk.LEFT)

        actions = tk.Frame(plot_head, bg=Palette.SURFACE)
        actions.pack(side=tk.RIGHT)
        for text, command in (
            ("Tighter", lambda: self._zoom(-10)),
            ("Wider", lambda: self._zoom(10)),
            ("Auto", self._reset_zoom),
        ):
            btn = action_button(actions, text, command)
            btn.configure(font=(FONT, 8, "bold"), padx=8, pady=4)
            btn.pack(side=tk.LEFT, padx=(0, 3))
        self.controls_toggle_btn = action_button(actions, "Hide controls", self._toggle_controls)
        self.controls_toggle_btn.configure(font=(FONT, 8, "bold"), padx=8, pady=4)
        self.controls_toggle_btn.pack(side=tk.LEFT)

        self.fig, self.ax = core.plt.subplots(1, 1, figsize=(14, 8), dpi=core.PLOT_DPI)
        self.fig.patch.set_facecolor(Palette.SURFACE)
        self.fig.subplots_adjust(top=0.98, left=0.072, right=0.80, bottom=0.13)
        self.canvas = core.FigureCanvasTkAgg(self.fig, master=self.plot_card)
        self.canvas.get_tk_widget().grid(row=1, column=0, sticky="nsew", padx=5, pady=(0, 5))

        self.controls_rail = tk.Frame(body, bg=Palette.BG, width=self.CONTROL_WIDTH)
        self.controls_rail.grid(row=0, column=1, sticky="ns")
        self.controls_rail.grid_propagate(False)
        self._build_compact_sequence_panel(self.controls_rail)
        self._build_compact_data_panel(self.controls_rail)
        self._build_compact_events_panel(self.controls_rail)

    def _duration_row(self, master: tk.Misc, label: str, default: int) -> tk.IntVar:
        var = tk.IntVar(value=default)
        row = tk.Frame(master, bg=Palette.SURFACE)
        row.pack(fill=tk.X, padx=10, pady=(0, 5))
        tk.Label(
            row,
            text=label,
            width=8,
            anchor="w",
            bg=Palette.SURFACE,
            fg=Palette.MUTED,
            font=(FONT, 8, "bold"),
        ).pack(side=tk.LEFT)
        minus = small_button(row, "-5", lambda: var.set(max(0, var.get() - 5)))
        minus.configure(font=(FONT, 8, "bold"), padx=6, pady=3)
        minus.pack(side=tk.LEFT)
        entry = tk.Entry(
            row,
            textvariable=var,
            width=4,
            justify="center",
            bg=Palette.SURFACE_ALT,
            fg=Palette.TEXT,
            relief="flat",
            highlightthickness=1,
            highlightbackground=Palette.BORDER,
            font=(FONT, 10, "bold"),
        )
        entry.pack(side=tk.LEFT, fill=tk.X, expand=True, padx=4, ipady=4)
        plus = small_button(row, "+5", lambda: var.set(var.get() + 5))
        plus.configure(font=(FONT, 8, "bold"), padx=6, pady=3)
        plus.pack(side=tk.LEFT)
        return var

    def _build_compact_sequence_panel(self, master: tk.Misc) -> None:
        card = Card(master)
        card.pack(fill=tk.X, pady=(0, 7))
        tk.Label(
            card,
            text="Test sequence",
            bg=Palette.SURFACE,
            fg=Palette.TEXT,
            font=(FONT, 10, "bold"),
        ).pack(anchor="w", padx=10, pady=(8, 5))

        self.wait_min = self._duration_row(card, "Wait", 0)
        self.stress_min = self._duration_row(card, "Stress", 30)
        self.cool_min = self._duration_row(card, "Cooling", 30)

        self.start_btn = action_button(card, "Start sequence", self._start_sequence, kind="primary")
        self.start_btn.configure(font=(FONT, 9, "bold"), pady=6)
        self.start_btn.pack(fill=tk.X, padx=10, pady=(3, 4))
        self.stop_btn = action_button(card, "Stop stress", self._ask_stop_stress, kind="danger")
        self.stop_btn.configure(font=(FONT, 9, "bold"), pady=5, state=tk.DISABLED)
        self.stop_btn.pack(fill=tk.X, padx=10, pady=(0, 4))
        self.skip_btn = action_button(card, "Skip cooling + finalize", self._ask_skip_cooling)
        self.skip_btn.configure(font=(FONT, 8, "bold"), pady=5, state=tk.DISABLED)
        self.skip_btn.pack(fill=tk.X, padx=10, pady=(0, 9))

    def _build_compact_data_panel(self, master: tk.Misc) -> None:
        card = Card(master)
        card.pack(fill=tk.X, pady=(0, 7))
        tk.Label(
            card,
            text="Data",
            bg=Palette.SURFACE,
            fg=Palette.TEXT,
            font=(FONT, 10, "bold"),
        ).pack(anchor="w", padx=10, pady=(8, 5))
        row = tk.Frame(card, bg=Palette.SURFACE)
        row.pack(fill=tk.X, padx=10, pady=(0, 4))
        self.start_log_btn = action_button(row, "Start", self._start_log, kind="success")
        self.start_log_btn.configure(font=(FONT, 8, "bold"), padx=6, pady=5)
        self.start_log_btn.pack(side=tk.LEFT, fill=tk.X, expand=True, padx=(0, 2))
        self.stop_log_btn = action_button(row, "Stop", self._stop_log)
        self.stop_log_btn.configure(font=(FONT, 8, "bold"), padx=6, pady=5)
        self.stop_log_btn.pack(side=tk.LEFT, fill=tk.X, expand=True, padx=(2, 0))
        self.save_btn = action_button(card, "Save XLSX", self._ask_write_excel)
        self.save_btn.configure(font=(FONT, 8, "bold"), pady=5)
        self.save_btn.pack(fill=tk.X, padx=10, pady=(0, 9))

    def _build_compact_events_panel(self, master: tk.Misc) -> None:
        card = Card(master)
        card.pack(fill=tk.X)
        tk.Label(
            card,
            text="Recent events",
            bg=Palette.SURFACE,
            fg=Palette.TEXT,
            font=(FONT, 10, "bold"),
        ).pack(anchor="w", padx=10, pady=(8, 4))
        self.log_labels = []
        for _ in range(2):
            label = tk.Label(
                card,
                text="",
                anchor="w",
                justify="left",
                wraplength=205,
                bg=Palette.SURFACE,
                fg=Palette.MUTED,
                font=(FONT, 8),
            )
            label.pack(fill=tk.X, padx=10, pady=(0, 4))
            self.log_labels.append(label)
        tk.Frame(card, height=4, bg=Palette.SURFACE).pack()

    def _toggle_controls(self) -> None:
        self.controls_visible = not self.controls_visible
        if self.controls_visible:
            self.controls_rail.grid()
            self.controls_toggle_btn.configure(text="Hide controls")
        else:
            self.controls_rail.grid_remove()
            self.controls_toggle_btn.configure(text="Show controls")
        self.after_idle(self.canvas.draw_idle)

    # ------------------------------------------------------------------
    # Non-blocking XLSX export / finalization
    # ------------------------------------------------------------------
    def _set_export_controls_busy(self, busy: bool) -> None:
        state = tk.DISABLED if busy else tk.NORMAL
        if hasattr(self, "save_btn"):
            self.save_btn.configure(state=state)
        if hasattr(self, "stop_log_btn"):
            self.stop_log_btn.configure(state=state)
        if self.export_finalizing:
            self.skip_btn.configure(state=tk.DISABLED)
            self.start_btn.configure(state=tk.DISABLED)

    def _begin_export(self, tag: str, *, stop_logging: bool, completion: str) -> None:
        if self.export_in_progress:
            self.log_msg("XLSX export already in progress")
            return
        if not self.logging:
            self.log_msg("No active log to export")
            return

        self.export_in_progress = True
        self.export_finalizing = stop_logging
        self._set_export_controls_busy(True)
        self.log_msg("Finalizing XLSX in background" if stop_logging else "Saving XLSX in background")

        def worker() -> None:
            success = True
            try:
                # Call the backend implementation directly. It may take several
                # seconds, but it no longer blocks Tk's event loop.
                core.UnifiedMonitor._write_excel(self, tag)
            except Exception:
                success = False
                logging.exception("XLSX export failed")

            def finish() -> None:
                if stop_logging:
                    self.logging = False
                    self.log_stress = False
                self.export_in_progress = False
                self.export_finalizing = False
                self._set_export_controls_busy(False)
                self.skip_btn.configure(state=tk.DISABLED)
                if not self.waiting and not self.stress_running:
                    self.start_btn.configure(state=tk.NORMAL)
                if success:
                    self.log_msg(completion)
                else:
                    self.log_msg("XLSX export failed; check monitor.log")

            if not self.stop.is_set() and self.winfo_exists():
                self.after(0, finish)

        Thread(target=worker, daemon=True, name=f"xlsx-export-{tag}").start()

    def _skip_cooling(self) -> None:
        if not (self.logging and self.log_stress) or self.export_in_progress:
            return
        self.cool_end = datetime.now()
        self._begin_export(
            "stress",
            stop_logging=True,
            completion="Cooling skipped; log finalized",
        )

    def _stop_log(self) -> None:
        if self.logging and not self.export_in_progress:
            self._begin_export(
                "manual",
                stop_logging=True,
                completion="Manual log finalized",
            )

    def _ask_write_excel(self) -> None:
        if self.export_in_progress:
            self.log_msg("XLSX export already in progress")
            return
        if not self.logging:
            self.log_msg("No active log to export")
            return
        if messagebox.askyesno("Save XLSX", "Save the current data to an Excel file?"):
            self._begin_export(
                "manual",
                stop_logging=False,
                completion="XLSX snapshot saved",
            )

    def _tick(self) -> None:
        if self.stop.is_set():
            return

        now = datetime.now()
        text = "Idle"
        bg, fg = Palette.NEUTRAL_BG, Palette.TEXT
        card_value, card_note, tone = "Idle", "Ready", "neutral"
        enable_skip = False

        if self.export_in_progress and self.export_finalizing:
            text = "Finalizing log"
            bg, fg = Palette.WARNING_BG, Palette.WARNING
            card_value, card_note, tone = "Finalizing", "Writing XLSX in background", "warning"
        elif self.waiting:
            remaining = self.wait_end - now
            if remaining.total_seconds() <= 0:
                self.waiting = False
                self._run_stress()
                self.tick_id = self.after(1000, self._tick)
                return
            seconds = max(0, int(remaining.total_seconds()))
            text = f"Waiting {seconds // 60:02}:{seconds % 60:02}"
            bg, fg = Palette.WARNING_BG, Palette.WARNING
            card_value, card_note, tone = "Waiting", f"Remaining {seconds // 60:02}:{seconds % 60:02}", "warning"
        elif self.stress_running:
            remaining = self.stress_end - now
            if remaining.total_seconds() <= 0:
                self._stop_stress()
            else:
                seconds = max(0, int(remaining.total_seconds()))
                text = f"Stress {seconds // 60:02}:{seconds % 60:02}"
                bg, fg = Palette.DANGER_BG, Palette.DANGER
                card_value, card_note, tone = "Stress", f"Remaining {seconds // 60:02}:{seconds % 60:02}", "danger"
        elif self.logging and self.log_stress:
            remaining = self.cool_end - now
            if remaining.total_seconds() <= 0:
                self._begin_export(
                    "stress",
                    stop_logging=True,
                    completion="Cooling finished; log finalized",
                )
                text = "Finalizing log"
                bg, fg = Palette.WARNING_BG, Palette.WARNING
                card_value, card_note, tone = "Finalizing", "Writing XLSX in background", "warning"
            else:
                seconds = max(0, int(remaining.total_seconds()))
                text = f"Cooling {seconds // 60:02}:{seconds % 60:02}"
                bg, fg = Palette.INFO_BG, Palette.INFO
                card_value, card_note, tone = "Cooling", f"Remaining {seconds // 60:02}:{seconds % 60:02}", "info"
                enable_skip = True
        elif self.logging:
            text = "Logging"
            bg, fg = Palette.SUCCESS_BG, Palette.SUCCESS
            card_value, card_note, tone = "Manual log", "Recording temperature data", "success"

        self.status_lbl.configure(text=text, bg=bg, fg=fg)
        self.phase_card.set(card_value, card_note, tone)
        self.skip_btn.configure(
            state=tk.NORMAL if enable_skip and not self.export_in_progress else tk.DISABLED
        )
        if self.logging:
            if self.export_in_progress:
                self.record_card.set("Saving", "XLSX export running", "warning")
            else:
                self.record_card.set("Recording", "Raw CSV active", "success")
        else:
            self.record_card.set("Off", "No active log", "neutral")

        self.tick_id = self.after(1000, self._tick)


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
        handlers=[
            logging.FileHandler(core.CSV_DIR / "monitor.log"),
            logging.StreamHandler(sys.stdout),
        ],
    )

    root = tk.Tk()
    root.title("Immersion Bench Thermal Monitor")
    root.geometry("1280x800")
    root.minsize(960, 600)
    if args.headless:
        root.withdraw()

    app = ResponsiveMonitor(root, headless=args.headless)

    def close_app() -> None:
        if app.export_in_progress:
            messagebox.showwarning(
                "Export in progress",
                "The XLSX file is still being written. Wait for finalization before closing the monitor.",
            )
            return
        app.close()
        root.destroy()

    root.protocol("WM_DELETE_WINDOW", close_app)
    try:
        root.mainloop()
    finally:
        if not app.export_in_progress:
            app.close()


if __name__ == "__main__":
    main()

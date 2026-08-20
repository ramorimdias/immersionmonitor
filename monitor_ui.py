#!/usr/bin/env python3
"""Reusable Tk widgets for the immersion bench operator interface."""

from __future__ import annotations

import tkinter as tk
from tkinter import ttk
from typing import Callable


class Palette:
    BG = "#f3f5f7"
    SURFACE = "#ffffff"
    SURFACE_ALT = "#f8fafc"
    BORDER = "#d9dee5"
    TEXT = "#18212f"
    MUTED = "#667085"
    ACCENT = "#c91c24"
    ACCENT_HOVER = "#a9161d"
    SUCCESS = "#18794e"
    SUCCESS_BG = "#eaf7f0"
    WARNING = "#a15c00"
    WARNING_BG = "#fff5e6"
    DANGER = "#b42318"
    DANGER_BG = "#fdf0ef"
    INFO = "#175cd3"
    INFO_BG = "#eef4ff"
    NEUTRAL_BG = "#eef1f5"
    PLOT_GRID = "#e8ebef"


FONT = "DejaVu Sans"


TONE = {
    "neutral": (Palette.TEXT, Palette.NEUTRAL_BG),
    "success": (Palette.SUCCESS, Palette.SUCCESS_BG),
    "warning": (Palette.WARNING, Palette.WARNING_BG),
    "danger": (Palette.DANGER, Palette.DANGER_BG),
    "info": (Palette.INFO, Palette.INFO_BG),
    "accent": (Palette.ACCENT, "#fff0f1"),
}


def configure_ttk(root: tk.Misc) -> None:
    """Apply the small set of ttk styles used by the clean UI."""
    style = ttk.Style(root)
    try:
        style.theme_use("clam")
    except tk.TclError:
        pass

    style.configure(
        "Workers.Treeview",
        background=Palette.SURFACE,
        fieldbackground=Palette.SURFACE,
        foreground=Palette.TEXT,
        rowheight=38,
        borderwidth=0,
        font=(FONT, 11),
    )
    style.configure(
        "Workers.Treeview.Heading",
        background=Palette.SURFACE_ALT,
        foreground=Palette.MUTED,
        relief="flat",
        font=(FONT, 10, "bold"),
        padding=(8, 9),
    )
    style.map("Workers.Treeview", background=[("selected", "#dfe7f2")], foreground=[("selected", Palette.TEXT)])
    style.configure("Clean.Vertical.TScrollbar", gripcount=0, borderwidth=0, arrowsize=14)


class Card(tk.Frame):
    """Simple bordered surface with an optional heading."""

    def __init__(self, master: tk.Misc, title: str | None = None, **kwargs) -> None:
        super().__init__(
            master,
            bg=Palette.SURFACE,
            highlightbackground=Palette.BORDER,
            highlightcolor=Palette.BORDER,
            highlightthickness=1,
            bd=0,
            **kwargs,
        )
        if title:
            tk.Label(
                self,
                text=title,
                bg=Palette.SURFACE,
                fg=Palette.TEXT,
                font=(FONT, 12, "bold"),
            ).pack(anchor="w", padx=16, pady=(14, 8))


class StatusCard(Card):
    """Compact status/metric card with title, value, and note."""

    def __init__(self, master: tk.Misc, title: str, value: str = "-", note: str = "") -> None:
        super().__init__(master)
        self.accent = tk.Frame(self, bg=Palette.BORDER, width=4)
        self.accent.pack(side=tk.LEFT, fill=tk.Y)

        body = tk.Frame(self, bg=Palette.SURFACE)
        body.pack(side=tk.LEFT, fill=tk.BOTH, expand=True, padx=14, pady=11)
        self.title_lbl = tk.Label(body, text=title, bg=Palette.SURFACE, fg=Palette.MUTED, font=(FONT, 9, "bold"))
        self.title_lbl.pack(anchor="w")
        self.value_lbl = tk.Label(body, text=value, bg=Palette.SURFACE, fg=Palette.TEXT, font=(FONT, 15, "bold"))
        self.value_lbl.pack(anchor="w", pady=(2, 0))
        self.note_lbl = tk.Label(body, text=note, bg=Palette.SURFACE, fg=Palette.MUTED, font=(FONT, 9))
        self.note_lbl.pack(anchor="w", pady=(2, 0))

    def set(self, value: str, note: str = "", tone: str = "neutral") -> None:
        fg, bg = TONE.get(tone, TONE["neutral"])
        self.accent.configure(bg=fg)
        self.value_lbl.configure(text=value, fg=fg)
        self.note_lbl.configure(text=note, fg=Palette.MUTED)
        self.configure(bg=bg)


class DurationControl(tk.Frame):
    """Touch-friendly integer duration control exposing a Tk IntVar."""

    def __init__(self, master: tk.Misc, label: str, default: int, step: int = 5) -> None:
        super().__init__(master, bg=Palette.SURFACE)
        self.var = tk.IntVar(value=default)
        self.step = step

        tk.Label(self, text=label, bg=Palette.SURFACE, fg=Palette.MUTED, font=(FONT, 9, "bold")).pack(anchor="w")
        row = tk.Frame(self, bg=Palette.SURFACE)
        row.pack(fill=tk.X, pady=(5, 0))

        self.minus = small_button(row, f"-{step}", lambda: self.var.set(max(0, self.var.get() - step)))
        self.minus.pack(side=tk.LEFT)
        entry = tk.Entry(
            row,
            textvariable=self.var,
            justify="center",
            width=5,
            bg=Palette.SURFACE_ALT,
            fg=Palette.TEXT,
            insertbackground=Palette.TEXT,
            relief="flat",
            bd=0,
            highlightthickness=1,
            highlightbackground=Palette.BORDER,
            highlightcolor=Palette.INFO,
            font=(FONT, 12, "bold"),
        )
        entry.pack(side=tk.LEFT, fill=tk.X, expand=True, padx=6, ipady=7)
        self.plus = small_button(row, f"+{step}", lambda: self.var.set(self.var.get() + step))
        self.plus.pack(side=tk.LEFT)


class PillLabel(tk.Label):
    """State label compatible with the legacy controller's configure calls."""

    def __init__(self, master: tk.Misc, text: str = "Idle") -> None:
        super().__init__(
            master,
            text=text,
            bg=Palette.NEUTRAL_BG,
            fg=Palette.TEXT,
            font=(FONT, 10, "bold"),
            padx=14,
            pady=8,
            bd=0,
        )


def action_button(
    master: tk.Misc,
    text: str,
    command: Callable[[], None],
    *,
    kind: str = "secondary",
    width: int | None = None,
) -> tk.Button:
    if kind == "primary":
        bg, fg, active = Palette.ACCENT, "#ffffff", Palette.ACCENT_HOVER
    elif kind == "danger":
        bg, fg, active = Palette.DANGER_BG, Palette.DANGER, "#f9dfdc"
    elif kind == "success":
        bg, fg, active = Palette.SUCCESS_BG, Palette.SUCCESS, "#dcefe5"
    else:
        bg, fg, active = Palette.SURFACE_ALT, Palette.TEXT, "#e8ecf1"

    options = dict(
        text=text,
        command=command,
        bg=bg,
        fg=fg,
        activebackground=active,
        activeforeground=fg,
        disabledforeground="#a0a8b2",
        relief="flat",
        bd=0,
        highlightthickness=0,
        cursor="hand2",
        font=(FONT, 10, "bold"),
        padx=14,
        pady=10,
    )
    if width is not None:
        options["width"] = width
    return tk.Button(master, **options)


def small_button(master: tk.Misc, text: str, command: Callable[[], None]) -> tk.Button:
    return tk.Button(
        master,
        text=text,
        command=command,
        bg=Palette.SURFACE_ALT,
        fg=Palette.TEXT,
        activebackground="#e8ecf1",
        activeforeground=Palette.TEXT,
        relief="flat",
        bd=0,
        highlightthickness=1,
        highlightbackground=Palette.BORDER,
        font=(FONT, 9, "bold"),
        padx=8,
        pady=5,
        cursor="hand2",
    )

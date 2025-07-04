#!/usr/bin/env python3
"""Cluster monitoring dashboard.

This script collects SoC and fluid temperatures from a cluster of
Raspberry Pis using SSH and an optional MCC-134 board. It can display a
Tkinter GUI or run headless and periodically save data to CSV files and
Excel workbooks.
"""

# ───────── DEFAULT SETTINGS ─────────
DEFAULT_NODES_FILE = "/home/motul/nodes_ips"  # ip [slots=N]
DEFAULT_CSV_DIR    = "/home/motul/temperatures"
FIGSIZE      = (15, 10)
RIGHT_MARGIN = 0.75                             # legend area
LEGEND_ANCHOR= (0.99, 1.05)
PLOT_DPI     = 100
DISPLAY_H    = 2                                # hours on GUI
MEM_CAP      = 100_000
STRESS_CPUS  = 8
SSH_OPTS     = "-o ConnectTimeout=5 -o StrictHostKeyChecking=accept-new -o LogLevel=ERROR"
BIG_FONT     = ("Helvetica", 14)                # global UI font
AVG_BUCKET   = 100                              # fallback bucket count
POINTS_PER_MIN = 2                              # ~2 points per minute
# ─────────────────────────────────

import os
import sys
import time
import socket
import contextlib
import subprocess
import shutil
import math
from datetime import datetime, timedelta
from threading  import Thread, Lock, Event
from pathlib    import Path
from collections import deque
from dataclasses import dataclass
import argparse
import logging
import pandas as pd
import tkinter as tk
from tkinter import ttk, messagebox

import matplotlib
matplotlib.use("TkAgg")
import matplotlib.pyplot as plt
from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg
from matplotlib.dates import AutoDateLocator, ConciseDateFormatter
plt.rcParams.update({"font.size": 12})

from daqhats import mcc134, HatIDs, hat_list, TcTypes

# These paths are initialised in ``main`` after command-line arguments are
# parsed.
CSV_DIR: Path
RAW_SOC: Path
RAW_FLUID: Path

# UI translations
TRANSLATIONS = {
    "Start Log": {"EN": "Start Log", "FR": "Démarrer log"},
    "Stop Log": {"EN": "Stop Log", "FR": "Arrêter log"},
    "Skip Cooling": {"EN": "Skip Cooling", "FR": "Ignorer refroidissement"},
    "Save XLSX": {"EN": "Save XLSX", "FR": "Enregistrer XLSX"},
    "Clear ALL": {"EN": "Clear ALL", "FR": "Tout effacer"},
    "Reboot Nodes": {"EN": "Reboot Nodes", "FR": "Redémarrer nœuds"},
    "Idle": {"EN": "Idle", "FR": "Inactif"},
    "Full Screen": {"EN": "Full Screen", "FR": "Plein écran"},
    "Windowed": {"EN": "Windowed", "FR": "Fenêtré"},
    "Stress": {"EN": "Stress", "FR": "Stress"},
    "Cooling": {"EN": "Cooling", "FR": "Refroidissement"},
    "Wait": {"EN": "Wait", "FR": "Attente"},
    "Start Stress": {"EN": "Start Stress", "FR": "Démarrer stress"},
    "Stop": {"EN": "Stop", "FR": "Arrêter"},
    "HAT ?": {"EN": "HAT ?", "FR": "HAT ?"},
    "Nodes ?": {"EN": "Nodes ?", "FR": "Nœuds ?"},
    "Temp ±10 °C": {"EN": "Temp ±10 °C", "FR": "Temp ±10 °C"},
    "MCC-134 HAT : OK": {"EN": "MCC-134 HAT : OK", "FR": "MCC-134 HAT : OK"},
    "MCC-134 HAT : NOT FOUND": {"EN": "MCC-134 HAT : NOT FOUND", "FR": "MCC-134 HAT : NON TROUVÉ"},
    "Nodes missing: ": {"EN": "Nodes missing: ", "FR": "Nœuds manquants : "},
    "All nodes reachable": {"EN": "All nodes reachable", "FR": "Tous les nœuds accessibles"},
    "Config": {"EN": "Config", "FR": "Config"},
    "Clear data": {"EN": "Clear data", "FR": "Effacer les données"},
    "Remove all collected data?": {"EN": "Remove all collected data?", "FR": "Supprimer toutes les données collectées ?"},
    "Reboot all nodes": {"EN": "Reboot all nodes", "FR": "Redémarrer tous les nœuds"},
    "Reboot ALL worker Raspberry Pis (host stays up)?": {"EN": "Reboot ALL worker Raspberry Pis (host stays up)?", "FR": "Redémarrer TOUS les Raspberry Pi (l’hôte reste actif) ?"},
    "Start stress": {"EN": "Start stress", "FR": "Démarrer le stress"},
    "Begin the stress sequence?": {"EN": "Begin the stress sequence?", "FR": "Commencer la séquence de stress ?"},
    "Stop stress": {"EN": "Stop stress", "FR": "Arrêter le stress"},
    "Stop the stress run?": {"EN": "Stop the stress run?", "FR": "Arrêter l’exécution du stress ?"},
    "stress-ng not installed": {"EN": "stress-ng not installed", "FR": "stress-ng non installé"},
    "Skip cooling": {"EN": "Skip cooling", "FR": "Ignorer le refroidissement"},
    "Skip cooling and finalize log?": {"EN": "Skip cooling and finalize log?", "FR": "Ignorer le refroidissement et finaliser le journal ?"},
    "Save data to an Excel file?": {"EN": "Save data to an Excel file?", "FR": "Enregistrer les données dans un fichier Excel ?"},
    "Temperature (°C)": {"EN": "Temperature (°C)", "FR": "Température (°C)"},
    "Time": {"EN": "Time", "FR": "Temps"},
    "minutes": {"EN": "minutes", "FR": "minutes"},
    "Logging…": {"EN": "Logging…", "FR": "Enregistrement…"},
}


class DataWriter:
    """Accumulate rows and write them to CSV periodically."""

    def __init__(self, path: Path, flush_every: int = 50) -> None:
        self.path = path
        self.flush_every = flush_every
        self.buffer: list[dict] = []
        self.lock = Lock()

    def write_row(self, row: pd.Series) -> None:
        """Add a single row to the buffer and flush if needed."""
        with self.lock:
            self.buffer.append(row.to_dict())
            if len(self.buffer) >= self.flush_every:
                self.flush()

    def write_df(self, df: pd.DataFrame) -> None:
        """Add multiple rows to the buffer from a DataFrame."""
        with self.lock:
            self.buffer.extend(df.to_dict(orient="records"))
            if len(self.buffer) >= self.flush_every:
                self.flush()

    def flush(self) -> None:
        """Write buffered rows to disk."""
        if not self.buffer:
            return
        header = not self.path.exists()
        pd.DataFrame(self.buffer).to_csv(
            self.path, mode="a", header=header, index=False
        )
        self.buffer.clear()

    def close(self) -> None:
        """Flush remaining rows."""
        with self.lock:
            self.flush()

# ───────────────────────────────────────────────────────────────
class UnifiedMonitor(ttk.Frame):
    COLORS = [
        "blue",
        "green",
        "pink",
        "purple",
        "red",
        "orange",
        "navy",
        "cyan",
        "brown",
        "gray",
        "olive",
        "lime",
        "teal",
        "maroon",
    ]
    NAMES = {0: "cold fluid", 1: "hot air", 2: "cold air", 3: "hot fluid"}
    FLUID_ORDER = [3, 0, 1, 2]

    def tr(self, text: str) -> str:
        """Return the translation for ``text`` in the current language."""
        return TRANSLATIONS.get(text, {}).get(self.lang, text)

    def _toggle_lang(self) -> None:
        self.lang = "FR" if self.lang == "EN" else "EN"
        self._apply_lang()

    def _apply_lang(self) -> None:
        """Update all static UI text to match ``self.lang``."""
        self.start_log_btn.config(text=self.tr("Start Log"))
        self.stop_log_btn.config(text=self.tr("Stop Log"))
        self.skip_btn.config(text=self.tr("Skip Cooling"))
        self.save_btn.config(text=self.tr("Save XLSX"))
        self.clear_btn.config(text=self.tr("Clear ALL"))
        self.reboot_btn.config(text=self.tr("Reboot Nodes"))
        fs = self.master.attributes("-fullscreen")
        self.full_btn.config(text=self.tr("Windowed") if fs else self.tr("Full Screen"))
        self.lang_btn.config(text="EN" if self.lang == "FR" else "FR")
        for key, lbl in self.spinner_labels.items():
            lbl.config(text=f"{self.tr(key)} (min):")
        self._show_connection_status()

    def __init__(self, master: tk.Tk, headless: bool = False) -> None:
        super().__init__(master)
        self.headless = headless
        if not self.headless:
            self.pack(fill=tk.BOTH, expand=True)
            ttk.Style(master).configure(".", font=BIG_FONT, padding=6)

        # live data
        self.cl_lock=Lock(); self.tc_lock=Lock()
        self.cl_df=pd.DataFrame(columns=["Time","Node","Temp","Clock"])
        self.tc_df=pd.DataFrame(columns=["Time","Channel","Temp"])

        # state flags
        self.stop=Event(); self.alive=True
        self.logging=False; self.log_stress=False
        self.waiting=False; self.wait_start=self.wait_end=None
        self.stress_running=False
        self.stress_start=self.stress_end=None
        self.cool_end=None; self.wait_minutes_effective=0
        self.manual_ylim=False; self.temp_ylim=[20,90]

        # event log (last 2 lines)
        self.log = deque(maxlen=2)
        self.log_labels: list[tk.Label] = []

        self.lang = "EN"
        self.spinner_labels: dict[str, tk.Label] = {}

        if not self.headless:
            self._build_ui()
        self.local_ip=socket.gethostbyname(socket.gethostname())
        self.node_ips=self._load_nodes(); self._show_connection_status()

        # workers
        for ip in self.node_ips:
            Thread(target=self._poll_node, args=(ip,), daemon=True).start()
        self.hat = self._init_hat()
        Thread(target=self._tc_worker, daemon=True).start()

        self.soc_writer = DataWriter(RAW_SOC)
        self.fluid_writer = DataWriter(RAW_FLUID)

        if not self.headless:
            self.plot_id = self.after(1000, self._refresh_plot)
            self.tick_id = self.after(1000, self._tick)

    # ───── UI ─────
    def _build_ui(self):
        # ---------- top bar ----------
        top=ttk.Frame(self); top.pack(fill=tk.X)
        self.start_log_btn = ttk.Button(top, text=self.tr("Start Log"), command=self._start_log)
        self.start_log_btn.pack(side=tk.LEFT,padx=4)
        self.stop_log_btn = ttk.Button(top, text=self.tr("Stop Log"), command=self._stop_log)
        self.stop_log_btn.pack(side=tk.LEFT,padx=4)
        self.skip_btn = ttk.Button(
            top,
            text=self.tr("Skip Cooling"),
            state=tk.DISABLED,
            command=self._ask_skip_cooling,
        )
        self.skip_btn.pack(side=tk.LEFT,padx=4)
        self.save_btn = ttk.Button(top, text=self.tr("Save XLSX"), command=self._ask_write_excel)
        self.save_btn.pack(side=tk.LEFT,padx=4)
        self.clear_btn = ttk.Button(top, text=self.tr("Clear ALL"), command=self._clear_all)
        self.clear_btn.pack(side=tk.LEFT,padx=4)
        self.reboot_btn=ttk.Button(top,text=self.tr("Reboot Nodes"),command=self._ask_reboot_nodes)
        self.reboot_btn.pack(side=tk.LEFT,padx=6)
        self.status_lbl=ttk.Label(top,text=self.tr("Idle"),width=20,anchor="center",background="white")
        self.status_lbl.pack(side=tk.LEFT,padx=12)
        self.full_btn=ttk.Button(top,text=self.tr("Full Screen"),command=self._toggle_full)
        self.full_btn.pack(side=tk.RIGHT,padx=4)
        self.lang_btn=ttk.Button(top,text="FR",command=self._toggle_lang,width=4)
        self.lang_btn.pack(side=tk.RIGHT,padx=4)

        # ---------- spinners ----------
        st=ttk.Frame(self); st.pack(fill=tk.X)
        self._add_spinner(st,"Stress",30,attr="stress_min")
        self._add_spinner(st,"Cooling",30,attr="cool_min")
        self._add_spinner(st,"Wait",0  ,attr="wait_min")
        self.start_btn=ttk.Button(st,text="Start Stress",command=self._start_sequence)
        self.start_btn.pack(side=tk.LEFT,padx=8)
        self.stop_btn =ttk.Button(st,text="Stop",state=tk.DISABLED,command=self._ask_stop_stress)
        self.stop_btn.pack(side=tk.LEFT)

        # ---------- single row: events + banners ----------
        row=ttk.Frame(self); row.pack(fill=tk.X,pady=(4,2))
        evf=ttk.Frame(row); evf.pack(side=tk.LEFT,fill=tk.X,expand=True)
        #ttk.Label(evf,text="Last events:").pack(anchor="w")
        self.log_labels=[ttk.Label(evf,anchor="w") for _ in range(2)]
        for lb in self.log_labels: lb.pack(fill=tk.X)
        banf=ttk.Frame(row); banf.pack(side=tk.RIGHT,anchor="ne")
        self.hat_banner  = ttk.Label(banf,text="HAT ?", width=24,anchor="center")
        self.node_banner = ttk.Label(banf,text="Nodes ?", width=36,anchor="center")
        self.hat_banner.pack(fill=tk.X)
        self.node_banner.pack(fill=tk.X,pady=(2,0))

        # ---------- plot ----------
        self.fig,self.ax=plt.subplots(1,1,figsize=FIGSIZE,dpi=PLOT_DPI)
        self.fig.subplots_adjust(top=0.97,left=0.06,right=RIGHT_MARGIN,bottom=0.145)
        self.canvas=FigureCanvasTkAgg(self.fig,master=self)
        self.canvas.get_tk_widget().pack(fill=tk.BOTH,expand=True)

        # ---------- zoom ----------
        z=ttk.Frame(self); z.pack(fill=tk.X,pady=2)
        ttk.Label(z,text=self.tr("Temp ±10 °C")).pack(side=tk.LEFT,padx=(6,0))
        ttk.Button(z,text="+",width=3,command=lambda:self._zoom(-10)).pack(side=tk.LEFT)
        ttk.Button(z,text="-",width=3,command=lambda:self._zoom( 10)).pack(side=tk.LEFT)

    def _add_spinner(self,frame,label,default,attr):
        lbl=ttk.Label(frame,text=f"{self.tr(label)} (min):")
        lbl.pack(side=tk.LEFT,padx=(6,2))
        self.spinner_labels[label]=lbl
        var=tk.IntVar(value=default); setattr(self,attr,var)
        ttk.Entry(frame,textvariable=var,width=6).pack(side=tk.LEFT)
        ttk.Button(frame,text="+5",width=3,command=lambda v=var:v.set(v.get()+5)).pack(side=tk.LEFT,padx=2)
        ttk.Button(frame,text="-5",width=3,command=lambda v=var:v.set(max(0,v.get()-5))).pack(side=tk.LEFT)

    # ───── event log helper ─────
    def log_msg(self, msg: str) -> None:
        """Add a message to the GUI log and the logger."""
        logging.info(msg)
        disp = self.tr(msg)
        self.log.appendleft(f"{datetime.now():%H:%M:%S}  {disp}")
        for i, lb in enumerate(self.log_labels):
            lb.config(text=self.log[i] if i < len(self.log) else "")

    # ───── banner helpers ─────
    def _show_connection_status(self):
        if hat_list(HatIDs.MCC_134):
            self.hat_banner.configure(text=self.tr("MCC-134 HAT : OK"),background="green",foreground="white")
        else:
            self.hat_banner.configure(text=self.tr("MCC-134 HAT : NOT FOUND"),background="red",foreground="white")
        bad=[]
        for ip in set(self.node_ips):
            try: subprocess.check_output(f"ssh {SSH_OPTS} pi@{ip} 'echo ok'",shell=True,timeout=4)
            except subprocess.SubprocessError: bad.append(ip)
        if bad:
            self.node_banner.configure(text=self.tr("Nodes missing: ")+", ".join(bad),
                                       background="red",foreground="white")
        else:
            self.node_banner.configure(text=self.tr("All nodes reachable"),
                                       background="green",foreground="white")

    # ───── helpers ─────
    def _toggle_full(self):
        fs=self.master.attributes("-fullscreen")
        self.master.attributes("-fullscreen",not fs)
        self.full_btn.config(text=self.tr("Windowed") if not fs else self.tr("Full Screen"))
    def _zoom(self,delta):
        self.manual_ylim=True
        mid=sum(self.temp_ylim)/2
        span=max((self.temp_ylim[1]-self.temp_ylim[0])+delta*2,10)
        self.temp_ylim=[mid-span/2, mid+span/2]
        self.ax.set_ylim(self.temp_ylim); self.canvas.draw_idle()

    # ───── logging control ─────
    def _start_log(self):
        if not self.logging:
            self.logging=True; self.log_stress=False; self.log_msg("Log started")
    def _stop_log(self):
        if self.logging:
            self._write_excel('manual'); self.logging=False; self.log_msg("Log stopped")
    def _skip_cooling(self) -> None:
        """Finalize the log immediately."""
        self._write_excel("stress")
        self.logging = False
        self.log_stress = False
        self.skip_btn.config(state=tk.DISABLED)
        self.log_msg("Cooling skipped")

    def _ask_skip_cooling(self) -> None:
        if self.logging and messagebox.askyesno(
            self.tr("Skip cooling"), self.tr("Skip cooling and finalize log?")
        ):
            self._skip_cooling()

    def _ask_write_excel(self) -> None:
        if messagebox.askyesno(self.tr("Save XLSX"), self.tr("Save data to an Excel file?")):
            self._write_excel('manual')

    # ───── reboot nodes button ─────
    def _ask_reboot_nodes(self):
        if not messagebox.askyesno(self.tr("Reboot all nodes"),
                                   self.tr("Reboot ALL worker Raspberry Pis (host stays up)?")):
            return
        self.reboot_btn.configure(state=tk.DISABLED)
        Thread(target=self._do_reboot_nodes,daemon=True).start()
    def _do_reboot_nodes(self):
        self.log_msg("Rebooting nodes…")
        workers=[ip for ip in set(self.node_ips) if ip!=self.local_ip]
        for ip in workers:
            subprocess.Popen(f"ssh {SSH_OPTS} pi@{ip} 'sudo reboot &'",shell=True)
        time.sleep(5)
        for ip in workers:
            while True:
                try:
                    subprocess.check_output(f"ssh {SSH_OPTS} pi@{ip} 'echo ok'",
                                            shell=True,timeout=5)
                    break
                except subprocess.SubprocessError: time.sleep(2)
        self.log_msg("All nodes back online")
        self._show_connection_status()
        self.reboot_btn.configure(state=tk.NORMAL)

    # ───── sequence (WAIT → STRESS) ─────
    def _start_sequence(self):
        if self.waiting or self.stress_running:
            return
        if not messagebox.askyesno(self.tr("Start stress"), self.tr("Begin the stress sequence?")):
            return
        wmin=self.wait_min.get()
        if wmin>0:
            self.waiting=True
            self.wait_start=datetime.now()
            self.wait_end=self.wait_start+timedelta(minutes=wmin)
            self.wait_minutes_effective=wmin
            self.status_lbl.configure(background="yellow",foreground="black")
            self.log_msg(f"Waiting {wmin} min before stress")
            self.start_btn.configure(state=tk.DISABLED)
        else: self._run_stress()

    # ───── stress start ─────
    def _run_stress(self):
        secs=self.stress_min.get()*60
        if not shutil.which("stress-ng"):
            messagebox.showerror("stress-ng", self.tr("stress-ng not installed")); return
        if not self.logging: self.logging=True
        self.log_stress=True; self.stress_running=True
        self.stress_start=datetime.now()
        self.stress_end=self.stress_start+timedelta(seconds=secs)
        self.cool_end = self.stress_end+timedelta(minutes=self.cool_min.get())
        Thread(target=self._stress_thread,args=(secs,),daemon=True).start()
        self.start_btn.configure(state=tk.DISABLED)
        self.stop_btn.configure(state=tk.NORMAL)
        self.log_msg(f"Stress started ({self.stress_min.get()} min)")

    def _ask_stop_stress(self) -> None:
        if messagebox.askyesno(self.tr("Stop stress"), self.tr("Stop the stress run?")):
            self._stop_stress()

    def _stop_stress(self):
        self._kill_stress(); self.stress_running=False
        self.start_btn.configure(state=tk.NORMAL)
        self.stop_btn.configure(state=tk.DISABLED)
        if self.logging and self.log_stress:
            self.cool_end = datetime.now()+timedelta(minutes=self.cool_min.get())
            self.skip_btn.configure(state=tk.NORMAL)
        self.log_msg("Stress stopped")

    # ───── stress helper thread ─────
    def _stress_thread(self, seconds:int):
        t_opt = f"--timeout {seconds}s" if seconds else ""
        gov = ("for g in /sys/devices/system/cpu/cpu*/cpufreq/scaling_governor; do "
               "echo performance | sudo -n tee \"$g\" >/dev/null; done; "
               "[ -e /sys/devices/system/cpu/cpufreq/boost ] && "
               "echo 1 | sudo -n tee /sys/devices/system/cpu/cpufreq/boost >/dev/null || true")
        subprocess.Popen(gov,shell=True)
        rem_cmd=("stress-ng --cpu 0 --cpu-load 100 --io 2 --matrix 0 "
                 "--vm 4 --vm-bytes 95% --memcpy 2 "+t_opt).replace('"','\\"')
        for ip in set(self.node_ips):
            subprocess.Popen(f"ssh {SSH_OPTS} pi@{ip} \"{gov}\"",shell=True)
            if ip!=self.local_ip:
                subprocess.Popen(f"ssh {SSH_OPTS} pi@{ip} "
                                 f"'nohup {rem_cmd} >/dev/null 2>&1 &'",shell=True)
    def _kill_stress(self):
        tools='pkill -9 -f "stress-ng" ; pkill -9 -f "stress "'
        subprocess.call(tools,shell=True)
        for ip in set(self.node_ips):
            subprocess.Popen(f"ssh {SSH_OPTS} pi@{ip} \"{tools}\"",shell=True)

    # ───── timer / banner ─────
    def _tick(self):
        if self.stop.is_set(): return
        now=datetime.now()
        bg,fg,txt="white","black",self.tr("Idle"); enable_skip=False
        if self.waiting:
            rem=self.wait_end-now
            if rem.total_seconds()<=0: self.waiting=False; self._run_stress(); return
            txt=f"{self.tr('Waiting')}: {rem.seconds//60:02}:{rem.seconds%60:02}"
            bg,fg="yellow","black"
        elif self.stress_running:
            rem=self.stress_end-now
            if rem.total_seconds()<=0: self._stop_stress()
            else:
                txt=f"{self.tr('Stress')}: {rem.seconds//60:02}:{rem.seconds%60:02}"
                bg,fg="red","white"
        elif self.logging and self.log_stress:
            rem=self.cool_end-now
            if rem.total_seconds()<=0:
                self._write_excel('stress')
                self.logging=False; self.log_stress=False; self.log_msg("Cooling finished")
            else:
                txt=f"{self.tr('Cooling')}: {rem.seconds//60:02}:{rem.seconds%60:02}"
                bg,fg="blue","white"; enable_skip=True
        elif self.logging: txt,bg,fg=self.tr("Logging…"),"lightgreen","black"
        self.status_lbl.configure(text=txt,background=bg,foreground=fg)
        self.skip_btn.configure(state=tk.NORMAL if enable_skip else tk.DISABLED)
        self.tick_id=self.after(1000,self._tick)

    # ───── node polling + raw CSV ─────
    def _load_nodes(self):
        if not os.path.exists(NODES_FILE):
            messagebox.showerror(self.tr("Config"), f"{NODES_FILE} missing"); sys.exit(1)
        ips=[]
        with open(NODES_FILE) as f:
            for ln in f:
                ln=ln.strip()
                if not ln or ln.startswith("#"): continue
                parts=ln.split(); ip=parts[0]
                slots=int(parts[1].split("=")[1]) if len(parts)>1 and "=" in parts[1] else 1
                ips.extend([ip]*slots)
        return ips
    def _save_raw_soc(self, row: pd.Series) -> None:
        """Queue a SoC temperature row for disk writing."""
        self.soc_writer.write_row(row)

    def _save_raw_fluid(self, rows_df: pd.DataFrame) -> None:
        """Queue fluid temperature rows for disk writing."""
        self.fluid_writer.write_df(rows_df)

    def _poll_node(self,ip):
        while not self.stop.is_set():
            t,c=self._read_stats(ip); now=datetime.now()
            if t==0: time.sleep(1); continue     # filter failed SSH reading
            row=pd.Series({"Time":now,"Node":ip,"Temp":t,"Clock":c})
            self._save_raw_soc(row)
            with self.cl_lock:
                self.cl_df=pd.concat([self.cl_df,row.to_frame().T],ignore_index=True)
                self._trim(self.cl_df)
            time.sleep(1)
    def _read_stats(self, ip: str) -> tuple[float, int]:
        """Read temperature and clock from a node via SSH."""
        try:
            if ip == self.local_ip:
                t = float(open("/sys/class/thermal/thermal_zone0/temp").read()) / 1000
                c = int(
                    open(
                        "/sys/devices/system/cpu/cpu0/cpufreq/scaling_cur_freq"
                    ).read()
                ) / 1000
                return t, c
            t = float(
                subprocess.check_output(
                    f"ssh {SSH_OPTS} pi@{ip} cat /sys/class/thermal/thermal_zone0/temp",
                    shell=True,
                ).strip()
            ) / 1000
            c = int(
                subprocess.check_output(
                    f"ssh {SSH_OPTS} pi@{ip} cat /sys/devices/system/cpu/cpu0/cpufreq/scaling_cur_freq",
                    shell=True,
                ).strip()
            ) / 1000
            return t, c
        except subprocess.SubprocessError as exc:
            logging.warning("SSH read failed for %s: %s", ip, exc)
            return 0, 0
        except Exception as exc:  # noqa: broad-except
            logging.exception("Error reading stats from %s", ip)
            return 0, 0
    @staticmethod
    def _trim(df):
        cutoff=datetime.now()-timedelta(hours=DISPLAY_H)
        df.drop(df[df.Time<cutoff].index,inplace=True)
        if len(df)>MEM_CAP: df.drop(df.index[:len(df)//2],inplace=True)

    # ───── MCC-134 ─────
    def _init_hat(self):
        """Initialise the MCC-134 board if present."""
        try:
            hats = hat_list(HatIDs.MCC_134)
            if not hats:
                return None
            addr = hats[0].address
            with contextlib.redirect_stderr(open(os.devnull, "w")):
                hat = mcc134(addr)
                for ch in range(4):
                    hat.tc_type_write(ch, TcTypes.TYPE_K)
            return hat
        except OSError as e:
            # gpiod_line_request_output failures end up here. Log a message so
            # the user knows to check GPIO permissions.
            logging.error("MCC-134 init failed: %s", e)
            return None
        except Exception:
            return None
    def _tc_worker(self):
        while not self.stop.is_set():
            if self.hat:
                try: self._tc_read()
                except Exception as e:
                    self.log_msg(f"MCC-134 error: {e}; re-init")
                    self.hat=None; time.sleep(1); self.hat=self._init_hat()
            time.sleep(1)
    def _tc_read(self):
        ts=datetime.now(); temps={}
        for ch in range(4):
            v=self.hat.t_in_read(ch)
            if v and v not in (mcc134.OPEN_TC_VALUE,mcc134.OVERRANGE_TC_VALUE,mcc134.COMMON_MODE_TC_VALUE):
                temps[ch]=round(v,2)
        if temps:
            df=pd.DataFrame([{"Time":ts,"Channel":c,"Temp":t} for c,t in temps.items()])
            self._save_raw_fluid(df)
            with self.tc_lock:
                self.tc_df=pd.concat([self.tc_df,df],ignore_index=True)
                self._trim(self.tc_df)

    # ───── plot refresh ─────
    def _refresh_plot(self):
        self.ax.cla(); lines=[]; labels=[]
        with self.cl_lock: d=self.cl_df.copy()
        cmap={ip:self.COLORS[i%len(self.COLORS)] for i,ip in enumerate(sorted(set(self.node_ips)))}
        for ip,col in cmap.items():
            s=d[(d.Node==ip)&(d.Temp>0)]
            if not s.empty:
                l,=self.ax.plot(s.Time,s.Temp,color=col,linewidth=2.2)
                tb=fr"$\mathbf{{{s.Temp.iloc[-1]:.1f}\,°C}}$"
                labels.append(f"{ip}  {s.Clock.iloc[-1]/1000:.2f} GHz\n{tb}"); lines.append(l)
        with self.tc_lock: f=self.tc_df.copy()
        offset=len(cmap)
        for idx,ch in enumerate(self.FLUID_ORDER):
            s=f[(f.Channel==ch)&(f.Temp>0)]
            if not s.empty:
                col=self.COLORS[(offset+idx)%len(self.COLORS)]
                l,=self.ax.plot(s.Time,s.Temp,color=col,linewidth=2.2)
                tb=fr"$\mathbf{{{s.Temp.iloc[-1]:.1f}\,°C}}$"
                labels.append(f"Ch {ch} ({self.NAMES[ch]})\n{tb}"); lines.append(l)

        all_vals=list(d.Temp[d.Temp>0])+list(f.Temp[f.Temp>0])
        if self.manual_ylim: self.ax.set_ylim(self.temp_ylim)
        elif all_vals:
            ymin=min(all_vals); ymax=max(all_vals); pad=5
            self.ax.set_ylim(ymin-pad, ymax+pad)
        self.ax.set_ylabel(self.tr("Temperature (°C)"))

        locator = AutoDateLocator(minticks=4, maxticks=8, interval_multiples=True)
        # help AutoDateLocator with short time spans
        locator.intervald[matplotlib.dates.SECONDLY] = [1, 2, 5, 10, 15, 30]
        locator.intervald[matplotlib.dates.MINUTELY] = [1, 2, 5, 10, 15, 30]
        fmt = ConciseDateFormatter(locator)
        all_t=list(d.Time)+list(f.Time)
        if all_t:
            xmin,xmax=min(all_t),max(all_t);  xmax=(xmin+timedelta(seconds=1)) if xmin==xmax else xmax
            self.ax.set_xlim(xmin,xmax); self.ax.xaxis.set_major_locator(locator); self.ax.xaxis.set_major_formatter(fmt)
        for t in self.ax.get_xticklabels(): t.set_rotation(45); t.set_ha('right')
        self.ax.yaxis.grid(True,ls="--",lw=0.5)
        if lines:
            self.ax.legend(lines,labels,loc="upper left",bbox_to_anchor=LEGEND_ANCHOR,fontsize=14)
        self.ax.set_xlabel(self.tr("Time"))
        self.canvas.draw()
        self.plot_id=self.after(1000,self._refresh_plot) if not self.stop.is_set() else None

    # ───── Excel export ─────
    def _avg_df(self, df, cols, keep=None):
        """Average ``cols`` to roughly ``POINTS_PER_MIN`` per minute."""
        if df.empty:
            return df
        if keep is None:
            keep = []

        if "rel_min" in df.columns:
            bucket = 1 / max(POINTS_PER_MIN, 1)
            df = df.assign(_bucket=(df["rel_min"] // bucket).astype(int))
        else:
            n = len(df)
            k = max(1, n // AVG_BUCKET)
            df = df.assign(_bucket=(df.index // k))

        g_cols = ["_bucket"] + [c for c in keep if c in df.columns]
        g = df.groupby(g_cols)

        agg = {c: "mean" for c in cols if c in df.columns}
        if "Time" in df.columns:
            agg["Time"] = "first"

        out = g.agg(agg).reset_index()
        
        out = out.drop(columns="_bucket", errors="ignore")

 
        return out
    def _write_excel(self, tag):
        if not self.logging: return
        try: import xlsxwriter
        except ImportError: self.log_msg("XlsxWriter missing"); return

        ts=datetime.now().strftime("%Y-%m-%d_%H%M")
        path=Path(CSV_DIR)/f"monitor_{ts}_{tag}.xlsx"

        if tag == "stress" and self.stress_start:
            offset = max(10, self.wait_minutes_effective)
            t0 = self.stress_start - timedelta(minutes=offset)
            t1 = self.cool_end

            # ensure any buffered rows are flushed before reading
            self.soc_writer.flush()
            self.fluid_writer.flush()

            soc_cols = ["Time", "Node", "Temp", "Clock"]
            if RAW_SOC.exists() and RAW_SOC.stat().st_size > 0:
                try:
                    soc_src = pd.read_csv(RAW_SOC, parse_dates=["Time"])
                except Exception:
                    soc_src = pd.DataFrame(columns=soc_cols)
            else:
                soc_src = pd.DataFrame(columns=soc_cols)
            cl = soc_src[(soc_src.Time >= t0) & (soc_src.Time <= t1)].reset_index(drop=True) if not soc_src.empty else soc_src

            fl_cols = ["Time", "Channel", "Temp"]
            if RAW_FLUID.exists() and RAW_FLUID.stat().st_size > 0:
                try:
                    fl_src = pd.read_csv(RAW_FLUID, parse_dates=["Time"])
                except Exception:
                    fl_src = pd.DataFrame(columns=fl_cols)
            else:
                fl_src = pd.DataFrame(columns=fl_cols)
            fl = fl_src[(fl_src.Time >= t0) & (fl_src.Time <= t1)].reset_index(drop=True) if not fl_src.empty else fl_src
        else:
            with self.cl_lock: cl=self.cl_df.copy().reset_index(drop=True)
            with self.tc_lock: fl=self.tc_df.copy().reset_index(drop=True)

        # relative-minute column
        cl["Time"] = pd.to_datetime(cl["Time"], errors="coerce")
        fl["Time"] = pd.to_datetime(fl["Time"], errors="coerce")
        ref=self.stress_start if self.stress_start else cl["Time"].iloc[0]
        cl["rel_min"]=cl["Time"].sub(ref).dt.total_seconds().div(60)
        fl["rel_min"]=fl["Time"].sub(ref).dt.total_seconds().div(60)

        # average down to keep file light while retaining node/channel info
        cl = self._avg_df(cl, ["Temp", "Clock", "rel_min"], ["Node"])
        fl = self._avg_df(fl, ["Temp", "rel_min"], ["Channel"])

        # pivot (ensure only numeric columns are aggregated)
        cl["Clock"] = pd.to_numeric(cl["Clock"], errors="coerce")
        cl["Temp"] = pd.to_numeric(cl["Temp"], errors="coerce")
        cw = (
            cl[["rel_min", "Node", "Clock"]]
            .pivot_table(index="rel_min", columns="Node", values="Clock")
            .reset_index()
        )
        sw = (
            cl[["rel_min", "Node", "Temp"]]
            .pivot_table(index="rel_min", columns="Node", values="Temp")
            .reset_index()
        )
        if not fl.empty:
            fl["Temp"] = pd.to_numeric(fl["Temp"], errors="coerce")
            fl = (
                fl[["rel_min", "Channel", "Temp"]]
                .pivot_table(index="rel_min", columns="Channel", values="Temp")
                .reindex(columns=sorted(self.NAMES.keys()), fill_value=pd.NA)
                .reset_index()
                .rename(columns=self.NAMES)
            )

        with pd.ExcelWriter(path,engine="xlsxwriter") as xw:
            cw.to_excel(xw,"clock",index=False); sw.to_excel(xw,"soc",index=False)
            if not fl.empty: fl.to_excel(xw,"fluid",index=False)
            wb=xw.book
            def chart(df, sheet, y):
                """Insert a line chart if there is at least one data series."""
                if df.empty or len(df.columns) <= 1:
                    return
                rows = len(df)
                # Use an XY scatter chart so the X axis is treated as numeric
                # rather than categorical. This ensures correct plotting when
                # the time points are not equally spaced.
                ch = wb.add_chart({
                    "type": "scatter",
                    "subtype": "straight",  # lines only, no markers
                })
                ch.show_blanks_as("span")  # connect across blank cells
                for col in range(1, len(df.columns)):
                    ch.add_series(
                        {
                            "name": [sheet, 0, col],
                            "categories": [sheet, 1, 0, rows, 0],  # rel_min as X
                            "values": [sheet, 1, col, rows, col],
                        }
                    )
                ch.set_x_axis({"name": self.tr("minutes")})
                ch.set_y_axis({"name": y})
                xw.sheets[sheet].insert_chart("H2", ch)
            chart(cw,"clock","GHz"); chart(sw,"soc","°C")
            if not fl.empty: chart(fl,"fluid","°C")
            for s in ("clock","soc","fluid"):
                if s in xw.sheets: xw.sheets[s].freeze_panes(1,1)
        self.log_msg(f"XLSX saved ({tag})")

        # flush raw CSVs to keep them small
        RAW_SOC.unlink(missing_ok=True); RAW_FLUID.unlink(missing_ok=True)

    # ───── clear ─────
    def _clear_all(self):
        if not messagebox.askyesno(self.tr("Clear data"), self.tr("Remove all collected data?")):
            return
        with self.cl_lock:
            self.cl_df = self.cl_df.iloc[0:0]
        with self.tc_lock:
            self.tc_df = self.tc_df.iloc[0:0]
        self.log_msg("Data cleared")

    # ───── shutdown ─────
    def close(self) -> None:
        """Stop workers and flush files."""
        if not self.alive:
            return
        self.alive = False
        self.stop.set()
        self._kill_stress()
        if hasattr(self, "plot_id"):
            try:
                self.after_cancel(self.plot_id)
            except tk.TclError:
                pass
        if hasattr(self, "tick_id"):
            try:
                self.after_cancel(self.tick_id)
            except tk.TclError:
                pass
        self.soc_writer.close()
        self.fluid_writer.close()
        if self.logging:
            tag = "stress" if self.log_stress else "manual"
            self._write_excel(tag)

# ───────── ENTRY ─────────
def main():
    parser = argparse.ArgumentParser(description="Cluster dashboard")
    parser.add_argument(
        "--nodes-file", default=DEFAULT_NODES_FILE, help="File with node IPs"
    )
    parser.add_argument(
        "--csv-dir", default=DEFAULT_CSV_DIR, help="Directory for CSV output"
    )
    parser.add_argument(
        "--headless", action="store_true", help="Run without showing the GUI"
    )
    args = parser.parse_args()

    global NODES_FILE, CSV_DIR, RAW_SOC, RAW_FLUID
    NODES_FILE = args.nodes_file
    CSV_DIR = Path(args.csv_dir)
    CSV_DIR.mkdir(parents=True, exist_ok=True)
    RAW_SOC = CSV_DIR / "raw_soc.csv"
    RAW_FLUID = CSV_DIR / "raw_fluid.csv"

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s: %(message)s",
        handlers=[
            logging.FileHandler(CSV_DIR / "monitor.log"),
            logging.StreamHandler(sys.stdout),
        ],
    )

    root = tk.Tk()
    root.title("Cluster Dashboard")
    root.geometry("1100x760")
    if args.headless:
        root.withdraw()
    ui = UnifiedMonitor(root, headless=args.headless)

    def _close() -> None:
        ui.close()
        root.destroy()

    root.protocol("WM_DELETE_WINDOW", _close)
    try:
        root.mainloop()
    finally:
        ui.close()

if __name__=="__main__":
    main()

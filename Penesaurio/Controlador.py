import os
import sqlite3
from datetime import datetime
import tkinter as tk
from tkinter import ttk
from werkzeug.security import generate_password_hash

try:
    from app import init_db, DB_PATH, encrypt_password, decrypt_password
except Exception:
    DB_PATH = os.path.join(os.path.dirname(__file__), "servicios.db")

    def init_db():
        return None

    def encrypt_password(value):
        return value

    def decrypt_password(value):
        return value


def get_conn():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def ensure_config(cur):
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS config (
            key TEXT PRIMARY KEY,
            value TEXT
        )
        """
    )


def get_config(cur, key, default=None):
    row = cur.execute(
        "SELECT value FROM config WHERE key = ?",
        (key,),
    ).fetchone()
    if row is None or row["value"] is None:
        return default
    return row["value"]


def set_config(cur, key, value):
    cur.execute(
        "INSERT OR REPLACE INTO config (key, value) VALUES (?, ?)",
        (key, str(value)),
    )


def parse_service_id(text):
    if "#" not in text:
        return None
    part = text.split("#", 1)[1]
    digits = []
    for ch in part:
        if ch.isdigit():
            digits.append(ch)
        else:
            break
    if not digits:
        return None
    return int("".join(digits))


def parse_mensaje_cliente(text):
    nombre = ""
    direccion = ""
    if text:
        parts = [p.strip() for p in text.split("|")]
        for part in parts:
            if ":" not in part:
                continue
            key, value = part.split(":", 1)
            key = key.strip().lower()
            value = value.strip()
            if key == "nombre":
                nombre = value
            elif key == "direccion":
                direccion = value
    return nombre, direccion


def hide_console_window():
    if os.name != "nt":
        return
    try:
        import ctypes

        hwnd = ctypes.windll.kernel32.GetConsoleWindow()
        if hwnd:
            ctypes.windll.user32.ShowWindow(hwnd, 0)
            ctypes.windll.kernel32.CloseHandle(hwnd)
    except Exception:
        pass


class ControladorApp:
    def __init__(self, root):
        self.root = root
        self.colors = {
            "bg": "#0b0f14",
            "panel": "#121822",
            "panel_alt": "#0f141d",
            "card": "#141b26",
            "border": "#1f2a36",
            "accent": "#f2c744",
            "accent_soft": "#f8e7a7",
            "text": "#e6eef8",
            "muted": "#9aa8b4",
            "success": "#2ecc71",
            "danger": "#e74c3c",
            "info": "#4da3ff",
            "warning": "#f39c12",
        }
        self.fonts = {
            "title": ("Bahnschrift", 20, "bold"),
            "subtitle": ("Segoe UI", 10),
            "section": ("Bahnschrift", 12, "bold"),
            "label": ("Segoe UI", 10),
            "small": ("Segoe UI", 9),
            "digit": ("Bahnschrift", 18, "bold"),
        }

        self.root.title("Centro de Control | Servicios")
        self.root.geometry("1020x720")
        self.root.minsize(920, 680)
        self.root.configure(bg=self.colors["bg"])

        init_db()

        self.qty_var = tk.StringVar(value="1")
        self.twilio_enabled = True

        self.build_ui()
        self.load_twilio_state()
        self.db_ok = True
        self.schedule_refresh()

    def apply_theme(self):
        style = ttk.Style()
        try:
            style.theme_use("clam")
        except Exception:
            pass

        style.configure(
            "Center.TNotebook",
            background=self.colors["bg"],
            borderwidth=0,
        )
        style.configure(
            "Center.TNotebook.Tab",
            background=self.colors["panel_alt"],
            foreground=self.colors["muted"],
            padding=[14, 8],
            focuscolor="",
        )
        style.map(
            "Center.TNotebook.Tab",
            background=[("selected", self.colors["card"])],
            foreground=[("selected", self.colors["text"])],
        )
        style.configure(
            "Center.Treeview",
            background=self.colors["panel"],
            fieldbackground=self.colors["panel"],
            foreground=self.colors["text"],
            bordercolor=self.colors["border"],
            rowheight=28,
        )
        style.configure(
            "Center.Treeview.Heading",
            background=self.colors["card"],
            foreground=self.colors["accent"],
            relief="flat",
        )
        style.map(
            "Center.Treeview",
            background=[("selected", self.colors["accent"])],
            foreground=[("selected", "#1b1404")],
        )

    def create_status_card(self, parent, title, value, accent, column):
        frame = tk.Frame(
            parent,
            bg=self.colors["card"],
            highlightbackground=self.colors["border"],
            highlightthickness=1,
        )
        frame.grid(row=0, column=column, sticky="nsew", padx=6)

        bar = tk.Frame(frame, bg=accent, height=4)
        bar.pack(fill="x")

        title_label = tk.Label(
            frame,
            text=title,
            fg=self.colors["muted"],
            bg=self.colors["card"],
            font=self.fonts["small"],
        )
        title_label.pack(anchor="w", padx=12, pady=(8, 0))

        value_label = tk.Label(
            frame,
            text=value,
            fg=self.colors["text"],
            bg=self.colors["card"],
            font=self.fonts["section"],
        )
        value_label.pack(anchor="w", padx=12, pady=(2, 10))
        return bar, value_label

    def create_card(self, parent, title, subtitle=None):
        card = tk.Frame(
            parent,
            bg=self.colors["card"],
            highlightbackground=self.colors["border"],
            highlightthickness=1,
        )
        header = tk.Frame(card, bg=self.colors["card"])
        header.pack(fill="x", padx=14, pady=(12, 6))

        title_label = tk.Label(
            header,
            text=title,
            fg=self.colors["text"],
            bg=self.colors["card"],
            font=self.fonts["section"],
        )
        title_label.pack(side="left")

        if subtitle:
            subtitle_label = tk.Label(
                header,
                text=subtitle,
                fg=self.colors["muted"],
                bg=self.colors["card"],
                font=self.fonts["small"],
            )
            subtitle_label.pack(side="left", padx=8)

        body = tk.Frame(card, bg=self.colors["card"])
        body.pack(fill="both", expand=True, padx=14, pady=(0, 12))
        return card, body

    def create_button(self, parent, text, command, variant="secondary"):
        if variant == "primary":
            bg = self.colors["accent"]
            fg = "#1b1404"
            active_bg = self.colors["accent_soft"]
        elif variant == "success":
            bg = self.colors["success"]
            fg = "#0b1b0f"
            active_bg = "#45e07d"
        elif variant == "danger":
            bg = self.colors["danger"]
            fg = "#fff"
            active_bg = "#ff6b5a"
        else:
            bg = self.colors["panel_alt"]
            fg = self.colors["text"]
            active_bg = self.colors["card"]

        btn = tk.Button(
            parent,
            text=text,
            command=command,
            bg=bg,
            fg=fg,
            activebackground=active_bg,
            activeforeground=fg,
            relief="flat",
            bd=0,
            highlightthickness=0,
            cursor="hand2",
            padx=12,
            pady=6,
            font=self.fonts["label"],
        )
        return btn

    def update_status_cards(self):
        now = datetime.now().strftime("%H:%M:%S")
        if hasattr(self, "card_sync_value"):
            self.card_sync_value.config(text=f"Actualizado {now}")

        if hasattr(self, "card_db_value"):
            if self.db_ok:
                self.card_db_value.config(
                    text="CONECTADA",
                    fg=self.colors["success"],
                )
                self.db_status_bar.config(bg=self.colors["success"])
            else:
                self.card_db_value.config(
                    text="ERROR",
                    fg=self.colors["danger"],
                )
                self.db_status_bar.config(bg=self.colors["danger"])

    def build_ui(self):
        self.apply_theme()
        bg = self.colors["bg"]

        header = tk.Frame(self.root, bg=bg)
        header.pack(fill="x", padx=20, pady=(16, 6))

        title = tk.Label(
            header,
            text="Centro de Control",
            fg=self.colors["accent"],
            bg=bg,
            font=self.fonts["title"],
        )
        title.pack(anchor="w")

        subtitle = tk.Label(
            header,
            text="Monitoreo y operaciones en tiempo real",
            fg=self.colors["muted"],
            bg=bg,
            font=self.fonts["subtitle"],
        )
        subtitle.pack(anchor="w", pady=(4, 0))

        status_row = tk.Frame(self.root, bg=bg)
        status_row.pack(fill="x", padx=20, pady=(0, 12))
        for idx in range(4):
            status_row.grid_columnconfigure(idx, weight=1)

        self.twilio_status_bar, self.card_twilio_value = self.create_status_card(
            status_row,
            "TWILIO",
            "ENCENDIDO",
            self.colors["success"],
            0,
        )
        self.db_status_bar, self.card_db_value = self.create_status_card(
            status_row,
            "BASE DE DATOS",
            "CONECTADA",
            self.colors["info"],
            1,
        )
        self.sync_status_bar, self.card_sync_value = self.create_status_card(
            status_row,
            "SINCRONÍA",
            "AUTO 5s",
            self.colors["accent"],
            2,
        )
        self.driver_status_bar, self.card_driver_value = self.create_status_card(
            status_row,
            "CONDUCTOR",
            "Sin conexión",
            self.colors["panel_alt"],
            3,
        )

        self.notebook = ttk.Notebook(self.root, style="Center.TNotebook")
        self.notebook.pack(fill="both", expand=True, padx=16, pady=(0, 16))

        self.tab_servicios = tk.Frame(self.notebook, bg=bg)
        self.tab_usuarios = tk.Frame(self.notebook, bg=bg)
        self.notebook.add(self.tab_servicios, text="Centro")
        self.notebook.add(self.tab_usuarios, text="Usuarios")

        self.build_services_tab()
        self.build_users_tab()

    def build_services_tab(self):
        bg = self.colors["bg"]

        left_col = tk.Frame(self.tab_servicios, bg=bg)
        right_col = tk.Frame(self.tab_servicios, bg=bg)
        left_col.grid(row=0, column=0, sticky="nsew", padx=(16, 8), pady=16)
        right_col.grid(row=0, column=1, sticky="nsew", padx=(8, 16), pady=16)
        self.tab_servicios.grid_columnconfigure(0, weight=1)
        self.tab_servicios.grid_columnconfigure(1, weight=1)
        self.tab_servicios.grid_rowconfigure(0, weight=1)

        twilio_card, twilio_body = self.create_card(
            left_col,
            "Canal Twilio",
            "Mensajería en vivo",
        )
        twilio_card.pack(fill="x", pady=(0, 12))

        status_row = tk.Frame(twilio_body, bg=self.colors["card"])
        status_row.pack(fill="x", pady=(0, 10))

        self.led = tk.Canvas(
            status_row,
            width=20,
            height=20,
            bg=self.colors["card"],
            highlightthickness=0,
        )
        self.led.pack(side="left")
        self.led_circle = self.led.create_oval(2, 2, 18, 18, fill=self.colors["success"])

        self.twilio_label = tk.Label(
            status_row,
            text="Twilio: ENCENDIDO",
            fg=self.colors["text"],
            bg=self.colors["card"],
            font=self.fonts["label"],
        )
        self.twilio_label.pack(side="left", padx=10)

        self.twilio_btn = self.create_button(
            status_row,
            "Apagar / Encender",
            self.toggle_twilio,
        )
        self.twilio_btn.pack(side="right")

        gen_card, gen_body = self.create_card(
            left_col,
            "Generador de servicios",
            "Inyección rápida de pedidos",
        )
        gen_card.pack(fill="both", expand=True)

        qty_label = tk.Label(
            gen_body,
            text="Cantidad de servicios",
            fg=self.colors["muted"],
            bg=self.colors["card"],
            font=self.fonts["small"],
        )
        qty_label.pack(anchor="w")

        qty_entry = tk.Entry(
            gen_body,
            textvariable=self.qty_var,
            justify="center",
            font=self.fonts["digit"],
            bg=self.colors["panel"],
            fg=self.colors["accent"],
            relief="flat",
            highlightthickness=1,
            highlightbackground=self.colors["border"],
            highlightcolor=self.colors["accent"],
            insertbackground=self.colors["accent"],
        )
        qty_entry.pack(fill="x", pady=(6, 12))

        pad = tk.Frame(gen_body, bg=self.colors["card"])
        pad.pack(fill="both")

        buttons = [
            ("1", lambda: self.append_qty("1"), "secondary"),
            ("2", lambda: self.append_qty("2"), "secondary"),
            ("3", lambda: self.append_qty("3"), "secondary"),
            ("4", lambda: self.append_qty("4"), "secondary"),
            ("5", lambda: self.append_qty("5"), "secondary"),
            ("6", lambda: self.append_qty("6"), "secondary"),
            ("7", lambda: self.append_qty("7"), "secondary"),
            ("8", lambda: self.append_qty("8"), "secondary"),
            ("9", lambda: self.append_qty("9"), "secondary"),
            ("Borrar", self.clear_qty, "danger"),
            ("0", lambda: self.append_qty("0"), "secondary"),
            ("Enviar", self.send_services, "success"),
        ]

        for idx, (label, cmd, variant) in enumerate(buttons):
            r = idx // 3
            c = idx % 3
            btn = self.create_button(pad, label, cmd, variant=variant)
            btn.config(width=8, height=2)
            btn.grid(row=r, column=c, padx=4, pady=4, sticky="nsew")

        for r in range(4):
            pad.grid_rowconfigure(r, weight=1)
        for c in range(3):
            pad.grid_columnconfigure(c, weight=1)

        hint = tk.Label(
            gen_body,
            text="Tip: usa el keypad y Enviar para crear servicios de prueba.",
            fg=self.colors["muted"],
            bg=self.colors["card"],
            font=self.fonts["small"],
        )
        hint.pack(anchor="w", pady=(10, 0))

        log_card, log_body = self.create_card(
            right_col,
            "Consola de eventos",
            "Servicios disponibles en tiempo real",
        )
        log_card.pack(fill="both", expand=True)

        list_header = tk.Frame(log_body, bg=self.colors["card"])
        list_header.pack(fill="x", pady=(6, 10))

        list_title = tk.Label(
            list_header,
            text="Servicios disponibles",
            fg=self.colors["muted"],
            bg=self.colors["card"],
            font=self.fonts["small"],
        )
        list_title.pack(side="left")

        self.log_count_label = tk.Label(
            list_header,
            text="0",
            fg=self.colors["accent"],
            bg=self.colors["card"],
            font=self.fonts["small"],
        )
        self.log_count_label.pack(side="right")

        divider = tk.Frame(log_body, bg=self.colors["border"], height=2)
        divider.pack(fill="x", pady=(0, 14))

        list_shell = tk.Frame(
            log_body,
            bg=self.colors["panel"],
            highlightbackground=self.colors["border"],
            highlightthickness=1,
        )
        list_shell.pack(fill="both", expand=True, pady=(0, 10))

        list_frame = tk.Frame(list_shell, bg=self.colors["panel"])
        list_frame.pack(fill="both", expand=True, padx=8, pady=8)

        self.log_list = tk.Listbox(
            list_frame,
            bg=self.colors["panel"],
            fg=self.colors["text"],
            selectbackground=self.colors["accent"],
            selectforeground="#1b1404",
            relief="flat",
            highlightthickness=1,
            highlightbackground=self.colors["border"],
            activestyle="none",
            font=self.fonts["small"],
        )
        self.log_list.pack(side="left", fill="both", expand=True)

        scrollbar = tk.Scrollbar(list_frame, command=self.log_list.yview)
        scrollbar.pack(side="right", fill="y")
        self.log_list.config(yscrollcommand=scrollbar.set)

        action_row = tk.Frame(log_body, bg=self.colors["card"])
        action_row.pack(fill="x")

        delete_btn = self.create_button(
            action_row,
            "Eliminar seleccionado",
            self.delete_selected,
            variant="danger",
        )
        delete_btn.pack(side="left")

        refresh_btn = self.create_button(
            action_row,
            "Recargar",
            self.refresh_log,
        )
        refresh_btn.pack(side="left", padx=8)

    def build_users_tab(self):
        bg = self.colors["bg"]

        header = tk.Frame(self.tab_usuarios, bg=bg)
        header.pack(fill="x", padx=16, pady=(16, 8))

        title = tk.Label(
            header,
            text="Conductores registrados",
            fg=self.colors["accent"],
            bg=bg,
            font=self.fonts["section"],
        )
        title.pack(side="left")

        refresh_btn = self.create_button(
            header,
            "Recargar",
            self.refresh_users,
        )
        refresh_btn.pack(side="right")

        table_frame = tk.Frame(self.tab_usuarios, bg=bg)
        table_frame.pack(fill="both", expand=True, padx=16, pady=(0, 16))

        columns = (
            "usuario",
            "nombre",
            "placa",
            "vehiculo",
            "modelo",
            "estado",
            "servicios",
            "ultimo",
            "ganancias",
            "cred",
        )
        self.users_table = ttk.Treeview(
            table_frame,
            columns=columns,
            show="headings",
            style="Center.Treeview",
        )
        self.users_table.heading("usuario", text="Usuario")
        self.users_table.heading("nombre", text="Nombre")
        self.users_table.heading("placa", text="Placa")
        self.users_table.heading("vehiculo", text="Vehículo")
        self.users_table.heading("modelo", text="Modelo")
        self.users_table.heading("estado", text="Estado")
        self.users_table.heading("servicios", text="Servicios")
        self.users_table.heading("ultimo", text="Último servicio")
        self.users_table.heading("ganancias", text="Ganancias")
        self.users_table.heading("cred", text="Credenciales")

        self.users_table.column("usuario", width=120, anchor="w")
        self.users_table.column("nombre", width=160, anchor="w")
        self.users_table.column("placa", width=90, anchor="center")
        self.users_table.column("vehiculo", width=160, anchor="w")
        self.users_table.column("modelo", width=80, anchor="center")
        self.users_table.column("estado", width=90, anchor="center")
        self.users_table.column("servicios", width=80, anchor="center")
        self.users_table.column("ultimo", width=120, anchor="center")
        self.users_table.column("ganancias", width=110, anchor="center")
        self.users_table.column("cred", width=90, anchor="center")

        self.users_columns_meta = {
            "usuario": {"weight": 1.1, "min": 90},
            "nombre": {"weight": 1.6, "min": 130},
            "placa": {"weight": 0.8, "min": 70},
            "vehiculo": {"weight": 1.6, "min": 130},
            "modelo": {"weight": 0.8, "min": 70},
            "estado": {"weight": 0.9, "min": 80},
            "servicios": {"weight": 0.8, "min": 70},
            "ultimo": {"weight": 1.0, "min": 110},
            "ganancias": {"weight": 1.0, "min": 90},
            "cred": {"weight": 0.6, "min": 60},
        }
        for col, meta in self.users_columns_meta.items():
            self.users_table.column(col, width=meta["min"], minwidth=meta["min"])

        v_scroll = tk.Scrollbar(table_frame, command=self.users_table.yview)
        h_scroll = tk.Scrollbar(table_frame, orient="horizontal", command=self.users_table.xview)
        self.users_table.config(yscrollcommand=v_scroll.set, xscrollcommand=h_scroll.set)

        self.users_table.grid(row=0, column=0, sticky="nsew")
        v_scroll.grid(row=0, column=1, sticky="ns")
        h_scroll.grid(row=1, column=0, sticky="ew")
        table_frame.grid_rowconfigure(0, weight=1)
        table_frame.grid_columnconfigure(0, weight=1)

        self.users_table.bind("<Configure>", self.on_users_table_resize)

        self.cred_col_index = columns.index("cred") + 1
        self.users_table.bind("<Button-1>", self.on_users_table_click)

        footnote = tk.Label(
            self.tab_usuarios,
            text="Métricas en tiempo real de conductores: servicios tomados y ganancias.",
            fg=self.colors["muted"],
            bg=bg,
            font=self.fonts["small"],
        )
        footnote.pack(anchor="w", padx=16, pady=(0, 12))

    def on_users_table_resize(self, event):
        if getattr(self, "_resizing_users_table", False):
            return
        if not hasattr(self, "users_columns_meta"):
            return
        total = max(event.width - 2, 100)
        min_total = sum(meta["min"] for meta in self.users_columns_meta.values())
        weight_sum = sum(meta["weight"] for meta in self.users_columns_meta.values())

        self._resizing_users_table = True
        try:
            if total <= min_total:
                for col, meta in self.users_columns_meta.items():
                    self.users_table.column(col, width=meta["min"], stretch=False)
                return

            extra = total - min_total
            for col, meta in self.users_columns_meta.items():
                width = meta["min"] + int(extra * meta["weight"] / weight_sum)
                self.users_table.column(col, width=width, stretch=False)
        finally:
            self._resizing_users_table = False

    def format_ts_short(self, ts_value):
        if not ts_value:
            return "-"
        try:
            parsed = datetime.strptime(ts_value, "%Y-%m-%d %H:%M:%S")
            return parsed.strftime("%d/%m %H:%M")
        except Exception:
            return ts_value

    def format_elapsed(self, ts_value):
        if not ts_value:
            return "--:--"
        try:
            parsed = datetime.strptime(ts_value, "%Y-%m-%d %H:%M:%S")
        except Exception:
            return "--:--"
        delta = datetime.now() - parsed
        total = int(delta.total_seconds())
        if total < 0:
            total = 0
        minutes, seconds = divmod(total, 60)
        hours, minutes = divmod(minutes, 60)
        if hours > 0:
            return f"{hours:d}:{minutes:02d}:{seconds:02d}"
        return f"{minutes:02d}:{seconds:02d}"

    def format_cop(self, value):
        try:
            value_int = int(value)
        except Exception:
            value_int = 0
        return f"${value_int:,.0f}"

    def on_users_table_click(self, event):
        region = self.users_table.identify("region", event.x, event.y)
        if region != "cell":
            return
        column = self.users_table.identify_column(event.x)
        if column != f"#{self.cred_col_index}":
            return
        row_id = self.users_table.identify_row(event.y)
        if not row_id:
            return
        self.show_credentials(row_id)

    def show_credentials(self, conductor_id):
        conn = get_conn()
        row = conn.execute(
            "SELECT usuario, password_enc FROM conductores WHERE id = ?",
            (conductor_id,),
        ).fetchone()
        conn.close()
        if row is None:
            return

        top = tk.Toplevel(self.root)
        top.title("Credenciales del conductor")
        top.geometry("380x260")
        top.configure(bg=self.colors["bg"])
        top.transient(self.root)
        top.grab_set()

        title = tk.Label(
            top,
            text="Credenciales",
            fg=self.colors["accent"],
            bg=self.colors["bg"],
            font=self.fonts["section"],
        )
        title.pack(anchor="w", padx=16, pady=(16, 4))

        user_label = tk.Label(
            top,
            text=f"Usuario: {row['usuario']}",
            fg=self.colors["text"],
            bg=self.colors["bg"],
            font=self.fonts["label"],
        )
        user_label.pack(anchor="w", padx=16, pady=(0, 10))

        note = tk.Label(
            top,
            text="La contraseña actual está cifrada.\nPuedes asignar una nueva abajo.",
            fg=self.colors["muted"],
            bg=self.colors["bg"],
            font=self.fonts["small"],
            justify="left",
        )
        note.pack(anchor="w", padx=16, pady=(0, 10))

        stored_password = ""
        try:
            if row["password_enc"]:
                stored_password = decrypt_password(row["password_enc"]) or ""
        except Exception:
            stored_password = ""

        password_visible = tk.BooleanVar(value=False)
        password_text = tk.StringVar(
            value="********" if stored_password else "Sin registro"
        )

        current_label = tk.Label(
            top,
            text="Contraseña actual",
            fg=self.colors["accent"],
            bg=self.colors["bg"],
            font=self.fonts["small"],
        )
        current_label.pack(anchor="w", padx=16)

        current_row = tk.Frame(top, bg=self.colors["bg"])
        current_row.pack(fill="x", padx=16, pady=(6, 12))

        current_value = tk.Label(
            current_row,
            textvariable=password_text,
            fg=self.colors["text"],
            bg=self.colors["bg"],
            font=self.fonts["label"],
        )
        current_value.pack(side="left")

        def toggle_password_view():
            if not stored_password:
                return
            if password_visible.get():
                password_visible.set(False)
                password_text.set("********")
                toggle_btn.config(text="Ver")
            else:
                password_visible.set(True)
                password_text.set(stored_password)
                toggle_btn.config(text="Ocultar")

        toggle_btn = self.create_button(
            current_row,
            "Ver",
            toggle_password_view,
        )
        if not stored_password:
            toggle_btn.config(state="disabled")
        toggle_btn.pack(side="right")

        pass_label = tk.Label(
            top,
            text="Nueva contraseña",
            fg=self.colors["accent"],
            bg=self.colors["bg"],
            font=self.fonts["small"],
        )
        pass_label.pack(anchor="w", padx=16)

        pass_entry = tk.Entry(
            top,
            show="*",
            bg=self.colors["panel"],
            fg=self.colors["text"],
            relief="flat",
            highlightthickness=1,
            highlightbackground=self.colors["border"],
            highlightcolor=self.colors["accent"],
            insertbackground=self.colors["accent"],
        )
        pass_entry.pack(fill="x", padx=16, pady=(6, 12))

        feedback = tk.Label(
            top,
            text="",
            fg=self.colors["accent_soft"],
            bg=self.colors["bg"],
            font=self.fonts["small"],
        )
        feedback.pack(anchor="w", padx=16, pady=(0, 8))

        def update_password():
            nonlocal stored_password
            new_pass = pass_entry.get().strip()
            if not new_pass:
                feedback.config(text="Escribe una contraseña nueva.")
                return
            enc_value = None
            try:
                enc_value = encrypt_password(new_pass)
            except Exception:
                enc_value = None
            conn = get_conn()
            conn.execute(
                "UPDATE conductores SET password_hash = ?, password_enc = ? WHERE id = ?",
                (generate_password_hash(new_pass), enc_value, conductor_id),
            )
            conn.commit()
            conn.close()
            feedback.config(text="Contraseña actualizada.")
            stored_password = new_pass
            password_visible.set(False)
            password_text.set("********")
            toggle_btn.config(text="Ver")

        btn_frame = tk.Frame(top, bg=self.colors["bg"])
        btn_frame.pack(fill="x", padx=16, pady=(6, 16))

        save_btn = self.create_button(
            btn_frame,
            "Actualizar clave",
            update_password,
            variant="success",
        )
        save_btn.pack(side="left")

        close_btn = self.create_button(
            btn_frame,
            "Cerrar",
            top.destroy,
        )
        close_btn.pack(side="right")

    def append_qty(self, digit):
        current = self.qty_var.get().strip()
        if current == "0":
            current = ""
        self.qty_var.set(f"{current}{digit}")

    def clear_qty(self):
        self.qty_var.set("")

    def load_twilio_state(self):
        conn = get_conn()
        cur = conn.cursor()
        ensure_config(cur)
        value = get_config(cur, "twilio_enabled", "1")
        conn.close()
        enabled = str(value).strip().lower() not in {"0", "false", "off", "no"}
        self.set_led(enabled)

    def set_led(self, enabled):
        self.twilio_enabled = enabled
        color = self.colors["success"] if enabled else self.colors["danger"]
        label = "Twilio: ENCENDIDO" if enabled else "Twilio: APAGADO"
        self.led.itemconfig(self.led_circle, fill=color)
        self.twilio_label.config(text=label, fg=self.colors["text"])
        if hasattr(self, "card_twilio_value"):
            self.card_twilio_value.config(
                text="ENCENDIDO" if enabled else "APAGADO",
                fg=color,
            )
        if hasattr(self, "twilio_status_bar"):
            self.twilio_status_bar.config(bg=color)

    def toggle_twilio(self):
        enabled = not self.twilio_enabled
        conn = get_conn()
        cur = conn.cursor()
        ensure_config(cur)
        set_config(cur, "twilio_enabled", "1" if enabled else "0")
        conn.commit()
        conn.close()
        self.set_led(enabled)

    def send_services(self):
        qty_text = self.qty_var.get().strip() or "1"
        try:
            qty = int(qty_text)
        except ValueError:
            self.log_list.insert("end", "Cantidad inválida.")
            return
        if qty <= 0:
            self.log_list.insert("end", "Cantidad inválida.")
            return
        if qty > 200:
            self.log_list.insert("end", "Cantidad muy alta (máx 200).")
            return

        conn = get_conn()
        cur = conn.cursor()
        ensure_config(cur)
        counter = get_config(cur, "test_counter", "1")
        try:
            next_counter = int(counter)
        except ValueError:
            next_counter = 1

        created = []
        for i in range(qty):
            idx = next_counter + i
            nombre = f"Prueba {idx}"
            direccion = f"Direccion Prueba {idx}"
            telefono = f"whatsapp:+57{3000000000 + idx}"
            mensaje_cliente = f"Nombre: {nombre} | Direccion: {direccion}"
            fecha = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            cur.execute(
                """
                INSERT INTO pedidos (cliente_telefono, mensaje_cliente, estado, timestamp)
                VALUES (?, ?, 'Disponible', ?)
                """,
                (telefono, mensaje_cliente, fecha),
            )
            pedido_id = cur.lastrowid
            created.append((pedido_id, nombre, direccion, telefono))

        next_counter = next_counter + qty
        set_config(cur, "test_counter", str(next_counter))
        conn.commit()
        conn.close()

        for pedido_id, nombre, direccion, telefono in created:
            self.log_list.insert(
                "end",
                f"Servicio #{pedido_id} - {nombre} - {direccion} - {telefono}",
            )
        self.refresh_log()

    def delete_selected(self):
        selection = list(self.log_list.curselection())
        if not selection:
            return
        selection.sort(reverse=True)
        for idx in selection:
            text = self.log_list.get(idx)
            pedido_id = parse_service_id(text)
            if pedido_id is not None:
                conn = get_conn()
                cur = conn.cursor()
                cur.execute("DELETE FROM pedidos WHERE id = ?", (pedido_id,))
                conn.commit()
                conn.close()
            self.log_list.delete(idx)
        self.refresh_log()

    def refresh_log(self):
        try:
            conn = get_conn()
            rows = conn.execute(
                """
                SELECT id, cliente_telefono, mensaje_cliente, timestamp
                FROM pedidos
                WHERE lower(estado) IN ('disponible', 'pendiente')
                ORDER BY id DESC
                """
            ).fetchall()
            conn.close()
        except Exception:
            rows = []
            self.db_ok = False

        self.log_list.delete(0, "end")
        for row in rows:
            nombre, direccion = parse_mensaje_cliente(row["mensaje_cliente"] or "")
            nombre = nombre or "Sin nombre"
            direccion = direccion or "Sin dirección"
            telefono = row["cliente_telefono"] or ""
            elapsed = self.format_elapsed(row["timestamp"])
            self.log_list.insert(
                "end",
                f"Servicio #{row['id']} - {nombre} - {direccion} - {telefono} | Tiempo {elapsed}",
            )
        if hasattr(self, "log_count_label"):
            self.log_count_label.config(text=str(len(rows)))

    def refresh_users(self):
        try:
            conn = get_conn()
            rows = conn.execute(
                """
                SELECT
                    c.id,
                    c.usuario,
                    c.nombre_real,
                    c.placa,
                    c.vehiculo,
                    c.modelo,
                    c.active_pedido_id,
                    (
                        SELECT COUNT(*)
                        FROM pedidos p
                        WHERE (
                            CASE
                                WHEN c.placa IS NOT NULL AND c.placa != ''
                                    THEN p.conductor_placa = c.placa
                                ELSE p.conductor_nombre = c.nombre_real
                            END
                        )
                    ) AS servicios,
                    (
                        SELECT MAX(timestamp)
                        FROM pedidos p
                        WHERE (
                            CASE
                                WHEN c.placa IS NOT NULL AND c.placa != ''
                                    THEN p.conductor_placa = c.placa
                                ELSE p.conductor_nombre = c.nombre_real
                            END
                        )
                    ) AS ultimo_servicio
                    ,
                    (
                        SELECT COALESCE(SUM(monto), 0)
                        FROM ganancias g
                        WHERE g.conductor_id = c.id
                    ) AS ganancias
                FROM conductores c
                ORDER BY c.id DESC
                """
            ).fetchall()
            conn.close()
        except Exception:
            rows = []
            self.db_ok = False

        for item in self.users_table.get_children():
            self.users_table.delete(item)

        for row in rows:
            usuario = row["usuario"] or "-"
            nombre = row["nombre_real"] or "-"
            placa = row["placa"] or "-"
            vehiculo = row["vehiculo"] or "-"
            modelo = row["modelo"] or "-"
            estado = "Ocupado" if row["active_pedido_id"] else "Libre"
            servicios = row["servicios"] if row["servicios"] is not None else 0
            ultimo = self.format_ts_short(row["ultimo_servicio"])
            ganancias = self.format_cop(row["ganancias"])
            self.users_table.insert(
                "",
                "end",
                iid=str(row["id"]),
                values=(
                    usuario,
                    nombre,
                    placa,
                    vehiculo,
                    modelo,
                    estado,
                    servicios,
                    ultimo,
                    ganancias,
                    "Ver",
                ),
            )

    def schedule_refresh(self):
        self.db_ok = True
        self.refresh_log()
        self.refresh_users()
        self.refresh_connected_conductor()
        self.update_status_cards()
        self.root.after(5000, self.schedule_refresh)

    def refresh_connected_conductor(self):
        label = "Sin conexión"
        color = self.colors["muted"]
        bar_color = self.colors["panel_alt"]

        try:
            conn = get_conn()
            rows = conn.execute(
                """
                SELECT nombre_real, usuario, placa, active_pedido_id
                FROM conductores
                WHERE active_pedido_id IS NOT NULL
                ORDER BY active_pedido_id DESC
                """
            ).fetchall()
            conn.close()
        except Exception:
            rows = []

        if rows:
            if len(rows) == 1:
                row = rows[0]
                nombre = row["nombre_real"] or row["usuario"] or "Conductor"
                placa = row["placa"] or "Sin placa"
                label = f"{nombre} ({placa})"
            else:
                label = f"{len(rows)} activos"
            color = self.colors["success"]
            bar_color = self.colors["success"]

        if hasattr(self, "card_driver_value"):
            self.card_driver_value.config(text=label, fg=color)
        if hasattr(self, "driver_status_bar"):
            self.driver_status_bar.config(bg=bar_color)


if __name__ == "__main__":
    hide_console_window()
    root = tk.Tk()
    app = ControladorApp(root)
    root.mainloop()


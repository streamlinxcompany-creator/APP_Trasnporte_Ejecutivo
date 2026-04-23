import base64
import hashlib
import json
import os
import re
import sqlite3
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timedelta
from secrets import token_urlsafe

from flask import Flask, request, render_template, redirect, url_for, session, flash, jsonify, Response, g
from werkzeug.security import generate_password_hash, check_password_hash

try:
    from twilio.rest import Client
except Exception:
    Client = None

try:
    from cryptography.fernet import Fernet, InvalidToken
except Exception:
    Fernet = None
    InvalidToken = Exception

try:
    import local_settings  # optional local-only credentials
except Exception:
    local_settings = None

from twilio_text_logic import handle_twilio_webhook

app = Flask(__name__)
app.secret_key = os.environ.get("FLASK_SECRET_KEY", "dev-secret-change")

DB_PATH = os.path.join(os.path.dirname(__file__), "servicios.db")
EXPIRATION_MINUTES = 12
EXPIRATION_CHECK_SECONDS = 30
TWILIO_SEND_RETRIES = 3
DEFAULT_MEMBERSHIP_DAYS = 30
PAYMENT_WHATSAPP_NUMBER = "573106269788"
PAYMENT_PENDING_COPY = "Recuerda pagar tu mensualidad para volver a ver el mapa y tomar viajes."
EXPIRATION_MESSAGE = (
    "No logramos asignarte conductor en este momento. "
    "Cancelamos la solicitud para no hacerte esperar. "
    "Si deseas intentarlo de nuevo, escribe NUEVO."
)
FOLLOW_UP_STEPS = [
    (
        2,
        "Seguimos buscando tu vehiculo. Apenas tengamos un conductor disponible te avisaremos.",
    ),
    (
        5,
        "Seguimos pendientes de tu solicitud y estamos haciendo todo lo posible para conseguirte un conductor pronto.",
    ),
    (
        9,
        "Hemos tratado de conseguirte un conductor, pero en este momento todos parecen estar ocupados. "
        "Cerraremos esta solicitud por ahora para no hacerte esperar mas. "
        "Si deseas intentarlo nuevamente en unos minutos, escribe NUEVO.",
    ),
]
SHORT_CANCEL_HINT = "Si deseas cancelar, escribe CANCELAR."
WHATSAPP_BUTTON_TEMPLATE_ENVS = {
    "save_location_confirm": "TWILIO_CONTENT_SID_SAVE_LOCATION_CONFIRM",
    "location_help_offer": "TWILIO_CONTENT_SID_LOCATION_HELP_OFFER",
    "location_help_steps": "TWILIO_CONTENT_SID_LOCATION_HELP_STEPS",
    "location_new_prompt": "TWILIO_CONTENT_SID_LOCATION_NEW_PROMPT",
    "location_required": "TWILIO_CONTENT_SID_LOCATION_REQUIRED",
    "location_saved_list": "TWILIO_CONTENT_SID_LOCATION_SAVED_LIST",
    "location_manage_action": "TWILIO_CONTENT_SID_LOCATION_MANAGE_ACTION",
}

TEST_COORDS = [
    (6.036734, -75.419024),
    (6.017470, -75.430035),
    (6.029867, -75.433825),
    (6.081244, -75.333961),
    (6.029867, -75.433825),
    (6.021925, -75.422297),
    (6.034995, -75.433154),
]

CONDUCTOR_SUBSCRIPTION_PROTECTED_ENDPOINTS = {
    "inicio",
    "dashboard",
    "tomar",
    "vaciar_tomados",
    "finalizar_servicio",
    "servicio_detalle",
    "servicio_detalle_api",
    "servicios_status_api",
    "servicios_list_api",
    "conductor_availability_api",
    "chat_api",
    "chat_send_api",
    "reenviar_asignacion",
}


def get_conn():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def db_now():
    return datetime.now()


def format_db_datetime(value):
    if not value:
        return ""
    if isinstance(value, datetime):
        return value.strftime("%Y-%m-%d %H:%M:%S")
    return str(value)


def parse_db_datetime(value):
    text = (value or "").strip()
    if not text:
        return None
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
        try:
            return datetime.strptime(text, fmt)
        except ValueError:
            continue
    return None


def compute_subscription_state(status_suscripto, fin_suscripcion, reference_dt=None):
    now = reference_dt or db_now()
    fin_dt = parse_db_datetime(fin_suscripcion)
    normalized_status = (status_suscripto or "inactivo").strip().lower()
    expired = bool(fin_dt and now > fin_dt)
    active = normalized_status == "activo" and fin_dt is not None and not expired
    if expired and normalized_status != "inactivo":
        normalized_status = "inactivo"
    return {
        "status": normalized_status,
        "fin_dt": fin_dt,
        "active": active,
        "expired": expired,
    }


def sync_expired_subscriptions(conn):
    rows = conn.execute(
        """
        SELECT id, status_suscripto, fin_suscripcion
        FROM conductores
        WHERE fin_suscripcion IS NOT NULL
        """
    ).fetchall()
    now = db_now()
    expired_ids = []
    for row in rows:
        state = compute_subscription_state(
            row["status_suscripto"],
            row["fin_suscripcion"],
            reference_dt=now,
        )
        if state["expired"]:
            expired_ids.append(row["id"])
    if expired_ids:
        conn.executemany(
            "UPDATE conductores SET status_suscripto = 'inactivo' WHERE id = ?",
            [(driver_id,) for driver_id in expired_ids],
        )
        conn.commit()
    return expired_ids


def build_whatsapp_payment_link():
    text = urllib.parse.quote(
        "Hola, quiero ponerme al dia con mi mensualidad para reactivar la app."
    )
    return f"https://wa.me/{PAYMENT_WHATSAPP_NUMBER}?text={text}"


def get_conductor_subscription_snapshot(row, reference_dt=None):
    now = reference_dt or db_now()
    state = compute_subscription_state(
        row["status_suscripto"] if row and "status_suscripto" in row.keys() else "inactivo",
        row["fin_suscripcion"] if row and "fin_suscripcion" in row.keys() else "",
        reference_dt=now,
    )
    fin_dt = state["fin_dt"]
    days_left = 0
    if fin_dt and state["active"]:
        delta = fin_dt - now
        days_left = max(0, delta.days + (1 if delta.seconds > 0 else 0))
    return {
        "status_suscripto": state["status"],
        "suscripcion_activa": state["active"],
        "fin_suscripcion": format_db_datetime(fin_dt) if fin_dt else "",
        "fin_suscripcion_short": fin_dt.strftime("%d/%m/%Y %H:%M") if fin_dt else "Sin fecha",
        "dias_restantes": days_left,
        "mensualidades_pagadas": row["mensualidades_pagadas"] if row and "mensualidades_pagadas" in row.keys() and row["mensualidades_pagadas"] is not None else 0,
        "dias_mensualidad": row["dias_mensualidad"] if row and "dias_mensualidad" in row.keys() and row["dias_mensualidad"] else DEFAULT_MEMBERSHIP_DAYS,
        "ultima_mensualidad_at": row["ultima_mensualidad_at"] if row and "ultima_mensualidad_at" in row.keys() else "",
    }


def get_conductor_row(conductor_id, conn=None, sync=True):
    own_conn = conn is None
    conn = conn or get_conn()
    if sync:
        sync_expired_subscriptions(conn)
    row = conn.execute(
        "SELECT * FROM conductores WHERE id = ?",
        (conductor_id,),
    ).fetchone()
    if own_conn:
        conn.close()
    return row


def conductor_has_active_subscription(conductor_id, conn=None):
    row = get_conductor_row(conductor_id, conn=conn, sync=True)
    if row is None:
        return False, None
    return get_conductor_subscription_snapshot(row)["suscripcion_activa"], row


def is_json_like_request():
    return (
        request.path.startswith("/api/")
        or request.headers.get("X-Requested-With") == "XMLHttpRequest"
        or request.accept_mimetypes.accept_json
    )


def get_config_value(key, default=None):
    conn = get_conn()
    row = conn.execute("SELECT value FROM config WHERE key = ?", (key,)).fetchone()
    conn.close()
    if row is None or row["value"] is None:
        return default
    return row["value"]


def set_config_value(key, value):
    conn = get_conn()
    conn.execute(
        "INSERT OR REPLACE INTO config (key, value) VALUES (?, ?)",
        (key, str(value)),
    )
    sync_expired_subscriptions(conn)
    conn.commit()
    conn.close()


def log_system_event(level, category, message, details=""):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    try:
        conn = get_conn()
        conn.execute(
            """
            INSERT INTO system_logs (level, category, message, details, timestamp)
            VALUES (?, ?, ?, ?, ?)
            """,
            (
                (level or "info").strip().lower(),
                (category or "general").strip().lower(),
                (message or "").strip()[:255],
                (details or "").strip()[:4000],
                timestamp,
            ),
        )
        conn.execute(
            """
            DELETE FROM system_logs
            WHERE id NOT IN (
                SELECT id
                FROM system_logs
                ORDER BY id DESC
                LIMIT 500
            )
            """
        )
        conn.commit()
        conn.close()
    except Exception as exc:
        print(f"No se pudo guardar log del sistema: {exc}")


def normalize_fernet_key(raw_key):
    if not raw_key:
        return None
    try:
        decoded = base64.urlsafe_b64decode(raw_key)
        if len(decoded) == 32:
            return raw_key
    except Exception:
        pass


def normalize_ui_mode(value):
    return "executive" if str(value or "").strip().lower() == "executive" else "classic"


def get_ui_mode():
    return normalize_ui_mode(get_config_value("ui_mode", "classic"))


def get_driver_active_service(conn, conductor_id, conductor_nombre, conductor_placa):
    if not conductor_id:
        return None

    active_row = conn.execute(
        "SELECT active_pedido_id, is_online FROM conductores WHERE id = ?",
        (conductor_id,),
    ).fetchone()
    active_pedido_id = active_row["active_pedido_id"] if active_row else None
    driver_online = (
        bool(active_row["is_online"])
        if active_row and active_row["is_online"] is not None
        else True
    )

    pedido_row = None
    if active_pedido_id:
        pedido_row = conn.execute(
            """
            SELECT *
            FROM pedidos
            WHERE id = ?
              AND estado = 'Tomado'
              AND conductor_nombre = ?
              AND conductor_placa = ?
            """,
            (active_pedido_id, conductor_nombre, conductor_placa),
        ).fetchone()

    if pedido_row is None and conductor_nombre and conductor_placa:
        pedido_row = conn.execute(
            """
            SELECT *
            FROM pedidos
            WHERE estado = 'Tomado'
              AND conductor_nombre = ?
              AND conductor_placa = ?
            ORDER BY id DESC
            LIMIT 1
            """,
            (conductor_nombre, conductor_placa),
        ).fetchone()
        recovered_id = pedido_row["id"] if pedido_row else None
        if recovered_id != active_pedido_id:
            conn.execute(
                "UPDATE conductores SET active_pedido_id = ? WHERE id = ?",
                (recovered_id, conductor_id),
            )

    if pedido_row is None and active_pedido_id:
        conn.execute(
            "UPDATE conductores SET active_pedido_id = NULL WHERE id = ?",
            (conductor_id,),
        )

    return {
        "pedido_row": pedido_row,
        "active_pedido_id": pedido_row["id"] if pedido_row else None,
        "driver_online": driver_online,
    }
    digest = hashlib.sha256(raw_key.encode("utf-8")).digest()
    return base64.urlsafe_b64encode(digest).decode("utf-8")


def get_password_key():
    env_key = os.environ.get("PASSWORD_CRYPT_KEY")
    if env_key:
        return env_key
    stored = get_config_value("password_key")
    if stored:
        return stored
    new_key = base64.urlsafe_b64encode(os.urandom(32)).decode("utf-8")
    set_config_value("password_key", new_key)
    return new_key


def xor_crypt(data_bytes, key_bytes):
    return bytes(b ^ key_bytes[i % len(key_bytes)] for i, b in enumerate(data_bytes))


def encrypt_password(plain_text):
    if plain_text is None:
        return None
    key = get_password_key()
    if Fernet is not None:
        f = Fernet(normalize_fernet_key(key))
        return "fernet:" + f.encrypt(plain_text.encode("utf-8")).decode("utf-8")
    key_bytes = hashlib.sha256(key.encode("utf-8")).digest()
    encrypted = xor_crypt(plain_text.encode("utf-8"), key_bytes)
    return "xor:" + base64.urlsafe_b64encode(encrypted).decode("utf-8")


def decrypt_password(enc_text):
    if not enc_text:
        return None
    key = get_password_key()
    try:
        if enc_text.startswith("fernet:") and Fernet is not None:
            token = enc_text.split(":", 1)[1]
            f = Fernet(normalize_fernet_key(key))
            return f.decrypt(token.encode("utf-8")).decode("utf-8")
        if enc_text.startswith("xor:"):
            payload = enc_text.split(":", 1)[1]
            data = base64.urlsafe_b64decode(payload.encode("utf-8"))
            key_bytes = hashlib.sha256(key.encode("utf-8")).digest()
            return xor_crypt(data, key_bytes).decode("utf-8")
    except Exception:
        return None
    return None


def init_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS conductores (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            usuario TEXT UNIQUE NOT NULL,
            password_hash TEXT NOT NULL,
            nombre_real TEXT,
            placa TEXT
        )
        """
    )

    cur.execute("PRAGMA table_info(conductores)")
    conductor_cols = [row[1] for row in cur.fetchall()]
    if "active_pedido_id" not in conductor_cols:
        cur.execute("ALTER TABLE conductores ADD COLUMN active_pedido_id INTEGER")
    if "vehiculo" not in conductor_cols:
        cur.execute("ALTER TABLE conductores ADD COLUMN vehiculo TEXT")
    if "modelo" not in conductor_cols:
        cur.execute("ALTER TABLE conductores ADD COLUMN modelo TEXT")
    if "password_enc" not in conductor_cols:
        cur.execute("ALTER TABLE conductores ADD COLUMN password_enc TEXT")
    if "is_online" not in conductor_cols:
        cur.execute("ALTER TABLE conductores ADD COLUMN is_online INTEGER DEFAULT 1")
        cur.execute("UPDATE conductores SET is_online = 1 WHERE is_online IS NULL")
    if "status_suscripto" not in conductor_cols:
        cur.execute("ALTER TABLE conductores ADD COLUMN status_suscripto TEXT DEFAULT 'inactivo'")
        cur.execute(
            "UPDATE conductores SET status_suscripto = 'inactivo' WHERE status_suscripto IS NULL OR trim(status_suscripto) = ''"
        )
    if "fin_suscripcion" not in conductor_cols:
        cur.execute("ALTER TABLE conductores ADD COLUMN fin_suscripcion TEXT")
    if "mensualidades_pagadas" not in conductor_cols:
        cur.execute("ALTER TABLE conductores ADD COLUMN mensualidades_pagadas INTEGER DEFAULT 0")
        cur.execute(
            "UPDATE conductores SET mensualidades_pagadas = 0 WHERE mensualidades_pagadas IS NULL"
        )
    if "dias_mensualidad" not in conductor_cols:
        cur.execute(
            f"ALTER TABLE conductores ADD COLUMN dias_mensualidad INTEGER DEFAULT {DEFAULT_MEMBERSHIP_DAYS}"
        )
        cur.execute(
            "UPDATE conductores SET dias_mensualidad = ? WHERE dias_mensualidad IS NULL",
            (DEFAULT_MEMBERSHIP_DAYS,),
        )
    if "ultima_mensualidad_at" not in conductor_cols:
        cur.execute("ALTER TABLE conductores ADD COLUMN ultima_mensualidad_at TEXT")

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS admins (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            usuario TEXT UNIQUE NOT NULL,
            password_hash TEXT NOT NULL,
            created_at TEXT
        )
        """
    )

    admin_row = cur.execute(
        "SELECT id FROM admins WHERE usuario = ?",
        ("admin",),
    ).fetchone()
    if admin_row is None:
        cur.execute(
            """
            INSERT INTO admins (usuario, password_hash, created_at)
            VALUES (?, ?, ?)
            """,
            (
                "admin",
                generate_password_hash("admin"),
                datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            ),
        )

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS usuarios (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            telefono TEXT UNIQUE NOT NULL,
            nombre TEXT NOT NULL,
            created_at TEXT,
            updated_at TEXT
        )
        """
    )

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS direcciones (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            usuario_id INTEGER NOT NULL,
            direccion TEXT NOT NULL,
            etiqueta TEXT,
            latitude TEXT,
            longitude TEXT,
            updated_at TEXT,
            created_at TEXT,
            FOREIGN KEY (usuario_id) REFERENCES usuarios(id)
        )
        """
    )

    cur.execute("PRAGMA table_info(direcciones)")
    direcciones_cols = [row[1] for row in cur.fetchall()]
    if "etiqueta" not in direcciones_cols:
        cur.execute("ALTER TABLE direcciones ADD COLUMN etiqueta TEXT")
    if "latitude" not in direcciones_cols:
        cur.execute("ALTER TABLE direcciones ADD COLUMN latitude TEXT")
    if "longitude" not in direcciones_cols:
        cur.execute("ALTER TABLE direcciones ADD COLUMN longitude TEXT")
    if "updated_at" not in direcciones_cols:
        cur.execute("ALTER TABLE direcciones ADD COLUMN updated_at TEXT")

    cur.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_direcciones_usuario
        ON direcciones(usuario_id)
        """
    )

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS config (
            key TEXT PRIMARY KEY,
            value TEXT
        )
        """
    )

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS system_logs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            level TEXT NOT NULL,
            category TEXT NOT NULL,
            message TEXT NOT NULL,
            details TEXT,
            timestamp TEXT
        )
        """
    )

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS conversaciones (
            telefono TEXT PRIMARY KEY,
            paso TEXT NOT NULL,
            servicio TEXT,
            nombre TEXT,
            direccion TEXT,
            meta TEXT,
            updated_at TEXT
        )
        """
    )

    cur.execute("PRAGMA table_info(conversaciones)")
    conversaciones_cols = [row[1] for row in cur.fetchall()]
    if "meta" not in conversaciones_cols:
        cur.execute("ALTER TABLE conversaciones ADD COLUMN meta TEXT")

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS ganancias (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            conductor_id INTEGER,
            monto INTEGER NOT NULL,
            timestamp TEXT,
            FOREIGN KEY (conductor_id) REFERENCES conductores(id)
        )
        """
    )

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS chat_mensajes (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            pedido_id INTEGER NOT NULL,
            sender TEXT NOT NULL,
            message TEXT NOT NULL,
            timestamp TEXT,
            FOREIGN KEY (pedido_id) REFERENCES pedidos(id)
        )
        """
    )

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS outbound_messages (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            telefono TEXT NOT NULL,
            body TEXT NOT NULL,
            source TEXT DEFAULT 'whatsapp',
            created_at TEXT
        )
        """
    )

    cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='pedidos'")
    pedidos_exists = cur.fetchone() is not None

    if not pedidos_exists:
        cur.execute(
            """
            CREATE TABLE pedidos (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                cliente_telefono TEXT,
                mensaje_cliente TEXT,
                estado TEXT DEFAULT 'Disponible',
                conductor_nombre TEXT,
                conductor_placa TEXT,
                chat_iniciado INTEGER DEFAULT 0,
                assignment_notified INTEGER DEFAULT 0,
                reminder_count INTEGER DEFAULT 0,
                timestamp TEXT
            )
            """
        )
    else:
        cur.execute("PRAGMA table_info(pedidos)")
        cols = [row[1] for row in cur.fetchall()]
        required = [
            "id",
            "cliente_telefono",
            "mensaje_cliente",
            "estado",
            "conductor_nombre",
            "conductor_placa",
            "chat_iniciado",
            "assignment_notified",
            "reminder_count",
            "timestamp",
        ]
        if not all(col in cols for col in required):
            cur.execute("ALTER TABLE pedidos RENAME TO pedidos_old")
            cur.execute(
                """
                CREATE TABLE pedidos (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    cliente_telefono TEXT,
                    mensaje_cliente TEXT,
                    estado TEXT DEFAULT 'Disponible',
                    conductor_nombre TEXT,
                    conductor_placa TEXT,
                    chat_iniciado INTEGER DEFAULT 0,
                    assignment_notified INTEGER DEFAULT 0,
                    reminder_count INTEGER DEFAULT 0,
                    timestamp TEXT
                )
                """
            )

            def pick_col(options):
                for col in options:
                    if col in cols:
                        return col
                return "NULL"

            cliente_col = pick_col(["cliente_telefono", "telefono", "cliente"])
            mensaje_col = pick_col(["mensaje_cliente", "mensaje", "texto"])
            estado_col = pick_col(["estado"])
            conductor_nombre_col = pick_col(["conductor_nombre"])
            conductor_placa_col = pick_col(["conductor_placa"])
            chat_iniciado_col = pick_col(["chat_iniciado"])
            ts_col = pick_col(["timestamp", "fecha_hora", "fecha"])

            estado_expr = estado_col if estado_col != "NULL" else "'Disponible'"
            conductor_nombre_expr = (
                conductor_nombre_col if conductor_nombre_col != "NULL" else "NULL"
            )
            conductor_placa_expr = (
                conductor_placa_col if conductor_placa_col != "NULL" else "NULL"
            )
            chat_iniciado_expr = chat_iniciado_col if chat_iniciado_col != "NULL" else "0"
            assignment_notified_expr = "0"
            reminder_count_expr = "0"
            ts_expr = ts_col if ts_col != "NULL" else "datetime('now')"

            cur.execute(
                f"""
                INSERT INTO pedidos (
                    cliente_telefono,
                    mensaje_cliente,
                    estado,
                    conductor_nombre,
                    conductor_placa,
                    chat_iniciado,
                    assignment_notified,
                    reminder_count,
                    timestamp
                )
                SELECT
                    {cliente_col},
                    {mensaje_col},
                    {estado_expr},
                    {conductor_nombre_expr},
                    {conductor_placa_expr},
                    {chat_iniciado_expr},
                    {assignment_notified_expr},
                    {reminder_count_expr},
                    {ts_expr}
                FROM pedidos_old
                """
            )
            cur.execute("DROP TABLE pedidos_old")
        else:
            if "assignment_notified" not in cols:
                cur.execute(
                    "ALTER TABLE pedidos ADD COLUMN assignment_notified INTEGER DEFAULT 0"
                )
                cur.execute(
                    "UPDATE pedidos SET assignment_notified = 0 WHERE assignment_notified IS NULL"
                )
            if "reminder_count" not in cols:
                cur.execute(
                    "ALTER TABLE pedidos ADD COLUMN reminder_count INTEGER DEFAULT 0"
                )
                cur.execute(
                    "UPDATE pedidos SET reminder_count = 0 WHERE reminder_count IS NULL"
                )

    conn.commit()
    conn.close()


def format_time(ts_value):
    if not ts_value:
        return ""
    try:
        parsed = datetime.strptime(ts_value, "%Y-%m-%d %H:%M:%S")
        return parsed.strftime("%H:%M")
    except Exception:
        return ts_value


def format_cop(value):
    try:
        value_int = int(value)
    except Exception:
        value_int = 0
    return f"${value_int:,.0f}"


def ensure_whatsapp_prefix(phone):
    if not phone:
        return phone
    if phone.startswith("whatsapp:"):
        return phone
    if phone.startswith("+"):
        return f"whatsapp:{phone}"
    return f"whatsapp:+{phone}"


def normalize_phone_for_wa(phone):
    if not phone:
        return ""
    if phone.startswith("whatsapp:"):
        phone = phone.split(":", 1)[1]
    return phone.replace("+", "").replace(" ", "")


def normalize_text(value):
    if not value:
        return ""
    text = value.strip().lower()
    replacements = {
        "Ã¡": "a",
        "Ã©": "e",
        "Ã­": "i",
        "Ã³": "o",
        "Ãº": "u",
        "Ã±": "n",
    }
    for key, val in replacements.items():
        text = text.replace(key, val)
    return " ".join(text.split())


def parse_coords_from_text(value):
    if not value:
        return None, None
    matches = re.findall(r"-?\d+(?:\.\d+)?", str(value))
    if len(matches) < 2:
        return None, None
    try:
        lat = float(matches[0])
        lng = float(matches[1])
    except Exception:
        return None, None
    return lat, lng


def format_saved_address(value):
    text = (value or "").strip().strip("[]")
    if not text:
        return ""
    normalized = normalize_text(text)
    if normalized.startswith("ubicacion:"):
        text = text.split(":", 1)[1].strip()
    return text


def is_reserved_direccion(value):
    text = normalize_text(value)
    return text in {
        "nueva",
        "nuevo",
        "otra",
        "otra direccion",
        "nueva direccion",
        "agregar",
        "agregar direccion",
    }


def build_request_message(nombre):
    if nombre:
        return f"Listo, {nombre}. Estamos buscando conductor. {SHORT_CANCEL_HINT}"
    return f"Listo. Estamos buscando conductor. {SHORT_CANCEL_HINT}"


def is_final_follow_up_step(step):
    return max(1, int(step or 0)) >= len(FOLLOW_UP_STEPS)


def build_follow_up_message(step):
    idx = max(1, min(step, len(FOLLOW_UP_STEPS))) - 1
    message = FOLLOW_UP_STEPS[idx][1]
    if is_final_follow_up_step(step):
        return message
    return f"{message} {SHORT_CANCEL_HINT}"


def parse_db_datetime(value):
    if not value:
        return None
    try:
        return datetime.strptime(value, "%Y-%m-%d %H:%M:%S")
    except Exception:
        return None


def get_config_value(key, default=None):
    try:
        conn = get_conn()
        row = conn.execute(
            "SELECT value FROM config WHERE key = ?",
            (key,),
        ).fetchone()
        conn.close()
        if row is None or row["value"] is None:
            return default
        return row["value"]
    except Exception:
        return default


def is_twilio_enabled():
    return True


def mask_value(value, prefix=6, suffix=4):
    if not value:
        return "-"
    text = str(value)
    if len(text) <= prefix + suffix:
        return text
    return f"{text[:prefix]}...{text[-suffix:]}"


def get_twilio_source_summary():
    env_sid = os.environ.get("TWILIO_ACCOUNT_SID")
    env_token = os.environ.get("TWILIO_AUTH_TOKEN")
    env_from = os.environ.get("TWILIO_WHATSAPP_FROM")
    local_sid = getattr(local_settings, "TWILIO_ACCOUNT_SID", None) if local_settings else None
    local_token = getattr(local_settings, "TWILIO_AUTH_TOKEN", None) if local_settings else None
    local_from = getattr(local_settings, "TWILIO_WHATSAPP_FROM", None) if local_settings else None
    source = "env" if any([env_sid, env_token, env_from]) else "local_settings"
    active_sid = env_sid or local_sid
    active_from = env_from or local_from
    return (
        f"Twilio source={source} sid={mask_value(active_sid)} "
        f"from={active_from or '-'}"
    )


def get_twilio_settings():
    account_sid = os.environ.get("TWILIO_ACCOUNT_SID")
    auth_token = os.environ.get("TWILIO_AUTH_TOKEN")
    from_number = os.environ.get("TWILIO_WHATSAPP_FROM")

    if local_settings:
        account_sid = account_sid or getattr(local_settings, "TWILIO_ACCOUNT_SID", None)
        auth_token = auth_token or getattr(local_settings, "TWILIO_AUTH_TOKEN", None)
        from_number = from_number or getattr(local_settings, "TWILIO_WHATSAPP_FROM", None)

    if not all([account_sid, auth_token, from_number]):
        return None, None, None, "Twilio no configurado"
    return account_sid, auth_token, from_number, ""


def get_twilio_client():
    account_sid, auth_token, from_number, error = get_twilio_settings()
    if error:
        return None, None, None, error
    if not Client:
        return None, account_sid, from_number, "SDK Twilio no disponible"
    return Client(account_sid, auth_token), account_sid, from_number, ""


def send_twilio_message_via_http(account_sid, auth_token, from_number, to_number, body):
    form_data = urllib.parse.urlencode(
        {
            "From": from_number,
            "To": to_number,
            "Body": body,
        }
    ).encode("utf-8")
    request = urllib.request.Request(
        f"https://api.twilio.com/2010-04-01/Accounts/{account_sid}/Messages.json",
        data=form_data,
        method="POST",
    )
    credentials = base64.b64encode(
        f"{account_sid}:{auth_token}".encode("utf-8")
    ).decode("ascii")
    request.add_header("Authorization", f"Basic {credentials}")
    request.add_header("Content-Type", "application/x-www-form-urlencoded")
    with urllib.request.urlopen(request, timeout=15) as response:
        payload = response.read().decode("utf-8", errors="ignore")
        status_code = getattr(response, "status", 200)
        if 200 <= status_code < 300:
            return True, payload
        return False, payload


def get_button_template_sid(template_key):
    env_name = WHATSAPP_BUTTON_TEMPLATE_ENVS.get(template_key, "")
    if not env_name:
        return ""
    value = os.environ.get(env_name, "").strip()
    if value:
        return value
    if local_settings:
        return str(getattr(local_settings, env_name, "") or "").strip()
    return ""


def record_outbound_message(phone, body, source="whatsapp"):
    try:
        conn = get_conn()
        conn.execute(
            """
            INSERT INTO outbound_messages (telefono, body, source, created_at)
            VALUES (?, ?, ?, ?)
            """,
            (
                ensure_whatsapp_prefix(phone),
                body,
                source,
                datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            ),
        )
        conn.commit()
        conn.close()
    except Exception as log_exc:
        print(f"No se pudo registrar mensaje saliente: {log_exc}")


def send_twilio_message_request(
    *,
    phone,
    body="",
    content_sid="",
    content_variables=None,
):
    if not is_twilio_enabled():
        print("Twilio desactivado: mensaje omitido.")
        log_system_event(
            "warn",
            "twilio",
            "Envio omitido porque Twilio esta apagado",
            f"{get_twilio_source_summary()} to={ensure_whatsapp_prefix(phone)} body={body[:180]}",
        )
        return False, "Twilio desactivado"

    print(f"Intentando envio WhatsApp. {get_twilio_source_summary()} to={ensure_whatsapp_prefix(phone)}")
    account_sid, auth_token, from_number, settings_error = get_twilio_settings()
    if settings_error:
        log_system_event("error", "twilio", "Twilio no configurado", settings_error)
        return False, settings_error

    to_number = ensure_whatsapp_prefix(phone)
    last_error = ""
    for attempt in range(1, TWILIO_SEND_RETRIES + 1):
        try:
            client, _, _, client_error = get_twilio_client()
            create_kwargs = {
                "from_": from_number,
                "to": to_number,
            }
            if content_sid:
                create_kwargs["content_sid"] = content_sid
                if content_variables:
                    create_kwargs["content_variables"] = json.dumps(
                        content_variables, ensure_ascii=True
                    )
            else:
                create_kwargs["body"] = body
            if client:
                try:
                    client.messages.create(**create_kwargs)
                except Exception as sdk_exc:
                    print(
                        f"SDK Twilio fallo en intento {attempt}, usando HTTP directo: {sdk_exc}"
                    )
                    form_data = {
                        "From": from_number,
                        "To": to_number,
                    }
                    if content_sid:
                        form_data["ContentSid"] = content_sid
                        if content_variables:
                            form_data["ContentVariables"] = json.dumps(
                                content_variables, ensure_ascii=True
                            )
                    else:
                        form_data["Body"] = body
                    ok_http, http_response = send_twilio_message_via_http(
                        account_sid,
                        auth_token,
                        from_number,
                        to_number,
                        body,
                    ) if not content_sid else send_twilio_message_via_http_content(
                        account_sid,
                        auth_token,
                        form_data,
                    )
                    if not ok_http:
                        raise RuntimeError(http_response or str(sdk_exc))
                    log_system_event(
                        "info",
                        "twilio",
                        "Envio WhatsApp exitoso por HTTP fallback",
                        f"to={to_number} intento={attempt} body={body[:180]} content_sid={content_sid or '-'}",
                    )
            else:
                if content_sid:
                    form_data = {
                        "From": from_number,
                        "To": to_number,
                        "ContentSid": content_sid,
                    }
                    if content_variables:
                        form_data["ContentVariables"] = json.dumps(
                            content_variables, ensure_ascii=True
                        )
                    ok_http, http_response = send_twilio_message_via_http_content(
                        account_sid,
                        auth_token,
                        form_data,
                    )
                else:
                    ok_http, http_response = send_twilio_message_via_http(
                        account_sid,
                        auth_token,
                        from_number,
                        to_number,
                        body,
                    )
                if not ok_http:
                    raise RuntimeError(client_error or http_response)
                log_system_event(
                    "info",
                    "twilio",
                    "Envio WhatsApp exitoso por HTTP directo",
                    f"to={to_number} intento={attempt} body={body[:180]} content_sid={content_sid or '-'}",
                )
            if client:
                log_system_event(
                    "info",
                    "twilio",
                    "Envio WhatsApp exitoso por SDK",
                    f"to={to_number} intento={attempt} body={body[:180]} content_sid={content_sid or '-'}",
                )
            record_outbound_message(
                to_number,
                body or f"[template:{content_sid}]",
                source="whatsapp-template" if content_sid else "whatsapp",
            )
            return True, ""
        except Exception as exc:
            last_error = str(exc)
            print(
                f"Error enviando mensaje Twilio (intento {attempt}): {exc} | "
                f"{get_twilio_source_summary()}"
            )
            log_system_event(
                "error",
                "twilio",
                f"Error enviando WhatsApp en intento {attempt}",
                f"to={to_number} body={body[:180]} error={last_error} source={get_twilio_source_summary()}",
            )
            if attempt < TWILIO_SEND_RETRIES:
                time.sleep(1)
    return False, last_error or "No se pudo enviar el mensaje."


def send_twilio_message_via_http_content(account_sid, auth_token, form_data):
    encoded = urllib.parse.urlencode(form_data).encode("utf-8")
    request = urllib.request.Request(
        f"https://api.twilio.com/2010-04-01/Accounts/{account_sid}/Messages.json",
        data=encoded,
        method="POST",
    )
    credentials = base64.b64encode(
        f"{account_sid}:{auth_token}".encode("utf-8")
    ).decode("ascii")
    request.add_header("Authorization", f"Basic {credentials}")
    request.add_header("Content-Type", "application/x-www-form-urlencoded")
    with urllib.request.urlopen(request, timeout=15) as response:
        payload = response.read().decode("utf-8", errors="ignore")
        status_code = getattr(response, "status", 200)
        if 200 <= status_code < 300:
            return True, payload
        return False, payload


def send_whatsapp_message(phone, body):
    return send_twilio_message_request(phone=phone, body=body)


def send_whatsapp_reply(phone, body, buttons_key="", buttons_variables=None):
    content_sid = get_button_template_sid(buttons_key)
    if not content_sid:
        return send_whatsapp_message(phone, body)
    return send_twilio_message_request(
        phone=phone,
        body=body,
        content_sid=content_sid,
        content_variables=buttons_variables or {},
    )


def verificar_expiracion_servicios():
    now = datetime.now()
    expired_phones = []
    follow_ups = []

    try:
        conn = get_conn()
        rows = conn.execute(
            """
            SELECT id, cliente_telefono, timestamp, reminder_count
            FROM pedidos
            WHERE lower(estado) IN ('disponible', 'pendiente')
              AND timestamp IS NOT NULL
            """,
        ).fetchall()

        for row in rows:
            created_at = parse_db_datetime(row["timestamp"])
            if created_at is None:
                continue

            elapsed_total = int((now - created_at).total_seconds())
            if elapsed_total < 0:
                elapsed_total = 0

            if elapsed_total >= EXPIRATION_MINUTES * 60:
                cur = conn.execute(
                    """
                    UPDATE pedidos
                    SET estado = 'expirado'
                    WHERE id = ? AND lower(estado) IN ('disponible', 'pendiente')
                    """,
                    (row["id"],),
                )
                if cur.rowcount:
                    expired_phones.append(row["cliente_telefono"])
                continue

            reminder_count = row["reminder_count"] or 0
            target_step = 0
            for minute_mark, _message in FOLLOW_UP_STEPS:
                if elapsed_total >= minute_mark * 60:
                    target_step += 1
            if target_step > reminder_count:
                next_step = reminder_count + 1
                follow_ups.append(
                    (
                        row["id"],
                        row["cliente_telefono"],
                        next_step,
                        build_follow_up_message(next_step),
                    )
                )

        for phone in expired_phones:
            if phone:
                conn.execute("DELETE FROM conversaciones WHERE telefono = ?", (phone,))

        conn.commit()
        conn.close()
    except Exception as exc:
        print(f"Error verificando expiracion de servicios: {exc}")
        return 0

    for phone in expired_phones:
        if not phone:
            continue
        ok, err = send_whatsapp_message(phone, EXPIRATION_MESSAGE)
        if not ok:
            print(f"No se pudo notificar expiracion a {phone}. {err}")
            log_system_event(
                "error",
                "reminder",
                "Fallo aviso de expiracion",
                f"telefono={phone} error={err}",
            )
        else:
            log_system_event(
                "warn",
                "reminder",
                "Servicio expirado por tiempo",
                f"telefono={phone}",
            )

    for pedido_id, phone, next_step, body in follow_ups:
        if not phone:
            continue
        ok, err = send_whatsapp_message(phone, body)
        if ok:
            try:
                conn = get_conn()
                if is_final_follow_up_step(next_step):
                    conn.execute(
                        """
                        UPDATE pedidos
                        SET reminder_count = ?, estado = 'expirado'
                        WHERE id = ?
                        """,
                        (next_step, pedido_id),
                    )
                    conn.execute(
                        "DELETE FROM conversaciones WHERE telefono = ?",
                        (phone,),
                    )
                else:
                    conn.execute(
                        "UPDATE pedidos SET reminder_count = ? WHERE id = ?",
                        (next_step, pedido_id),
                    )
                conn.commit()
                conn.close()
                log_system_event(
                    "info",
                    "reminder",
                    (
                        "Solicitud cerrada tras ultimo recordatorio"
                        if is_final_follow_up_step(next_step)
                        else f"Recordatorio enviado #{next_step}"
                    ),
                    f"pedido={pedido_id} telefono={phone}",
                )
            except Exception as exc:
                print(f"No se pudo guardar seguimiento enviado para {pedido_id}: {exc}")
        else:
            print(f"No se pudo enviar seguimiento a {phone}. {err}")
            log_system_event(
                "error",
                "reminder",
                f"Fallo recordatorio #{next_step}",
                f"pedido={pedido_id} telefono={phone} error={err}",
            )

    return len(expired_phones)


def start_expiration_worker():
    def loop():
        print("Worker de recordatorios iniciado.")
        log_system_event(
            "info",
            "worker",
            "Worker de recordatorios iniciado",
            threading.current_thread().name,
        )
        while True:
            verificar_expiracion_servicios()
            retry_assignment_notifications()
            time.sleep(EXPIRATION_CHECK_SECONDS)

    worker = threading.Thread(target=loop, name="servicios-expiracion", daemon=True)
    worker.start()
    return worker


expiration_worker = None


def ensure_expiration_worker(debug_mode=False):
    global expiration_worker
    if expiration_worker and expiration_worker.is_alive():
        return expiration_worker
    if debug_mode and os.environ.get("WERKZEUG_RUN_MAIN") != "true":
        return None
    expiration_worker = start_expiration_worker()
    return expiration_worker


def ensure_runtime_workers(reason="runtime"):
    worker_was_alive = bool(expiration_worker and expiration_worker.is_alive())
    worker = ensure_expiration_worker(debug_mode=False)
    if worker and worker.is_alive() and not worker_was_alive:
        details = f"reason={reason} thread={worker.name}"
        print(f"Worker de recordatorios activo. {details}")
        log_system_event("info", "worker", "Worker de recordatorios activo", details)
    return worker


def retry_assignment_notifications():
    try:
        conn = get_conn()
        rows = conn.execute(
            """
            SELECT id, cliente_telefono, conductor_nombre, conductor_placa
            FROM pedidos
            WHERE estado = 'Tomado'
              AND COALESCE(assignment_notified, 0) = 0
              AND cliente_telefono IS NOT NULL
              AND cliente_telefono != ''
            ORDER BY id ASC
            """
        ).fetchall()
        conn.close()
    except Exception as exc:
        print(f"Error consultando notificaciones pendientes: {exc}")
        return 0

    sent_count = 0
    for row in rows:
        enviado, razon = send_assignment_message(
            row["cliente_telefono"],
            row["conductor_nombre"] or "Tu conductor",
            row["conductor_placa"] or "-",
        )
        if not enviado:
            print(
                f"No se pudo reenviar asignacion a {row['cliente_telefono']}. {razon}"
            )
            log_system_event(
                "error",
                "assignment",
                "Fallo reintento de asignacion",
                f"pedido={row['id']} telefono={row['cliente_telefono']} error={razon}",
            )
            continue
        try:
            conn = get_conn()
            conn.execute(
                "UPDATE pedidos SET assignment_notified = 1 WHERE id = ?",
                (row["id"],),
            )
            conn.commit()
            conn.close()
            sent_count += 1
            log_system_event(
                "info",
                "assignment",
                "Asignacion reenviada al cliente",
                f"pedido={row['id']} telefono={row['cliente_telefono']} placa={row['conductor_placa'] or '-'}",
            )
        except Exception as exc:
            print(
                f"No se pudo marcar asignacion enviada para pedido {row['id']}: {exc}"
            )
    return sent_count


def send_assignment_message(phone, conductor_nombre, conductor_placa):
    body = (
        f"Listo, tu servicio fue tomado por {conductor_nombre} "
        f"con placas {conductor_placa}. Ya va en camino."
    )
    return send_whatsapp_message(phone, body)


def send_chat_messages(phone, conductor_placa, conductor_message, include_system):
    ok_msg, err_msg = send_whatsapp_message(phone, conductor_message)
    if ok_msg:
        return True, ""
    return False, err_msg


def queue_assignment_notification(pedido_id, phone, conductor_nombre, conductor_placa):
    if not phone:
        return

    def run():
        enviado, razon = send_assignment_message(
            phone,
            conductor_nombre,
            conductor_placa,
        )
        if enviado:
            try:
                conn = get_conn()
                conn.execute(
                    "UPDATE pedidos SET assignment_notified = 1 WHERE id = ?",
                    (pedido_id,),
                )
                conn.commit()
                conn.close()
                log_system_event(
                    "info",
                    "assignment",
                    "Asignacion enviada al tomar servicio",
                    f"pedido={pedido_id} telefono={phone} placa={conductor_placa}",
                )
            except Exception as exc:
                print(
                    f"No se pudo marcar asignacion enviada para pedido {pedido_id}: {exc}"
                )
        else:
            detalle = razon or "Revisa Twilio y que el numero este habilitado."
            log_system_event(
                "error",
                "assignment",
                "Fallo asignacion al tomar servicio",
                f"pedido={pedido_id} telefono={phone} error={detalle}",
            )

    threading.Thread(
        target=run,
        name=f"pedido-{pedido_id}-assignment",
        daemon=True,
    ).start()


def queue_service_completion_notification(pedido_id, phone):
    if not phone:
        return

    def run():
        ok, err = send_whatsapp_message(
            phone,
            "Conductor fuera de linea. Gracias por preferirnos. Si necesitas otro viaje, solo avisame.",
        )
        if not ok:
            detalle = err or "No se pudo notificar al cliente."
            log_system_event(
                "error",
                "completion",
                "Fallo notificacion al finalizar servicio",
                f"pedido={pedido_id} telefono={phone} error={detalle}",
            )
        else:
            log_system_event(
                "info",
                "completion",
                "Notificacion enviada al finalizar servicio",
                f"pedido={pedido_id} telefono={phone}",
            )

    threading.Thread(
        target=run,
        name=f"pedido-{pedido_id}-completion",
        daemon=True,
    ).start()


def build_map_url():
    token = os.environ.get("MAPBOX_TOKEN")
    if local_settings:
        token = token or getattr(local_settings, "MAPBOX_TOKEN", None)
    if not token:
        return None
    # Carmen de Viboral, Antioquia (lon, lat)
    center = "-75.335,6.085"
    return (
        "https://api.mapbox.com/styles/v1/mapbox/dark-v11/static/"
        f"{center},14,0/900x900?access_token={token}"
    )


def parse_detalles(mensaje_cliente):
    detalles = {}
    if not mensaje_cliente:
        return detalles
    partes = [p.strip() for p in mensaje_cliente.split("|")]
    for parte in partes:
        if ":" in parte:
            clave, valor = parte.split(":", 1)
            detalles[clave.strip()] = valor.strip()
    return detalles


def extract_cliente_info(mensaje_cliente):
    detalles = parse_detalles(mensaje_cliente or "")
    lower = {k.strip().lower(): v for k, v in detalles.items()}
    nombre = lower.get("nombre", "") or ""
    direccion = lower.get("direccion", "") or ""
    return nombre, direccion


def format_ts_short(ts_value):
    if not ts_value:
        return "-"
    try:
        parsed = datetime.strptime(ts_value, "%Y-%m-%d %H:%M:%S")
        return parsed.strftime("%d/%m %H:%M")
    except Exception:
        return ts_value


def elapsed_seconds(ts_value):
    if not ts_value:
        return 0
    try:
        parsed = datetime.strptime(ts_value, "%Y-%m-%d %H:%M:%S")
    except Exception:
        return 0
    delta = datetime.now() - parsed
    total = int(delta.total_seconds())
    if total < 0:
        total = 0
    return total
@app.before_request
def ensure_background_runtime():
    ensure_runtime_workers(reason=request.path or "request")


@app.before_request
def enforce_conductor_subscription():
    endpoint = request.endpoint or ""
    if not session.get("conductor_id"):
        return None
    if endpoint == "static" or endpoint.startswith("admin"):
        return None
    if endpoint in {
        "login",
        "logout",
        "webhook",
        "perfil",
        "payment_pending",
        "index_root",
    }:
        return None
    if endpoint not in CONDUCTOR_SUBSCRIPTION_PROTECTED_ENDPOINTS:
        return None

    conn = get_conn()
    sync_expired_subscriptions(conn)
    row = conn.execute(
        """
        SELECT *
        FROM conductores
        WHERE id = ?
        """,
        (session.get("conductor_id"),),
    ).fetchone()
    conn.close()

    if row is None:
        session.clear()
        if is_json_like_request():
            return jsonify({"ok": False, "error": "Sesion invalida.", "redirect": url_for("login")}), 401
        return redirect(url_for("login"))

    g.subscription_info = get_conductor_subscription_snapshot(row)
    if g.subscription_info["suscripcion_activa"]:
        return None

    if is_json_like_request():
        return (
            jsonify(
                {
                    "ok": False,
                    "error": PAYMENT_PENDING_COPY,
                    "payment_pending": True,
                    "redirect": url_for("payment_pending"),
                }
            ),
            402,
        )
    flash(PAYMENT_PENDING_COPY)
    return redirect(url_for("payment_pending"))


def admin_session_active():
    return bool(session.get("admin_id"))


def pedido_to_dict(row):
    return {
        "id": row["id"],
        "cliente_telefono": row["cliente_telefono"],
        "mensaje_cliente": row["mensaje_cliente"],
        "estado": row["estado"] if "estado" in row.keys() else None,
        "conductor_nombre": row["conductor_nombre"] if "conductor_nombre" in row.keys() else None,
        "conductor_placa": row["conductor_placa"] if "conductor_placa" in row.keys() else None,
        "timestamp": row["timestamp"],
        "hora": format_time(row["timestamp"]),
        "chat_iniciado": row["chat_iniciado"] if "chat_iniciado" in row.keys() else 0,
        "detalles": parse_detalles(row["mensaje_cliente"]),
    }


@app.route("/webhook", methods=["POST"])
def webhook():
    return handle_twilio_webhook(
        request.values,
        get_conn=get_conn,
        format_saved_address=format_saved_address,
        is_reserved_direccion=is_reserved_direccion,
        parse_coords_from_text=parse_coords_from_text,
        reply_sender=send_whatsapp_reply,
    )


@app.route("/login", methods=["GET", "POST"])
def login():
    if request.method == "POST":
        action = request.form.get("action", "login")
        usuario = request.form.get("usuario", "").strip()
        password = request.form.get("password", "")
        confirm_password = request.form.get("confirm_password", "")

        if not usuario or not password:
            flash("Usuario y contrasena son obligatorios.")
            return render_template("login.html")

        conn = get_conn()
        admin_row = conn.execute(
            "SELECT * FROM admins WHERE usuario = ?",
            (usuario,),
        ).fetchone()

        if admin_row is not None:
            if action == "register":
                flash("El usuario admin esta reservado. Inicia sesion.")
                conn.close()
                return render_template("login.html")
            if check_password_hash(admin_row["password_hash"], password):
                conn.close()
                session.clear()
                session["admin_id"] = admin_row["id"]
                session["admin_usuario"] = admin_row["usuario"]
                return redirect(url_for("admin_dashboard"))
            flash("Credenciales invalidas.")
            conn.close()
            return render_template("login.html")

        row = conn.execute(
            "SELECT * FROM conductores WHERE usuario = ?", (usuario,)
        ).fetchone()

        if action == "register":
            if not confirm_password:
                flash("Confirma la contrasena para registrarte.")
                conn.close()
                return render_template("login.html")
            if password != confirm_password:
                flash("Las contrasenas no coinciden.")
                conn.close()
                return render_template("login.html")
            if row is not None:
                flash("El usuario ya existe. Inicia sesion.")
                conn.close()
                return render_template("login.html")
            password_hash = generate_password_hash(password)
            conn.execute(
                "INSERT INTO conductores (usuario, password_hash) VALUES (?, ?)",
                (usuario, password_hash),
            )
            conn.commit()
            row = conn.execute(
                "SELECT * FROM conductores WHERE usuario = ?", (usuario,)
            ).fetchone()
        else:
            if row is None:
                flash("El usuario no existe. Registra una cuenta nueva.")
                conn.close()
                return render_template("login.html")
            if not check_password_hash(row["password_hash"], password):
                flash("Credenciales invalidas.")
                conn.close()
                return render_template("login.html")

        conn.close()
        session["conductor_id"] = row["id"]
        session["conductor_usuario"] = row["usuario"]
        session["conductor_nombre"] = row["nombre_real"]
        session["conductor_placa"] = row["placa"]
        session["conductor_vehiculo"] = row["vehiculo"] if "vehiculo" in row.keys() else None
        session["conductor_modelo"] = row["modelo"] if "modelo" in row.keys() else None

        if not get_conductor_subscription_snapshot(row)["suscripcion_activa"]:
            return redirect(url_for("payment_pending"))

        if not row["nombre_real"] or not row["placa"] or not row["vehiculo"] or not row["modelo"]:
            return redirect(url_for("perfil"))
        return redirect(url_for("inicio"))

    return render_template("login.html")


@app.route("/")
def index_root():
    if session.get("admin_id"):
        return redirect(url_for("admin_dashboard"))
    if session.get("conductor_id"):
        row = get_conductor_row(session.get("conductor_id"))
        if row is not None and not get_conductor_subscription_snapshot(row)["suscripcion_activa"]:
            return redirect(url_for("payment_pending"))
        if (
            not session.get("conductor_nombre")
            or not session.get("conductor_placa")
            or not session.get("conductor_vehiculo")
            or not session.get("conductor_modelo")
        ):
            return redirect(url_for("perfil"))
        return redirect(url_for("inicio"))
    return redirect(url_for("login"))


@app.route("/suscripcion/pago-pendiente")
def payment_pending():
    if not session.get("conductor_id"):
        return redirect(url_for("login"))

    conn = get_conn()
    sync_expired_subscriptions(conn)
    row = conn.execute(
        "SELECT * FROM conductores WHERE id = ?",
        (session.get("conductor_id"),),
    ).fetchone()
    conn.close()

    if row is None:
        session.clear()
        return redirect(url_for("login"))

    subscription = get_conductor_subscription_snapshot(row)
    if subscription["suscripcion_activa"]:
        if (
            not session.get("conductor_nombre")
            or not session.get("conductor_placa")
            or not session.get("conductor_vehiculo")
            or not session.get("conductor_modelo")
        ):
            return redirect(url_for("perfil"))
        return redirect(url_for("inicio"))
    return render_template(
        "payment_pending.html",
        conductor_usuario=row["usuario"],
        conductor_nombre=row["nombre_real"] or row["usuario"],
        subscription=subscription,
        payment_pending_copy=PAYMENT_PENDING_COPY,
        payment_whatsapp_link=build_whatsapp_payment_link(),
        payment_whatsapp_number="+57 3106269788",
    )


@app.route("/perfil", methods=["GET", "POST"])
def perfil():
    if not session.get("conductor_id"):
        return redirect(url_for("login"))

    if (
        session.get("conductor_nombre")
        and session.get("conductor_placa")
        and session.get("conductor_vehiculo")
        and session.get("conductor_modelo")
    ):
        return redirect(url_for("inicio"))

    if request.method == "POST":
        nombre = request.form.get("nombre_real", "").strip()
        placa = request.form.get("placa", "").strip().upper()
        vehiculo = request.form.get("vehiculo", "").strip()
        modelo = request.form.get("modelo", "").strip()

        if not nombre or not placa or not vehiculo or not modelo:
            flash("Nombre, placa, vehiculo y modelo son obligatorios.")
            return render_template("profile.html")

        conn = get_conn()
        conn.execute(
            "UPDATE conductores SET nombre_real = ?, placa = ?, vehiculo = ?, modelo = ? WHERE id = ?",
            (nombre, placa, vehiculo, modelo, session["conductor_id"]),
        )
        conn.commit()
        conn.close()

        session["conductor_nombre"] = nombre
        session["conductor_placa"] = placa
        session["conductor_vehiculo"] = vehiculo
        session["conductor_modelo"] = modelo
        return redirect(url_for("inicio"))

    return render_template("profile.html")


@app.route("/admin")
def admin_dashboard():
    if not admin_session_active():
        return redirect(url_for("login"))
    return render_template("admin.html", admin_usuario=session.get("admin_usuario"))


@app.route("/admin/logout")
def admin_logout():
    session.clear()
    return redirect(url_for("login"))


@app.route("/admin/api/overview")
def admin_overview_api():
    if not admin_session_active():
        return jsonify({"ok": False, "error": "No autenticado."}), 401

    try:
        conn = get_conn()
        sync_expired_subscriptions(conn)
        services_row = conn.execute(
            """
            SELECT COUNT(*) AS total
            FROM pedidos
            WHERE lower(estado) IN ('disponible', 'pendiente')
            """
        ).fetchone()
        drivers_total_row = conn.execute(
            "SELECT COUNT(*) AS total FROM conductores"
        ).fetchone()
        subs_active_row = conn.execute(
            """
            SELECT COUNT(*) AS total
            FROM conductores
            WHERE lower(COALESCE(status_suscripto, 'inactivo')) = 'activo'
            """
        ).fetchone()
        subs_inactive_row = conn.execute(
            """
            SELECT COUNT(*) AS total
            FROM conductores
            WHERE lower(COALESCE(status_suscripto, 'inactivo')) != 'activo'
            """
        ).fetchone()
        earnings_row = conn.execute(
            "SELECT COALESCE(SUM(monto), 0) AS total FROM ganancias"
        ).fetchone()
        active_rows = conn.execute(
            """
            SELECT nombre_real, usuario, placa, active_pedido_id
            FROM conductores
            WHERE active_pedido_id IS NOT NULL
            ORDER BY active_pedido_id DESC
            """
        ).fetchall()
        conn.close()
        db_ok = True
    except Exception:
        services_row = {"total": 0}
        drivers_total_row = {"total": 0}
        subs_active_row = {"total": 0}
        subs_inactive_row = {"total": 0}
        earnings_row = {"total": 0}
        active_rows = []
        db_ok = False

    active_drivers = []
    for row in active_rows:
        active_drivers.append(
            {
                "nombre": row["nombre_real"] or row["usuario"] or "Conductor",
                "placa": row["placa"] or "",
            }
        )

    return jsonify(
        {
            "ok": True,
            "db_ok": db_ok,
            "now": datetime.now().strftime("%H:%M:%S"),
            "twilio_enabled": is_twilio_enabled(),
            "services_count": int(services_row["total"]) if services_row else 0,
            "active_drivers_count": len(active_drivers),
            "active_drivers": active_drivers,
            "drivers_total": int(drivers_total_row["total"]) if drivers_total_row else 0,
            "subscription_active_count": int(subs_active_row["total"]) if subs_active_row else 0,
            "subscription_inactive_count": int(subs_inactive_row["total"]) if subs_inactive_row else 0,
            "earnings_total": int(earnings_row["total"]) if earnings_row else 0,
        }
    )


@app.route("/admin/api/services")
def admin_services_api():
    if not admin_session_active():
        return jsonify({"ok": False, "error": "No autenticado."}), 401

    try:
        conn = get_conn()
        rows = conn.execute(
            """
            SELECT id, cliente_telefono, mensaje_cliente, estado, timestamp
            FROM pedidos
            WHERE lower(estado) IN ('disponible', 'pendiente')
            ORDER BY id DESC
            """
        ).fetchall()
        conn.close()
    except Exception:
        rows = []

    servicios = []
    for row in rows:
        nombre, direccion = extract_cliente_info(row["mensaje_cliente"] or "")
        servicios.append(
            {
                "id": row["id"],
                "cliente_telefono": row["cliente_telefono"] or "",
                "nombre": nombre or "Sin nombre",
                "direccion": direccion or "Sin direccion",
                "estado": row["estado"] or "",
                "timestamp": row["timestamp"],
                "elapsed": elapsed_seconds(row["timestamp"]),
            }
        )

    return jsonify({"ok": True, "servicios": servicios})


@app.route("/admin/api/services/generate", methods=["POST"])
def admin_generate_services_api():
    if not admin_session_active():
        return jsonify({"ok": False, "error": "No autenticado."}), 401

    payload = request.get_json(silent=True) or {}
    qty_text = str(payload.get("qty") or request.form.get("qty", "")).strip()
    try:
        qty = int(qty_text or "0")
    except Exception:
        qty = 0

    if qty <= 0 or qty > 200:
        return jsonify({"ok": False, "error": "Cantidad invalida."}), 400

    try:
        conn = get_conn()
        counter_row = conn.execute(
            "SELECT value FROM config WHERE key = ?",
            ("test_counter",),
        ).fetchone()
        counter_text = counter_row["value"] if counter_row and counter_row["value"] is not None else "1"
        try:
            next_counter = int(counter_text)
        except Exception:
            next_counter = 1

        created = []
        for i in range(qty):
            idx = next_counter + i
            nombre = f"Prueba {idx}"
            coord = TEST_COORDS[idx % len(TEST_COORDS)] if TEST_COORDS else None
            if coord:
                lat, lng = coord
                direccion = f"{lat:.6f}, {lng:.6f}"
                mensaje_cliente = (
                    f"Nombre: {nombre} | Direccion: {direccion} | Latitude: {lat} | Longitude: {lng}"
                )
            else:
                direccion = f"Direccion Prueba {idx}"
                mensaje_cliente = f"Nombre: {nombre} | Direccion: {direccion}"
            telefono = f"whatsapp:+57{3000000000 + idx}"
            fecha = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            cur = conn.execute(
                """
                INSERT INTO pedidos (cliente_telefono, mensaje_cliente, estado, timestamp)
                VALUES (?, ?, 'Disponible', ?)
                """,
                (telefono, mensaje_cliente, fecha),
            )
            created.append(cur.lastrowid)

        conn.execute(
            "INSERT OR REPLACE INTO config (key, value) VALUES (?, ?)",
            ("test_counter", str(next_counter + qty)),
        )
        conn.commit()
        conn.close()
    except Exception as exc:
        return jsonify({"ok": False, "error": f"No se pudo crear. {exc}"}), 500

    return jsonify({"ok": True, "created": created})


@app.route("/admin/api/services/generate_coords", methods=["POST"])
def admin_generate_services_coords_api():
    if not admin_session_active():
        return jsonify({"ok": False, "error": "No autenticado."}), 401

    payload = request.get_json(silent=True) or {}
    coords_raw = payload.get("coords") or ""
    label_prefix = (payload.get("label_prefix") or "Prueba").strip()

    lines = [line.strip() for line in str(coords_raw).splitlines() if line.strip()]
    coords = []
    for line in lines:
        parts = [p.strip() for p in line.split(",")]
        if len(parts) < 2:
            continue
        try:
            lat = float(parts[0])
            lng = float(parts[1])
        except Exception:
            continue
        coords.append((lat, lng))

    if not coords:
        return jsonify({"ok": False, "error": "Sin coordenadas validas."}), 400

    try:
        conn = get_conn()
        counter_row = conn.execute(
            "SELECT value FROM config WHERE key = ?",
            ("test_counter",),
        ).fetchone()
        counter_text = counter_row["value"] if counter_row and counter_row["value"] is not None else "1"
        try:
            next_counter = int(counter_text)
        except Exception:
            next_counter = 1

        created = []
        for lat, lng in coords:
            idx = next_counter
            next_counter += 1
            nombre = f"{label_prefix} {idx}"
            direccion = f"{lat:.6f}, {lng:.6f}"
            telefono = f"whatsapp:+57{3000000000 + idx}"
            mensaje_cliente = (
                f"Nombre: {nombre} | Direccion: {direccion} | Latitude: {lat} | Longitude: {lng}"
            )
            fecha = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            cur = conn.execute(
                """
                INSERT INTO pedidos (cliente_telefono, mensaje_cliente, estado, timestamp)
                VALUES (?, ?, 'Disponible', ?)
                """,
                (telefono, mensaje_cliente, fecha),
            )
            created.append(cur.lastrowid)

        conn.execute(
            "INSERT OR REPLACE INTO config (key, value) VALUES (?, ?)",
            ("test_counter", str(next_counter)),
        )
        conn.commit()
        conn.close()
    except Exception as exc:
        return jsonify({"ok": False, "error": f"No se pudo crear. {exc}"}), 500

    return jsonify({"ok": True, "created": created})

@app.route("/admin/api/services/delete", methods=["POST"])
def admin_delete_service_api():
    if not admin_session_active():
        return jsonify({"ok": False, "error": "No autenticado."}), 401

    payload = request.get_json(silent=True) or {}
    service_id = payload.get("id") or request.form.get("id")
    try:
        service_id = int(service_id)
    except Exception:
        return jsonify({"ok": False, "error": "ID invalido."}), 400

    try:
        conn = get_conn()
        conn.execute("DELETE FROM pedidos WHERE id = ?", (service_id,))
        conn.commit()
        conn.close()
    except Exception as exc:
        return jsonify({"ok": False, "error": str(exc)}), 500

    return jsonify({"ok": True})


@app.route("/admin/api/drivers")
def admin_drivers_api():
    if not admin_session_active():
        return jsonify({"ok": False, "error": "No autenticado."}), 401

    try:
        conn = get_conn()
        sync_expired_subscriptions(conn)
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
                c.status_suscripto,
                c.fin_suscripcion,
                c.mensualidades_pagadas,
                c.dias_mensualidad,
                c.ultima_mensualidad_at,
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
                ) AS ultimo_servicio,
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

    conductores = []
    for row in rows:
        subscription = get_conductor_subscription_snapshot(row)
        conductores.append(
            {
                "id": row["id"],
                "usuario": row["usuario"] or "-",
                "nombre": row["nombre_real"] or "-",
                "placa": row["placa"] or "-",
                "vehiculo": row["vehiculo"] or "-",
                "modelo": row["modelo"] or "-",
                "estado": "Ocupado" if row["active_pedido_id"] else "Libre",
                "servicios": row["servicios"] if row["servicios"] is not None else 0,
                "ultimo": format_ts_short(row["ultimo_servicio"]),
                "ganancias": format_cop(row["ganancias"]),
                "status_suscripto": subscription["status_suscripto"],
                "suscripcion_activa": subscription["suscripcion_activa"],
                "fin_suscripcion": subscription["fin_suscripcion_short"],
                "dias_restantes": subscription["dias_restantes"],
                "mensualidades_pagadas": subscription["mensualidades_pagadas"],
                "dias_mensualidad": subscription["dias_mensualidad"],
                "ultima_mensualidad_at": format_ts_short(subscription["ultima_mensualidad_at"]),
            }
        )

    return jsonify({"ok": True, "conductores": conductores})


@app.route("/admin/api/driver/subscription", methods=["POST"])
def admin_driver_subscription_api():
    if not admin_session_active():
        return jsonify({"ok": False, "error": "No autenticado."}), 401

    payload = request.get_json(silent=True) or {}
    conductor_id = payload.get("id") or request.form.get("id", "")
    action = (payload.get("action") or request.form.get("action", "")).strip().lower()
    days_value = payload.get("days")
    if days_value is None:
        days_value = request.form.get("days", "")

    try:
        conductor_id = int(conductor_id)
    except Exception:
        return jsonify({"ok": False, "error": "ID invalido."}), 400

    try:
        extra_days = int(days_value) if str(days_value).strip() else 0
    except Exception:
        return jsonify({"ok": False, "error": "Dias invalidos."}), 400

    conn = get_conn()
    sync_expired_subscriptions(conn)
    row = conn.execute(
        "SELECT * FROM conductores WHERE id = ?",
        (conductor_id,),
    ).fetchone()
    if row is None:
        conn.close()
        return jsonify({"ok": False, "error": "Conductor no encontrado."}), 404

    now = db_now()
    base_fin = parse_db_datetime(row["fin_suscripcion"])
    if base_fin is None or base_fin < now:
        base_fin = now

    if action == "set_plan_days":
        if extra_days <= 0:
            conn.close()
            return jsonify({"ok": False, "error": "Define una cantidad valida de dias."}), 400
        conn.execute(
            "UPDATE conductores SET dias_mensualidad = ? WHERE id = ?",
            (extra_days, conductor_id),
        )
        message = f"Dias del plan actualizados a {extra_days}."
    elif action == "renew":
        plan_days = row["dias_mensualidad"] or DEFAULT_MEMBERSHIP_DAYS
        new_end = base_fin + timedelta(days=plan_days)
        conn.execute(
            """
            UPDATE conductores
            SET status_suscripto = 'activo',
                fin_suscripcion = ?,
                mensualidades_pagadas = COALESCE(mensualidades_pagadas, 0) + 1,
                ultima_mensualidad_at = ?
            WHERE id = ?
            """,
            (format_db_datetime(new_end), format_db_datetime(now), conductor_id),
        )
        message = f"Mensualidad aplicada por {plan_days} dias."
    elif action == "add_days":
        if extra_days <= 0:
            conn.close()
            return jsonify({"ok": False, "error": "Escribe cuantos dias extra quieres sumar."}), 400
        new_end = base_fin + timedelta(days=extra_days)
        conn.execute(
            """
            UPDATE conductores
            SET status_suscripto = 'activo',
                fin_suscripcion = ?
            WHERE id = ?
            """,
            (format_db_datetime(new_end), conductor_id),
        )
        message = f"Se agregaron {extra_days} dias extra."
    elif action == "deactivate":
        conn.execute(
            """
            UPDATE conductores
            SET status_suscripto = 'inactivo'
            WHERE id = ?
            """,
            (conductor_id,),
        )
        message = "Suscripcion desactivada."
    else:
        conn.close()
        return jsonify({"ok": False, "error": "Accion invalida."}), 400

    conn.commit()
    updated = conn.execute(
        "SELECT * FROM conductores WHERE id = ?",
        (conductor_id,),
    ).fetchone()
    conn.close()

    snapshot = get_conductor_subscription_snapshot(updated)
    log_system_event(
        "info",
        "membership",
        "Cambio de suscripcion desde admin",
        f"conductor_id={conductor_id} action={action} status={snapshot['status_suscripto']} fin={snapshot['fin_suscripcion']}",
    )
    return jsonify({"ok": True, "message": message, "subscription": snapshot})


@app.route("/admin/api/driver/credentials")
def admin_driver_credentials_api():
    if not admin_session_active():
        return jsonify({"ok": False, "error": "No autenticado."}), 401

    conductor_id = request.args.get("id", "").strip()
    try:
        conductor_id = int(conductor_id)
    except Exception:
        return jsonify({"ok": False, "error": "ID invalido."}), 400

    conn = get_conn()
    row = conn.execute(
        "SELECT usuario, password_enc FROM conductores WHERE id = ?",
        (conductor_id,),
    ).fetchone()
    conn.close()

    if row is None:
        return jsonify({"ok": False, "error": "Conductor no encontrado."}), 404

    password_plain = ""
    if row["password_enc"]:
        try:
            password_plain = decrypt_password(row["password_enc"]) or ""
        except Exception:
            password_plain = ""

    return jsonify(
        {
            "ok": True,
            "usuario": row["usuario"],
            "password": password_plain,
        }
    )


@app.route("/admin/api/driver/password", methods=["POST"])
def admin_driver_password_api():
    if not admin_session_active():
        return jsonify({"ok": False, "error": "No autenticado."}), 401

    payload = request.get_json(silent=True) or {}
    conductor_id = payload.get("id") or request.form.get("id", "")
    new_password = (payload.get("password") or request.form.get("password", "")).strip()

    try:
        conductor_id = int(conductor_id)
    except Exception:
        return jsonify({"ok": False, "error": "ID invalido."}), 400

    if not new_password:
        return jsonify({"ok": False, "error": "Contrasena vacia."}), 400

    enc_value = None
    try:
        enc_value = encrypt_password(new_password)
    except Exception:
        enc_value = None

    try:
        conn = get_conn()
        conn.execute(
            "UPDATE conductores SET password_hash = ?, password_enc = ? WHERE id = ?",
            (generate_password_hash(new_password), enc_value, conductor_id),
        )
        conn.commit()
        conn.close()
    except Exception as exc:
        return jsonify({"ok": False, "error": str(exc)}), 500

    return jsonify({"ok": True})


@app.route("/admin/api/twilio/toggle", methods=["POST"])
def admin_twilio_toggle_api():
    if not admin_session_active():
        return jsonify({"ok": False, "error": "No autenticado."}), 401
    return jsonify(
        {
            "ok": False,
            "error": "Twilio queda siempre activo. Esta opcion fue removida.",
            "enabled": True,
        }
    ), 400


@app.route("/admin/api/logs")
def admin_logs_api():
    if not admin_session_active():
        return jsonify({"ok": False, "error": "No autenticado."}), 401

    limit_text = request.args.get("limit", "80").strip()
    try:
        limit = int(limit_text)
    except Exception:
        limit = 80
    limit = max(10, min(limit, 200))

    try:
        conn = get_conn()
        rows = conn.execute(
            """
            SELECT id, level, category, message, details, timestamp
            FROM system_logs
            ORDER BY id DESC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()
        conn.close()
    except Exception as exc:
        return jsonify({"ok": False, "error": str(exc)}), 500

    logs = []
    for row in rows:
        logs.append(
            {
                "id": row["id"],
                "level": row["level"],
                "category": row["category"],
                "message": row["message"],
                "details": row["details"] or "",
                "timestamp": row["timestamp"] or "",
            }
        )
    return jsonify({"ok": True, "logs": logs, "twilio_source": get_twilio_source_summary()})


@app.route("/admin/api/logs/clear", methods=["POST"])
def admin_logs_clear_api():
    if not admin_session_active():
        return jsonify({"ok": False, "error": "No autenticado."}), 401

    try:
        conn = get_conn()
        conn.execute("DELETE FROM system_logs")
        conn.commit()
        conn.close()
    except Exception as exc:
        return jsonify({"ok": False, "error": str(exc)}), 500

    log_system_event("warn", "admin", "Logs limpiados desde admin", "")
    return jsonify({"ok": True})


def build_panel_context():
    conn = get_conn()
    active_state = get_driver_active_service(
        conn,
        session.get("conductor_id"),
        session.get("conductor_nombre"),
        session.get("conductor_placa"),
    )
    active_pedido_id = active_state["active_pedido_id"] if active_state else None
    driver_online = active_state["driver_online"] if active_state else True

    active_pedido = None
    chat_messages = []
    if active_pedido_id:
        pedido_row = active_state["pedido_row"] if active_state else None
        if pedido_row and pedido_row["estado"] == "Tomado":
            active_pedido = pedido_to_dict(pedido_row)
            chat_rows = conn.execute(
                """
                SELECT id, sender, message, timestamp
                FROM chat_mensajes
                WHERE pedido_id = ?
                ORDER BY id ASC
                """,
                (active_pedido_id,),
            ).fetchall()
            chat_messages = [
                {
                    "id": row["id"],
                    "sender": row["sender"],
                    "message": row["message"],
                    "timestamp": row["timestamp"],
                }
                for row in chat_rows
            ]
        else:
            active_pedido_id = None

    rows = []
    if not active_pedido_id and driver_online:
        rows = conn.execute(
            """
            SELECT id, estado, cliente_telefono, mensaje_cliente, timestamp
            FROM pedidos
            WHERE lower(estado) IN ('disponible', 'pendiente')
            ORDER BY id ASC
            """
        ).fetchall()

    rows_mios = conn.execute(
        """
        SELECT id, estado, cliente_telefono, mensaje_cliente, timestamp
        FROM pedidos
        WHERE estado IN ('Tomado', 'Completado')
          AND conductor_nombre = ? AND conductor_placa = ?
        ORDER BY id DESC
        """,
        (session.get("conductor_nombre"), session.get("conductor_placa")),
    ).fetchall()

    ganancias_row = conn.execute(
        "SELECT COALESCE(SUM(monto), 0) as total FROM ganancias WHERE conductor_id = ?",
        (session.get("conductor_id"),),
    ).fetchone()
    conn.commit()
    conn.close()

    pedidos = []
    for row in rows:
        pedidos.append(
            {
                "id": row["id"],
                "estado": row["estado"],
                "cliente_telefono": row["cliente_telefono"],
                "mensaje_cliente": row["mensaje_cliente"],
                "timestamp": row["timestamp"],
                "hora": format_time(row["timestamp"]),
                "detalles": parse_detalles(row["mensaje_cliente"]),
            }
        )

    pedidos_mios = []
    for row in rows_mios:
        pedidos_mios.append(
            {
                "id": row["id"],
                "estado": row["estado"],
                "cliente_telefono": row["cliente_telefono"],
                "mensaje_cliente": row["mensaje_cliente"],
                "timestamp": row["timestamp"],
                "hora": format_time(row["timestamp"]),
                "detalles": parse_detalles(row["mensaje_cliente"]),
            }
        )

    ganancias_total = ganancias_row["total"] if ganancias_row else 0
    map_url = build_map_url()

    return {
        "pedidos": pedidos,
        "pedidos_mios": pedidos_mios,
        "ganancias_total": ganancias_total,
        "ganancias_total_formatted": format_cop(ganancias_total),
        "map_url": map_url,
        "active_pedido": active_pedido,
        "chat_messages": chat_messages,
        "driver_online": driver_online,
        "ui_mode": get_ui_mode(),
    }


@app.route("/inicio")
def inicio():
    if not session.get("conductor_id"):
        return redirect(url_for("login"))

    if (
        not session.get("conductor_nombre")
        or not session.get("conductor_placa")
        or not session.get("conductor_vehiculo")
        or not session.get("conductor_modelo")
    ):
        return redirect(url_for("perfil"))

    context = build_panel_context()
    context.update(
        {
            "conductor_nombre": session.get("conductor_nombre"),
            "conductor_placa": session.get("conductor_placa"),
            "default_page": "inicio",
        }
    )

    return render_template("index.html", **context)


@app.route("/dashboard", methods=["GET"])
def dashboard():
    if not session.get("conductor_id"):
        return redirect(url_for("login"))

    if (
        not session.get("conductor_nombre")
        or not session.get("conductor_placa")
        or not session.get("conductor_vehiculo")
        or not session.get("conductor_modelo")
    ):
        return redirect(url_for("perfil"))

    context = build_panel_context()
    context.update(
        {
            "conductor_nombre": session.get("conductor_nombre"),
            "conductor_placa": session.get("conductor_placa"),
            "default_page": "servicios",
        }
    )

    return render_template("index.html", **context)


@app.route("/tomar", methods=["POST"])
def tomar():
    if not session.get("conductor_id"):
        return redirect(url_for("login"))

    if not session.get("conductor_nombre") or not session.get("conductor_placa"):
        return redirect(url_for("perfil"))

    is_xhr = request.headers.get("X-Requested-With") == "XMLHttpRequest"
    pedido_id = request.form.get("pedido_id", "").strip()
    if not pedido_id:
        if is_xhr:
            return jsonify({"ok": False, "error": "Pedido invalido."}), 400
        flash("Pedido invalido.")
        return redirect(url_for("dashboard"))

    conn = get_conn()
    try:
        conn.execute("BEGIN IMMEDIATE")
        availability_row = conn.execute(
            "SELECT is_online FROM conductores WHERE id = ?",
            (session.get("conductor_id"),),
        ).fetchone()
        is_online = (
            bool(availability_row["is_online"])
            if availability_row and availability_row["is_online"] is not None
            else True
        )
        if not is_online:
            conn.rollback()
            conn.close()
            if is_xhr:
                return jsonify({"ok": False, "error": "Activa el modo disponible para tomar servicios."}), 409
            flash("Activa el modo disponible para tomar servicios.")
            return redirect(url_for("dashboard"))

        active_state = get_driver_active_service(
            conn,
            session.get("conductor_id"),
            session.get("conductor_nombre"),
            session.get("conductor_placa"),
        )
        if active_state and active_state["active_pedido_id"]:
            conn.rollback()
            conn.close()
            if is_xhr:
                return jsonify({"ok": False, "error": "Ya tienes un viaje activo."}), 409
            flash("Ya tienes un viaje activo.")
            return redirect(url_for("dashboard"))

        cur = conn.execute(
            """
            UPDATE pedidos
            SET estado = 'Tomado', conductor_nombre = ?, conductor_placa = ?, assignment_notified = 0
            WHERE id = ? AND lower(estado) IN ('disponible', 'pendiente')
            """,
            (session["conductor_nombre"], session["conductor_placa"], pedido_id),
        )
        if cur.rowcount == 0:
            conn.rollback()
            conn.close()
            if is_xhr:
                return jsonify({"ok": False, "error": "Este servicio ya fue tomado."}), 409
            flash("Este servicio ya fue tomado por otro conductor.")
            return redirect(url_for("dashboard"))

        row = conn.execute(
            "SELECT * FROM pedidos WHERE id = ?",
            (pedido_id,),
        ).fetchone()
        conn.execute(
            "UPDATE conductores SET active_pedido_id = ? WHERE id = ?",
            (pedido_id, session.get("conductor_id")),
        )
        conn.commit()
    except Exception as exc:
        try:
            conn.rollback()
        except Exception:
            pass
        conn.close()
        if is_xhr:
            return jsonify({"ok": False, "error": f"No se pudo tomar el servicio. {exc}"}), 500
        flash("No se pudo tomar el servicio en este momento.")
        return redirect(url_for("dashboard"))
    conn.close()

    cliente_telefono = row["cliente_telefono"] if row else ""
    if is_xhr:
        if cliente_telefono:
            queue_assignment_notification(
                pedido_id,
                cliente_telefono,
                session["conductor_nombre"],
                session["conductor_placa"],
            )
        return jsonify(
            {
                "ok": True,
                "pedido": pedido_to_dict(row),
                "active_pedido_id": row["id"] if row else None,
                "notification_queued": bool(cliente_telefono),
                "redirect_url": url_for("dashboard"),
                "ui_mode": get_ui_mode(),
            }
        )

    if cliente_telefono:
        enviado, razon = send_assignment_message(
            cliente_telefono,
            session["conductor_nombre"],
            session["conductor_placa"],
        )
        if enviado:
            try:
                conn = get_conn()
                conn.execute(
                    "UPDATE pedidos SET assignment_notified = 1 WHERE id = ?",
                    (pedido_id,),
                )
                conn.commit()
                conn.close()
                log_system_event(
                    "info",
                    "assignment",
                    "Asignacion enviada al tomar servicio",
                    f"pedido={pedido_id} telefono={cliente_telefono} placa={session['conductor_placa']}",
                )
            except Exception as exc:
                print(
                    f"No se pudo marcar asignacion enviada para pedido {pedido_id}: {exc}"
                )
        if not enviado:
            detalle = razon or "Revisa Twilio y que el numero este habilitado."
            log_system_event(
                "error",
                "assignment",
                "Fallo asignacion al tomar servicio",
                f"pedido={pedido_id} telefono={cliente_telefono} error={detalle}",
            )
            flash(f"No se pudo enviar el mensaje al cliente. {detalle}")

    flash("Servicio tomado. Aqui tienes los datos del cliente.")
    return redirect(url_for("dashboard"))


@app.route("/vaciar_tomados", methods=["POST"])
def vaciar_tomados():
    if not session.get("conductor_id"):
        return redirect(url_for("login"))

    conn = get_conn()
    conn.execute(
        """
        UPDATE pedidos
        SET estado = 'Completado'
        WHERE estado = 'Tomado' AND conductor_nombre = ? AND conductor_placa = ?
        """,
        (session.get("conductor_nombre"), session.get("conductor_placa")),
    )
    conn.commit()
    conn.close()

    flash("Servicios tomados limpiados (marcados como completados).")
    return redirect(url_for("dashboard"))


@app.route("/finalizar", methods=["POST"])
def finalizar_servicio():
    if not session.get("conductor_id"):
        return redirect(url_for("login"))

    is_xhr = request.headers.get("X-Requested-With") == "XMLHttpRequest"
    conn = get_conn()
    active_row = conn.execute(
        "SELECT active_pedido_id FROM conductores WHERE id = ?",
        (session.get("conductor_id"),),
    ).fetchone()
    active_id = active_row["active_pedido_id"] if active_row else None

    if not active_id:
        conn.close()
        if is_xhr:
            return jsonify({"ok": False, "error": "No tienes viaje activo."}), 400
        flash("No tienes viaje activo.")
        return redirect(url_for("dashboard"))

    pedido_row = conn.execute(
        "SELECT * FROM pedidos WHERE id = ?", (active_id,)
    ).fetchone()

    if pedido_row:
        conn.execute(
            """
            INSERT INTO chat_mensajes (pedido_id, sender, message, timestamp)
            VALUES (?, 'sistema', ?, ?)
            """,
            (
                active_id,
                "Conductor fuera de linea. Gracias por preferirnos. Si necesitas otro viaje, solo avisame.",
                datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            ),
        )

    conn.execute(
        "UPDATE pedidos SET estado = 'Completado' WHERE id = ?",
        (active_id,),
    )
    conn.execute(
        "UPDATE conductores SET active_pedido_id = NULL WHERE id = ?",
        (session.get("conductor_id"),),
    )
    conn.commit()
    conn.close()

    warning = ""
    if is_xhr:
        if pedido_row and pedido_row["cliente_telefono"]:
            queue_service_completion_notification(
                active_id,
                pedido_row["cliente_telefono"],
            )
        return jsonify(
            {
                "ok": True,
                "notification_queued": bool(
                    pedido_row and pedido_row["cliente_telefono"]
                ),
            }
        )

    if pedido_row:
        ok, err = send_whatsapp_message(
            pedido_row["cliente_telefono"],
            "Conductor fuera de linea. Gracias por preferirnos. Si necesitas otro viaje, solo avisame.",
        )
        if not ok:
            warning = err or "No se pudo notificar al cliente."

    if warning:
        flash(warning)
    else:
        flash("Servicio finalizado.")
    return redirect(url_for("dashboard"))


@app.route("/servicio/<int:pedido_id>")
def servicio_detalle(pedido_id):
    if not session.get("conductor_id"):
        return redirect(url_for("login"))

    if not session.get("conductor_nombre") or not session.get("conductor_placa"):
        return redirect(url_for("perfil"))

    conn = get_conn()
    row = conn.execute(
        "SELECT * FROM pedidos WHERE id = ?", (pedido_id,)
    ).fetchone()
    conn.close()

    if row is None:
        flash("Servicio no encontrado.")
        return redirect(url_for("dashboard"))

    if (
        row["conductor_nombre"] != session.get("conductor_nombre")
        or row["conductor_placa"] != session.get("conductor_placa")
    ):
        flash("No tienes acceso a este servicio.")
        return redirect(url_for("dashboard"))

    pedido = {
        "id": row["id"],
        "cliente_telefono": row["cliente_telefono"],
        "mensaje_cliente": row["mensaje_cliente"],
        "timestamp": row["timestamp"],
        "hora": format_time(row["timestamp"]),
        "detalles": parse_detalles(row["mensaje_cliente"]),
    }

    return render_template(
        "servicio.html",
        pedido=pedido,
        conductor_nombre=session.get("conductor_nombre"),
        conductor_placa=session.get("conductor_placa"),
    )


@app.route("/api/servicio/<int:pedido_id>")
def servicio_detalle_api(pedido_id):
    if not session.get("conductor_id"):
        return jsonify({"ok": False, "error": "No autenticado."}), 401

    conn = get_conn()
    row = conn.execute(
        "SELECT * FROM pedidos WHERE id = ?", (pedido_id,)
    ).fetchone()
    conn.close()

    if row is None:
        return jsonify({"ok": False, "error": "Servicio no encontrado."}), 404

    estado = row["estado"]
    if estado == "Tomado":
        if (
            row["conductor_nombre"] != session.get("conductor_nombre")
            or row["conductor_placa"] != session.get("conductor_placa")
        ):
            return jsonify({"ok": False, "error": "Sin acceso."}), 403

    return jsonify({"ok": True, "pedido": pedido_to_dict(row)})


@app.route("/api/servicios/status")
def servicios_status_api():
    if not session.get("conductor_id"):
        return jsonify({"ok": False, "error": "No autenticado."}), 401

    conn = get_conn()
    driver_row = conn.execute(
        "SELECT is_online FROM conductores WHERE id = ?",
        (session.get("conductor_id"),),
    ).fetchone()
    is_online = bool(driver_row["is_online"]) if driver_row and driver_row["is_online"] is not None else True
    if not is_online:
        conn.close()
        return jsonify({"ok": True, "count": 0, "max_id": 0, "online": False})

    row = conn.execute(
        """
        SELECT COUNT(*) AS count, MAX(id) AS max_id
        FROM pedidos
        WHERE lower(estado) IN ('disponible', 'pendiente')
        """
    ).fetchone()
    conn.close()

    count_val = row["count"] if row and row["count"] is not None else 0
    max_id_val = row["max_id"] if row and row["max_id"] is not None else 0
    return jsonify({"ok": True, "count": count_val, "max_id": max_id_val, "online": True})


@app.route("/api/servicios/list")
def servicios_list_api():
    if not session.get("conductor_id"):
        return jsonify({"ok": False, "error": "No autenticado."}), 401

    try:
        conn = get_conn()
        driver_row = conn.execute(
            "SELECT is_online FROM conductores WHERE id = ?",
            (session.get("conductor_id"),),
        ).fetchone()
        is_online = bool(driver_row["is_online"]) if driver_row and driver_row["is_online"] is not None else True
        if not is_online:
            conn.close()
            return jsonify({"ok": True, "servicios": [], "online": False})
        rows = conn.execute(
            """
            SELECT id, cliente_telefono, mensaje_cliente, timestamp
            FROM pedidos
            WHERE lower(estado) IN ('disponible', 'pendiente')
            ORDER BY id ASC
            """
        ).fetchall()
        conn.close()
    except Exception:
        rows = []

    servicios = []
    for row in rows:
        detalles = parse_detalles(row["mensaje_cliente"] or "")
        direccion = detalles.get("Direccion", row["mensaje_cliente"] or "")
        lat = detalles.get("Latitude") or detalles.get("Latitud") or ""
        lng = detalles.get("Longitude") or detalles.get("Longitud") or ""
        if (not lat or not lng) and direccion:
            parsed_lat, parsed_lng = parse_coords_from_text(direccion)
            if parsed_lat is not None and parsed_lng is not None:
                lat = str(parsed_lat)
                lng = str(parsed_lng)
        servicios.append(
            {
                "id": row["id"],
                "telefono": row["cliente_telefono"] or "",
                "hora": format_time(row["timestamp"]),
                "direccion": format_saved_address(direccion),
                "nombre": detalles.get("Nombre", "Cliente"),
                "lat": lat,
                "lng": lng,
            }
        )

    return jsonify({"ok": True, "servicios": servicios, "online": True})


@app.route("/api/conductor/availability", methods=["POST"])
def conductor_availability_api():
    if not session.get("conductor_id"):
        return jsonify({"ok": False, "error": "No autenticado."}), 401

    data = request.get_json(silent=True) or {}
    enabled = data.get("enabled")
    if enabled is None:
        enabled = request.form.get("enabled")

    if isinstance(enabled, str):
        enabled = enabled.strip().lower() in {"1", "true", "on", "si", "yes"}
    else:
        enabled = bool(enabled)

    conn = get_conn()
    conn.execute(
        "UPDATE conductores SET is_online = ? WHERE id = ?",
        (1 if enabled else 0, session.get("conductor_id")),
    )
    conn.commit()
    conn.close()
    return jsonify({"ok": True, "online": enabled})


@app.route("/api/conductor/ui-mode", methods=["GET", "POST"])
def conductor_ui_mode_api():
    if not session.get("conductor_id"):
        return jsonify({"ok": False, "error": "No autenticado."}), 401

    if request.method == "GET":
        return jsonify({"ok": True, "ui_mode": get_ui_mode()})

    data = request.get_json(silent=True) or {}
    ui_mode = normalize_ui_mode(data.get("ui_mode") or request.form.get("ui_mode"))
    set_config_value("ui_mode", ui_mode)
    return jsonify({"ok": True, "ui_mode": ui_mode})


@app.route("/api/conductor/active-service")
def conductor_active_service_api():
    if not session.get("conductor_id"):
        return jsonify({"ok": False, "error": "No autenticado."}), 401

    conn = get_conn()
    state = get_driver_active_service(
        conn,
        session.get("conductor_id"),
        session.get("conductor_nombre"),
        session.get("conductor_placa"),
    )

    pedido_row = state["pedido_row"] if state else None
    messages = []
    if pedido_row:
        chat_rows = conn.execute(
            """
            SELECT id, sender, message, timestamp
            FROM chat_mensajes
            WHERE pedido_id = ?
            ORDER BY id ASC
            """,
            (pedido_row["id"],),
        ).fetchall()
        messages = [
            {
                "id": row["id"],
                "sender": row["sender"],
                "message": row["message"],
                "timestamp": row["timestamp"],
            }
            for row in chat_rows
        ]
    conn.commit()
    conn.close()

    return jsonify(
        {
            "ok": True,
            "has_active_service": bool(pedido_row),
            "pedido": pedido_to_dict(pedido_row) if pedido_row else None,
            "chat_messages": messages,
            "ui_mode": get_ui_mode(),
        }
    )


@app.route("/api/chat/<int:pedido_id>")
def chat_api(pedido_id):
    if not session.get("conductor_id"):
        return jsonify({"ok": False, "error": "No autenticado."}), 401

    conn = get_conn()
    row = conn.execute(
        "SELECT * FROM pedidos WHERE id = ?", (pedido_id,)
    ).fetchone()
    if row is None:
        conn.close()
        return jsonify({"ok": True, "messages": [], "inactive": True, "reason": "Servicio no encontrado."})

    if row["estado"] != "Tomado":
        conn.close()
        return jsonify({"ok": True, "messages": [], "inactive": True, "reason": "Servicio no activo."})

    if (
        row["conductor_nombre"] != session.get("conductor_nombre")
        or row["conductor_placa"] != session.get("conductor_placa")
    ):
        conn.close()
        return jsonify({"ok": False, "error": "Sin acceso."}), 403

    since_id = request.args.get("since_id", "0")
    try:
        since_id_val = int(since_id)
    except Exception:
        since_id_val = 0

    chat_rows = conn.execute(
        """
        SELECT id, sender, message, timestamp
        FROM chat_mensajes
        WHERE pedido_id = ? AND id > ?
        ORDER BY id ASC
        """,
        (pedido_id, since_id_val),
    ).fetchall()
    conn.close()

    messages = [
        {
            "id": row["id"],
            "sender": row["sender"],
            "message": row["message"],
            "timestamp": row["timestamp"],
        }
        for row in chat_rows
    ]
    return jsonify({"ok": True, "messages": messages})


@app.route("/api/chat/send", methods=["POST"])
def chat_send_api():
    if not session.get("conductor_id"):
        return jsonify({"ok": False, "error": "No autenticado."}), 401

    pedido_id = request.form.get("pedido_id", "").strip()
    message = request.form.get("message", "").strip()
    if not pedido_id or not message:
        return jsonify({"ok": False, "error": "Mensaje vacio."}), 400

    conn = get_conn()
    row = conn.execute(
        "SELECT * FROM pedidos WHERE id = ?", (pedido_id,)
    ).fetchone()
    if row is None:
        conn.close()
        return jsonify({"ok": False, "error": "Servicio no encontrado."}), 404

    if row["estado"] != "Tomado":
        conn.close()
        return jsonify({"ok": False, "error": "Servicio no activo."}), 400

    if (
        row["conductor_nombre"] != session.get("conductor_nombre")
        or row["conductor_placa"] != session.get("conductor_placa")
    ):
        conn.close()
        return jsonify({"ok": False, "error": "Sin acceso."}), 403

    include_system = row["chat_iniciado"] == 0
    inserted = []
    cur = conn.cursor()
    sent_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    if include_system:
        cur.execute(
            "UPDATE pedidos SET chat_iniciado = 1 WHERE id = ?",
            (pedido_id,),
        )

    cur.execute(
        """
        INSERT INTO chat_mensajes (pedido_id, sender, message, timestamp)
        VALUES (?, 'conductor', ?, ?)
        """,
        (pedido_id, message, sent_at),
    )
    inserted.append(
        {
            "id": cur.lastrowid,
            "sender": "conductor",
            "message": message,
            "timestamp": sent_at,
        }
    )
    conn.commit()
    conn.close()

    cliente_telefono = row["cliente_telefono"]
    enviado, razon = send_chat_messages(
        cliente_telefono,
        session.get("conductor_placa"),
        message,
        include_system,
    )

    if not enviado:
        return jsonify(
            {
                "ok": True,
                "warning": razon or "No se pudo enviar a WhatsApp.",
                "messages": inserted,
                "chat_started": True,
            }
        )

    return jsonify({"ok": True, "messages": inserted, "chat_started": True})


@app.route("/servicio/<int:pedido_id>/reenviar", methods=["POST"])
def reenviar_asignacion(pedido_id):
    if not session.get("conductor_id"):
        return redirect(url_for("login"))

    if not session.get("conductor_nombre") or not session.get("conductor_placa"):
        return redirect(url_for("perfil"))

    conn = get_conn()
    row = conn.execute(
        "SELECT * FROM pedidos WHERE id = ?", (pedido_id,)
    ).fetchone()
    conn.close()

    if row is None:
        flash("Servicio no encontrado.")
        return redirect(url_for("dashboard"))

    if (
        row["conductor_nombre"] != session.get("conductor_nombre")
        or row["conductor_placa"] != session.get("conductor_placa")
    ):
        flash("No tienes acceso a este servicio.")
        return redirect(url_for("dashboard"))

    cliente_telefono = row["cliente_telefono"]
    enviado, razon = send_assignment_message(
        cliente_telefono,
        session.get("conductor_nombre"),
        session.get("conductor_placa"),
    )
    if enviado:
        try:
            conn = get_conn()
            conn.execute(
                "UPDATE pedidos SET assignment_notified = 1 WHERE id = ?",
                (pedido_id,),
            )
            conn.commit()
            conn.close()
        except Exception as exc:
            print(
                f"No se pudo marcar asignacion reenviada para pedido {pedido_id}: {exc}"
            )
        flash("Mensaje enviado al cliente.")
    else:
        detalle = razon or "Revisa Twilio."
        flash(f"No se pudo enviar el mensaje al cliente. {detalle}")

    return redirect(url_for("servicio_detalle", pedido_id=pedido_id))


@app.route("/logout")
def logout():
    session.clear()
    return redirect(url_for("login"))


init_db()
print(get_twilio_source_summary())
log_system_event("info", "startup", "Aplicacion iniciada", get_twilio_source_summary())

if __name__ == "__main__":
    debug_flag = os.environ.get("PENESAURIO_DEBUG", "1").strip().lower()
    debug_mode = debug_flag not in {"0", "false", "no", "off"}
    ensure_expiration_worker(debug_mode=debug_mode)
    app.run(host="0.0.0.0", port=5000, debug=debug_mode)
else:
    ensure_expiration_worker(debug_mode=False)



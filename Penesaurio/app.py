import base64
import hashlib
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

from flask import Flask, request, render_template, redirect, url_for, session, flash, jsonify, Response
from werkzeug.security import generate_password_hash, check_password_hash
from twilio.twiml.messaging_response import MessagingResponse

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

app = Flask(__name__)
app.secret_key = os.environ.get("FLASK_SECRET_KEY", "dev-secret-change")

DB_PATH = os.path.join(os.path.dirname(__file__), "servicios.db")
EXPIRATION_MINUTES = 10
FOLLOW_UP_INTERVAL_MINUTES = 2
EXPIRATION_CHECK_SECONDS = 30
TWILIO_SEND_RETRIES = 3
EXPIRATION_MESSAGE = (
    "No logramos asignarte conductor en 10 minutos. "
    "Cancelamos la solicitud para no hacerte esperar. "
    "Si deseas intentarlo de nuevo, escribe NUEVO."
)
FOLLOW_UP_MESSAGES = [
    "Seguimos buscando tu conductor.",
    "Aun estamos haciendo lo posible para encontrar un conductor.",
    "Seguimos en la busqueda.",
    "Seguimos pendientes de tu servicio.",
]
SHORT_CANCEL_HINT = "Si deseas cancelar, escribe CANCELAR."

TEST_COORDS = [
    (6.036734, -75.419024),
    (6.017470, -75.430035),
    (6.029867, -75.433825),
    (6.081244, -75.333961),
    (6.029867, -75.433825),
    (6.021925, -75.422297),
    (6.034995, -75.433154),
]


def get_conn():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


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
            created_at TEXT,
            FOREIGN KEY (usuario_id) REFERENCES usuarios(id)
        )
        """
    )

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
            updated_at TEXT
        )
        """
    )

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


def get_usuario_by_telefono(cur, telefono):
    return cur.execute(
        "SELECT * FROM usuarios WHERE telefono = ?", (telefono,)
    ).fetchone()


def upsert_usuario(cur, telefono, nombre, timestamp):
    usuario = get_usuario_by_telefono(cur, telefono)
    if usuario:
        cur.execute(
            "UPDATE usuarios SET nombre = ?, updated_at = ? WHERE id = ?",
            (nombre, timestamp, usuario["id"]),
        )
        return usuario["id"]
    cur.execute(
        """
        INSERT INTO usuarios (telefono, nombre, created_at, updated_at)
        VALUES (?, ?, ?, ?)
        """,
        (telefono, nombre, timestamp, timestamp),
    )
    return cur.lastrowid


def get_direcciones(cur, usuario_id):
    if not usuario_id:
        return []
    rows = cur.execute(
        """
        SELECT id, direccion
        FROM direcciones
        WHERE usuario_id = ?
        ORDER BY id ASC
        """,
        (usuario_id,),
    ).fetchall()
    if not rows:
        return []
    clean_rows = []
    to_delete = []
    for row in rows:
        direccion = row["direccion"] or ""
        if is_reserved_direccion(direccion):
            to_delete.append((row["id"],))
        else:
            clean_rows.append(row)
    if to_delete:
        cur.executemany("DELETE FROM direcciones WHERE id = ?", to_delete)
    return clean_rows


def build_direcciones_prompt(direcciones):
    if not direcciones:
        return ""
    lineas = ["Selecciona una de tus ubicaciones guardadas o escribe una nueva:"]
    for idx, row in enumerate(direcciones, start=1):
        direccion = format_saved_address(row["direccion"])
        lineas.append(f"{idx}. [{direccion}]")
    lineas.append("Escribe NUEVA para agregar otra direcciÃ³n.")
    return "\n".join(lineas)


def direccion_existe(cur, usuario_id, direccion):
    if not usuario_id or not direccion:
        return False
    row = cur.execute(
        """
        SELECT 1
        FROM direcciones
        WHERE usuario_id = ? AND lower(direccion) = lower(?)
        """,
        (usuario_id, direccion),
    ).fetchone()
    return row is not None


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


def get_location_payload(values):
    lat = (values.get("Latitude") or "").strip()
    lon = (values.get("Longitude") or "").strip()
    addr = (values.get("Address") or "").strip()
    label = (values.get("Label") or "").strip()
    coords = f"{lat},{lon}" if lat and lon else ""
    direccion = addr or label or coords
    return {
        "direccion": direccion,
        "latitude": lat,
        "longitude": lon,
        "coords": coords,
    }


def build_request_message(nombre):
    if nombre:
        return f"Listo, {nombre}. Estamos buscando conductor. {SHORT_CANCEL_HINT}"
    return f"Listo. Estamos buscando conductor. {SHORT_CANCEL_HINT}"


def build_follow_up_message(step):
    idx = max(1, min(step, len(FOLLOW_UP_MESSAGES))) - 1
    return f"{FOLLOW_UP_MESSAGES[idx]} {SHORT_CANCEL_HINT}"


def build_welcome_back_message(nombre):
    return f"Hola {nombre}. Donde te recogemos hoy?"


def build_new_customer_message():
    return "Bienvenido a Transporte Ejecutivo. Como te llamas?"


def build_name_ack_message(nombre):
    return f"Gracias, {nombre}. Donde te recogemos?"


def build_open_service_status_message():
    return f"Seguimos buscando tu conductor. {SHORT_CANCEL_HINT}"


def get_latest_search_service(cur, telefono):
    return cur.execute(
        """
        SELECT *
        FROM pedidos
        WHERE cliente_telefono = ?
          AND lower(estado) IN ('disponible', 'pendiente')
        ORDER BY id DESC
        LIMIT 1
        """,
        (telefono,),
    ).fetchone()


def get_latest_taken_service(cur, telefono):
    return cur.execute(
        """
        SELECT *
        FROM pedidos
        WHERE cliente_telefono = ?
          AND estado = 'Tomado'
        ORDER BY id DESC
        LIMIT 1
        """,
        (telefono,),
    ).fetchone()


def build_customer_request_payload(nombre, direccion, location_payload):
    partes = [f"Nombre: {nombre}", f"Direccion: {direccion}"]
    latitude = location_payload.get("latitude")
    longitude = location_payload.get("longitude")
    if (not latitude or not longitude) and direccion:
        parsed_lat, parsed_lng = parse_coords_from_text(direccion)
        if parsed_lat is not None and parsed_lng is not None:
            latitude = str(parsed_lat)
            longitude = str(parsed_lng)
    if latitude and longitude:
        partes.append(f"Latitude: {latitude}")
        partes.append(f"Longitude: {longitude}")
    return " | ".join(partes)


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
    value = get_config_value("twilio_enabled", "1")
    value_text = str(value).strip().lower()
    return value_text not in {"0", "false", "off", "no"}


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


def send_whatsapp_message(phone, body):
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
            if client:
                try:
                    client.messages.create(
                        from_=from_number,
                        to=to_number,
                        body=body,
                    )
                except Exception as sdk_exc:
                    print(
                        f"SDK Twilio fallo en intento {attempt}, usando HTTP directo: {sdk_exc}"
                    )
                    ok_http, http_response = send_twilio_message_via_http(
                        account_sid,
                        auth_token,
                        from_number,
                        to_number,
                        body,
                    )
                    if not ok_http:
                        raise RuntimeError(http_response or str(sdk_exc))
                    log_system_event(
                        "info",
                        "twilio",
                        "Envio WhatsApp exitoso por HTTP fallback",
                        f"to={to_number} intento={attempt} body={body[:180]}",
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
                    f"to={to_number} intento={attempt} body={body[:180]}",
                )
            if client:
                log_system_event(
                    "info",
                    "twilio",
                    "Envio WhatsApp exitoso por SDK",
                    f"to={to_number} intento={attempt} body={body[:180]}",
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
            target_step = min(
                elapsed_total // (FOLLOW_UP_INTERVAL_MINUTES * 60),
                len(FOLLOW_UP_MESSAGES),
            )
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
                conn.execute(
                    "UPDATE pedidos SET reminder_count = ? WHERE id = ?",
                    (next_step, pedido_id),
                )
                conn.commit()
                conn.close()
                log_system_event(
                    "info",
                    "reminder",
                    f"Recordatorio enviado #{next_step}",
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
    body = f"Listo. {conductor_nombre} ({conductor_placa}) va en camino."
    return send_whatsapp_message(phone, body)


def send_chat_messages(phone, conductor_placa, conductor_message, include_system):
    system_body = f"Tu conductor ({conductor_placa}) entro al chat."
    ok_system = True
    err_system = ""
    if include_system:
        ok_system, err_system = send_whatsapp_message(phone, system_body)

    ok_msg, err_msg = send_whatsapp_message(phone, conductor_message)
    if ok_system and ok_msg:
        return True, ""
    return False, err_msg or err_system


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


def respond_client(phone, text):
    """
    Respuesta directa por TwiML (evita depender de la salida a internet).
    """
    resp = MessagingResponse()
    if text:
        resp.message(text)
    xml = str(resp)
    print(f"TwiML -> {xml}")
    return xml, 200, {"Content-Type": "text/xml; charset=utf-8"}


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
        "detalles": parse_detalles(row["mensaje_cliente"]),
    }


@app.route("/webhook", methods=["POST"])
def webhook():
    telefono = request.values.get("From", "")
    mensaje = request.values.get("Body", "")
    fecha_actual = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"Webhook recibido de {telefono}: {mensaje}")

    try:
        conn = get_conn()
        cur = conn.cursor()

        mensaje_limpio = (mensaje or "").strip()
        location_payload = get_location_payload(request.values)
        mensaje_lower = mensaje_limpio.lower()
        open_service = get_latest_search_service(cur, telefono)
        taken_service = get_latest_taken_service(cur, telefono)

        if mensaje_lower in {
            "hola",
            "buenas",
            "buenos dias",
            "buenas tardes",
            "buenas noches",
            "nuevo",
            "nuevo servicio",
            "solicitud",
            "solicitar",
            "servicio",
            "pedido",
            "pedir",
        }:
            if open_service:
                conn.close()
                return respond_client(telefono, build_open_service_status_message())

            cur.execute("DELETE FROM conversaciones WHERE telefono = ?", (telefono,))
            usuario = get_usuario_by_telefono(cur, telefono)
            if usuario:
                cur.execute(
                    """
                    INSERT INTO conversaciones (telefono, paso, nombre, updated_at)
                    VALUES (?, 'direccion', ?, ?)
                    """,
                    (telefono, usuario["nombre"], fecha_actual),
                )
                conn.commit()
                direcciones = get_direcciones(cur, usuario["id"])
                lista = build_direcciones_prompt(direcciones)
                respuesta = build_welcome_back_message(usuario["nombre"])
                if lista:
                    respuesta = f"{respuesta}\n{lista}"
                conn.close()
                return respond_client(telefono, respuesta)

            cur.execute(
                """
                INSERT INTO conversaciones (telefono, paso, updated_at)
                VALUES (?, 'nombre', ?)
                """,
                (telefono, fecha_actual),
            )
            conn.commit()
            conn.close()
            return respond_client(telefono, build_new_customer_message())

        if mensaje_lower == "cancelar":
            if open_service:
                cur.execute(
                    """
                    UPDATE pedidos
                    SET estado = 'Cancelado'
                    WHERE cliente_telefono = ?
                      AND lower(estado) IN ('disponible', 'pendiente')
                    """,
                    (telefono,),
                )
                cur.execute("DELETE FROM conversaciones WHERE telefono = ?", (telefono,))
                conn.commit()
                conn.close()
                return respond_client(telefono, "Listo. Cancelamos tu solicitud.")

            if taken_service:
                conn.close()
                return respond_client(
                    telefono,
                    "Tu servicio ya fue asignado. Escribenos por este chat y te ayudamos.",
                )

            conversation_row = cur.execute(
                "SELECT 1 FROM conversaciones WHERE telefono = ?",
                (telefono,),
            ).fetchone()
            if conversation_row:
                cur.execute("DELETE FROM conversaciones WHERE telefono = ?", (telefono,))
                conn.commit()
                conn.close()
                return respond_client(telefono, "Listo. Cancelamos este proceso.")

            cur.execute("DELETE FROM conversaciones WHERE telefono = ?", (telefono,))
            conn.commit()
            conn.close()
            return respond_client(telefono, "No tienes una solicitud activa.")

        if mensaje_lower in {"salir", "reset"}:
            cur.execute("DELETE FROM conversaciones WHERE telefono = ?", (telefono,))
            conn.commit()
            conn.close()
            return respond_client(telefono, "Listo. Reiniciamos la conversacion.")

        row = cur.execute(
            "SELECT * FROM conversaciones WHERE telefono = ?", (telefono,)
        ).fetchone()

        if row is None:
            if taken_service:
                if mensaje_limpio:
                    cur.execute(
                        """
                        INSERT INTO chat_mensajes (pedido_id, sender, message, timestamp)
                        VALUES (?, 'cliente', ?, ?)
                        """,
                        (taken_service["id"], mensaje_limpio, fecha_actual),
                    )
                    conn.commit()
                conn.close()
                return respond_client(
                    telefono,
                    "Mensaje recibido. Tu conductor te respondera pronto.",
                )

            if open_service:
                conn.close()
                return respond_client(telefono, build_open_service_status_message())

            cur.execute("DELETE FROM conversaciones WHERE telefono = ?", (telefono,))
            usuario = get_usuario_by_telefono(cur, telefono)
            if usuario:
                cur.execute(
                    """
                    INSERT INTO conversaciones (telefono, paso, nombre, updated_at)
                    VALUES (?, 'direccion', ?, ?)
                    """,
                    (telefono, usuario["nombre"], fecha_actual),
                )
                conn.commit()
                direcciones = get_direcciones(cur, usuario["id"])
                lista = build_direcciones_prompt(direcciones)
                respuesta = build_welcome_back_message(usuario["nombre"])
                if lista:
                    respuesta = f"{respuesta}\n{lista}"
                conn.close()
                return respond_client(telefono, respuesta)

            cur.execute(
                """
                INSERT INTO conversaciones (telefono, paso, updated_at)
                VALUES (?, 'nombre', ?)
                """,
                (telefono, fecha_actual),
            )
            conn.commit()
            conn.close()
            return respond_client(telefono, build_new_customer_message())

        paso = row["paso"]

        if paso == "nombre":
            if not mensaje_limpio:
                respuesta_texto = "Dime tu nombre para continuar."
            else:
                usuario_id = upsert_usuario(cur, telefono, mensaje_limpio, fecha_actual)
                cur.execute(
                    """
                    UPDATE conversaciones
                    SET nombre = ?, paso = 'direccion', updated_at = ?
                    WHERE telefono = ?
                    """,
                    (mensaje_limpio, fecha_actual, telefono),
                )
                conn.commit()
                direcciones = get_direcciones(cur, usuario_id)
                lista = build_direcciones_prompt(direcciones)
                respuesta_texto = build_name_ack_message(mensaje_limpio)
                if lista:
                    respuesta_texto = f"{respuesta_texto}\n{lista}"
            conn.close()
        elif paso == "direccion":
            if not mensaje_limpio and location_payload.get("direccion"):
                mensaje_limpio = location_payload["direccion"]

            if not mensaje_limpio:
                conn.close()
                return respond_client(
                    telefono, "Escribe la direccion o comparte tu ubicacion."
                )
            if is_reserved_direccion(mensaje_limpio):
                conn.close()
                return respond_client(
                    telefono,
                    "Listo. Escribe la nueva direccion o comparte tu ubicacion.",
                )

            usuario = get_usuario_by_telefono(cur, telefono)
            if usuario is None and row["nombre"]:
                usuario_id = upsert_usuario(cur, telefono, row["nombre"], fecha_actual)
                usuario = get_usuario_by_telefono(cur, telefono)
            else:
                usuario_id = usuario["id"] if usuario else None

            nombre = ""
            if usuario and usuario["nombre"]:
                nombre = usuario["nombre"]
            elif row["nombre"]:
                nombre = row["nombre"]

            direcciones = get_direcciones(cur, usuario_id)
            direccion = None
            direccion_nueva = False

            if direcciones and mensaje_limpio.isdigit():
                idx = int(mensaje_limpio)
                if 1 <= idx <= len(direcciones):
                    direccion = direcciones[idx - 1]["direccion"]
                    direccion_nueva = False
                else:
                    lista = build_direcciones_prompt(direcciones)
                    respuesta_texto = "No veo esa opcion."
                    if lista:
                        respuesta_texto = f"{respuesta_texto}\n{lista}"
                    else:
                        respuesta_texto = f"{respuesta_texto} Escribe la direccion completa."
                    conn.close()
                    return respond_client(telefono, respuesta_texto)
            else:
                direccion = mensaje_limpio
                if is_reserved_direccion(direccion):
                    conn.close()
                    return respond_client(
                        telefono,
                        "Listo. Escribe la nueva direccion o comparte tu ubicacion.",
                    )
                direccion_nueva = True
                if direccion_existe(cur, usuario_id, direccion):
                    direccion_nueva = False

            if usuario_id and direccion_nueva:
                cur.execute(
                    """
                    INSERT INTO direcciones (usuario_id, direccion, created_at)
                    VALUES (?, ?, ?)
                    """,
                    (usuario_id, direccion, fecha_actual),
                )

            mensaje_cliente = build_customer_request_payload(
                nombre,
                direccion,
                location_payload,
            )

            cur.execute(
                """
                INSERT INTO pedidos (cliente_telefono, mensaje_cliente, estado, timestamp)
                VALUES (?, ?, 'Pendiente', ?)
                """,
                (telefono, mensaje_cliente, fecha_actual),
            )
            cur.execute("DELETE FROM conversaciones WHERE telefono = ?", (telefono,))
            conn.commit()
            conn.close()

            respuesta_texto = build_request_message(nombre)
        else:
            cur.execute("DELETE FROM conversaciones WHERE telefono = ?", (telefono,))
            conn.commit()
            conn.close()
            respuesta_texto = "Vamos de nuevo. Que servicio deseas?"
    except Exception as exc:
        print(f"Error BD: {exc}")
        respuesta_texto = "Tuvimos un problema procesando tu mensaje. Intenta de nuevo."

    return respond_client(telefono, respuesta_texto)


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

        if not row["nombre_real"] or not row["placa"] or not row["vehiculo"] or not row["modelo"]:
            return redirect(url_for("perfil"))
        return redirect(url_for("inicio"))

    return render_template("login.html")


@app.route("/")
def index_root():
    if session.get("admin_id"):
        return redirect(url_for("admin_dashboard"))
    if session.get("conductor_id"):
        if (
            not session.get("conductor_nombre")
            or not session.get("conductor_placa")
            or not session.get("conductor_vehiculo")
            or not session.get("conductor_modelo")
        ):
            return redirect(url_for("perfil"))
        return redirect(url_for("inicio"))
    return redirect(url_for("login"))


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
            }
        )

    return jsonify({"ok": True, "conductores": conductores})


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

    enabled = not is_twilio_enabled()
    set_config_value("twilio_enabled", "1" if enabled else "0")
    log_system_event(
        "warn",
        "admin",
        f"Twilio {'encendido' if enabled else 'apagado'} desde admin",
        get_twilio_source_summary(),
    )
    return jsonify({"ok": True, "enabled": enabled})


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
    active_row = conn.execute(
        "SELECT active_pedido_id, is_online FROM conductores WHERE id = ?",
        (session.get("conductor_id"),),
    ).fetchone()
    active_pedido_id = active_row["active_pedido_id"] if active_row else None
    driver_online = bool(active_row["is_online"]) if active_row and active_row["is_online"] is not None else True

    active_pedido = None
    chat_messages = []
    if active_pedido_id:
        pedido_row = conn.execute(
            "SELECT * FROM pedidos WHERE id = ?", (active_pedido_id,)
        ).fetchone()
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
            conn.execute(
                "UPDATE conductores SET active_pedido_id = NULL WHERE id = ?",
                (session.get("conductor_id"),),
            )
            conn.commit()
            active_pedido_id = None

    rows = []
    if not active_pedido_id and driver_online:
        rows = conn.execute(
            """
            SELECT id, cliente_telefono, mensaje_cliente, timestamp
            FROM pedidos
            WHERE lower(estado) IN ('disponible', 'pendiente')
            ORDER BY id ASC
            """
        ).fetchall()

    rows_mios = conn.execute(
        """
        SELECT id, cliente_telefono, mensaje_cliente, timestamp
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
    conn.close()

    pedidos = []
    for row in rows:
        pedidos.append(
            {
                "id": row["id"],
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

    pedido_id = request.form.get("pedido_id", "").strip()
    if not pedido_id:
        if request.headers.get("X-Requested-With") == "XMLHttpRequest":
            return jsonify({"ok": False, "error": "Pedido invalido."}), 400
        flash("Pedido invalido.")
        return redirect(url_for("dashboard"))

    conn = get_conn()
    availability_row = conn.execute(
        "SELECT is_online FROM conductores WHERE id = ?",
        (session.get("conductor_id"),),
    ).fetchone()
    is_online = bool(availability_row["is_online"]) if availability_row and availability_row["is_online"] is not None else True
    if not is_online:
        conn.close()
        if request.headers.get("X-Requested-With") == "XMLHttpRequest":
            return jsonify({"ok": False, "error": "Activa el modo disponible para tomar servicios."}), 409
        flash("Activa el modo disponible para tomar servicios.")
        return redirect(url_for("dashboard"))

    active_row = conn.execute(
        "SELECT active_pedido_id FROM conductores WHERE id = ?",
        (session.get("conductor_id"),),
    ).fetchone()
    if active_row and active_row["active_pedido_id"]:
        conn.close()
        if request.headers.get("X-Requested-With") == "XMLHttpRequest":
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
    conn.commit()

    if cur.rowcount == 0:
        conn.close()
        if request.headers.get("X-Requested-With") == "XMLHttpRequest":
            return jsonify({"ok": False, "error": "Este servicio ya fue tomado."}), 409
        flash("Este servicio ya fue tomado por otro conductor.")
        return redirect(url_for("dashboard"))

    row = conn.execute(
        "SELECT * FROM pedidos WHERE id = ?", (pedido_id,)
    ).fetchone()

    conn.execute(
        "UPDATE conductores SET active_pedido_id = ? WHERE id = ?",
        (pedido_id, session.get("conductor_id")),
    )
    conn.commit()
    conn.close()

    cliente_telefono = row["cliente_telefono"] if row else ""
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
        if request.headers.get("X-Requested-With") == "XMLHttpRequest":
            if not enviado:
                detalle = razon or "Revisa Twilio y que el numero este habilitado."
                log_system_event(
                    "error",
                    "assignment",
                    "Fallo asignacion al tomar servicio",
                    f"pedido={pedido_id} telefono={cliente_telefono} error={detalle}",
                )
                return jsonify(
                    {"ok": True, "pedido": pedido_to_dict(row), "warning": detalle}
                )
            return jsonify({"ok": True, "pedido": pedido_to_dict(row)})
        if not enviado:
            detalle = razon or "Revisa Twilio y que el numero este habilitado."
            log_system_event(
                "error",
                "assignment",
                "Fallo asignacion al tomar servicio",
                f"pedido={pedido_id} telefono={cliente_telefono} error={detalle}",
            )
            flash(f"No se pudo enviar el mensaje al cliente. {detalle}")

    if request.headers.get("X-Requested-With") == "XMLHttpRequest":
        return jsonify({"ok": True, "pedido": pedido_to_dict(row)})

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

    conn = get_conn()
    active_row = conn.execute(
        "SELECT active_pedido_id FROM conductores WHERE id = ?",
        (session.get("conductor_id"),),
    ).fetchone()
    active_id = active_row["active_pedido_id"] if active_row else None

    if not active_id:
        conn.close()
        if request.headers.get("X-Requested-With") == "XMLHttpRequest":
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
                "Conductor fuera de linea.",
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
    if pedido_row:
        ok, err = send_whatsapp_message(
            pedido_row["cliente_telefono"],
            "Conductor fuera de linea.",
        )
        if not ok:
            warning = err or "No se pudo notificar al cliente."

    if request.headers.get("X-Requested-With") == "XMLHttpRequest":
        if warning:
            return jsonify({"ok": True, "warning": warning})
        return jsonify({"ok": True})

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
    if include_system:
        cur.execute(
            """
            INSERT INTO chat_mensajes (pedido_id, sender, message, timestamp)
            VALUES (?, 'sistema', ?, ?)
            """,
            (
                pedido_id,
                f"Tu conductor ({session.get('conductor_placa')}) entro al chat.",
                datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            ),
        )
        inserted.append(
            {
                "id": cur.lastrowid,
                "sender": "sistema",
                "message": f"Tu conductor ({session.get('conductor_placa')}) entro al chat.",
            }
        )
        cur.execute(
            "UPDATE pedidos SET chat_iniciado = 1 WHERE id = ?",
            (pedido_id,),
        )

    cur.execute(
        """
        INSERT INTO chat_mensajes (pedido_id, sender, message, timestamp)
        VALUES (?, 'conductor', ?, ?)
        """,
        (pedido_id, message, datetime.now().strftime("%Y-%m-%d %H:%M:%S")),
    )
    inserted.append(
        {
            "id": cur.lastrowid,
            "sender": "conductor",
            "message": message,
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
            }
        )

    return jsonify({"ok": True, "messages": inserted})


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
    debug_mode = True
    ensure_expiration_worker(debug_mode=debug_mode)
    app.run(host="0.0.0.0", port=5000, debug=debug_mode)
else:
    ensure_expiration_worker(debug_mode=False)



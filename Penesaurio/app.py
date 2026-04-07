import base64
import hashlib
import os
import sqlite3
import subprocess
import sys
import threading
import time
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
EXPIRATION_MINUTES = 5
EXPIRATION_CHECK_SECONDS = 30
EXPIRATION_MESSAGE = (
    "Lo sentimos, en este momento todos nuestros vehículos ejecutivos en La Ceja se "
    "encuentran en servicio. Hemos cancelado tu solicitud para no hacerte esperar. "
    "Por favor, intenta de nuevo en unos minutos."
)


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
                    timestamp
                )
                SELECT
                    {cliente_col},
                    {mensaje_col},
                    {estado_expr},
                    {conductor_nombre_expr},
                    {conductor_placa_expr},
                    {chat_iniciado_expr},
                    {ts_expr}
                FROM pedidos_old
                """
            )
            cur.execute("DROP TABLE pedidos_old")

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
        direccion = row["direccion"]
        lineas.append(f"{idx}. [{direccion}]")
    lineas.append("Escribe NUEVA para agregar otra dirección.")
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
        "á": "a",
        "é": "e",
        "í": "i",
        "ó": "o",
        "ú": "u",
        "ñ": "n",
    }
    for key, val in replacements.items():
        text = text.replace(key, val)
    return " ".join(text.split())


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


def get_location_text(values):
    lat = (values.get("Latitude") or "").strip()
    lon = (values.get("Longitude") or "").strip()
    addr = (values.get("Address") or "").strip()
    label = (values.get("Label") or "").strip()
    if lat and lon:
        coords = f"{lat},{lon}"
        if addr:
            return f"{addr} ({coords})"
        if label:
            return f"{label} ({coords})"
        return f"Ubicacion: {coords}"
    if addr:
        return addr
    if label:
        return label
    return ""


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


def get_twilio_client():
    account_sid = os.environ.get("TWILIO_ACCOUNT_SID")
    auth_token = os.environ.get("TWILIO_AUTH_TOKEN")
    from_number = os.environ.get("TWILIO_WHATSAPP_FROM")

    if local_settings:
        account_sid = account_sid or getattr(local_settings, "TWILIO_ACCOUNT_SID", None)
        auth_token = auth_token or getattr(local_settings, "TWILIO_AUTH_TOKEN", None)
        from_number = from_number or getattr(local_settings, "TWILIO_WHATSAPP_FROM", None)

    if not all([account_sid, auth_token, from_number, Client]):
        return None, None, "Twilio no configurado"

    return Client(account_sid, auth_token), from_number, ""


def send_whatsapp_message(phone, body):
    if not is_twilio_enabled():
        print("Twilio desactivado: mensaje omitido.")
        return True, "Twilio desactivado"

    client, from_number, error = get_twilio_client()
    if not client:
        return False, error

    to_number = ensure_whatsapp_prefix(phone)
    try:
        client.messages.create(
            from_=from_number,
            to=to_number,
            body=body,
        )
        return True, ""
    except Exception as exc:
        print(f"Error enviando mensaje Twilio: {exc}")
        return False, str(exc)


def verificar_expiracion_servicios():
    cutoff = datetime.now() - timedelta(minutes=EXPIRATION_MINUTES)
    cutoff_text = cutoff.strftime("%Y-%m-%d %H:%M:%S")
    expired_phones = []

    try:
        conn = get_conn()
        rows = conn.execute(
            """
            SELECT id, cliente_telefono
            FROM pedidos
            WHERE lower(estado) = 'pendiente'
              AND timestamp IS NOT NULL
              AND timestamp <= ?
            """,
            (cutoff_text,),
        ).fetchall()

        for row in rows:
            cur = conn.execute(
                """
                UPDATE pedidos
                SET estado = 'expirado'
                WHERE id = ? AND lower(estado) = 'pendiente'
                """,
                (row["id"],),
            )
            if cur.rowcount:
                expired_phones.append(row["cliente_telefono"])

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

    return len(expired_phones)


def start_expiration_worker():
    def loop():
        while True:
            verificar_expiracion_servicios()
            time.sleep(EXPIRATION_CHECK_SECONDS)

    worker = threading.Thread(target=loop, name="servicios-expiracion", daemon=True)
    worker.start()
    return worker


def send_assignment_message(phone, conductor_nombre, conductor_placa):
    body = (
        f"Tu servicio ha sido asignado a {conductor_nombre} en el vehiculo "
        f"de placas {conductor_placa}. Se pondra en contacto contigo ahora."
    )
    return send_whatsapp_message(phone, body)


def send_chat_messages(phone, conductor_placa, conductor_message, include_system):
    system_body = (
        f"El conductor del vehiculo {conductor_placa} se ha conectado al chat."
    )
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
        location_text = get_location_text(request.values)
        mensaje_lower = mensaje_limpio.lower()

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
                respuesta = (
                    f"Hola {usuario['nombre']}, bienvenido de nuevo a Transporte Ejecutivo. "
                    "¿A dónde te llevamos hoy?"
                )
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
            return respond_client(
                telefono,
                "Bienvenido a Transporte Ejecutivo, es un placer saludarte. "
                "Para iniciar tu perfil con nosotros, ¿podrías decirnos cuál es tu nombre?",
            )

        if mensaje_lower in {"cancelar", "salir", "reset"}:
            cur.execute("DELETE FROM conversaciones WHERE telefono = ?", (telefono,))
            conn.commit()
            conn.close()
            return respond_client(
                telefono,
                "Conversacion reiniciada. Que servicio deseas ahora?",
            )

        row = cur.execute(
            "SELECT * FROM conversaciones WHERE telefono = ?", (telefono,)
        ).fetchone()

        if row is None:
            active = cur.execute(
                """
                SELECT p.id
                FROM pedidos p
                JOIN conductores c ON c.active_pedido_id = p.id
                WHERE p.cliente_telefono = ? AND p.estado = 'Tomado'
                ORDER BY p.id DESC
                LIMIT 1
                """,
                (telefono,),
            ).fetchone()
            if active:
                if mensaje_limpio:
                    cur.execute(
                        """
                        INSERT INTO chat_mensajes (pedido_id, sender, message, timestamp)
                        VALUES (?, 'cliente', ?, ?)
                        """,
                        (active["id"], mensaje_limpio, fecha_actual),
                    )
                    conn.commit()
                conn.close()
                return respond_client(
                    telefono,
                    "Mensaje recibido. Tu conductor te respondera pronto. "
                    "Si necesitas un nuevo servicio escribe NUEVO.",
                )

            # No hay conversacion activa ni viaje activo: iniciar flujo normal
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
                respuesta = (
                    f"Hola {usuario['nombre']}, bienvenido de nuevo a Transporte Ejecutivo. "
                    "¿A dónde te llevamos hoy?"
                )
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
            return respond_client(
                telefono,
                "Bienvenido a Transporte Ejecutivo, es un placer saludarte. "
                "Para iniciar tu perfil con nosotros, ¿podrías decirnos cuál es tu nombre?",
            )

        paso = row["paso"]

        if paso == "nombre":
            if not mensaje_limpio:
                respuesta_texto = "Por favor dinos tu nombre para continuar."
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
                respuesta_texto = (
                    f"Gracias, {mensaje_limpio}. ¿A dónde te llevamos hoy?"
                )
                if lista:
                    respuesta_texto = f"{respuesta_texto}\n{lista}"
            conn.close()
        elif paso == "direccion":
            if not mensaje_limpio and location_text:
                mensaje_limpio = location_text
                mensaje_lower = mensaje_limpio.lower()

            if not mensaje_limpio:
                conn.close()
                return respond_client(
                    telefono, "Por favor escribe la dirección o punto de recogida."
                )
            if is_reserved_direccion(mensaje_limpio):
                conn.close()
                return respond_client(
                    telefono,
                    "Perfecto. Escribe la nueva dirección o punto de recogida.",
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
                    respuesta_texto = "No encontré esa opción."
                    if lista:
                        respuesta_texto = f"{respuesta_texto}\n{lista}"
                    else:
                        respuesta_texto = (
                            f"{respuesta_texto} Escribe la dirección completa, por favor."
                        )
                    conn.close()
                    return respond_client(telefono, respuesta_texto)
            else:
                direccion = mensaje_limpio
                if is_reserved_direccion(direccion):
                    conn.close()
                    return respond_client(
                        telefono,
                        "Perfecto. Escribe la nueva dirección o punto de recogida.",
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

            mensaje_cliente = f"Nombre: {nombre} | Direccion: {direccion}"

            cur.execute(
                """
                INSERT INTO pedidos (cliente_telefono, mensaje_cliente, estado, timestamp)
                VALUES (?, ?, 'Disponible', ?)
                """,
                (telefono, mensaje_cliente, fecha_actual),
            )
            cur.execute("DELETE FROM conversaciones WHERE telefono = ?", (telefono,))
            conn.commit()
            conn.close()

            respuesta_texto = (
                f"Perfecto, {nombre}. Hemos recibido tu solicitud con éxito. "
                "En este momento, nuestra central está validando la disponibilidad de "
                "nuestros conductores ejecutivos en la zona de La Ceja para asignarte "
                "la mejor unidad. Por favor, danos un momento..."
            )
        else:
            cur.execute("DELETE FROM conversaciones WHERE telefono = ?", (telefono,))
            conn.commit()
            conn.close()
            respuesta_texto = (
                "Vamos de nuevo. Que servicio deseas hoy?"
            )
    except Exception as exc:
        print(f"Error BD: {exc}")
        respuesta_texto = (
            "Tuvimos un problema procesando tu mensaje. Intenta de nuevo."
        )

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
        return redirect(url_for("dashboard"))

    return render_template("login.html")


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
        return redirect(url_for("dashboard"))

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
        return redirect(url_for("dashboard"))

    return render_template("profile.html")


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

    conn = get_conn()
    active_row = conn.execute(
        "SELECT active_pedido_id FROM conductores WHERE id = ?",
        (session.get("conductor_id"),),
    ).fetchone()
    active_pedido_id = active_row["active_pedido_id"] if active_row else None

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
    rows_mios = []
    if not active_pedido_id:
        rows = conn.execute(
            """
            SELECT id, cliente_telefono, mensaje_cliente, timestamp
            FROM pedidos
            WHERE lower(estado) IN ('disponible', 'pendiente')
            ORDER BY id DESC
            """
        ).fetchall()

        rows_mios = conn.execute(
            """
            SELECT id, cliente_telefono, mensaje_cliente, timestamp
            FROM pedidos
            WHERE estado = 'Tomado' AND conductor_nombre = ? AND conductor_placa = ?
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

    return render_template(
        "index.html",
        pedidos=pedidos,
        pedidos_mios=pedidos_mios,
        conductor_nombre=session.get("conductor_nombre"),
        conductor_placa=session.get("conductor_placa"),
        ganancias_total=ganancias_total,
        ganancias_total_formatted=format_cop(ganancias_total),
        map_url=map_url,
        active_pedido=active_pedido,
        chat_messages=chat_messages,
    )


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
        SET estado = 'Tomado', conductor_nombre = ?, conductor_placa = ?
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
        if request.headers.get("X-Requested-With") == "XMLHttpRequest":
            if not enviado:
                detalle = razon or "Revisa Twilio y que el numero este habilitado."
                return jsonify(
                    {"ok": True, "pedido": pedido_to_dict(row), "warning": detalle}
                )
            return jsonify({"ok": True, "pedido": pedido_to_dict(row)})
        if not enviado:
            detalle = razon or "Revisa Twilio y que el numero este habilitado."
            flash(f"No se pudo enviar el mensaje al cliente. {detalle}")

    if request.headers.get("X-Requested-With") == "XMLHttpRequest":
        return jsonify({"ok": True, "pedido": pedido_to_dict(row)})

    flash("Servicio tomado. Aqui tienes los datos del cliente.")
    return redirect(url_for("servicio_detalle", pedido_id=pedido_id))


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
    return jsonify({"ok": True, "count": count_val, "max_id": max_id_val})


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
                f"El conductor del vehiculo {session.get('conductor_placa')} se ha conectado al chat.",
                datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            ),
        )
        inserted.append(
            {
                "id": cur.lastrowid,
                "sender": "sistema",
                "message": f"El conductor del vehiculo {session.get('conductor_placa')} se ha conectado al chat.",
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

def start_controlador():
    script_path = os.path.join(os.path.dirname(__file__), "Controlador.py")
    if not os.path.exists(script_path):
        print("Controlador.py no encontrado, omitiendo arranque.")
        return
    try:
        creationflags = 0
        if os.name == "nt" and hasattr(subprocess, "CREATE_NEW_CONSOLE"):
            creationflags = subprocess.CREATE_NEW_CONSOLE
        subprocess.Popen(
            [sys.executable, script_path],
            cwd=os.path.dirname(__file__),
            creationflags=creationflags,
        )
        print("Controlador iniciado.")
    except Exception as exc:
        print(f"No se pudo iniciar Controlador: {exc}")

if __name__ == "__main__":
    debug_mode = True
    if os.environ.get("WERKZEUG_RUN_MAIN") == "true" or not debug_mode:
        start_controlador()
        start_expiration_worker()
    app.run(host="0.0.0.0", port=5000, debug=debug_mode)

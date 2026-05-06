import json
import math
import os
import re
from datetime import datetime

from twilio.twiml.messaging_response import MessagingResponse
from trust_network import (
    complete_registration_with_reserved_code,
    create_client_code_for_phone,
    extract_code_from_text,
    format_expiration,
    reserve_code_for_phone,
)

try:
    from groq import Groq
except Exception:
    Groq = None


SHORT_CANCEL_HINT = "Si deseas cancelar, escribe *CANCELAR*."
MAICOL_SOURCE_PATH = r"C:\Users\Juan Pablo\Desktop\Maicol\MaicolSistem.py"
DEFAULT_GROQ_TEXT_MODEL = "llama-3.3-70b-versatile"
FIRST_CONTACT_WELCOME = "*Hola, bienvenid@ a Zipp.*"
MAP_ONLY_REJECTION_MESSAGE = (
    "*Necesito tu ubicacion actual* desde WhatsApp para recogerte.\n"
    "No puedo tomar direcciones escritas.\n"
    "Si quieres, te explico como enviarla."
)
MAP_ONLY_HELP_MESSAGE = (
    "Te explico rapido.\n\n"
    "*Android*\n"
    "1. Toca el clip.\n"
    "2. Elige la opcion *Ubicacion*.\n"
    "3. Toca en *Ubicacion actual*.\n\n"
    "*iPhone*\n"
    "1. Toca el simbolo *+*.\n"
    "2. Elige la opcion *Ubicacion*.\n"
    "3. Toca en *Enviar mi ubicacion actual*.\n\n"
    "Si puedes, activa el GPS."
)
MAP_HELP_KEYWORDS = {
    "si",
    "sip",
    "s",
    "ayudame",
    "ayuda",
    "instrucciones",
    "ver instrucciones",
    "no se",
    "nose",
    "explicame",
    "expliqueme",
    "como",
    "como hago",
}
MAP_DECLINE_HELP_KEYWORDS = {
    "no",
    "gracias",
    "ya se",
    "listo",
}
LOCATION_EDIT_KEYWORDS = {
    "editar ubicaciones",
    "editar ubicacion",
    "administrar ubicaciones",
    "gestionar ubicaciones",
}
LOCATION_LIST_KEYWORDS = {
    "guardadas",
    "ubicaciones guardadas",
    "ver guardadas",
    "mostrar guardadas",
    "mis guardadas",
    "mis ubicaciones",
    "volver ubicaciones",
}
LOCATION_NEW_KEYWORDS = {
    "nueva",
    "nueva ubicacion",
    "nueva ubicación",
}

BUTTON_PAYLOAD_ALIASES = {
    "save_yes": "si",
    "save_no": "no",
    "out_of_zone_yes": "si",
    "out_of_zone_no": "no",
    "show_saved_locations": "guardadas",
    "show_saved": "guardadas",
    "new_location": "nueva",
    "new_shared_location": "nueva",
    "manage_locations": "editar ubicaciones",
    "back_to_location_menu": "volver ubicaciones",
    "location_help": "ver instrucciones",
    "view_instructions": "ver instrucciones",
    "manage_rename": "renombrar",
    "manage_delete": "eliminar",
    "manage_back": "volver ubicaciones",
    "generate_invite_code": "generar codigo",
}
OUT_OF_ZONE_ACCEPT_KEYWORDS = {
    "si",
    "sí",
    "dale",
    "ok",
    "okay",
    "listo",
    "de una",
    "hagale",
    "hágale",
    "mandalo",
    "mandalo pues",
    "mandamelo",
    "envialo",
    "envialo pues",
    "enviamelo",
    "continuar",
}
OUT_OF_ZONE_DECLINE_KEYWORDS = {
    "no",
    "cancelar",
    "mejor no",
    "no gracias",
    "dejalo asi",
    "dejarlo asi",
}
INTERMUNICIPAL_LABEL = "Intermunicipal"
DEFAULT_COVERAGE_CENTER_LAT = 6.030589
DEFAULT_COVERAGE_CENTER_LNG = -75.431704
DEFAULT_COVERAGE_RADIUS_METERS = 6000

GREETING_KEYWORDS = {
    "hola",
    "ola",
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
}
FAQ_KEYWORDS = (
    "tarifa",
    "precio",
    "valor",
    "cuanto",
    "coste",
    "costo",
    "cobertura",
    "horario",
    "medio de pago",
    "pago",
)
PRICE_KEYWORDS = (
    "tarifa",
    "precio",
    "valor",
    "cuanto",
    "coste",
    "costo",
    "vale",
    "cuesta",
    "cobran",
)
QUESTION_SIGNAL_KEYWORDS = (
    "pregunta",
    "duda",
    "consulta",
    "quiero saber",
    "quisiera saber",
    "me gustaria saber",
    "me puedes decir",
    "me podrias decir",
    "podrias decirme",
    "podria decirme",
    "informacion",
    "tienen",
    "manejan",
    "aceptan",
    "trabajan",
    "hacen",
)
QUESTION_PREFIXES = (
    "que ",
    "como ",
    "cuando ",
    "donde ",
    "cual ",
    "cuanto ",
)
FAQ_SERVICE_KEYWORDS = (
    "servicio",
    "carrera",
    "viaje",
    "traslado",
    "aeropuerto",
    "cobertura",
    "horario",
    "pago",
)
OPEN_SERVICE_STATUS_KEYWORDS = (
    "demora",
    "cuanto tiempo",
    "cuanto se demora",
    "cuanto falta",
    "en cuanto",
    "cuando llega",
    "ya viene",
    "siguen buscando",
    "estado",
    "tarda",
    "tiempo",
)
BOOKING_KEYWORDS = (
    "taxi",
    "servicio",
    "recog",
    "buscar",
    "llevar",
    "traslado",
    "carrera",
    "viaje",
    "moverme",
    "necesito",
)
ADDRESS_KEYWORDS = (
    "calle",
    "carrera",
    "avenida",
    "av",
    "diag",
    "transversal",
    "sector",
    "barrio",
    "centro",
    "parque",
    "clinica",
    "hospital",
    "aeropuerto",
)
NAME_BLOCKED_KEYWORDS = set(GREETING_KEYWORDS) | set(BOOKING_KEYWORDS) | set(
    FAQ_KEYWORDS
) | set(PRICE_KEYWORDS) | set(QUESTION_SIGNAL_KEYWORDS) | {
    "para",
    "porfa",
    "por favor",
    "quiero",
    "quisiera",
    "necesito",
    "un",
    "una",
}


def _read_maicol_source():
    try:
        with open(MAICOL_SOURCE_PATH, "r", encoding="utf-8") as file_obj:
            return file_obj.read()
    except Exception:
        return ""


def _extract_source_constant(source_text, name, default=""):
    if not source_text:
        return default
    match = re.search(
        rf"^{re.escape(name)}\s*=\s*([\"'])(.*?)\1",
        source_text,
        flags=re.MULTILINE,
    )
    if not match:
        return default
    return (match.group(2) or default).strip()


def load_maicol_groq_settings():
    source = _read_maicol_source()
    api_key = os.environ.get("GROQ_API_KEY") or _extract_source_constant(
        source, "API_KEY"
    )
    model = os.environ.get("TWILIO_GROQ_MODEL") or _extract_source_constant(
        source, "MODELO_TEXTO", DEFAULT_GROQ_TEXT_MODEL
    )
    return {
        "api_key": api_key or "",
        "model": model or DEFAULT_GROQ_TEXT_MODEL,
        "source_path": MAICOL_SOURCE_PATH,
    }


def normalize_user_text(value):
    text = (value or "").strip().lower()
    replacements = {
        "á": "a",
        "é": "e",
        "í": "i",
        "ó": "o",
        "ú": "u",
        "ñ": "n",
    }
    for key, replacement in replacements.items():
        text = text.replace(key, replacement)
    return " ".join(text.split())


def clean_name_candidate(value):
    text = (value or "").strip()
    if not text:
        return ""
    text = re.sub(
        r"^(hola|ola|buenas|buenos dias|buenas tardes|buenas noches)\s+",
        "",
        text,
        flags=re.IGNORECASE,
    ).strip()
    text = re.sub(
        r"^(me llamo|mi nombre es|soy|a nombre de|a nombre del|a nombre de la|nombre de)\s+",
        "",
        text,
        flags=re.IGNORECASE,
    ).strip(" .,;:!?")
    if not text or len(text) > 50:
        return ""
    if any(char.isdigit() for char in text):
        return ""
    words = [word for word in text.split() if word]
    if not words or len(words) > 4:
        return ""
    normalized = normalize_user_text(text)
    if normalized in GREETING_KEYWORDS:
        return ""
    normalized_tokens = set(normalized.split())
    blocked_single = {keyword for keyword in NAME_BLOCKED_KEYWORDS if " " not in keyword}
    blocked_multi = [keyword for keyword in NAME_BLOCKED_KEYWORDS if " " in keyword]
    if normalized_tokens & blocked_single:
        return ""
    if any(keyword in normalized for keyword in blocked_multi):
        return ""
    return " ".join(word.capitalize() for word in words)


def extract_name_from_text(value):
    text = (value or "").strip()
    if not text:
        return ""
    patterns = [
        r"\ba nombre de\s+([^\d,.;:!?]{2,40})",
        r"\ba nombre del\s+([^\d,.;:!?]{2,40})",
        r"\ba nombre de la\s+([^\d,.;:!?]{2,40})",
        r"\bme llamo\s+([^\d,.;:!?]{2,40})",
        r"\bmi nombre es\s+([^\d,.;:!?]{2,40})",
        r"\bsoy\s+([^\d,.;:!?]{2,40})",
    ]
    for pattern in patterns:
        match = re.search(pattern, text, flags=re.IGNORECASE)
        if match:
            candidate = clean_name_candidate(match.group(1))
            if candidate:
                return candidate
    return clean_name_candidate(text)


def looks_like_address(value):
    text = normalize_user_text(value)
    if not text:
        return False
    if any(keyword in text for keyword in ADDRESS_KEYWORDS):
        return True
    if re.search(r"\d", text):
        return True
    if "," in text and len(text) > 8:
        return True
    return False


def extract_saved_address_choice(value):
    text = normalize_user_text(value)
    if not text:
        return 0
    if text.isdigit():
        return int(text)
    match = re.search(r"\b(?:opcion|la|direccion|ubicacion)?\s*(\d{1,2})\b", text)
    if not match:
        return 0
    try:
        return int(match.group(1))
    except Exception:
        return 0


def looks_like_coordinates(value):
    text = (value or "").strip()
    if not text:
        return False
    return re.fullmatch(r"-?\d+(?:\.\d+)?\s*,\s*-?\d+(?:\.\d+)?", text) is not None


def is_shared_location_payload(location_payload):
    if not location_payload:
        return False
    lat = (location_payload.get("latitude") or "").strip()
    lon = (location_payload.get("longitude") or "").strip()
    return bool(lat and lon)


def wants_location_help(text):
    normalized = normalize_user_text(text)
    if not normalized:
        return False
    if normalized in MAP_HELP_KEYWORDS:
        return True
    help_phrases = (
        "no se como",
        "no se hacerlo",
        "no se enviar",
        "como hacerlo",
        "como lo hago",
        "como hago",
        "como envio",
        "como enviar",
        "como comparto",
        "como compartir",
        "explicame como",
        "ayudame a",
    )
    if any(phrase in normalized for phrase in help_phrases):
        return True
    if re.search(r"\bcomo\b.*\bhago\b", normalized):
        return True
    if re.search(r"\bno se\b.*\bcomo\b", normalized):
        return True
    if "ubicacion" in normalized and any(
        keyword in normalized for keyword in ("como", "enviar", "compart", "ayuda", "explica")
    ):
        return True
    return False


def declines_location_help(text):
    normalized = normalize_user_text(text)
    if not normalized:
        return False
    return normalized in MAP_DECLINE_HELP_KEYWORDS


def wants_location_management(text):
    normalized = normalize_user_text(text)
    if not normalized:
        return False
    return normalized in LOCATION_EDIT_KEYWORDS


def wants_saved_locations_list(text):
    normalized = normalize_user_text(text)
    if not normalized:
        return False
    return normalized in LOCATION_LIST_KEYWORDS


def wants_new_location(text):
    normalized = normalize_user_text(text)
    if not normalized:
        return False
    return normalized in LOCATION_NEW_KEYWORDS


def get_incoming_message_text(values):
    button_payload = normalize_user_text(values.get("ButtonPayload", ""))
    button_text = (values.get("ButtonText") or "").strip()
    body = (values.get("Body") or "").strip()
    if button_payload and button_payload in BUTTON_PAYLOAD_ALIASES:
        return BUTTON_PAYLOAD_ALIASES[button_payload]
    if button_text:
        return button_text
    if body:
        lines = [line.strip() for line in body.splitlines() if line.strip()]
        if lines:
            last_line = normalize_user_text(lines[-1])
            if (
                last_line in BUTTON_PAYLOAD_ALIASES
                or last_line in LOCATION_LIST_KEYWORDS
                or last_line in LOCATION_EDIT_KEYWORDS
                or last_line in LOCATION_NEW_KEYWORDS
                or last_line in {"si", "no", "renombrar", "eliminar", "ver instrucciones"}
            ):
                return lines[-1]
    return body


def build_map_only_rejection_message():
    return MAP_ONLY_REJECTION_MESSAGE


def build_map_only_help_message():
    return f"*Como compartir tu ubicacion actual*\n\n{MAP_ONLY_HELP_MESSAGE}"


def build_waiting_for_map_message():
    return "*Perfecto.* Quedo atento a tu ubicacion actual."


def build_save_location_question():
    return (
        "*Perfecto.* Ya tengo el punto de recogida.\n"
        "*Quieres guardarla?*\n"
        "Responde *SI* o *NO*."
    )


def build_location_label_request():
    return "*Como quieres llamarla?*"


def build_location_reference_request_message():
    return (
        "*Genial, ya tengo tu ubicacion.*\n"
        "*Ahora enviame porfa la direccion breve* para llegar mejor.\n"
        "Ej: *Calle 19 #20-31*"
    )


def build_reference_ack_name_request_message():
    return (
        "*Perfecto.* Ya tengo tu ubicacion y tu referencia.\n"
        "*Ahora cuentame tu nombre.*"
    )


def build_location_saved_message(label):
    return f"*Listo.* La guarde como *{label}*."


def build_location_manage_intro(direcciones, format_saved_address):
    if not direcciones:
        return "*Aun no tienes ubicaciones guardadas.*"
    lineas = ["*Tus ubicaciones:*"]
    for idx, row in enumerate(direcciones, start=1):
        lineas.extend(build_saved_location_option_lines(row, idx, format_saved_address))
    lineas.append("*Escribe el numero* que quieres editar.")
    return "\n".join(lineas)


def build_location_manage_actions(row, index, format_saved_address):
    nombre = get_saved_location_display(row, index, format_saved_address)
    return f"*{nombre}*\n*Que deseas hacer?*"


def build_location_rename_prompt(row, index, format_saved_address):
    nombre = get_saved_location_display(row, index, format_saved_address)
    return f"*Nuevo nombre* para *{nombre}*:"


def build_location_deleted_message(label):
    return f"*Listo.* Elimine *{label}*."


def build_location_renamed_message(label):
    return f"*Listo.* Ahora se llama *{label}*."


def get_saved_location_display(row, index=0, format_saved_address=None):
    etiqueta = (row["etiqueta"] or "").strip() if row else ""
    if etiqueta:
        return etiqueta
    direccion = (row["direccion"] or "").strip() if row else ""
    if format_saved_address:
        direccion = format_saved_address(direccion)
    if direccion and not looks_like_coordinates(direccion):
        return direccion
    if index:
        return f"Ubicacion guardada {index}"
    return "Ubicacion guardada"


def get_saved_location_secondary_text(row, format_saved_address=None):
    direccion = (row["direccion"] or "").strip() if row else ""
    if format_saved_address:
        direccion = format_saved_address(direccion)
    etiqueta = (row["etiqueta"] or "").strip() if row else ""
    if not direccion:
        return ""
    if etiqueta and normalize_user_text(etiqueta) == normalize_user_text(direccion):
        return ""
    return direccion


def build_saved_location_option_lines(row, index=0, format_saved_address=None):
    primary = get_saved_location_display(row, index, format_saved_address)
    secondary = get_saved_location_secondary_text(row, format_saved_address)
    lines = [f"{index}. {primary}"] if index else [primary]
    if secondary:
        lines.append(f"   - {secondary}")
    return lines


def build_saved_location_payload(row):
    direccion = (row["direccion"] or "").strip() if row else ""
    etiqueta = (row["etiqueta"] or "").strip() if row else ""
    latitude = (row["latitude"] or "").strip() if row else ""
    longitude = (row["longitude"] or "").strip() if row else ""
    return {
        "direccion": etiqueta or direccion,
        "raw_direccion": direccion,
        "latitude": latitude,
        "longitude": longitude,
        "coords": f"{latitude},{longitude}" if latitude and longitude else "",
        "label": etiqueta,
    }


def build_location_required_message(nombre=""):
    if nombre:
        return (
            f"Hola {nombre}, *que bueno verte de nuevo.*\n"
            "*Enviame tu ubicacion actual* o elige una guardada."
        )
    return (
        "*Que bueno tenerte por aqui.*\n"
        "*Enviame tu ubicacion actual* o elige una guardada."
    )


def build_new_location_prompt_message():
    return (
        "*Enviame tu ubicacion actual* desde WhatsApp para guardarla.\n"
        "Si la necesitas, toca *INSTRUCCIONES*."
    )


def parse_float_or_none(value):
    try:
        return float(str(value).strip())
    except Exception:
        return None


def haversine_distance_meters(lat1, lng1, lat2, lng2):
    if None in {lat1, lng1, lat2, lng2}:
        return None
    radius = 6371000.0
    lat1_rad = math.radians(lat1)
    lat2_rad = math.radians(lat2)
    d_lat = math.radians(lat2 - lat1)
    d_lng = math.radians(lng2 - lng1)
    a = (
        math.sin(d_lat / 2) ** 2
        + math.cos(lat1_rad) * math.cos(lat2_rad) * math.sin(d_lng / 2) ** 2
    )
    return radius * (2 * math.atan2(math.sqrt(a), math.sqrt(1 - a)))


def get_service_coverage_config(cur):
    data = {}
    try:
        row = cur.execute(
            "SELECT value FROM config WHERE key = ?",
            ("service_coverage_config",),
        ).fetchone()
        raw_value = row["value"] if row and row["value"] is not None else ""
        if raw_value:
            parsed = json.loads(raw_value)
            if isinstance(parsed, dict):
                data = parsed
    except Exception:
        data = {}

    try:
        center_lat = float(data.get("center_lat", DEFAULT_COVERAGE_CENTER_LAT))
    except Exception:
        center_lat = DEFAULT_COVERAGE_CENTER_LAT
    try:
        center_lng = float(data.get("center_lng", DEFAULT_COVERAGE_CENTER_LNG))
    except Exception:
        center_lng = DEFAULT_COVERAGE_CENTER_LNG
    try:
        radius_meters = int(
            round(float(data.get("radius_meters", DEFAULT_COVERAGE_RADIUS_METERS)))
        )
    except Exception:
        radius_meters = DEFAULT_COVERAGE_RADIUS_METERS
    radius_meters = max(500, min(radius_meters, 50000))

    return {
        "center_lat": center_lat,
        "center_lng": center_lng,
        "radius_meters": radius_meters,
    }


def get_request_coordinates(raw_direccion, location_payload, parse_coords_from_text):
    latitude = parse_float_or_none((location_payload or {}).get("latitude"))
    longitude = parse_float_or_none((location_payload or {}).get("longitude"))
    if latitude is not None and longitude is not None:
        return latitude, longitude
    parsed = parse_coords_from_text(raw_direccion or "")
    if not parsed:
        return None, None
    return parsed[0], parsed[1]


def is_intermunicipal_request(
    cur,
    raw_direccion,
    location_payload,
    parse_coords_from_text,
):
    lat, lng = get_request_coordinates(
        raw_direccion, location_payload, parse_coords_from_text
    )
    if lat is None or lng is None:
        return False
    coverage = get_service_coverage_config(cur)
    distance_meters = haversine_distance_meters(
        coverage["center_lat"],
        coverage["center_lng"],
        lat,
        lng,
    )
    if distance_meters is None:
        return False
    return distance_meters > coverage["radius_meters"]


def build_out_of_zone_confirmation_message():
    return (
        "*Principalmente trabajamos en La Ceja.*\n"
        "La ubicacion que me diste esta por fuera de nuestra zona principal, pero podemos intentar la solicitud por si hay un conductor cerca.\n"
        "Responde *SI* para enviarla o *NO* para cambiar la ubicacion."
    )


def respond_out_of_zone_confirmation(phone, reply_sender=None):
    message = build_out_of_zone_confirmation_message()
    return respond_client(
        phone,
        message,
        reply_sender=reply_sender,
        buttons_key="out_of_zone_confirm" if reply_sender else "",
        buttons_variables={"1": message} if reply_sender else None,
    )


def parse_meta_json(raw_value):
    text = (raw_value or "").strip()
    if not text:
        return {}
    try:
        parsed = json.loads(text)
    except Exception:
        return {}
    return parsed if isinstance(parsed, dict) else {}


def dump_meta_json(data):
    return json.dumps(data or {}, ensure_ascii=True)


def enrich_request_payload_for_dispatch(
    cur,
    direccion,
    location_payload,
    parse_coords_from_text,
    display_label="",
):
    payload = dict(location_payload or {})
    payload["direccion"] = payload.get("direccion") or display_label or direccion or ""
    payload["raw_direccion"] = (
        payload.get("raw_direccion") or direccion or payload["direccion"]
    )
    payload["label"] = payload.get("label") or display_label or payload["direccion"]
    latitude = payload.get("latitude") or ""
    longitude = payload.get("longitude") or ""
    if (not latitude or not longitude) and payload["raw_direccion"]:
        parsed_lat, parsed_lng = parse_coords_from_text(payload["raw_direccion"])
        if parsed_lat is not None and parsed_lng is not None:
            latitude = str(parsed_lat)
            longitude = str(parsed_lng)
    if latitude and longitude:
        payload["latitude"] = latitude
        payload["longitude"] = longitude
        payload["coords"] = payload.get("coords") or f"{latitude},{longitude}"
    if is_intermunicipal_request(
        cur,
        payload["raw_direccion"],
        payload,
        parse_coords_from_text,
    ):
        payload["service_type"] = INTERMUNICIPAL_LABEL
    return payload


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


def get_direcciones(cur, usuario_id, is_reserved_direccion):
    if not usuario_id:
        return []
    rows = cur.execute(
        """
        SELECT id, direccion, etiqueta, latitude, longitude
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


def build_direcciones_prompt(direcciones, format_saved_address):
    if not direcciones:
        return ""
    lineas = ["*Ubicaciones guardadas:*", ""]
    for idx, row in enumerate(direcciones, start=1):
        lineas.extend(build_saved_location_option_lines(row, idx, format_saved_address))
        if idx < len(direcciones):
            lineas.append("")
    lineas.append("*Elige una* o usa *NUEVA* / *EDITAR UBICACIONES*.")
    return "\n".join(lineas)


def build_location_prompt_with_saved_options(nombre, direcciones, format_saved_address):
    base = build_location_required_message(nombre)
    lista = build_direcciones_prompt(direcciones, format_saved_address)
    if lista:
        return f"{base}\n{lista}"
    return base


def direccion_existe(cur, usuario_id, direccion, latitude="", longitude=""):
    if not usuario_id or not direccion:
        return False
    if latitude and longitude:
        row = cur.execute(
            """
            SELECT 1
            FROM direcciones
            WHERE usuario_id = ? AND latitude = ? AND longitude = ?
            """,
            (usuario_id, latitude, longitude),
        ).fetchone()
    else:
        row = cur.execute(
            """
            SELECT 1
            FROM direcciones
            WHERE usuario_id = ? AND lower(direccion) = lower(?)
            """,
            (usuario_id, direccion),
        ).fetchone()
    return row is not None


def get_location_payload(values):
    body = (values.get("Body") or "").strip()
    lat = (values.get("Latitude") or "").strip()
    lon = (values.get("Longitude") or "").strip()
    addr = (values.get("Address") or "").strip()
    label = (values.get("Label") or "").strip()
    if (not lat or not lon) and looks_like_coordinates(body):
        parts = [chunk.strip() for chunk in body.split(",", 1)]
        if len(parts) == 2:
            lat = lat or parts[0]
            lon = lon or parts[1]
    coords = f"{lat},{lon}" if lat and lon else ""
    direccion = addr or label or coords
    return {
        "direccion": direccion,
        "raw_direccion": direccion,
        "latitude": lat,
        "longitude": lon,
        "coords": coords,
    }


def build_request_message(nombre):
    if nombre:
        return f"*Listo, {nombre}.* Ya buscamos conductor.\n{SHORT_CANCEL_HINT}"
    return f"*Listo.* Ya buscamos conductor.\n{SHORT_CANCEL_HINT}"


def build_request_message_for_saved_location(nombre, label):
    if nombre:
        return f"*Listo, {nombre}.* Ya buscamos carro para *{label}*.\n{SHORT_CANCEL_HINT}"
    return f"*Listo.* Ya buscamos carro para *{label}*.\n{SHORT_CANCEL_HINT}"


def build_welcome_back_message(nombre, original_text=""):
    return build_location_required_message(nombre)


def build_new_customer_message(original_text=""):
    return build_invitation_required_message()


def build_invitation_required_message():
    return (
        "*Hola. Bienvenid@ a Zipp.*\n\n"
        "Nos encantaria llevarte a tu destino, pero para garantizar la seguridad y exclusividad de nuestra comunidad, "
        "operamos por invitacion.\n\n"
        "Por favor, ingresa tu codigo de invitacion ahora. Si no tienes uno, pidele a un amigo que ya use Zipp "
        "que te genere un codigo desde su perfil.\n\n"
        "Aqui te espero."
    )


def build_invitation_verified_message(inviter_name):
    inviter = (inviter_name or "tu contacto").strip()
    return (
        f"*Codigo verificado.* Ahora eres parte de la red de confianza de *{inviter}*.\n"
        "*Cual es tu nombre?*"
    )


def build_name_required_for_invitation_message():
    return (
        "*Necesito tu nombre para completar el registro.*\n"
        "Escribelo tal como quieres aparecer en Zipp."
    )


def build_invitation_completed_message(nombre):
    return (
        f"*Gracias, {nombre}.* Tu registro quedo activo en la red de confianza de Zipp.\n"
        "*Enviame tu ubicacion actual* cuando quieras pedir tu servicio."
    )


def wants_invitation_code(text):
    normalized = normalize_user_text(text)
    return normalized in {
        "generar codigo",
        "generar codigo de invitacion",
        "codigo",
        "codigo de invitacion",
        "crear codigo",
        "invitar",
        "invitar amigo",
        "invitar a un amigo",
    }


def append_invitation_action(text):
    base = (text or "").strip()
    action = "_Para invitar a un amigo, escribe_ *GENERAR CODIGO*."
    if not base:
        return action
    if "GENERAR CODIGO" in base.upper():
        return base
    return f"{base}\n\n{action}"


def build_generated_invitation_message(code_row):
    return (
        "*Codigo de invitacion generado.*\n"
        f"*{code_row['codigo']}*\n\n"
        f"Este codigo tiene validez hasta *{format_expiration(code_row['expira_en'])}*.\n"
        "Compartelo solo con una persona de confianza. Cuando se use, quedara invalidado."
    )


def build_name_ack_message(nombre):
    return f"*Gracias, {nombre}.*\n*Enviame tu ubicacion actual.*"


def build_known_name_direction_message(nombre):
    return f"*Perfecto, {nombre}.*\n*Enviame tu ubicacion actual.*"


def detect_greeting_phrase(text):
    normalized = normalize_user_text(text)
    if "buenas noches" in normalized:
        return "Buenas noches"
    if "buenas tardes" in normalized:
        return "Buenas tardes"
    if "buenos dias" in normalized:
        return "Buenos dias"
    if "hola" in normalized or "ola" in normalized or "buenas" in normalized:
        return "Hola"
    return "Hola"


def build_service_name_request_message(original_text=""):
    return build_new_customer_message(original_text)


def build_missing_name_message(original_text=""):
    return build_name_required_for_invitation_message()


def build_known_user_reply_message(nombre, original_text="", reply=""):
    intro = f"Hola {nombre}."
    if reply:
        return f"{intro}\n{reply}"
    return intro


def build_new_user_reply_message(original_text="", reply=""):
    intro = FIRST_CONTACT_WELCOME
    if reply:
        return f"{intro}\n{reply}"
    return intro


def build_named_support_reply(nombre="", reply=""):
    if not reply:
        return ""
    if nombre:
        return f"Claro, {nombre}.\n{reply}"
    return reply


def ensure_first_contact_welcome(reply):
    text = (reply or "").strip()
    if not text:
        return FIRST_CONTACT_WELCOME
    text = re.sub(
        r"^(hola|buenas|buenos dias|buenas tardes|buenas noches)[,!.:\s]+",
        "",
        text,
        flags=re.IGNORECASE,
    ).strip()
    if FIRST_CONTACT_WELCOME in text:
        return text
    return f"{FIRST_CONTACT_WELCOME}\n{text}"


def is_price_question_text(text):
    normalized = normalize_user_text(text)
    if not normalized:
        return False
    return any(keyword in normalized for keyword in PRICE_KEYWORDS)


def has_question_signal(text):
    raw = (text or "").strip()
    normalized = normalize_user_text(text)
    if not normalized:
        return False
    if "?" in raw or "¿" in raw:
        return True
    if normalized.startswith(QUESTION_PREFIXES):
        return True
    return any(keyword in normalized for keyword in QUESTION_SIGNAL_KEYWORDS)


def is_faq_style_message(text, location_payload=None):
    normalized = normalize_user_text(text)
    if not normalized:
        return False
    question_signal = has_question_signal(text)
    if location_payload and location_payload.get("direccion"):
        return False
    if is_price_question_text(text):
        return True
    if any(keyword in normalized for keyword in FAQ_KEYWORDS) and question_signal:
        return True
    if question_signal and any(
        keyword in normalized for keyword in FAQ_SERVICE_KEYWORDS
    ):
        return True
    if looks_like_address(text):
        return False
    return False


def build_beta_pricing_message(nombre="", original_text="", next_prompt=""):
    cierre = (
        next_prompt
        or "*Si quieres,* te ayudo a pedir tu servicio."
    )
    return f"*Aun no compartimos tarifas* por este medio.\n{cierre}"


def build_general_faq_message(nombre="", original_text="", next_prompt=""):
    cierre = (
        next_prompt
        or "*Si quieres,* te ayudo a pedir tu servicio."
    )
    return f"Con gusto te ayudo.\n{cierre}"


def build_continue_service_name_request_message():
    return "*Para pedir tu servicio,* cuentame tu nombre."


def build_address_ack_name_request_message():
    return build_reference_ack_name_request_message()


def build_open_service_status_message():
    return f"*Seguimos buscando tu conductor.*\n{SHORT_CANCEL_HINT}"


def extract_request_field(payload, field_name):
    if not payload:
        return ""
    prefix = f"{field_name.lower()}:"
    for part in str(payload).split("|"):
        chunk = part.strip()
        if chunk.lower().startswith(prefix):
            return chunk.split(":", 1)[1].strip()
    return ""


def has_open_service_status_question(text):
    normalized = normalize_user_text(text)
    if not normalized:
        return False
    return any(keyword in normalized for keyword in OPEN_SERVICE_STATUS_KEYWORDS)


def is_booking_request_text(text):
    normalized = normalize_user_text(text)
    if not normalized:
        return False
    return any(keyword in normalized for keyword in BOOKING_KEYWORDS)


def is_clear_service_request(text, location_payload=None):
    normalized = normalize_user_text(text)
    if not normalized:
        return False
    if location_payload and location_payload.get("direccion"):
        return True
    if is_faq_style_message(text, location_payload=location_payload):
        return False
    if has_question_signal(text):
        return False
    return is_booking_request_text(text)


def build_open_service_eta_message(service_row):
    nombre = extract_request_field(
        service_row["mensaje_cliente"] if service_row else "",
        "Nombre",
    )
    direccion = extract_request_field(
        service_row["mensaje_cliente"] if service_row else "",
        "Direccion",
    )
    intro = f"*{nombre}, tu solicitud sigue activa.*" if nombre else "*Tu solicitud sigue activa.*"
    if direccion:
        visible_location = (
            "tu ubicacion compartida" if looks_like_coordinates(direccion) else direccion
        )
        return f"{intro}\nRecogida: *{visible_location}*.\nTe avisamos por aqui apenas se asigne un conductor.\n{SHORT_CANCEL_HINT}"
    return f"{intro}\nTe avisamos por aqui apenas se asigne un conductor.\n{SHORT_CANCEL_HINT}"


def build_open_service_reassurance_message(service_row):
    nombre = extract_request_field(
        service_row["mensaje_cliente"] if service_row else "",
        "Nombre",
    )
    if nombre:
        return f"*{nombre},* estamos aca para servirte. Cualquier cosa me avisas."
    return "*Estamos aca para servirte.* Cualquier cosa me avisas."


def is_open_service_soft_ping(text):
    normalized = normalize_user_text(text)
    if not normalized:
        return False
    if normalized in GREETING_KEYWORDS:
        return True
    if has_open_service_status_question(text):
        return False
    if normalized in {"ok", "okay", "vale", "listo", "dale", "gracias", "ok gracias"}:
        return True
    word_count = len([word for word in normalized.split() if word.strip()])
    return word_count > 0 and word_count <= 4 and not is_booking_request_text(text)


def get_latest_search_service(cur, telefono):
    row = cur.execute(
        """
        SELECT *
        FROM pedidos
        WHERE cliente_telefono = ?
        ORDER BY id DESC
        LIMIT 1
        """,
        (telefono,),
    ).fetchone()
    if row and (row["estado"] or "").strip().lower() in {"disponible", "pendiente"}:
        return row
    return None


def get_latest_taken_service(cur, telefono):
    row = cur.execute(
        """
        SELECT *
        FROM pedidos
        WHERE cliente_telefono = ?
        ORDER BY id DESC
        LIMIT 1
        """,
        (telefono,),
    ).fetchone()
    if row and (row["estado"] or "").strip() == "Tomado":
        return row
    return None


def build_customer_request_payload(
    nombre,
    direccion,
    location_payload,
    parse_coords_from_text,
    display_label="",
):
    raw_direccion = (location_payload.get("raw_direccion") or direccion or "").strip()
    public_location = direccion or raw_direccion or location_payload.get("direccion") or "Ubicacion compartida"
    partes = [f"Nombre: {nombre}", f"Direccion: {public_location}"]
    pickup_note = (location_payload.get("pickup_note") or "").strip()
    latitude = location_payload.get("latitude")
    longitude = location_payload.get("longitude")
    if pickup_note and normalize_user_text(pickup_note) != normalize_user_text(public_location):
        partes.append(f"Referencia: {pickup_note}")
    if (not latitude or not longitude) and raw_direccion:
        parsed_lat, parsed_lng = parse_coords_from_text(raw_direccion)
        if parsed_lat is not None and parsed_lng is not None:
            latitude = str(parsed_lat)
            longitude = str(parsed_lng)
    if raw_direccion and normalize_user_text(raw_direccion) != normalize_user_text(public_location):
        partes.append(f"Mapa: {raw_direccion}")
    if latitude and longitude:
        partes.append(f"Latitude: {latitude}")
        partes.append(f"Longitude: {longitude}")
    service_type = (location_payload.get("service_type") or "").strip()
    if service_type:
        partes.append(f"Tipo: {service_type}")
    return " | ".join(partes)


def respond_client(
    phone,
    text,
    reply_sender=None,
    buttons_key="",
    buttons_variables=None,
):
    if reply_sender and buttons_key:
        sent_ok, _sent_error = reply_sender(
            phone,
            text,
            buttons_key=buttons_key,
            buttons_variables=buttons_variables or {},
        )
        if not sent_ok:
            resp = MessagingResponse()
            if text:
                resp.message(text)
            xml = str(resp)
            print(f"TwiML fallback -> {xml}")
            return xml, 200, {"Content-Type": "text/xml; charset=utf-8"}
        resp = MessagingResponse()
        xml = str(resp)
        print(f"TwiML -> {xml}")
        return xml, 200, {"Content-Type": "text/xml; charset=utf-8"}

    resp = MessagingResponse()
    if text:
        resp.message(text)
    xml = str(resp)
    print(f"TwiML -> {xml}")
    return xml, 200, {"Content-Type": "text/xml; charset=utf-8"}


def set_conversation(cur, telefono, paso, timestamp, nombre="", direccion="", servicio="", meta=""):
    cur.execute("DELETE FROM conversaciones WHERE telefono = ?", (telefono,))
    cur.execute(
        """
        INSERT INTO conversaciones (telefono, paso, servicio, nombre, direccion, meta, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (telefono, paso, servicio, nombre, direccion, meta, timestamp),
    )


def persist_new_address(cur, usuario_id, direccion, timestamp, etiqueta="", latitude="", longitude=""):
    if not usuario_id or not direccion:
        return
    if direccion_existe(cur, usuario_id, direccion, latitude, longitude):
        return
    cur.execute(
        """
        INSERT INTO direcciones (
            usuario_id, direccion, etiqueta, latitude, longitude, created_at, updated_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (usuario_id, direccion, etiqueta, latitude, longitude, timestamp, timestamp),
    )


def create_pending_request(
    cur,
    telefono,
    nombre,
    direccion,
    location_payload,
    timestamp,
    usuario_id,
    parse_coords_from_text,
    display_label="",
):
    mensaje_cliente = build_customer_request_payload(
        nombre,
        direccion,
        location_payload,
        parse_coords_from_text,
        display_label=display_label,
    )
    cur.execute(
        """
        INSERT INTO pedidos (cliente_telefono, mensaje_cliente, estado, timestamp)
        VALUES (?, ?, 'Pendiente', ?)
        """,
        (telefono, mensaje_cliente, timestamp),
    )
    cur.execute("DELETE FROM conversaciones WHERE telefono = ?", (telefono,))


def queue_pending_request_with_zone_check(
    cur,
    telefono,
    nombre,
    direccion,
    location_payload,
    timestamp,
    usuario_id,
    parse_coords_from_text,
    display_label="",
):
    dispatch_payload = enrich_request_payload_for_dispatch(
        cur,
        direccion,
        location_payload,
        parse_coords_from_text,
        display_label=display_label,
    )
    if (dispatch_payload.get("service_type") or "").strip() == INTERMUNICIPAL_LABEL:
        set_conversation(
            cur,
            telefono,
            "confirmar_fuera_zona",
            timestamp,
            nombre=nombre,
            direccion=dispatch_payload.get("raw_direccion") or direccion,
            meta=dump_meta_json(
                {
                    "pending_request": {
                        "direccion": dispatch_payload.get("direccion") or "",
                        "raw_direccion": dispatch_payload.get("raw_direccion") or "",
                        "latitude": dispatch_payload.get("latitude") or "",
                        "longitude": dispatch_payload.get("longitude") or "",
                        "coords": dispatch_payload.get("coords") or "",
                        "pickup_note": dispatch_payload.get("pickup_note") or "",
                        "label": dispatch_payload.get("label") or display_label or "",
                        "service_type": dispatch_payload.get("service_type") or "",
                    }
                }
            ),
        )
        return build_out_of_zone_confirmation_message()

    create_pending_request(
        cur,
        telefono,
        nombre,
        dispatch_payload.get("raw_direccion") or direccion,
        dispatch_payload,
        timestamp,
        usuario_id,
        parse_coords_from_text,
        display_label=display_label or dispatch_payload.get("label") or "",
    )
    return ""


class TwilioGroqAssistant:
    def __init__(self):
        settings = load_maicol_groq_settings()
        self.api_key = settings["api_key"]
        self.model = settings["model"]
        self.source_path = settings["source_path"]
        self.client = self._init_client()

    def _init_client(self):
        if not Groq or not self.api_key.strip():
            return None
        try:
            return Groq(api_key=self.api_key)
        except Exception:
            return None

    def build_system_prompt(self):
        return (
            "Eres el asistente de WhatsApp de Zipp. "
            "Debes responder breve, claro, amable y cercano. "
            "Ayudas a reservar servicios, aclarar dudas basicas y entender mensajes libres del cliente. "
            "El cliente siempre debe sentirse acompanado y bien atendido. "
            "En el primer mensaje, saluda con calidez antes de responder. "
            "No inventes conductores, tiempos exactos, cobertura total ni tarifas exactas. "
            "Si el cliente escribe que quiere un servicio, un carro, una carrera o un traslado, entiende eso como solicitud de servicio. "
            "Si aun no sabes su nombre, pide el nombre. Si ya sabes el nombre pero falta ubicacion, pide la ubicacion. "
            "Si preguntan por precio o tarifa, explica que estan en fase beta y que no puedes compartir tarifas por ahora. "
            "Si el mensaje suena a pregunta, responde la duda primero. "
            "Aunque mencione carrera, viaje o destino, no lo trates como reserva si claramente es una pregunta. "
            "Luego invita a continuar la conversacion sin cerrarla. "
            "Si el cliente quiere pedir servicio, intenta identificar nombre y direccion de recogida. "
            "Si no estas seguro, responde de forma corta y orienta al siguiente paso."
        )

    def _ask_llm_raw(self, messages, temperature=0.2, max_tokens=220, model=None):
        if not self.client:
            return ""
        try:
            completion = self.client.chat.completions.create(
                model=model or self.model,
                messages=messages,
                temperature=temperature,
                max_tokens=max_tokens,
            )
            return (completion.choices[0].message.content or "").strip()
        except Exception:
            if model and model != self.model:
                try:
                    completion = self.client.chat.completions.create(
                        model=self.model,
                        messages=messages,
                        temperature=temperature,
                        max_tokens=max_tokens,
                    )
                    return (completion.choices[0].message.content or "").strip()
                except Exception:
                    return ""
            return ""

    def _parse_json_from_text(self, text):
        raw = (text or "").strip()
        if not raw:
            return None
        try:
            return json.loads(raw)
        except Exception:
            pass
        match = re.search(r"\{.*\}", raw, flags=re.DOTALL)
        if not match:
            match = re.search(r"\[.*\]", raw, flags=re.DOTALL)
        if not match:
            return None
        try:
            return json.loads(match.group(0))
        except Exception:
            return None

    def extract_customer_name_agent(self, text, conversation_step="", known_name=""):
        raw_text = (text or "").strip()
        if not raw_text:
            return ""
        if not self.client:
            return extract_name_from_text(raw_text)

        prompt = (
            "Extrae SOLO el nombre real de la persona desde un mensaje de WhatsApp.\n"
            "Responde SOLO en JSON con esta estructura exacta: {\"name\":\"...\"}\n"
            "Si no hay un nombre claro, responde {\"name\":\"\"}\n"
            "No devuelvas conectores, saludos ni frases completas.\n"
            "Ejemplos:\n"
            "- 'con Andrea' => Andrea\n"
            "- 'hablas con Andrea Maria' => Andrea Maria\n"
            "- 'a nombre de Juan' => Juan\n"
            "- 'soy Carlos' => Carlos\n"
            "- 'buenas tardes' => ''\n"
            f"Paso actual: {conversation_step or 'ninguno'}\n"
            f"Nombre conocido: {known_name or 'ninguno'}\n"
            f"Mensaje: {raw_text}"
        )
        raw = self._ask_llm_raw(
            [
                {
                    "role": "system",
                    "content": (
                        "Eres un extractor de nombres. "
                        "Tu unica funcion es devolver el nombre limpio o vacio en JSON."
                    ),
                },
                {"role": "user", "content": prompt},
            ],
            temperature=0,
            max_tokens=60,
        )
        parsed = self._parse_json_from_text(raw)
        if isinstance(parsed, dict):
            candidate = clean_name_candidate(parsed.get("name") or "")
            if candidate:
                return candidate
        return extract_name_from_text(raw_text)

    def _heuristic_analysis(
        self,
        text,
        conversation_step="",
        has_open_service=False,
        has_taken_service=False,
        known_name="",
        location_payload=None,
    ):
        normalized = normalize_user_text(text)
        name = extract_name_from_text(text)
        address = ""
        saved_address_index = extract_saved_address_choice(text)
        if location_payload and location_payload.get("direccion"):
            address = location_payload["direccion"]
        elif looks_like_address(text):
            address = (text or "").strip()

        if conversation_step == "nombre":
            return {
                "intent": "provide_name" if name else "unknown",
                "reply": "",
                "customer_name": name,
                "pickup_address": "",
                "saved_address_index": 0,
                "source": "fallback",
                "raw": "",
            }

        if conversation_step == "direccion":
            if wants_location_help(text):
                return {
                    "intent": "faq",
                    "reply": build_map_only_help_message(),
                    "customer_name": "",
                    "pickup_address": "",
                    "saved_address_index": 0,
                    "source": "fallback",
                    "raw": "",
                }
            return {
                "intent": "provide_address" if (address or saved_address_index) else "unknown",
                "reply": "",
                "customer_name": "",
                "pickup_address": address,
                "saved_address_index": saved_address_index,
                "source": "fallback",
                "raw": "",
            }

        if normalized == "cancelar":
            return {
                "intent": "cancel",
                "reply": "",
                "customer_name": "",
                "pickup_address": "",
                "saved_address_index": 0,
                "source": "fallback",
                "raw": "",
            }

        if wants_location_help(text):
            return {
                "intent": "faq",
                "reply": build_map_only_help_message(),
                "customer_name": "",
                "pickup_address": "",
                "saved_address_index": 0,
                "source": "fallback",
                "raw": "",
            }

        if has_open_service and any(
            word in normalized for word in ("donde", "estado", "demora", "falta", "siguen")
        ):
            return {
                "intent": "status",
                "reply": build_open_service_status_message(),
                "customer_name": "",
                "pickup_address": "",
                "saved_address_index": 0,
                "source": "fallback",
                "raw": "",
            }

        if has_taken_service:
            return {
                "intent": "driver_message",
                "reply": "*Mensaje recibido.* Tu conductor te responde por aqui.",
                "customer_name": "",
                "pickup_address": "",
                "saved_address_index": 0,
                "source": "fallback",
                "raw": "",
            }

        if is_price_question_text(text):
            return {
                "intent": "faq",
                "reply": build_beta_pricing_message(known_name, text),
                "customer_name": "",
                "pickup_address": "",
                "saved_address_index": 0,
                "source": "fallback",
                "raw": "",
            }

        if is_faq_style_message(text, location_payload=location_payload):
            return {
                "intent": "faq",
                "reply": build_general_faq_message(known_name, text),
                "customer_name": "",
                "pickup_address": "",
                "saved_address_index": 0,
                "source": "fallback",
                "raw": "",
            }
        if any(keyword in normalized for keyword in FAQ_KEYWORDS):
            return {
                "intent": "faq",
                "reply": build_general_faq_message(
                    known_name,
                    text,
                    next_prompt=(
                        "Si deseas, tambien puedo ayudarte a pedir tu servicio. "
                        "Solo cuentame desde donde te recogemos."
                    ),
                ),
                "customer_name": "",
                "pickup_address": "",
                "saved_address_index": 0,
                "source": "fallback",
                "raw": "",
            }

        if name and (address or any(word in normalized for word in BOOKING_KEYWORDS)):
            return {
                "intent": "new_service",
                "reply": "",
                "customer_name": name,
                "pickup_address": address,
                "saved_address_index": saved_address_index,
                "source": "fallback",
                "raw": "",
            }

        if address:
            return {
                "intent": "new_service",
                "reply": "",
                "customer_name": known_name or "",
                "pickup_address": address,
                "saved_address_index": saved_address_index,
                "source": "fallback",
                "raw": "",
            }

        if any(keyword in normalized for keyword in BOOKING_KEYWORDS):
            return {
                "intent": "new_service",
                "reply": (
                    "*Claro.* Te ayudo con tu servicio.\n*Enviame tu ubicacion actual.*"
                    if known_name
                    else "*Claro.* Te ayudo con tu servicio.\n*Como te llamas?*"
                ),
                "customer_name": name,
                "pickup_address": "",
                "saved_address_index": 0,
                "source": "fallback",
                "raw": "",
            }

        if normalized in GREETING_KEYWORDS:
            return {
                "intent": "greeting",
                "reply": "",
                "customer_name": "",
                "pickup_address": "",
                "saved_address_index": 0,
                "source": "fallback",
                "raw": "",
            }

        return {
            "intent": "unknown",
            "reply": (
                "Con gusto te ayudo.\n*Enviame tu ubicacion actual* o elige una guardada."
                if known_name
                else "Con gusto te ayudo.\n*Enviame tu ubicacion actual* o cuentame tu nombre."
            ),
            "customer_name": "",
            "pickup_address": "",
            "saved_address_index": saved_address_index,
            "source": "fallback",
            "raw": "",
        }

    def analyze_message(
        self,
        phone,
        text,
        conversation_step="",
        has_open_service=False,
        has_taken_service=False,
        known_name="",
        saved_addresses=None,
        location_payload=None,
        open_service_context="",
    ):
        raw_text = (text or "").strip()
        groq_available = bool(self.client)
        extracted_name_agent = self.extract_customer_name_agent(
            text,
            conversation_step=conversation_step,
            known_name=known_name,
        )

        def build_fallback_result():
            fallback = self._heuristic_analysis(
                text,
                conversation_step=conversation_step,
                has_open_service=has_open_service,
                has_taken_service=has_taken_service,
                known_name=known_name,
                location_payload=location_payload,
            )
            if extracted_name_agent and not fallback.get("customer_name"):
                fallback["customer_name"] = extracted_name_agent
            if conversation_step == "nombre" and extracted_name_agent:
                fallback["intent"] = "provide_name"
            fallback["groq_available"] = groq_available
            fallback["groq_used"] = False
            return fallback

        if not groq_available or not raw_text:
            return build_fallback_result()

        saved_lines = []
        for idx, row in enumerate(saved_addresses or [], start=1):
            direccion = get_saved_location_display(row, idx)
            if direccion:
                saved_lines.append(f"{idx}. {direccion}")
        saved_block = "\n".join(saved_lines) if saved_lines else "Sin ubicaciones guardadas."

        prompt = (
            "Analiza el mensaje de un cliente para una app de transporte.\n"
            "Responde SOLO en JSON con estas keys exactas:\n"
            "intent, reply, customer_name, pickup_address, saved_address_index.\n"
            "Intent permitidos: greeting, new_service, provide_name, provide_address, cancel, status, faq, driver_message, unknown.\n"
            f"Telefono: {phone}\n"
            f"Paso actual: {conversation_step or 'ninguno'}\n"
            f"Nombre conocido: {known_name or 'ninguno'}\n"
            f"Hay servicio en busqueda: {'si' if has_open_service else 'no'}\n"
            f"Hay servicio tomado: {'si' if has_taken_service else 'no'}\n"
            f"Contexto del servicio activo: {open_service_context or 'ninguno'}\n"
            f"Ubicacion compartida: {(location_payload or {}).get('direccion') or 'no'}\n"
            f"Ubicaciones guardadas:\n{saved_block}\n"
            f"Mensaje del cliente:\n{text}\n"
            "Reglas:\n"
            "- Si el cliente quiere pedir servicio, usa new_service.\n"
            "- Mensajes como 'hola para un servicio', 'necesito un carro' o 'quiero un traslado' son new_service.\n"
            "- Si el cliente dice cosas como 'como lo hago', 'como envio mi ubicacion', 'no se como hacerlo' o pide ayuda para compartir ubicacion, usa faq y explica paso a paso como enviar la ubicacion por WhatsApp.\n"
            "- Si el mensaje dice que es una pregunta o suena claramente a pregunta, prioriza faq sobre new_service.\n"
            "- Si menciona carrera, viaje, ruta o destino dentro de una pregunta, sigue siendo faq.\n"
            "- Si el cliente pregunta por precio o tarifa, usa faq y responde en tono beta sin dar valores exactos.\n"
            "- Si ya envio nombre, extraelo en customer_name.\n"
            "- Si ya envio una ubicacion compartida, extraela en pickup_address.\n"
            "- Si elige una guardada, pon el numero en saved_address_index.\n"
            "- reply debe ser breve y util para WhatsApp.\n"
            "- Si ya existe un servicio activo y el cliente pregunta por tiempo o estado, no pidas otra ubicacion.\n"
            "- Si no hace falta reply, usa cadena vacia."
        )

        raw = self._ask_llm_raw(
            [
                {"role": "system", "content": self.build_system_prompt()},
                {"role": "user", "content": prompt},
            ],
            temperature=0.2,
            max_tokens=220,
        )
        parsed = self._parse_json_from_text(raw)
        if not isinstance(parsed, dict):
            return build_fallback_result()

        intent = str(parsed.get("intent") or "").strip().lower()
        if intent not in {
            "greeting",
            "new_service",
            "provide_name",
            "provide_address",
            "cancel",
            "status",
            "faq",
            "driver_message",
            "unknown",
        }:
            return build_fallback_result()

        support = self._heuristic_analysis(
            text,
            conversation_step=conversation_step,
            has_open_service=has_open_service,
            has_taken_service=has_taken_service,
            known_name=known_name,
            location_payload=location_payload,
        )

        reply = str(parsed.get("reply") or "").strip()
        customer_name = clean_name_candidate(parsed.get("customer_name") or "")
        pickup_address = str(parsed.get("pickup_address") or "").strip()
        saved_address_index = parsed.get("saved_address_index") or 0
        try:
            saved_address_index = int(saved_address_index)
        except Exception:
            saved_address_index = 0

        if not customer_name and extracted_name_agent:
            customer_name = extracted_name_agent
        if not customer_name and support["customer_name"]:
            customer_name = support["customer_name"]
        if not pickup_address and support["pickup_address"]:
            pickup_address = support["pickup_address"]
        if not saved_address_index and support["saved_address_index"]:
            saved_address_index = support["saved_address_index"]
        if not reply and intent in {"faq", "status", "driver_message"}:
            reply = support["reply"]

        if wants_location_help(text):
            intent = "faq"
            reply = build_map_only_help_message()

        if conversation_step == "nombre" and customer_name:
            intent = "provide_name"
        if conversation_step == "direccion" and (pickup_address or saved_address_index):
            intent = "provide_address"

        if (
            intent == "new_service"
            and is_faq_style_message(text, location_payload=location_payload)
            and not pickup_address
            and not saved_address_index
        ):
            intent = "faq"
            if not reply:
                reply = support["reply"]

        return {
            "intent": intent,
            "reply": reply[:400],
            "customer_name": customer_name,
            "pickup_address": pickup_address,
            "saved_address_index": saved_address_index,
            "source": "groq",
            "groq_available": True,
            "groq_used": True,
            "raw": raw,
        }


_TWILIO_GROQ_ASSISTANT = None


def get_twilio_groq_assistant():
    global _TWILIO_GROQ_ASSISTANT
    if _TWILIO_GROQ_ASSISTANT is None:
        _TWILIO_GROQ_ASSISTANT = TwilioGroqAssistant()
    return _TWILIO_GROQ_ASSISTANT


def resolve_saved_address(direcciones, choice):
    if not choice:
        return ""
    if choice < 1 or choice > len(direcciones):
        return ""
    return direcciones[choice - 1]["direccion"]


def resolve_saved_address_row(direcciones, choice):
    if not choice:
        return None
    if choice < 1 or choice > len(direcciones):
        return None
    return direcciones[choice - 1]


def start_location_management_flow(
    cur,
    telefono,
    timestamp,
    usuario,
    format_saved_address,
    is_reserved_direccion,
):
    direcciones = get_direcciones(cur, usuario["id"], is_reserved_direccion)
    set_conversation(
        cur,
        telefono,
        "editar_ubicaciones",
        timestamp,
        nombre=usuario["nombre"] or "",
    )
    return build_location_manage_intro(direcciones, format_saved_address)


def start_known_user_flow(
    cur,
    telefono,
    usuario,
    timestamp,
    format_saved_address,
    is_reserved_direccion,
    original_text="",
):
    set_conversation(
        cur,
        telefono,
        "direccion",
        timestamp,
        nombre=usuario["nombre"] or "",
    )
    direcciones = get_direcciones(cur, usuario["id"], is_reserved_direccion)
    return append_invitation_action(
        build_location_prompt_with_saved_options(
            usuario["nombre"],
            direcciones,
            format_saved_address,
        )
    )


def start_new_user_flow(cur, telefono, timestamp, direccion="", original_text="", meta=""):
    if direccion and parse_meta_json(meta):
        set_conversation(cur, telefono, "detalle_recogida", timestamp, direccion="", meta=meta)
        return build_location_reference_request_message()
    set_conversation(cur, telefono, "nombre", timestamp, direccion=direccion, meta=meta)
    if direccion:
        return build_address_ack_name_request_message()
    return build_new_customer_message(original_text)


def handle_ai_preconversation(
    cur,
    telefono,
    mensaje_limpio,
    location_payload,
    fecha_actual,
    usuario,
    format_saved_address,
    is_reserved_direccion,
    parse_coords_from_text,
    debug_hook=None,
):
    if wants_location_help(mensaje_limpio):
        return build_map_only_help_message()

    if declines_location_help(mensaje_limpio):
        return build_waiting_for_map_message()

    if looks_like_address(mensaje_limpio) and not is_shared_location_payload(location_payload):
        return build_map_only_rejection_message()

    assistant = get_twilio_groq_assistant()
    usuario_id = usuario["id"] if usuario else None
    direcciones = get_direcciones(cur, usuario_id, is_reserved_direccion)
    analysis = assistant.analyze_message(
        telefono,
        mensaje_limpio,
        known_name=(usuario["nombre"] or "") if usuario else "",
        saved_addresses=direcciones,
        location_payload=location_payload,
    )
    if debug_hook:
        debug_hook({"stage": "preconversation", "analysis": analysis})

    nombre_detectado = analysis["customer_name"] or ((usuario["nombre"] or "") if usuario else "")
    direccion_row = resolve_saved_address_row(direcciones, analysis["saved_address_index"])
    direccion_detectada = analysis["pickup_address"] or (
        direccion_row["direccion"] if direccion_row else ""
    )
    if not direccion_detectada and is_shared_location_payload(location_payload):
        direccion_detectada = location_payload["direccion"] or location_payload.get("coords", "")

    # Flujo principal por bloques: si claramente quiere pedir servicio,
    # no dejamos que una respuesta libre de la IA desordene la conversacion.
    if is_clear_service_request(mensaje_limpio, location_payload=location_payload):
        if usuario and usuario["nombre"] and not direccion_detectada:
            return start_known_user_flow(
                cur,
                telefono,
                usuario,
                fecha_actual,
                format_saved_address,
                is_reserved_direccion,
                mensaje_limpio,
            )

        if nombre_detectado and direccion_detectada:
            if usuario is None:
                usuario_id = upsert_usuario(cur, telefono, nombre_detectado, fecha_actual)
                usuario = get_usuario_by_telefono(cur, telefono)
            else:
                usuario_id = usuario["id"]
            if is_shared_location_payload(location_payload):
                set_conversation(
                    cur,
                    telefono,
                    "detalle_recogida",
                    fecha_actual,
                    nombre=nombre_detectado,
                    meta=dump_meta_json(location_payload),
                )
                return build_location_reference_request_message()
            if direccion_row:
                saved_payload = build_saved_location_payload(direccion_row)
                selected_label = get_saved_location_display(
                    direccion_row,
                    analysis["saved_address_index"],
                    format_saved_address,
                )
                out_of_zone_message = queue_pending_request_with_zone_check(
                    cur,
                    telefono,
                    nombre_detectado,
                    saved_payload["raw_direccion"] or saved_payload["direccion"],
                    saved_payload,
                    fecha_actual,
                    usuario_id,
                    parse_coords_from_text,
                    display_label=selected_label,
                )
                if out_of_zone_message:
                    return out_of_zone_message
                return build_request_message_for_saved_location(
                    nombre_detectado, selected_label
                )
            return build_map_only_rejection_message()

        if nombre_detectado:
            if usuario is None:
                upsert_usuario(cur, telefono, nombre_detectado, fecha_actual)
            usuario_actualizado = get_usuario_by_telefono(cur, telefono)
            set_conversation(cur, telefono, "direccion", fecha_actual, nombre=nombre_detectado)
            direcciones = get_direcciones(
                cur,
                usuario_actualizado["id"] if usuario_actualizado else None,
                is_reserved_direccion,
            )
            lista = build_direcciones_prompt(direcciones, format_saved_address)
            respuesta = (
                build_name_ack_message(nombre_detectado)
                if analysis["customer_name"]
                else build_known_name_direction_message(nombre_detectado)
            )
            if lista:
                respuesta = f"{respuesta}\n{lista}"
            return respuesta

        if direccion_detectada:
            return start_new_user_flow(
                cur,
                telefono,
                fecha_actual,
                direccion=direccion_detectada,
                original_text=mensaje_limpio,
                meta=dump_meta_json(location_payload) if is_shared_location_payload(location_payload) else "",
            )

        set_conversation(cur, telefono, "nombre", fecha_actual)
        return build_service_name_request_message(mensaje_limpio)

    if analysis["intent"] == "faq" and analysis["reply"]:
        set_conversation(
            cur,
            telefono,
            "faq",
            fecha_actual,
            nombre=(usuario["nombre"] or "") if usuario else "",
        )
        if usuario and usuario["nombre"]:
            return build_named_support_reply(usuario["nombre"], analysis["reply"])
        return build_new_user_reply_message(mensaje_limpio, analysis["reply"])

    if analysis["intent"] == "greeting":
        if usuario and usuario["nombre"]:
            return start_known_user_flow(
                cur,
                telefono,
                usuario,
                fecha_actual,
                format_saved_address,
                is_reserved_direccion,
                mensaje_limpio,
            )
        return start_new_user_flow(
            cur,
            telefono,
            fecha_actual,
            original_text=mensaje_limpio,
        )

    if analysis["intent"] == "new_service":
        nombre = nombre_detectado
        direccion = direccion_detectada

        if nombre and direccion:
            if usuario is None:
                usuario_id = upsert_usuario(cur, telefono, nombre, fecha_actual)
                usuario = get_usuario_by_telefono(cur, telefono)
            else:
                usuario_id = usuario["id"]
            if is_shared_location_payload(location_payload):
                set_conversation(
                    cur,
                    telefono,
                    "detalle_recogida",
                    fecha_actual,
                    nombre=nombre,
                    meta=dump_meta_json(location_payload),
                )
                return build_location_reference_request_message()
            if direccion_row:
                saved_payload = build_saved_location_payload(direccion_row)
                selected_label = get_saved_location_display(
                    direccion_row,
                    analysis["saved_address_index"],
                    format_saved_address,
                )
                out_of_zone_message = queue_pending_request_with_zone_check(
                    cur,
                    telefono,
                    nombre,
                    saved_payload["raw_direccion"] or saved_payload["direccion"],
                    saved_payload,
                    fecha_actual,
                    usuario_id,
                    parse_coords_from_text,
                    display_label=selected_label,
                )
                if out_of_zone_message:
                    return out_of_zone_message
                return build_request_message_for_saved_location(
                    nombre, selected_label
                )
            return build_map_only_rejection_message()

        if nombre:
            if usuario is None:
                upsert_usuario(cur, telefono, nombre, fecha_actual)
            usuario_actualizado = get_usuario_by_telefono(cur, telefono)
            set_conversation(cur, telefono, "direccion", fecha_actual, nombre=nombre)
            direcciones = get_direcciones(
                cur,
                usuario_actualizado["id"] if usuario_actualizado else None,
                is_reserved_direccion,
            )
            lista = build_direcciones_prompt(direcciones, format_saved_address)
            if analysis["customer_name"]:
                respuesta = build_name_ack_message(nombre)
            else:
                respuesta = build_known_name_direction_message(nombre)
            if lista:
                respuesta = f"{respuesta}\n{lista}"
            return respuesta

        if direccion:
            return start_new_user_flow(
                cur,
                telefono,
                fecha_actual,
                direccion=direccion,
                original_text=mensaje_limpio,
                meta=dump_meta_json(location_payload) if is_shared_location_payload(location_payload) else "",
            )

        if is_booking_request_text(mensaje_limpio):
            set_conversation(cur, telefono, "nombre", fecha_actual)
            return build_service_name_request_message(mensaje_limpio)

    if analysis["reply"]:
        if is_booking_request_text(mensaje_limpio):
            set_conversation(cur, telefono, "nombre", fecha_actual)
            return build_service_name_request_message(mensaje_limpio)
        if usuario and usuario["nombre"]:
            return build_named_support_reply(usuario["nombre"], analysis["reply"])
        return ensure_first_contact_welcome(analysis["reply"])

    return None


def handle_twilio_webhook(
    values,
    *,
    get_conn,
    format_saved_address,
    is_reserved_direccion,
    parse_coords_from_text,
    reply_sender=None,
    debug_hook=None,
):
    telefono = values.get("From", "")
    mensaje = get_incoming_message_text(values)
    fecha_actual = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(
        "Webhook recibido de "
        f"{telefono}: body={values.get('Body', '')} "
        f"button_text={values.get('ButtonText', '')} "
        f"button_payload={values.get('ButtonPayload', '')} "
        f"-> mensaje={mensaje}"
    )

    conn = None
    try:
        conn = get_conn()
        cur = conn.cursor()

        mensaje_limpio = (mensaje or "").strip()
        location_payload = get_location_payload(values)
        mensaje_lower = normalize_user_text(mensaje_limpio)
        open_service = get_latest_search_service(cur, telefono)
        taken_service = get_latest_taken_service(cur, telefono)
        usuario_inicial = get_usuario_by_telefono(cur, telefono)
        invitation_row = cur.execute(
            "SELECT * FROM conversaciones WHERE telefono = ?",
            (telefono,),
        ).fetchone()

        if wants_invitation_code(mensaje_limpio):
            if not usuario_inicial:
                set_conversation(cur, telefono, "invite_codigo", fecha_actual)
                conn.commit()
                conn.close()
                return respond_client(telefono, build_invitation_required_message())
            code_row, code_error = create_client_code_for_phone(cur, telefono)
            if not code_row:
                conn.commit()
                conn.close()
                return respond_client(
                    telefono,
                    code_error or "No pudimos generar el codigo en este momento.",
                )
            conn.commit()
            conn.close()
            return respond_client(telefono, build_generated_invitation_message(code_row))

        if (
            not usuario_inicial
            and invitation_row
            and invitation_row["paso"] in {"invite_codigo", "invite_nombre"}
        ):
            if invitation_row["paso"] == "invite_codigo":
                code_text = extract_code_from_text(mensaje_limpio)
                code_row, code_error = reserve_code_for_phone(cur, code_text, telefono)
                if not code_row:
                    conn.commit()
                    conn.close()
                    return respond_client(
                        telefono,
                        f"{code_error}\n\n{build_invitation_required_message()}",
                    )
                inviter = cur.execute(
                    "SELECT nombre FROM clientes_confianza WHERE id = ?",
                    (code_row["creador_node_id"],),
                ).fetchone()
                cur.execute(
                    """
                    UPDATE conversaciones
                    SET paso = 'invite_nombre', servicio = ?, nombre = ?, updated_at = ?
                    WHERE telefono = ?
                    """,
                    (
                        code_row["codigo"],
                        inviter["nombre"] if inviter else "Zipp",
                        fecha_actual,
                        telefono,
                    ),
                )
                conn.commit()
                conn.close()
                return respond_client(
                    telefono,
                    build_invitation_verified_message(inviter["nombre"] if inviter else "Zipp"),
                )

            assistant = get_twilio_groq_assistant()
            candidate_name = assistant.extract_customer_name_agent(
                mensaje_limpio,
                conversation_step="nombre",
                known_name="",
            )
            if not candidate_name:
                conn.close()
                return respond_client(telefono, build_name_required_for_invitation_message())
            registration, registration_error = complete_registration_with_reserved_code(
                cur,
                telefono,
                candidate_name,
                invitation_row["servicio"],
            )
            if not registration:
                cur.execute("DELETE FROM conversaciones WHERE telefono = ?", (telefono,))
                conn.commit()
                conn.close()
                return respond_client(
                    telefono,
                    f"{registration_error}\n\n{build_invitation_required_message()}",
                )
            cur.execute("DELETE FROM conversaciones WHERE telefono = ?", (telefono,))
            conn.commit()
            conn.close()
            return respond_client(
                telefono,
                append_invitation_action(build_invitation_completed_message(candidate_name)),
            )

        if usuario_inicial and invitation_row and invitation_row["paso"] in {"invite_codigo", "invite_nombre"}:
            cur.execute("DELETE FROM conversaciones WHERE telefono = ?", (telefono,))
            conn.commit()

        if not usuario_inicial:
            code_text = extract_code_from_text(mensaje_limpio)
            if "ZIPP-" in mensaje_limpio.upper():
                code_row, code_error = reserve_code_for_phone(cur, code_text, telefono)
                if not code_row:
                    conn.commit()
                    conn.close()
                    return respond_client(
                        telefono,
                        f"{code_error}\n\n{build_invitation_required_message()}",
                    )
                inviter = cur.execute(
                    "SELECT nombre FROM clientes_confianza WHERE id = ?",
                    (code_row["creador_node_id"],),
                ).fetchone()
                set_conversation(
                    cur,
                    telefono,
                    "invite_nombre",
                    fecha_actual,
                    nombre=inviter["nombre"] if inviter else "Zipp",
                    servicio=code_row["codigo"],
                )
                conn.commit()
                conn.close()
                return respond_client(
                    telefono,
                    build_invitation_verified_message(inviter["nombre"] if inviter else "Zipp"),
                )
            set_conversation(cur, telefono, "invite_codigo", fecha_actual)
            conn.commit()
            conn.close()
            return respond_client(telefono, build_invitation_required_message())

        if taken_service and not open_service:
            cur.execute("DELETE FROM conversaciones WHERE telefono = ?", (telefono,))
            previous_client_message = cur.execute(
                """
                SELECT 1
                FROM chat_mensajes
                WHERE pedido_id = ? AND sender = 'cliente'
                LIMIT 1
                """,
                (taken_service["id"],),
            ).fetchone()
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
            if previous_client_message is None:
                return respond_client(
                    telefono,
                    "*Mensaje enviado.* Chat directo con tu conductor activo.",
                )
            return respond_client(telefono, "")

        if mensaje_lower in GREETING_KEYWORDS:
            if open_service:
                conn.close()
                return respond_client(
                    telefono,
                    build_open_service_reassurance_message(open_service),
                )

            usuario = get_usuario_by_telefono(cur, telefono)
            if usuario:
                respuesta = start_known_user_flow(
                    cur,
                    telefono,
                    usuario,
                    fecha_actual,
                    format_saved_address,
                    is_reserved_direccion,
                    mensaje_limpio,
                )
                conn.commit()
                conn.close()
                return respond_client(
                    telefono,
                    respuesta,
                    reply_sender=reply_sender,
                    buttons_key="location_saved_list",
                    buttons_variables={"1": respuesta},
                )

            respuesta = start_new_user_flow(
                cur,
                telefono,
                fecha_actual,
                original_text=mensaje_limpio,
            )
            conn.commit()
            conn.close()
            return respond_client(telefono, respuesta)

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
                return respond_client(telefono, "*Listo.* Cancelamos tu solicitud.")

            conversation_row = cur.execute(
                "SELECT 1 FROM conversaciones WHERE telefono = ?",
                (telefono,),
            ).fetchone()
            if conversation_row:
                cur.execute("DELETE FROM conversaciones WHERE telefono = ?", (telefono,))
                conn.commit()
                conn.close()
                return respond_client(telefono, "*Listo.* Cancelamos este proceso.")

            cur.execute("DELETE FROM conversaciones WHERE telefono = ?", (telefono,))
            conn.commit()
            conn.close()
            return respond_client(telefono, "*No tienes una solicitud activa.*")

        if mensaje_lower in {"salir", "reset"}:
            cur.execute("DELETE FROM conversaciones WHERE telefono = ?", (telefono,))
            conn.commit()
            conn.close()
            return respond_client(telefono, "*Listo.* Reiniciamos la conversacion.")

        row = cur.execute(
            "SELECT * FROM conversaciones WHERE telefono = ?",
            (telefono,),
        ).fetchone()

        if wants_location_help(mensaje_limpio) and not (
            row and row["paso"] in {"detalle_recogida", "guardar_ubicacion_confirm", "guardar_ubicacion_nombre", "confirmar_fuera_zona"}
        ):
            conn.close()
            help_text = build_map_only_help_message()
            return respond_client(
                telefono,
                help_text,
                reply_sender=reply_sender,
                buttons_key="location_help_steps",
                buttons_variables={"1": help_text},
            )

        if row is None:
            if open_service:
                if has_open_service_status_question(mensaje_limpio):
                    conn.close()
                    return respond_client(
                        telefono,
                        build_open_service_eta_message(open_service),
                    )

                assistant = get_twilio_groq_assistant()
                analysis = assistant.analyze_message(
                    telefono,
                    mensaje_limpio,
                    has_open_service=True,
                    location_payload=location_payload,
                    open_service_context=open_service["mensaje_cliente"] or "",
                )
                if debug_hook:
                    debug_hook({"stage": "open_service", "analysis": analysis})
                if is_open_service_soft_ping(mensaje_limpio):
                    conn.close()
                    return respond_client(
                        telefono,
                        build_open_service_reassurance_message(open_service),
                    )
                respuesta_abierta = analysis["reply"] or build_open_service_eta_message(
                    open_service
                )
                if has_open_service_status_question(mensaje_limpio) and (
                    "direccion de recogida" in normalize_user_text(respuesta_abierta)
                    or "ubicacion de recogida" in normalize_user_text(respuesta_abierta)
                ):
                    respuesta_abierta = build_open_service_eta_message(open_service)
                conn.close()
                return respond_client(telefono, respuesta_abierta)

            usuario = get_usuario_by_telefono(cur, telefono)

            if wants_location_management(mensaje_limpio) and usuario:
                respuesta = start_location_management_flow(
                    cur,
                    telefono,
                    fecha_actual,
                    usuario,
                    format_saved_address,
                    is_reserved_direccion,
                )
                conn.commit()
                conn.close()
                return respond_client(telefono, respuesta)

            if not mensaje_limpio and is_shared_location_payload(location_payload):
                if usuario and usuario["nombre"]:
                    set_conversation(
                        cur,
                        telefono,
                        "detalle_recogida",
                        fecha_actual,
                        nombre=usuario["nombre"],
                        meta=dump_meta_json(location_payload),
                    )
                    conn.commit()
                    conn.close()
                    return respond_client(
                        telefono,
                        build_location_reference_request_message(),
                    )
                respuesta = start_new_user_flow(
                    cur,
                    telefono,
                    fecha_actual,
                    direccion=location_payload["direccion"] or location_payload.get("coords", ""),
                    original_text=mensaje_limpio,
                    meta=dump_meta_json(location_payload),
                )
                conn.commit()
                conn.close()
                return respond_client(telefono, respuesta)

            respuesta_ai = handle_ai_preconversation(
                cur,
                telefono,
                mensaje_limpio,
                location_payload,
                fecha_actual,
                usuario,
                format_saved_address,
                is_reserved_direccion,
                parse_coords_from_text,
                debug_hook=debug_hook,
            )
            if respuesta_ai:
                conn.commit()
                conn.close()
                if respuesta_ai == build_out_of_zone_confirmation_message():
                    return respond_out_of_zone_confirmation(
                        telefono,
                        reply_sender=reply_sender,
                    )
                return respond_client(telefono, respuesta_ai)

            if usuario:
                respuesta = start_known_user_flow(
                    cur,
                    telefono,
                    usuario,
                    fecha_actual,
                    format_saved_address,
                    is_reserved_direccion,
                    mensaje_limpio,
                )
            else:
                respuesta = start_new_user_flow(
                    cur,
                    telefono,
                    fecha_actual,
                    original_text=mensaje_limpio,
                )
            conn.commit()
            conn.close()
            return respond_client(
                telefono,
                respuesta,
                reply_sender=reply_sender if usuario else None,
                buttons_key="location_saved_list" if usuario else "",
                buttons_variables={"1": respuesta} if usuario else None,
            )

        paso = row["paso"]
        row_meta = parse_meta_json(row["meta"] if "meta" in row.keys() else "")
        assistant = get_twilio_groq_assistant()

        if paso == "confirmar_fuera_zona":
            normalized_answer = normalize_user_text(mensaje_limpio)
            usuario = get_usuario_by_telefono(cur, telefono)
            usuario_id = usuario["id"] if usuario else None
            pending_request = row_meta.get("pending_request") or {}
            pending_payload = {
                "direccion": pending_request.get("direccion") or row["direccion"] or "",
                "raw_direccion": pending_request.get("raw_direccion") or row["direccion"] or "",
                "latitude": pending_request.get("latitude") or "",
                "longitude": pending_request.get("longitude") or "",
                "coords": pending_request.get("coords") or "",
                "pickup_note": pending_request.get("pickup_note") or pending_request.get("direccion") or "",
                "label": pending_request.get("label") or "",
                "service_type": pending_request.get("service_type") or INTERMUNICIPAL_LABEL,
            }

            if normalized_answer in OUT_OF_ZONE_ACCEPT_KEYWORDS:
                create_pending_request(
                    cur,
                    telefono,
                    row["nombre"] or (usuario["nombre"] if usuario else ""),
                    pending_payload["raw_direccion"] or pending_payload["direccion"],
                    pending_payload,
                    fecha_actual,
                    usuario_id,
                    parse_coords_from_text,
                    display_label=pending_payload["label"] or pending_payload["direccion"] or "Ubicacion compartida",
                )
                conn.commit()
                conn.close()
                return respond_client(
                    telefono,
                    build_request_message(
                        row["nombre"] or (usuario["nombre"] if usuario else "")
                    ),
                )

            if normalized_answer in OUT_OF_ZONE_DECLINE_KEYWORDS:
                nombre_cliente = row["nombre"] or (usuario["nombre"] if usuario else "")
                set_conversation(
                    cur,
                    telefono,
                    "direccion",
                    fecha_actual,
                    nombre=nombre_cliente,
                )
                direcciones = get_direcciones(
                    cur,
                    usuario_id,
                    is_reserved_direccion,
                )
                respuesta = build_location_prompt_with_saved_options(
                    nombre_cliente,
                    direcciones,
                    format_saved_address,
                )
                conn.commit()
                conn.close()
                return respond_client(
                    telefono,
                    f"*Listo.* Enviame otra ubicacion.\n{respuesta}",
                    reply_sender=reply_sender if usuario else None,
                    buttons_key="location_saved_list" if usuario else "",
                    buttons_variables={"1": respuesta} if usuario else None,
                )

            conn.close()
            return respond_out_of_zone_confirmation(
                telefono,
                reply_sender=reply_sender,
            )

        if paso == "guardar_ubicacion_confirm":
            if normalize_user_text(mensaje_limpio) in {"si", "guardar"}:
                cur.execute(
                    """
                    UPDATE conversaciones
                    SET paso = 'guardar_ubicacion_nombre', updated_at = ?
                    WHERE telefono = ?
                    """,
                    (fecha_actual, telefono),
                )
                conn.commit()
                conn.close()
                return respond_client(telefono, build_location_label_request())

            if normalize_user_text(mensaje_limpio) in {"no", "no guardar"}:
                pending_payload = {
                    "direccion": row["direccion"] or row_meta.get("direccion") or "",
                    "raw_direccion": row_meta.get("raw_direccion") or row["direccion"] or "",
                    "latitude": row_meta.get("latitude") or "",
                    "longitude": row_meta.get("longitude") or "",
                    "coords": row_meta.get("coords") or "",
                    "pickup_note": row["direccion"] or "",
                    "label": "",
                }
                usuario = get_usuario_by_telefono(cur, telefono)
                usuario_id = usuario["id"] if usuario else None
                out_of_zone_message = queue_pending_request_with_zone_check(
                    cur,
                    telefono,
                    row["nombre"] or (usuario["nombre"] if usuario else ""),
                    pending_payload["raw_direccion"] or pending_payload["direccion"],
                    pending_payload,
                    fecha_actual,
                    usuario_id,
                    parse_coords_from_text,
                    display_label=pending_payload["direccion"] or "Ubicacion compartida",
                )
                conn.commit()
                conn.close()
                if out_of_zone_message:
                    return respond_out_of_zone_confirmation(
                        telefono,
                        reply_sender=reply_sender,
                    )
                return respond_client(
                    telefono,
                    build_request_message(row["nombre"] or (usuario["nombre"] if usuario else "")),
                )

            conn.close()
            return respond_client(
                telefono,
                "Responde *SI* si deseas guardarla o *NO* para continuar.",
            )

        if paso == "guardar_ubicacion_nombre":
            etiqueta = (mensaje_limpio or "").strip().strip(".,;:!?")
            if not etiqueta or len(etiqueta) > 50:
                conn.close()
                return respond_client(
                    telefono,
                    "*Escribe un nombre corto.* Ejemplo: *Casa* o *Oficina*.",
                )

            usuario = get_usuario_by_telefono(cur, telefono)
            usuario_id = usuario["id"] if usuario else None
            saved_reference = (row["direccion"] or "").strip()
            saved_address = saved_reference or row_meta.get("raw_direccion") or etiqueta
            pending_payload = {
                "direccion": saved_reference or etiqueta,
                "raw_direccion": saved_address,
                "latitude": row_meta.get("latitude") or "",
                "longitude": row_meta.get("longitude") or "",
                "coords": row_meta.get("coords") or "",
                "pickup_note": saved_reference,
                "label": etiqueta,
            }
            persist_new_address(
                cur,
                usuario_id,
                saved_address,
                fecha_actual,
                etiqueta=etiqueta,
                latitude=pending_payload["latitude"],
                longitude=pending_payload["longitude"],
            )
            out_of_zone_message = queue_pending_request_with_zone_check(
                cur,
                telefono,
                row["nombre"] or (usuario["nombre"] if usuario else ""),
                pending_payload["raw_direccion"] or row["direccion"] or etiqueta,
                pending_payload,
                fecha_actual,
                usuario_id,
                parse_coords_from_text,
                display_label=etiqueta,
            )
            conn.commit()
            conn.close()
            if out_of_zone_message:
                return respond_out_of_zone_confirmation(
                    telefono,
                    reply_sender=reply_sender,
                )
            return respond_client(
                telefono,
                f"{build_location_saved_message(etiqueta)}\n{build_request_message_for_saved_location(row['nombre'] or (usuario['nombre'] if usuario else ''), etiqueta)}",
            )

        if paso == "editar_ubicaciones":
            usuario = get_usuario_by_telefono(cur, telefono)
            usuario_id = usuario["id"] if usuario else None
            direcciones = get_direcciones(cur, usuario_id, is_reserved_direccion)
            if wants_new_location(mensaje_limpio):
                cur.execute(
                    """
                    UPDATE conversaciones
                    SET paso = 'direccion', updated_at = ?
                    WHERE telefono = ?
                    """,
                    (fecha_actual, telefono),
                )
                conn.commit()
                conn.close()
                nueva_texto = build_new_location_prompt_message()
                return respond_client(
                    telefono,
                    nueva_texto,
                    reply_sender=reply_sender,
                    buttons_key="location_new_prompt",
                    buttons_variables={"1": nueva_texto},
                )
            choice = extract_saved_address_choice(mensaje_limpio)
            selected_row = resolve_saved_address_row(direcciones, choice)
            if not selected_row:
                conn.close()
                return respond_client(
                    telefono,
                    build_location_manage_intro(direcciones, format_saved_address),
                    reply_sender=reply_sender,
                    buttons_key="location_saved_list",
                    buttons_variables={"1": build_location_manage_intro(direcciones, format_saved_address)},
                )
            cur.execute(
                """
                UPDATE conversaciones
                SET paso = 'editar_ubicacion_accion', servicio = ?, updated_at = ?
                WHERE telefono = ?
                """,
                (str(selected_row["id"]), fecha_actual, telefono),
            )
            conn.commit()
            conn.close()
            return respond_client(
                telefono,
                build_location_manage_actions(selected_row, choice, format_saved_address),
                reply_sender=reply_sender,
                buttons_key="location_manage_action",
                buttons_variables={"1": build_location_manage_actions(selected_row, choice, format_saved_address)},
            )

        if paso == "editar_ubicacion_accion":
            usuario = get_usuario_by_telefono(cur, telefono)
            usuario_id = usuario["id"] if usuario else None
            direcciones = get_direcciones(cur, usuario_id, is_reserved_direccion)
            selected_id = int(row["servicio"] or "0") if (row["servicio"] or "0").isdigit() else 0
            selected_row = None
            selected_index = 0
            for idx, saved_row in enumerate(direcciones, start=1):
                if saved_row["id"] == selected_id:
                    selected_row = saved_row
                    selected_index = idx
                    break
            if selected_row is None:
                respuesta = start_location_management_flow(
                    cur,
                    telefono,
                    fecha_actual,
                    usuario,
                    format_saved_address,
                    is_reserved_direccion,
                )
                conn.commit()
                conn.close()
                return respond_client(telefono, respuesta)
                

            accion = normalize_user_text(mensaje_limpio)
            if accion == "eliminar":
                label = get_saved_location_display(selected_row, selected_index, format_saved_address)
                cur.execute("DELETE FROM direcciones WHERE id = ?", (selected_id,))
                respuesta = (
                    f"{build_location_deleted_message(label)}\n"
                    f"{start_known_user_flow(cur, telefono, usuario, fecha_actual, format_saved_address, is_reserved_direccion)}"
                )
                conn.commit()
                conn.close()
                return respond_client(telefono, respuesta)

            if accion in {"renombrar", "cambiar nombre", "editar"}:
                cur.execute(
                    """
                    UPDATE conversaciones
                    SET paso = 'editar_ubicacion_nombre', updated_at = ?
                    WHERE telefono = ?
                    """,
                    (fecha_actual, telefono),
                )
                conn.commit()
                conn.close()
                return respond_client(
                    telefono,
                    build_location_rename_prompt(selected_row, selected_index, format_saved_address),
                )

            conn.close()
            return respond_client(
                telefono,
                build_location_manage_actions(selected_row, selected_index, format_saved_address),
                reply_sender=reply_sender,
                buttons_key="location_manage_action",
                buttons_variables={"1": build_location_manage_actions(selected_row, selected_index, format_saved_address)},
            )

        if paso == "editar_ubicacion_nombre":
            usuario = get_usuario_by_telefono(cur, telefono)
            usuario_id = usuario["id"] if usuario else None
            etiqueta = (mensaje_limpio or "").strip().strip(".,;:!?")
            if not etiqueta or len(etiqueta) > 50:
                conn.close()
                return respond_client(
                    telefono,
                    "*Escribe un nombre corto.* Ejemplo: *Casa* o *Oficina*.",
                )
            selected_id = int(row["servicio"] or "0") if (row["servicio"] or "0").isdigit() else 0
            cur.execute(
                """
                UPDATE direcciones
                SET etiqueta = ?, updated_at = ?
                WHERE id = ? AND usuario_id = ?
                """,
                (etiqueta, fecha_actual, selected_id, usuario_id),
            )
            respuesta = (
                f"{build_location_renamed_message(etiqueta)}\n"
                f"{start_known_user_flow(cur, telefono, usuario, fecha_actual, format_saved_address, is_reserved_direccion)}"
            )
            conn.commit()
            conn.close()
            return respond_client(telefono, respuesta)

        if paso == "faq":
            usuario = get_usuario_by_telefono(cur, telefono)
            nombre = ""
            usuario_id = None
            if usuario and usuario["nombre"]:
                nombre = usuario["nombre"]
                usuario_id = usuario["id"]
            elif row["nombre"]:
                nombre = row["nombre"]

            direcciones = get_direcciones(cur, usuario_id, is_reserved_direccion)
            analysis = assistant.analyze_message(
                telefono,
                mensaje_limpio,
                known_name=nombre,
                saved_addresses=direcciones,
                location_payload=location_payload,
            )
            if debug_hook:
                debug_hook({"stage": "faq_followup", "analysis": analysis})

            if analysis["intent"] == "faq" and analysis["reply"]:
                conn.commit()
                conn.close()
                return respond_client(
                    telefono,
                    build_named_support_reply(nombre, analysis["reply"]),
                )

            if analysis["intent"] == "new_service":
                if (
                    analysis["pickup_address"]
                    and not analysis["saved_address_index"]
                    and not is_shared_location_payload(location_payload)
                ):
                    rejection_text = build_map_only_rejection_message()
                    conn.close()
                    return respond_client(
                        telefono,
                        rejection_text,
                        reply_sender=reply_sender,
                        buttons_key="location_help_offer",
                        buttons_variables={"1": rejection_text},
                    )

                nombre_detectado = analysis["customer_name"] or nombre
                direccion_row = resolve_saved_address_row(
                    direcciones, analysis["saved_address_index"]
                )
                direccion = (
                    analysis["pickup_address"]
                    or (direccion_row["direccion"] if direccion_row else "")
                )

                if not direccion and is_shared_location_payload(location_payload):
                    direccion = location_payload["direccion"] or location_payload.get("coords", "")

                if nombre_detectado and direccion:
                    if usuario is None:
                        usuario_id = upsert_usuario(
                            cur, telefono, nombre_detectado, fecha_actual
                        )
                    if is_shared_location_payload(location_payload):
                        cur.execute(
                            """
                            UPDATE conversaciones
                            SET paso = 'detalle_recogida', nombre = ?, direccion = '', meta = ?, updated_at = ?
                            WHERE telefono = ?
                            """,
                            (
                                nombre_detectado,
                                dump_meta_json(location_payload),
                                fecha_actual,
                                telefono,
                            ),
                        )
                        conn.commit()
                        conn.close()
                        return respond_client(
                            telefono,
                            build_location_reference_request_message(),
                        )

                    if direccion_row:
                        saved_payload = build_saved_location_payload(direccion_row)
                        out_of_zone_message = queue_pending_request_with_zone_check(
                            cur,
                            telefono,
                            nombre_detectado,
                            saved_payload["raw_direccion"] or saved_payload["direccion"],
                            saved_payload,
                            fecha_actual,
                            usuario_id,
                            parse_coords_from_text,
                            display_label=get_saved_location_display(
                                direccion_row,
                                analysis["saved_address_index"],
                                format_saved_address,
                            ),
                        )
                        conn.commit()
                        conn.close()
                        if out_of_zone_message:
                            return respond_out_of_zone_confirmation(
                                telefono,
                                reply_sender=reply_sender,
                            )
                        return respond_client(
                            telefono,
                            build_request_message_for_saved_location(
                                nombre_detectado,
                                get_saved_location_display(
                                    direccion_row,
                                    analysis["saved_address_index"],
                                    format_saved_address,
                                ),
                            ),
                        )

                    conn.close()
                    return respond_client(
                        telefono,
                        build_map_only_rejection_message(),
                        reply_sender=reply_sender,
                        buttons_key="location_help_offer",
                    )

                if nombre_detectado:
                    if usuario is None:
                        usuario_id = upsert_usuario(
                            cur, telefono, nombre_detectado, fecha_actual
                        )
                    cur.execute(
                        """
                        UPDATE conversaciones
                        SET nombre = ?, paso = 'direccion', updated_at = ?
                        WHERE telefono = ?
                        """,
                        (nombre_detectado, fecha_actual, telefono),
                    )
                    direcciones = get_direcciones(
                        cur, usuario_id, is_reserved_direccion
                    )
                    respuesta_texto = build_name_ack_message(nombre_detectado)
                    lista = build_direcciones_prompt(
                        direcciones, format_saved_address
                    )
                    if lista:
                        respuesta_texto = f"{respuesta_texto}\n{lista}"
                    conn.commit()
                    conn.close()
                    return respond_client(
                        telefono,
                        respuesta_texto,
                        reply_sender=reply_sender,
                        buttons_key="location_saved_list",
                        buttons_variables={"1": respuesta_texto},
                    )

                if direccion:
                    cur.execute(
                        """
                        UPDATE conversaciones
                        SET direccion = ?, paso = 'nombre', meta = ?, updated_at = ?
                        WHERE telefono = ?
                        """,
                        (
                            direccion,
                            dump_meta_json(location_payload) if is_shared_location_payload(location_payload) else "",
                            fecha_actual,
                            telefono,
                        ),
                    )
                    conn.commit()
                    conn.close()
                    return respond_client(
                        telefono,
                        build_address_ack_name_request_message(),
                    )

                cur.execute(
                    """
                    UPDATE conversaciones
                    SET paso = 'nombre', updated_at = ?
                    WHERE telefono = ?
                    """,
                    (fecha_actual, telefono),
                )
                conn.commit()
                conn.close()
                return respond_client(
                    telefono,
                    build_continue_service_name_request_message(),
                )

            if analysis["reply"]:
                conn.commit()
                conn.close()
                return respond_client(
                    telefono,
                    build_named_support_reply(nombre, analysis["reply"]),
                )

            conn.commit()
            conn.close()
            fallback_text = (
                build_location_prompt_with_saved_options(
                    nombre,
                    direcciones,
                    format_saved_address,
                )
                if nombre
                else "Con gusto.\n*Enviame tu ubicacion actual* o cuentame tu nombre."
            )
            return respond_client(
                telefono,
                fallback_text,
            )

        if paso == "nombre":
            if wants_location_help(mensaje_limpio):
                conn.close()
                return respond_client(telefono, build_map_only_help_message())

            candidate_name = assistant.extract_customer_name_agent(
                mensaje_limpio,
                conversation_step="nombre",
                known_name=row["nombre"] or "",
            )
            analysis = None
            if mensaje_limpio:
                analysis = assistant.analyze_message(
                    telefono,
                    mensaje_limpio,
                    conversation_step="nombre",
                    location_payload=location_payload,
                    known_name=row["nombre"] or "",
                )
                if debug_hook:
                    debug_hook({"stage": "nombre", "analysis": analysis})
                if not candidate_name:
                    candidate_name = analysis["customer_name"]
                if not candidate_name and analysis["intent"] == "faq" and analysis["reply"]:
                    conn.close()
                    if row["nombre"]:
                        return respond_client(
                            telefono,
                            build_named_support_reply(
                                row["nombre"], analysis["reply"]
                            ),
                        )
                    return respond_client(
                        telefono,
                        build_new_user_reply_message(
                            mensaje_limpio, analysis["reply"]
                        ),
                    )

            if not candidate_name:
                conn.close()
                return respond_client(
                    telefono,
                    build_missing_name_message(mensaje_limpio),
                )

            usuario_id = upsert_usuario(cur, telefono, candidate_name, fecha_actual)
            direccion_pendiente = (row["direccion"] or "").strip()
            pending_location = parse_meta_json(row["meta"] if "meta" in row.keys() else "")

            if direccion_pendiente and pending_location:
                cur.execute(
                    """
                    UPDATE conversaciones
                    SET nombre = ?, paso = 'guardar_ubicacion_confirm', updated_at = ?
                    WHERE telefono = ?
                    """,
                    (candidate_name, fecha_actual, telefono),
                )
                conn.commit()
                conn.close()
                return respond_client(
                    telefono,
                    build_save_location_question(),
                    reply_sender=reply_sender,
                    buttons_key="save_location_confirm",
                    buttons_variables={"1": build_save_location_question()},
                )

            cur.execute(
                """
                UPDATE conversaciones
                SET nombre = ?, paso = 'direccion', updated_at = ?
                WHERE telefono = ?
                """,
                (candidate_name, fecha_actual, telefono),
            )
            direcciones = get_direcciones(cur, usuario_id, is_reserved_direccion)
            respuesta_texto = build_name_ack_message(candidate_name)
            lista = build_direcciones_prompt(direcciones, format_saved_address)
            if lista:
                respuesta_texto = f"{respuesta_texto}\n{lista}"
            conn.commit()
            conn.close()
            return respond_client(
                telefono,
                respuesta_texto,
                reply_sender=reply_sender,
                buttons_key="location_saved_list",
                buttons_variables={"1": respuesta_texto},
            )

        if paso == "detalle_recogida":
            if is_shared_location_payload(location_payload):
                cur.execute(
                    """
                    UPDATE conversaciones
                    SET meta = ?, updated_at = ?
                    WHERE telefono = ?
                    """,
                    (dump_meta_json(location_payload), fecha_actual, telefono),
                )
                conn.commit()
                conn.close()
                return respond_client(
                    telefono,
                    build_location_reference_request_message(),
                )

            referencia = (mensaje_limpio or "").strip().strip(".,")
            if not referencia:
                conn.close()
                return respond_client(
                    telefono,
                    build_location_reference_request_message(),
                )

            if len(referencia) > 140:
                conn.close()
                return respond_client(
                    telefono,
                    "*Escribeme una referencia corta.* Ejemplo: barrio, unidad, torre o apartamento.",
                )

            if row["nombre"]:
                cur.execute(
                    """
                    UPDATE conversaciones
                    SET direccion = ?, paso = 'guardar_ubicacion_confirm', updated_at = ?
                    WHERE telefono = ?
                    """,
                    (referencia, fecha_actual, telefono),
                )
                conn.commit()
                conn.close()
                return respond_client(
                    telefono,
                    build_save_location_question(),
                    reply_sender=reply_sender,
                    buttons_key="save_location_confirm",
                    buttons_variables={"1": build_save_location_question()},
                )

            cur.execute(
                """
                UPDATE conversaciones
                SET direccion = ?, paso = 'nombre', updated_at = ?
                WHERE telefono = ?
                """,
                (referencia, fecha_actual, telefono),
            )
            conn.commit()
            conn.close()
            return respond_client(
                telefono,
                build_reference_ack_name_request_message(),
            )

        if paso == "direccion":
            if wants_location_help(mensaje_limpio):
                conn.close()
                help_text = build_map_only_help_message()
                return respond_client(
                    telefono,
                    help_text,
                    reply_sender=reply_sender,
                    buttons_key="location_help_steps",
                    buttons_variables={"1": help_text},
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

            direcciones = get_direcciones(cur, usuario_id, is_reserved_direccion)

            if wants_location_management(mensaje_limpio) and usuario:
                respuesta = start_location_management_flow(
                    cur,
                    telefono,
                    fecha_actual,
                    usuario,
                    format_saved_address,
                    is_reserved_direccion,
                )
                conn.commit()
                conn.close()
                return respond_client(
                    telefono,
                    respuesta,
                    reply_sender=reply_sender,
                    buttons_key="location_saved_list",
                    buttons_variables={"1": respuesta},
                )

            if wants_new_location(mensaje_limpio):
                conn.close()
                nueva_texto = build_new_location_prompt_message()
                return respond_client(
                    telefono,
                    nueva_texto,
                    reply_sender=reply_sender,
                    buttons_key="location_new_prompt",
                    buttons_variables={"1": nueva_texto},
                )

            if wants_saved_locations_list(mensaje_limpio):
                lista = build_direcciones_prompt(direcciones, format_saved_address)
                lista_texto = (
                    lista
                    or "*Aun no tienes ubicaciones guardadas.*\n*Enviame tu ubicacion actual.*"
                )
                conn.close()
                return respond_client(
                    telefono,
                    lista_texto,
                    reply_sender=reply_sender,
                    buttons_key="location_saved_list",
                    buttons_variables={"1": lista_texto},
                )

            if is_shared_location_payload(location_payload):
                cur.execute(
                    """
                    UPDATE conversaciones
                    SET paso = 'detalle_recogida', direccion = '', meta = ?, updated_at = ?
                    WHERE telefono = ?
                    """,
                    (
                        dump_meta_json(location_payload),
                        fecha_actual,
                        telefono,
                    ),
                )
                conn.commit()
                conn.close()
                return respond_client(
                    telefono,
                    build_location_reference_request_message(),
                )

            if not mensaje_limpio:
                prompt_text = build_location_prompt_with_saved_options(
                    nombre,
                    direcciones,
                    format_saved_address,
                )
                conn.close()
                return respond_client(
                    telefono,
                    prompt_text,
                    reply_sender=reply_sender,
                    buttons_key="location_saved_list",
                    buttons_variables={"1": prompt_text},
                )

            if is_reserved_direccion(mensaje_limpio):
                conn.close()
                return respond_client(
                    telefono,
                    "*Listo.* Enviame tu nueva ubicacion actual.",
                )

            analysis = assistant.analyze_message(
                telefono,
                mensaje_limpio,
                conversation_step="direccion",
                known_name=nombre,
                saved_addresses=direcciones,
                location_payload=location_payload,
            )
            if debug_hook:
                debug_hook({"stage": "direccion", "analysis": analysis})
            if analysis["intent"] == "faq" and analysis["reply"]:
                conn.close()
                return respond_client(
                    telefono,
                    build_named_support_reply(nombre, analysis["reply"]),
                )

            choice = extract_saved_address_choice(mensaje_limpio)
            direccion_row = resolve_saved_address_row(direcciones, choice)
            direccion = direccion_row["direccion"] if direccion_row else ""

            if not direccion:
                direccion = analysis["pickup_address"] or resolve_saved_address(
                    direcciones, analysis["saved_address_index"]
                )

            if direccion_row:
                saved_payload = build_saved_location_payload(direccion_row)
                selected_label = get_saved_location_display(
                    direccion_row, choice, format_saved_address
                )
                out_of_zone_message = queue_pending_request_with_zone_check(
                    cur,
                    telefono,
                    nombre,
                    saved_payload["raw_direccion"] or saved_payload["direccion"],
                    saved_payload,
                    fecha_actual,
                    usuario_id,
                    parse_coords_from_text,
                    display_label=selected_label,
                )
                conn.commit()
                conn.close()
                if out_of_zone_message:
                    return respond_out_of_zone_confirmation(
                        telefono,
                        reply_sender=reply_sender,
                    )
                return respond_client(
                    telefono,
                    build_request_message_for_saved_location(nombre, selected_label),
                )

            if analysis["saved_address_index"]:
                direccion_row = resolve_saved_address_row(
                    direcciones, analysis["saved_address_index"]
                )
                if direccion_row:
                    saved_payload = build_saved_location_payload(direccion_row)
                    selected_label = get_saved_location_display(
                        direccion_row,
                        analysis["saved_address_index"],
                        format_saved_address,
                    )
                    out_of_zone_message = queue_pending_request_with_zone_check(
                        cur,
                        telefono,
                        nombre,
                        saved_payload["raw_direccion"] or saved_payload["direccion"],
                        saved_payload,
                        fecha_actual,
                        usuario_id,
                        parse_coords_from_text,
                        display_label=selected_label,
                    )
                    conn.commit()
                    conn.close()
                    if out_of_zone_message:
                        return respond_out_of_zone_confirmation(
                            telefono,
                            reply_sender=reply_sender,
                        )
                    return respond_client(
                        telefono,
                        build_request_message_for_saved_location(nombre, selected_label),
                    )

            if looks_like_address(mensaje_limpio) and not is_shared_location_payload(location_payload):
                rejection_text = build_map_only_rejection_message()
                conn.close()
                return respond_client(
                    telefono,
                    rejection_text,
                    reply_sender=reply_sender,
                    buttons_key="location_help_offer",
                    buttons_variables={"1": rejection_text},
                )

            required_text = build_location_prompt_with_saved_options(
                nombre,
                direcciones,
                format_saved_address,
            )
            conn.close()
            return respond_client(
                telefono,
                required_text,
                reply_sender=reply_sender,
                buttons_key="location_saved_list",
                buttons_variables={"1": required_text},
            )

        cur.execute("DELETE FROM conversaciones WHERE telefono = ?", (telefono,))
        conn.commit()
        conn.close()
        return respond_client(telefono, "*Vamos de nuevo.* Que servicio deseas?")
    except Exception as exc:
        print(f"Error BD: {exc}")
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass
        return respond_client(
            telefono,
            "Hubo un inconveniente.\n*Escribeme de nuevo* y te ayudo.",
        )

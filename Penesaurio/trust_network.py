import secrets
from datetime import datetime, timedelta, timezone

try:
    from zoneinfo import ZoneInfo
except Exception:
    ZoneInfo = None


ADMIN_ROOT_PHONE = "zipp:admin-root"
ADMIN_ROOT_NAME = "Nodo P Zipp"
CODE_PREFIX = "ZIPP"
CLIENT_INVITE_HOURS = 2
LEADER_INVITE_HOURS = 1
try:
    BOGOTA_TZ = ZoneInfo("America/Bogota") if ZoneInfo else timezone(timedelta(hours=-5))
except Exception:
    BOGOTA_TZ = timezone(timedelta(hours=-5))


def now_local():
    return datetime.now(BOGOTA_TZ).replace(tzinfo=None)


def now_text():
    return now_local().strftime("%Y-%m-%d %H:%M:%S")


def parse_datetime(value):
    text = (value or "").strip()
    if not text:
        return None
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
        try:
            return datetime.strptime(text, fmt)
        except ValueError:
            continue
    return None


def format_expiration(value):
    parsed = parse_datetime(value) if isinstance(value, str) else value
    if not parsed:
        return ""
    return parsed.strftime("%I:%M %p").lower().replace("am", "a. m.").replace("pm", "p. m.")


def normalize_code(value):
    text = (value or "").strip().upper().replace(" ", "")
    if text and not text.startswith(f"{CODE_PREFIX}-") and len(text) >= 4:
        text = f"{CODE_PREFIX}-{text}"
    return text


def extract_code_from_text(value):
    text = (value or "").upper()
    marker = f"{CODE_PREFIX}-"
    if marker in text:
        start = text.find(marker)
        token = []
        for char in text[start:]:
            if char.isalnum() or char == "-":
                token.append(char)
            elif token:
                break
        return normalize_code("".join(token))
    return normalize_code(text)


def init_trust_schema(conn):
    cur = conn.cursor()
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS clientes_confianza (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            usuario_id INTEGER,
            nombre TEXT NOT NULL,
            telefono TEXT UNIQUE NOT NULL,
            padre_id INTEGER,
            estado TEXT NOT NULL DEFAULT 'activo',
            es_admin INTEGER DEFAULT 0,
            creado_en TEXT,
            updated_at TEXT,
            FOREIGN KEY (usuario_id) REFERENCES usuarios(id),
            FOREIGN KEY (padre_id) REFERENCES clientes_confianza(id)
        )
        """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS codigos_confianza (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            codigo TEXT UNIQUE NOT NULL,
            tipo TEXT NOT NULL,
            estado TEXT NOT NULL DEFAULT 'disponible',
            creador_node_id INTEGER,
            usado_por_node_id INTEGER,
            reservado_telefono TEXT,
            creado_en TEXT,
            expira_en TEXT NOT NULL,
            reservado_en TEXT,
            usado_en TEXT,
            FOREIGN KEY (creador_node_id) REFERENCES clientes_confianza(id),
            FOREIGN KEY (usado_por_node_id) REFERENCES clientes_confianza(id)
        )
        """
    )
    cur.execute(
        "CREATE INDEX IF NOT EXISTS idx_clientes_confianza_padre ON clientes_confianza(padre_id)"
    )
    cur.execute(
        "CREATE INDEX IF NOT EXISTS idx_codigos_confianza_estado ON codigos_confianza(estado, expira_en)"
    )
    ensure_admin_root(cur)


def ensure_admin_root(cur):
    current = cur.execute(
        "SELECT * FROM clientes_confianza WHERE telefono = ?",
        (ADMIN_ROOT_PHONE,),
    ).fetchone()
    if current:
        return current
    stamp = now_text()
    cur.execute(
        """
        INSERT INTO clientes_confianza (
            usuario_id, nombre, telefono, padre_id, estado, es_admin, creado_en, updated_at
        )
        VALUES (NULL, ?, ?, NULL, 'activo', 1, ?, ?)
        """,
        (ADMIN_ROOT_NAME, ADMIN_ROOT_PHONE, stamp, stamp),
    )
    return cur.execute(
        "SELECT * FROM clientes_confianza WHERE telefono = ?",
        (ADMIN_ROOT_PHONE,),
    ).fetchone()


def expire_unused_codes(cur):
    stamp = now_text()
    cur.execute(
        """
        UPDATE codigos_confianza
        SET estado = 'expirado'
        WHERE estado IN ('disponible', 'reservado')
          AND expira_en <= ?
        """,
        (stamp,),
    )


def code_seconds_left(row):
    if not row:
        return 0
    expira_en = parse_datetime(row["expira_en"])
    if not expira_en:
        return 0
    return max(0, int((expira_en - now_local()).total_seconds()))


def make_invite_code(cur):
    alphabet = "ABCDEFGHJKLMNPQRSTUVWXYZ23456789"
    while True:
        token = "".join(secrets.choice(alphabet) for _ in range(8))
        code = f"{CODE_PREFIX}-{token}"
        exists = cur.execute(
            "SELECT 1 FROM codigos_confianza WHERE codigo = ?",
            (code,),
        ).fetchone()
        if not exists:
            return code


def get_trust_node_by_phone(cur, telefono):
    return cur.execute(
        "SELECT * FROM clientes_confianza WHERE telefono = ?",
        (telefono,),
    ).fetchone()


def get_or_create_node_for_usuario(cur, usuario, parent_id=None):
    node = get_trust_node_by_phone(cur, usuario["telefono"])
    if node:
        return node
    if parent_id is None:
        parent_id = ensure_admin_root(cur)["id"]
    stamp = now_text()
    cur.execute(
        """
        INSERT INTO clientes_confianza (
            usuario_id, nombre, telefono, padre_id, estado, es_admin, creado_en, updated_at
        )
        VALUES (?, ?, ?, ?, 'activo', 0, ?, ?)
        """,
        (usuario["id"], usuario["nombre"], usuario["telefono"], parent_id, stamp, stamp),
    )
    return get_trust_node_by_phone(cur, usuario["telefono"])


def create_code(cur, tipo, creador_node_id, hours):
    expire_unused_codes(cur)
    stamp = now_text()
    expira_en = (now_local() + timedelta(hours=hours)).strftime("%Y-%m-%d %H:%M:%S")
    code = make_invite_code(cur)
    cur.execute(
        """
        INSERT INTO codigos_confianza (
            codigo, tipo, estado, creador_node_id, creado_en, expira_en
        )
        VALUES (?, ?, 'disponible', ?, ?, ?)
        """,
        (code, tipo, creador_node_id, stamp, expira_en),
    )
    return cur.execute(
        "SELECT * FROM codigos_confianza WHERE codigo = ?",
        (code,),
    ).fetchone()


def create_leader_code(cur):
    root = ensure_admin_root(cur)
    return create_code(cur, "lider", root["id"], LEADER_INVITE_HOURS)


def create_client_code_for_phone(cur, telefono):
    usuario = cur.execute("SELECT * FROM usuarios WHERE telefono = ?", (telefono,)).fetchone()
    if not usuario:
        return None, "Solo los clientes registrados pueden generar codigos de invitacion."
    node = get_or_create_node_for_usuario(cur, usuario)
    return create_code(cur, "cliente", node["id"], CLIENT_INVITE_HOURS), ""


def reserve_code_for_phone(cur, codigo, telefono):
    expire_unused_codes(cur)
    normalized = normalize_code(codigo)
    code = cur.execute(
        "SELECT * FROM codigos_confianza WHERE codigo = ?",
        (normalized,),
    ).fetchone()
    if not code:
        return None, "No encontramos ese codigo de invitacion. Verifica que este escrito completo."
    if code["estado"] == "usado":
        return None, "Ese codigo ya fue utilizado y quedo invalidado. Pidele a tu contacto que genere uno nuevo."
    if code["estado"] == "expirado" or code_seconds_left(code) <= 0:
        cur.execute(
            "UPDATE codigos_confianza SET estado = 'expirado' WHERE id = ?",
            (code["id"],),
        )
        return None, "Ese codigo ya expiro. Pidele a tu contacto que genere uno nuevo."
    if code["estado"] == "reservado" and code["reservado_telefono"] != telefono:
        return None, "Ese codigo ya esta en proceso de registro y no puede volver a usarse."
    if code["estado"] == "disponible":
        cur.execute(
            """
            UPDATE codigos_confianza
            SET estado = 'reservado', reservado_telefono = ?, reservado_en = ?
            WHERE id = ?
            """,
            (telefono, now_text(), code["id"]),
        )
    return cur.execute(
        "SELECT * FROM codigos_confianza WHERE id = ?",
        (code["id"],),
    ).fetchone(), ""


def complete_registration_with_reserved_code(cur, telefono, nombre, codigo):
    code = cur.execute(
        "SELECT * FROM codigos_confianza WHERE codigo = ?",
        (normalize_code(codigo),),
    ).fetchone()
    if not code:
        return None, "No encontramos el codigo de invitacion."
    if code["estado"] == "usado":
        return None, "Ese codigo ya fue utilizado y quedo invalidado."
    if code["estado"] == "expirado" or code_seconds_left(code) <= 0:
        cur.execute(
            "UPDATE codigos_confianza SET estado = 'expirado' WHERE id = ?",
            (code["id"],),
        )
        return None, "Ese codigo ya expiro. Pidele a tu contacto que genere uno nuevo."
    if code["estado"] == "reservado" and code["reservado_telefono"] != telefono:
        return None, "Ese codigo ya esta reservado para otro registro."

    parent_id = code["creador_node_id"] or ensure_admin_root(cur)["id"]
    inviter = cur.execute(
        "SELECT * FROM clientes_confianza WHERE id = ?",
        (parent_id,),
    ).fetchone()
    if not inviter:
        parent_id = ensure_admin_root(cur)["id"]

    stamp = now_text()
    usuario = cur.execute("SELECT * FROM usuarios WHERE telefono = ?", (telefono,)).fetchone()
    if usuario:
        cur.execute(
            "UPDATE usuarios SET nombre = ?, updated_at = ? WHERE id = ?",
            (nombre, stamp, usuario["id"]),
        )
        usuario_id = usuario["id"]
    else:
        cur.execute(
            """
            INSERT INTO usuarios (telefono, nombre, created_at, updated_at)
            VALUES (?, ?, ?, ?)
            """,
            (telefono, nombre, stamp, stamp),
        )
        usuario_id = cur.lastrowid

    existing_node = get_trust_node_by_phone(cur, telefono)
    if existing_node:
        node_id = existing_node["id"]
        cur.execute(
            """
            UPDATE clientes_confianza
            SET usuario_id = ?, nombre = ?, padre_id = COALESCE(padre_id, ?), updated_at = ?
            WHERE id = ?
            """,
            (usuario_id, nombre, parent_id, stamp, node_id),
        )
    else:
        cur.execute(
            """
            INSERT INTO clientes_confianza (
                usuario_id, nombre, telefono, padre_id, estado, es_admin, creado_en, updated_at
            )
            VALUES (?, ?, ?, ?, 'activo', 0, ?, ?)
            """,
            (usuario_id, nombre, telefono, parent_id, stamp, stamp),
        )
        node_id = cur.lastrowid

    cur.execute(
        """
        UPDATE codigos_confianza
        SET estado = 'usado', usado_por_node_id = ?, usado_en = ?
        WHERE id = ?
        """,
        (node_id, stamp, code["id"]),
    )
    node = cur.execute("SELECT * FROM clientes_confianza WHERE id = ?", (node_id,)).fetchone()
    inviter = cur.execute("SELECT * FROM clientes_confianza WHERE id = ?", (parent_id,)).fetchone()
    return {"node": node, "inviter": inviter, "code": code}, ""


def trust_path(cur, node_id):
    path = []
    current = cur.execute("SELECT * FROM clientes_confianza WHERE id = ?", (node_id,)).fetchone()
    while current:
        path.append(current)
        if not current["padre_id"]:
            break
        current = cur.execute(
            "SELECT * FROM clientes_confianza WHERE id = ?",
            (current["padre_id"],),
        ).fetchone()
    path.reverse()
    return path


def count_descendants(cur, node_id):
    children = cur.execute(
        "SELECT id FROM clientes_confianza WHERE padre_id = ?",
        (node_id,),
    ).fetchall()
    return len(children) + sum(count_descendants(cur, child["id"]) for child in children)


def build_graph_payload(cur, selected_root_id=None):
    expire_unused_codes(cur)
    roots = cur.execute(
        """
        SELECT *
        FROM clientes_confianza
        WHERE padre_id IS NULL
        ORDER BY es_admin DESC, creado_en ASC
        """
    ).fetchall()
    if not roots:
        roots = [ensure_admin_root(cur)]

    root_ids = {row["id"] for row in roots}
    if not selected_root_id or selected_root_id not in root_ids:
        selected_root_id = ensure_admin_root(cur)["id"]

    nodes = []
    links = []

    def walk(node, depth=0):
        nodes.append(
            {
                "id": node["id"],
                "name": node["nombre"],
                "code": node["telefono"],
                "root": bool(node["es_admin"]) or node["padre_id"] is None,
                "admin": bool(node["es_admin"]),
                "depth": depth,
            }
        )
        children = cur.execute(
            "SELECT * FROM clientes_confianza WHERE padre_id = ? ORDER BY creado_en ASC",
            (node["id"],),
        ).fetchall()
        for child in children:
            links.append({"source": node["id"], "target": child["id"]})
            walk(child, depth + 1)

    selected_root = cur.execute(
        "SELECT * FROM clientes_confianza WHERE id = ?",
        (selected_root_id,),
    ).fetchone()
    if selected_root:
        walk(selected_root)

    leader_summaries = []
    for root in roots:
        leader_summaries.append(
            {
                "id": root["id"],
                "nombre": root["nombre"],
                "referidos": count_descendants(cur, root["id"]),
                "total_red": count_descendants(cur, root["id"]) + 1,
                "codigo_cliente": root["telefono"],
            }
        )

    available_codes = cur.execute(
        """
        SELECT c.*, creator.nombre AS creador_nombre
        FROM codigos_confianza c
        LEFT JOIN clientes_confianza creator ON creator.id = c.creador_node_id
        WHERE c.estado IN ('disponible', 'reservado')
        ORDER BY c.expira_en ASC
        """
    ).fetchall()
    return {
        "leaders": leader_summaries,
        "selected_leader": selected_root_id,
        "graph": {"nodes": nodes, "links": links},
        "codes": [
            {
                "codigo": row["codigo"],
                "tipo": row["tipo"],
                "estado": row["estado"],
                "creador": row["creador_nombre"] or "Admin",
                "seconds_left": code_seconds_left(row),
                "expira_en": format_expiration(row["expira_en"]),
            }
            for row in available_codes
            if code_seconds_left(row) > 0
        ],
        "updated_at": now_text(),
    }

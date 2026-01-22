#!/usr/bin/env python3
# safegrid-dashboard/whatsapp_webhook.py

import os
import json
import re
import time
import socket
import subprocess
from typing import Any, Dict, List, Optional, Tuple

from flask import Flask, request, Response

try:
    from twilio.twiml.messaging_response import MessagingResponse  # type: ignore
except Exception:
    MessagingResponse = None  # type: ignore

try:
    from twilio.request_validator import RequestValidator  # type: ignore
except Exception:
    RequestValidator = None  # type: ignore

try:
    from openai import OpenAI
except Exception:
    OpenAI = None  # type: ignore


# Snapshot import (robusto)
build_snapshot = None
try:
    from snapshot_db import build_snapshot as _bs  # type: ignore
    build_snapshot = _bs
except Exception:
    try:
        from snapshot_db import build_snapshot_db as _bs  # type: ignore
        build_snapshot = _bs
    except Exception:
        build_snapshot = None


app = Flask(__name__)

MODEL = os.getenv("SAFEGRID_OPENAI_MODEL", "gpt-4o-mini")
MAX_ROWS = int(os.getenv("SAFEGRID_MAX_ROWS", "60"))
TOMA_POINTS = int(os.getenv("SAFEGRID_TOMA_POINTS", "60"))
TTL_SEC = int(os.getenv("SAFEGRID_TTL_SEC", "10"))

WLAN_IFACE = os.getenv("SAFEGRID_WLAN_IFACE", "wlan0")
DEFAULT_SSID = os.getenv("SAFEGRID_SSID", "SafeGrid")
BROKER = os.getenv("SAFEGRID_MQTT_HOST", "192.168.4.1")
MQTT_PORT = int(os.getenv("SAFEGRID_MQTT_PORT", "1883"))
CAP_FALLBACK_MBPS = float(os.getenv("SAFEGRID_CAP_MBPS", "20"))

# Performance knobs
SNAP_CACHE_SEC = float(os.getenv("SAFEGRID_SNAP_CACHE_SEC", "2.0"))
AI_MAX_OUTPUT_TOKENS = int(os.getenv("SAFEGRID_AI_MAX_TOKENS", "110"))
AI_TIMEOUT_SEC = float(os.getenv("SAFEGRID_AI_TIMEOUT_SEC", "12.0"))
AI_CACHE_SEC = float(os.getenv("SAFEGRID_AI_CACHE_SEC", "20.0"))
AI_CACHE_MAX = int(os.getenv("SAFEGRID_AI_CACHE_MAX", "80"))
AI_SNAPSHOT_MAX_CHARS = int(os.getenv("SAFEGRID_AI_SNAPSHOT_CHARS", "1800"))

# Global caches (per worker process)
_SNAP_CACHE: Dict[str, Any] = {"ts": 0.0, "snap": None}
_AI_CACHE: Dict[str, Tuple[float, str]] = {}


# -------------------------
# OpenAI client (cacheado)
# -------------------------
_OPENAI_CLIENT = None


def _get_openai_client():
    global _OPENAI_CLIENT
    if _OPENAI_CLIENT is not None:
        return _OPENAI_CLIENT
    if not OpenAI:
        return None
    if not os.getenv("OPENAI_API_KEY"):
        return None
    try:
        # openai python sdk soporta timeout en el cliente en muchas versiones recientes.
        _OPENAI_CLIENT = OpenAI(timeout=AI_TIMEOUT_SEC)  # type: ignore[arg-type]
    except Exception:
        try:
            _OPENAI_CLIENT = OpenAI()
        except Exception:
            _OPENAI_CLIENT = None
    return _OPENAI_CLIENT


# -------------------------
# Twilio signature (opcional)
# -------------------------
def _twilio_validate_request() -> bool:
    token = os.getenv("TWILIO_AUTH_TOKEN", "").strip()
    if not token or not RequestValidator:
        return True
    try:
        validator = RequestValidator(token)
        url = request.url
        post_vars = request.form.to_dict(flat=True) if request.form else {}
        signature = request.headers.get("X-Twilio-Signature", "")
        return bool(validator.validate(url, post_vars, signature))
    except Exception:
        return False


# -------------------------
# Helpers
# -------------------------
def _run(cmd: List[str], timeout: float = 2.0) -> str:
    try:
        p = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
        if p.returncode != 0:
            return ""
        return (p.stdout or "").strip()
    except Exception:
        return ""


def _check_tcp(host: str, port: int, timeout: float = 1.0) -> bool:
    try:
        with socket.create_connection((host, int(port)), timeout=timeout):
            return True
    except Exception:
        return False


def get_active_ssid() -> str:
    out = _run(["nmcli", "-t", "-f", "GENERAL.CONNECTION", "dev", "show", WLAN_IFACE])
    conn = ""
    if out and ":" in out:
        conn = out.split(":", 1)[1].strip()
    if conn:
        ssid = _run(["nmcli", "-t", "-f", "802-11-wireless.ssid", "connection", "show", conn])
        if ssid and ":" in ssid:
            val = ssid.split(":", 1)[1].strip()
            if val:
                return val
    return DEFAULT_SSID


def list_saved_ssids(limit: int = 8) -> List[str]:
    out = _run(["nmcli", "-t", "-f", "NAME,TYPE", "connection", "show"])
    if not out:
        return []

    wifi_conns: List[str] = []
    for line in out.splitlines():
        parts = line.split(":")
        if len(parts) >= 2 and parts[1] in ("wifi", "802-11-wireless"):
            wifi_conns.append(parts[0])

    ssids: List[str] = []
    for name in wifi_conns:
        ssid = _run(["nmcli", "-t", "-f", "802-11-wireless.ssid", "connection", "show", name])
        if ssid and ":" in ssid:
            val = ssid.split(":", 1)[1].strip()
            if val and val not in ssids:
                ssids.append(val)
        if len(ssids) >= limit:
            break
    return ssids


def _ts_to_local_str(ts: Any) -> str:
    try:
        return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(int(ts)))
    except Exception:
        return str(ts)


def _get_latest_rows(snapshot: Dict[str, Any], table: str) -> List[Dict[str, Any]]:
    return snapshot.get("tables", {}).get(table, {}).get("latest") or []


def _get_last_row(snapshot: Dict[str, Any], table: str) -> Dict[str, Any]:
    rows = _get_latest_rows(snapshot, table)
    return rows[0] if rows else {}


def _normalize_toma(s: Any) -> str:
    s = str(s).strip().lower()
    s = s.replace("tomas", "toma")
    s = s.replace("toma ", "toma")
    return s


def _alias_keys(toma_key: str) -> List[str]:
    t = _normalize_toma(toma_key)
    out = {t}
    if t.isdigit():
        out.add(f"toma{t}")
    m = re.fullmatch(r"toma(\d+)", t)
    if m:
        out.add(m.group(1))
    return list(out)


def _find_last_sample_for_toma(snapshot: Dict[str, Any], toma_key: str) -> Optional[Dict[str, Any]]:
    rows = _get_latest_rows(snapshot, "toma_samples")
    wanted = set(_alias_keys(toma_key))
    for r in rows:
        t = _normalize_toma(r.get("toma", ""))
        if t in wanted:
            return r
    return None


def _online_map(snapshot: Dict[str, Any]) -> Dict[str, Any]:
    out: Dict[str, Any] = {}

    mp = snapshot.get("derived", {}).get("tomas_by_ttl")
    if isinstance(mp, dict) and mp:
        now = int(snapshot.get("now_ts") or time.time())
        ttl = int(snapshot.get("ttl_sec") or TTL_SEC)
        for k, r in mp.items():
            kk = str(k).lower()
            if not kk.startswith("toma"):
                continue
            ts = int((r or {}).get("ts") or 0)
            age = int((r or {}).get("age_sec") or (now - ts if ts else 10**9))
            out[kk] = 1 if ts and age <= ttl else 0
        if out:
            return out

    for r in _get_latest_rows(snapshot, "toma_status"):
        t = r.get("toma")
        if t is None:
            continue
        key = _normalize_toma(t)
        try:
            out[key] = int(r.get("online"))
        except Exception:
            out[key] = r.get("online")
    return out


def _parse_clients(net_row: Dict[str, Any]) -> List[str]:
    raw = net_row.get("clients_json")
    if not raw:
        return []
    try:
        data = raw
        if isinstance(raw, str):
            data = json.loads(raw)
        if isinstance(data, list):
            return [str(x) for x in data if x]
        if isinstance(data, dict):
            if "clients" in data and isinstance(data["clients"], list):
                return [str(x) for x in data["clients"] if x]
            return [str(k) for k in data.keys()]
    except Exception:
        return []
    return []


def _build_snapshot_uncached() -> Dict[str, Any]:
    if not build_snapshot:
        return {}
    try:
        return build_snapshot(max_rows_per_table=MAX_ROWS, toma_points=TOMA_POINTS, ttl_sec=TTL_SEC)
    except TypeError:
        try:
            return build_snapshot(max_rows_per_table=MAX_ROWS)
        except Exception:
            return {}
    except Exception:
        return {}


def _get_snapshot_cached() -> Dict[str, Any]:
    now = time.time()
    ts = float(_SNAP_CACHE.get("ts") or 0.0)
    snap = _SNAP_CACHE.get("snap")
    if snap and (now - ts) <= SNAP_CACHE_SEC:
        return snap
    snap = _build_snapshot_uncached()
    _SNAP_CACHE["ts"] = now
    _SNAP_CACHE["snap"] = snap
    return snap


def snapshot_compact(snapshot: Dict[str, Any], max_chars: int = AI_SNAPSHOT_MAX_CHARS) -> str:
    try:
        net = _get_last_row(snapshot, "net_samples")
        pi = _get_last_row(snapshot, "pi_samples")
        tomas = snapshot.get("derived", {}).get("tomas_by_ttl") or {}
        alerts = snapshot.get("alerts") or {}
        ctx = {
            "now_ts": snapshot.get("now_ts"),
            "ttl_sec": snapshot.get("ttl_sec"),
            "ssid": get_active_ssid(),
            "net": net,
            "pi": pi,
            "tomas_by_ttl": tomas,
            "alerts": alerts,
        }
        s = json.dumps(ctx, default=str, ensure_ascii=False)
        return s[:max_chars]
    except Exception:
        s = json.dumps(snapshot, default=str, ensure_ascii=False)
        return s[:max_chars]


def _simple_math_answer(txt: str) -> Optional[str]:
    t = (txt or "").strip().lower().replace(" ", "")
    m = re.fullmatch(r"(-?\d+)([+\-*])(-?\d+)", t)
    if not m:
        return None
    a = int(m.group(1))
    op = m.group(2)
    b = int(m.group(3))
    if op == "+":
        return str(a + b)
    if op == "-":
        return str(a - b)
    if op == "*":
        return str(a * b)
    return None


def _extract_ai_trigger(txt: str) -> Optional[str]:
    raw = (txt or "").strip()
    t = raw.lower().strip()
    if t.startswith("safe:"):
        return raw.split(":", 1)[1].strip()
    if t.startswith("safe "):
        return raw[5:].strip()
    if t.startswith("ai:"):
        return raw.split(":", 1)[1].strip()
    return None


# -------------------------
# Deterministic answers (rápidas)
# -------------------------
def format_estado(snapshot: Dict[str, Any]) -> str:
    net = _get_last_row(snapshot, "net_samples")
    pi = _get_last_row(snapshot, "pi_samples")

    db_path = os.getenv("SAFEGRID_DB_PATH") or snapshot.get("db_path") or ""
    db_ok = bool(db_path) and os.path.exists(db_path)

    mqtt_ok = _check_tcp(BROKER, MQTT_PORT, timeout=0.7)
    onmap = _online_map(snapshot)

    def _sortkey(x: str) -> int:
        m = re.search(r"(\d+)$", x)
        return int(m.group(1)) if m else 9999

    online = sorted([k for k, v in onmap.items() if v == 1], key=_sortkey)

    lines: List[str] = []
    lines.append("Estado SafeGrid")
    lines.append(f"• DB: {'OK' if db_ok else 'NO'} | MQTT: {'OK' if mqtt_ok else 'NO'}")

    if pi:
        lines.append(f"• Pi: {pi.get('temp_c')}°C | CPU {pi.get('cpu_pct')}% | RAM {pi.get('ram_used_gb')}/{pi.get('ram_total_gb')} GB")

    if net:
        lines.append(f"• Red: hi {net.get('hi_mbps')} / med {net.get('med_mbps')} / low {net.get('low_mbps')} Mbps")

    if onmap:
        lines.append(f"• Tomas ON: {', '.join(online) if online else 'ninguna'}")

    return "\n".join(lines)


def format_tomas_conectadas(snapshot: Dict[str, Any]) -> str:
    onmap = _online_map(snapshot)
    if not onmap:
        return "Tomas conectadas: sin datos."

    def _sortkey(x: str) -> int:
        m = re.search(r"(\d+)$", x)
        return int(m.group(1)) if m else 9999

    online = sorted([k for k, v in onmap.items() if v == 1], key=_sortkey)
    return "Tomas ON: " + (", ".join(online) if online else "ninguna")


def format_toma(snapshot: Dict[str, Any], toma_query: str) -> str:
    onmap = _online_map(snapshot)
    aliases = _alias_keys(toma_query)

    status: Optional[Any] = None
    for k in aliases:
        if k in onmap:
            status = onmap[k]
            break

    pretty = f"toma{toma_query}" if str(toma_query).isdigit() else str(toma_query)

    if status == 0:
        return f"{pretty}: OFFLINE."

    r = _find_last_sample_for_toma(snapshot, toma_query)
    if not r:
        if status is None:
            return f"{pretty}: no registrada."
        return f"{pretty}: ONLINE, sin lectura reciente."

    tname = r.get("toma")
    on_txt = "ONLINE" if status == 1 else "DESCONOCIDO"
    ts = _ts_to_local_str(r.get("ts"))
    return f"{tname}: {on_txt}. I={r.get('amperaje')}A, P={r.get('potencia_w')}W, {r.get('estado')}, RSSI {r.get('rssi')}, {ts}"


def format_red(snapshot: Dict[str, Any]) -> str:
    net = _get_last_row(snapshot, "net_samples")
    if not net:
        return "Red: sin datos net_samples."

    ssid = get_active_ssid()

    try:
        hi = float(net.get("hi_mbps") or 0)
        med = float(net.get("med_mbps") or 0)
        low = float(net.get("low_mbps") or 0)
        cap = float(net.get("cap_mbps") or 0)
    except Exception:
        hi = med = low = 0.0
        cap = 0.0

    if cap <= 0.1:
        cap = CAP_FALLBACK_MBPS

    total = hi + med + low
    pct = (total / cap * 100.0) if cap else 0.0

    if pct >= 80:
        estado = "SATURADA"
    elif pct >= 50:
        estado = "ALTA"
    else:
        estado = "OK"

    qos_src = net.get("qos_src") or "?"
    clients = _parse_clients(net)
    clients_txt = ", ".join(clients[:4]) if clients else "ninguno"

    return (
        f"Red {ssid}: {estado}.\n"
        f"• Uso: {total:.3f}/{cap:.1f} Mbps ({pct:.1f}%) | QoS: {qos_src}\n"
        f"• Clientes: {clients_txt}"
    )


def format_redes_guardadas() -> str:
    ssids = list_saved_ssids(limit=10)
    if not ssids:
        return f"Redes guardadas: no pude leer nmcli. SSID actual: {get_active_ssid()}."
    return "SSIDs guardados: " + ", ".join(ssids)


def format_clientes(snapshot: Dict[str, Any]) -> str:
    net = _get_last_row(snapshot, "net_samples")
    clients = _parse_clients(net)
    if not clients:
        return "Clientes: ninguno en snapshot."
    return "Clientes: " + ", ".join(clients[:10])


# -------------------------
# Intent parsing
# -------------------------
def _wants_connected_tomas(txt: str) -> bool:
    t = txt.lower()
    return any(p in t for p in [
        "cuales tomas estan conectadas",
        "qué tomas están conectadas",
        "que tomas estan conectadas",
        "tomas conectadas",
        "tomas online",
        "tomas en linea",
        "tomas en línea",
        "cuales tomas estan online",
        "cuáles tomas están online",
        "que tomas estan online",
    ])


def _parse_toma_specific(txt: str) -> Optional[str]:
    t = txt.strip().lower()
    t = t.replace("tomas", "toma")
    if not t.startswith("toma"):
        return None
    m = re.match(r"^toma\s*[:#]?\s*(\d+)\s*$", t)
    if m:
        return m.group(1)
    m = re.match(r"^toma(\d+)\s*$", t)
    if m:
        return m.group(1)
    return None


def _wants_red(txt: str) -> bool:
    t = txt.lower()
    return any(w in t for w in [
        "red", "wifi", "wi-fi", "satur", "qos", "cola", "trafico", "tráfico", "capacidad", "ancho de banda"
    ])


def _wants_ssids(txt: str) -> bool:
    t = txt.lower()
    return any(w in t for w in [
        "ssid", "redes guardadas", "lista de redes", "mis redes", "nombres de redes"
    ])


def _wants_clientes(txt: str) -> bool:
    t = (txt or "").lower()
    return any(w in t for w in [
        "clientes",
        "hosts",
        "conectados",
        "conectadas",
        "dispositivo",
        "dispositivos",
        "equipos",
        "quien esta conectado",
        "quién está conectado",
        "qué dispositivos",
        "que dispositivos",
    ])


def _looks_like_safegrid_topic(txt: str) -> bool:
    t = (txt or "").lower()
    return any(w in t for w in [
        "safegrid", "toma", "tomas", "red", "wifi", "qos", "dscp", "cola", "colas",
        "satur", "capacidad", "ancho de banda", "mqtt", "clientes", "hosts"
    ])


def _looks_like_network_recommendation(txt: str) -> bool:
    t = (txt or "").lower()
    return any(w in t for w in [
        "recomienda", "recomendacion", "recomendación", "mejora", "optimiza", "optimizar",
        "como mejoro", "cómo mejoro", "que hago", "qué hago", "consejo", "diagnostico", "diagnóstico",
        "latencia", "jitter", "pérdida", "perdida", "packet loss", "prioridad", "dscp", "colas"
    ])


# -------------------------
# OpenAI (optimizado)
# -------------------------
def _ai_reply(user_text: str, snapshot: Dict[str, Any]) -> str:
    client = _get_openai_client()
    if not client:
        return "AI OFF (falta OPENAI_API_KEY). Usa: estado / red / clientes / tomas / toma1"

    # Cache AI (reduce latencia en repetidos)
    now = time.time()
    k = user_text.strip().lower()
    cached = _AI_CACHE.get(k)
    if cached and (now - cached[0]) <= AI_CACHE_SEC:
        return cached[1]

    instructions = (
        "Eres el asistente de SafeGrid.\n"
        "Responde en español, MUY breve (máx 3 líneas).\n"
        "Si la pregunta es de SafeGrid/red/QoS/tomas, usa SOLO el snapshot.\n"
        "Si no es de SafeGrid, respóndela normalmente.\n"
        "No inventes datos.\n"
    )

    # Si no es tema SafeGrid, NO mandes snapshot (menos tokens = más rápido)
    if _looks_like_safegrid_topic(user_text):
        compact = snapshot_compact(snapshot)
        content = f"Usuario: {user_text}\nSnapshot(JSON): {compact}"
    else:
        content = f"Usuario: {user_text}"

    try:
        resp = client.responses.create(
            model=MODEL,
            max_output_tokens=AI_MAX_OUTPUT_TOKENS,
            instructions=instructions,
            input=[{"role": "user", "content": content}],
        )
        out = (resp.output_text or "").strip() or "No pude responder."
    except Exception as e:
        out = f"AI error: {e}"

    # mantener cache acotado
    _AI_CACHE[k] = (now, out)
    if len(_AI_CACHE) > AI_CACHE_MAX:
        # purge simple: borra los más viejos
        items = sorted(_AI_CACHE.items(), key=lambda kv: kv[1][0])
        for kk, _vv in items[: max(1, len(items) - AI_CACHE_MAX)]:
            _AI_CACHE.pop(kk, None)

    return out


# -------------------------
# Route
# -------------------------
@app.route("/whatsapp", methods=["GET", "POST"])
def whatsapp_webhook():
    if MessagingResponse is None:
        return Response("Missing twilio. Activate venv and: pip install twilio\n", status=500)

    if request.method == "POST" and not _twilio_validate_request():
        return Response("Invalid signature", status=403)

    user_text = (request.values.get("Body") or "").strip()
    txt = user_text.lower().strip()

    # Respuestas instantáneas sin snapshot
    sm = _simple_math_answer(user_text)
    if sm is not None:
        resp = MessagingResponse()
        resp.message(sm)
        return Response(str(resp), mimetype="application/xml")

    if txt.startswith("/help") or txt in ("help", "ayuda"):
        resp = MessagingResponse()
        resp.message("Comandos: estado, red, clientes, tomas, toma1.\nAI: safe <pregunta>")
        return Response(str(resp), mimetype="application/xml")

    # Debug raw (sí necesita snapshot)
    if txt.startswith("/raw"):
        snapshot = _get_snapshot_cached()
        payload = json.dumps(snapshot, indent=2, default=str, ensure_ascii=False)[:3500]
        resp = MessagingResponse()
        resp.message(payload)
        return Response(str(resp), mimetype="application/xml")

    # Force AI
    forced_ai = _extract_ai_trigger(user_text)
    if forced_ai is not None:
        snapshot = _get_snapshot_cached()
        if not forced_ai:
            answer = "Usa: safe <pregunta>. Ej: safe recomiéndame QoS"
        else:
            answer = _ai_reply(forced_ai, snapshot)
        resp = MessagingResponse()
        resp.message(answer)
        return Response(str(resp), mimetype="application/xml")

    # Determinísticos (necesitan snapshot)
    if txt in ("estado", "status", "/estado"):
        snapshot = _get_snapshot_cached()
        resp = MessagingResponse()
        resp.message(format_estado(snapshot))
        return Response(str(resp), mimetype="application/xml")

    if txt in ("tomas", "/tomas") or _wants_connected_tomas(txt):
        snapshot = _get_snapshot_cached()
        resp = MessagingResponse()
        resp.message(format_tomas_conectadas(snapshot))
        return Response(str(resp), mimetype="application/xml")

    toma_q = _parse_toma_specific(txt)
    if toma_q:
        snapshot = _get_snapshot_cached()
        resp = MessagingResponse()
        resp.message(format_toma(snapshot, toma_q))
        return Response(str(resp), mimetype="application/xml")

    if _wants_ssids(txt):
        resp = MessagingResponse()
        resp.message(format_redes_guardadas())
        return Response(str(resp), mimetype="application/xml")

    if _wants_clientes(txt):
        snapshot = _get_snapshot_cached()
        resp = MessagingResponse()
        resp.message(format_clientes(snapshot))
        return Response(str(resp), mimetype="application/xml")

    if _wants_red(txt):
        snapshot = _get_snapshot_cached()
        resp = MessagingResponse()
        resp.message(format_red(snapshot))
        return Response(str(resp), mimetype="application/xml")

    # Recomendaciones de red -> AI con snapshot
    if _looks_like_network_recommendation(user_text) or _looks_like_safegrid_topic(user_text):
        snapshot = _get_snapshot_cached()
        answer = _ai_reply(user_text, snapshot)
        resp = MessagingResponse()
        resp.message(answer)
        return Response(str(resp), mimetype="application/xml")

    # Default: AI sin snapshot (más rápido)
    answer = _ai_reply(user_text, {})
    resp = MessagingResponse()
    resp.message(answer)
    return Response(str(resp), mimetype="application/xml")


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=3000)

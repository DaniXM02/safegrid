#!/usr/bin/env python3
# safegrid-dashboard/dashboard_server.py

import os
import json
import time
import re
import socket
import subprocess
from typing import Any, Dict, List, Optional

from flask import Flask, render_template, jsonify, request, Response, stream_with_context

# -------------------------
# Snapshot import (robusto)
# -------------------------
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

# -------------------------
# OpenAI opcional
# -------------------------
try:
    from openai import OpenAI
except Exception:
    OpenAI = None  # type: ignore


def _get_openai_client():
    if not OpenAI:
        return None
    if not os.getenv("OPENAI_API_KEY"):
        return None
    try:
        return OpenAI()
    except Exception:
        return None


app = Flask(__name__)

# -------------------------
# Config
# -------------------------
MODEL = os.getenv("SAFEGRID_OPENAI_MODEL", "gpt-4o-mini")
MAX_ROWS = int(os.getenv("SAFEGRID_MAX_ROWS", "120"))
TOMA_POINTS = int(os.getenv("SAFEGRID_TOMA_POINTS", "120"))
TTL_SEC = int(os.getenv("SAFEGRID_TTL_SEC", "10"))

WLAN_IFACE = os.getenv("SAFEGRID_WLAN_IFACE", "wlan0")
DEFAULT_SSID = os.getenv("SAFEGRID_SSID", "SafeGrid")

BROKER = os.getenv("SAFEGRID_MQTT_HOST", "192.168.4.1")
MQTT_PORT = int(os.getenv("SAFEGRID_MQTT_PORT", "1883"))

# fallback cap si viene 0
CAP_FALLBACK_MBPS = float(os.getenv("SAFEGRID_CAP_MBPS", "20"))

NGROK_SH = os.getenv("SAFEGRID_NGROK_SH", "/home/teleadmin/safegrid-dashboard/ngrok_url.sh")

_EXPECTED_TOMAS = [x.strip() for x in (os.getenv("SAFEGRID_TOMAS", "toma1,toma2,toma3")).split(",") if x.strip()]
_EXPECTED_TOMAS = _EXPECTED_TOMAS or ["toma1", "toma2", "toma3"]

# -------------------------
# Utils
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


def _last_from_table(snap: Dict[str, Any], table: str) -> Dict[str, Any]:
    rows = snap.get("tables", {}).get(table, {}).get("latest") or []
    return rows[0] if rows else {}


def _net_state(util_pct: float) -> str:
    if util_pct >= 80:
        return "SATURADA"
    if util_pct >= 50:
        return "ALTA"
    return "OK"


def _qos_panel(util_pct: float) -> Dict[str, Any]:
    if util_pct < 40:
        return {"congestion": "SIN_CONGESTIÓN", "qos": "OK", "advice": "Red ligera: QoS no es crítico. Perfecto para demostración base."}
    if util_pct < 80:
        return {"congestion": "MODERADA", "qos": "ÚTIL", "advice": "Buen escenario para demostrar QoS (prioridad alertas/VoIP)."}
    if util_pct < 95:
        return {"congestion": "FUERTE", "qos": "LÍMITE", "advice": "Colas y retrasos altos: QoS no salva todo, pero aún ayuda."}
    return {"congestion": "CRÍTICA", "qos": "INVIABLE", "advice": "Red inestable: mejora canal/ancho de banda o reduce carga."}


def _normalize_toma(text: str) -> Optional[str]:
    t = text.strip().lower()
    m = re.match(r"^toma[\s\-_]*([0-9]+)$", t)
    if not m:
        return None
    return f"toma{m.group(1)}"


def _tomas_detail_from_ttl(snap: Dict[str, Any]) -> List[Dict[str, Any]]:
    now = int(snap.get("now_ts") or time.time())
    ttl = int(snap.get("ttl_sec") or TTL_SEC)

    mp = snap.get("derived", {}).get("tomas_by_ttl") or {}

    keys = set(_EXPECTED_TOMAS)
    for k in mp.keys():
        if str(k).lower().startswith("toma"):
            keys.add(str(k).lower())

    out: List[Dict[str, Any]] = []
    for k in sorted(keys):
        r = mp.get(k, {}) if isinstance(mp, dict) else {}
        ts = int(r.get("ts") or 0)
        age = int(r.get("age_sec") or (now - ts if ts else 10**9))
        online = 1 if ts and age <= ttl else 0
        out.append({
            "toma": k,
            "online": bool(online),
            "age_sec": age,
            "ts": ts,
            "last_local": time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(ts)) if ts else None,
            "amperaje": r.get("amperaje"),
            "potencia_w": r.get("potencia_w"),
            "estado": r.get("estado"),
            "rssi": r.get("rssi"),
        })
    return out


def _ngrok_url() -> str:
    if not NGROK_SH or not os.path.exists(NGROK_SH):
        return ""
    out = _run(["bash", NGROK_SH], timeout=1.5)
    return out.strip()


def _calc_net_fields(snap: Dict[str, Any]) -> Dict[str, Any]:
    net = _last_from_table(snap, "net_samples")
    try:
        hi = float(net.get("hi_mbps") or 0)
        med = float(net.get("med_mbps") or 0)
        low = float(net.get("low_mbps") or 0)
        cap = float(net.get("cap_mbps") or 0)
    except Exception:
        hi = med = low = cap = 0.0

    # fallback cap si viene 0
    if cap <= 0.1:
        cap = CAP_FALLBACK_MBPS

    total = hi + med + low
    util = (total / cap * 100.0) if cap else 0.0
    return {
        "hi_mbps": hi, "med_mbps": med, "low_mbps": low, "cap_mbps": cap,
        "total_mbps": total, "util_pct": util,
        "state": _net_state(util),
        "qos_src": net.get("qos_src") if net else None,
        "qos_panel": _qos_panel(util),
    }

# -------------------------
# AI
# -------------------------
def _ai_reply(user_text: str, snap: Dict[str, Any]) -> str:
    client = _get_openai_client()
    if not client:
        return "AI OFF (falta OPENAI_API_KEY)."

    ctx = {
        "ssid": get_active_ssid(),
        "ttl_sec": int(snap.get("ttl_sec") or TTL_SEC),
        "net": _calc_net_fields(snap),
        "pi": _last_from_table(snap, "pi_samples") or {},
        "tomas": _tomas_detail_from_ttl(snap),
        "alerts120": (snap.get("alerts") or {}).get("count_120s", None),
    }

    instructions = (
        "Eres el asistente del Proyecto Tráfico (SafeGrid).\n"
        "Responde en español y corto (máximo 6 líneas), sin markdown.\n"
        "Usa SOLO el contexto JSON. No inventes.\n"
        "Si preguntan por QoS: DSCP, colas, pérdida, jitter, congestión.\n"
    )

    resp = client.responses.create(
        model=MODEL,
        instructions=instructions,
        input=[{
            "role": "user",
            "content": f"Pregunta: {user_text}\n\nContexto(DB): {json.dumps(ctx, default=str)}"
        }]
    )
    return (resp.output_text or "").strip() or "No pude responder."

# -------------------------
# Routes
# -------------------------
@app.route("/")
def index():
    return render_template("index.html", ttl_sec=TTL_SEC)

# Ngrok SOLO se calcula cuando el usuario lo pide (botón)
@app.route("/api/ngrok")
def api_ngrok():
    return jsonify({"url": _ngrok_url() or ""})

@app.route("/api/state")
def api_state():
    if not build_snapshot:
        return jsonify({"error": "snapshot_db no expone build_snapshot"}), 500

    snap = build_snapshot(max_rows_per_table=MAX_ROWS, toma_points=TOMA_POINTS, ttl_sec=TTL_SEC)

    db_path = os.getenv("SAFEGRID_DB_PATH") or snap.get("db_path") or ""
    db_ok = bool(db_path) and os.path.exists(db_path)
    mqtt_ok = _check_tcp(BROKER, MQTT_PORT, timeout=1.0)
    ai_ok = bool(_get_openai_client())

    out = {
        "generated_at_local": time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()),
        "ssid": get_active_ssid(),
        "ttl_sec": int(snap.get("ttl_sec") or TTL_SEC),
        "services": {"db": db_ok, "mqtt": mqtt_ok, "ai": ai_ok},
        "db_path": db_path,
        "ngrok": "",
        "pi": _last_from_table(snap, "pi_samples") or {},
        "net": _calc_net_fields(snap),
        "alerts": snap.get("alerts") or {"count_120s": 0, "series": []},
        "series": {
            "net": snap.get("tables", {}).get("net_samples", {}).get("series") or [],
            "tomas": snap.get("tables", {}).get("toma_samples", {}).get("series_per_toma") or {},
        },
        "tomas_detail": _tomas_detail_from_ttl(snap),
    }
    return jsonify(out)

@app.route("/api/stream")
def api_stream():
    if not build_snapshot:
        return jsonify({"error": "snapshot_db no expone build_snapshot"}), 500

    def gen():
        while True:
            snap = build_snapshot(max_rows_per_table=MAX_ROWS, toma_points=TOMA_POINTS, ttl_sec=TTL_SEC)

            db_path = os.getenv("SAFEGRID_DB_PATH") or snap.get("db_path") or ""
            db_ok = bool(db_path) and os.path.exists(db_path)

            payload = {
                "generated_at_local": time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()),
                "ssid": get_active_ssid(),
                "ttl_sec": int(snap.get("ttl_sec") or TTL_SEC),
                "services": {
                    "db": db_ok,
                    "mqtt": _check_tcp(BROKER, MQTT_PORT, timeout=0.5),
                    "ai": bool(_get_openai_client())
                },
                "ngrok": "",
                "net": _calc_net_fields(snap),
                "pi": _last_from_table(snap, "pi_samples") or {},
                "alerts": snap.get("alerts") or {"count_120s": 0, "series": []},
                "series": {
                    "net": snap.get("tables", {}).get("net_samples", {}).get("series") or [],
                    "tomas": snap.get("tables", {}).get("toma_samples", {}).get("series_per_toma") or {},
                },
                "tomas_detail": _tomas_detail_from_ttl(snap),
            }
            yield f"data: {json.dumps(payload, default=str)}\n\n"
            time.sleep(1.0)

    return Response(stream_with_context(gen()), mimetype="text/event-stream")

@app.route("/api/chat", methods=["POST"])
def api_chat():
    data = request.get_json(silent=True) or {}
    text = (data.get("text") or "").strip()
    if not text:
        return jsonify({"reply": "Estoy en modo DB. Usa: estado / red / tomas / toma1. Para AI: safe <pregunta>"}), 200

    if not build_snapshot:
        return jsonify({"reply": "Error: snapshot_db no expone build_snapshot."}), 500

    snap = build_snapshot(max_rows_per_table=MAX_ROWS, toma_points=TOMA_POINTS, ttl_sec=TTL_SEC)

    raw = text.strip()
    t = raw.lower().strip()

    # AI trigger: "safe ..." o "safe: ..."
    ai_text = None
    if t.startswith("safe:"):
        ai_text = raw.split(":", 1)[1].strip()
    elif t.startswith("safe "):
        ai_text = raw[5:].strip()
    elif t.startswith("ai:"):
        ai_text = raw.split(":", 1)[1].strip()

    if ai_text is not None:
        if not ai_text:
            return jsonify({"reply": "Usa: safe <tu pregunta>. Ej: safe como está la red"}), 200
        try:
            r = _ai_reply(ai_text, snap)
            return jsonify({"reply": r}), 200
        except Exception:
            return jsonify({"reply": "AI error. Revisa OPENAI_API_KEY en /etc/safegrid.env"}), 200

    # DB mode
    if t in ("estado", "status"):
        tomas = _tomas_detail_from_ttl(snap)
        on = [x["toma"] for x in tomas if x["online"]]
        off = [x["toma"] for x in tomas if not x["online"]]
        net = _calc_net_fields(snap)
        qp = net["qos_panel"]
        return jsonify({"reply": "\n".join([
            f"SSID {get_active_ssid()}",
            f"Tomas ON: {', '.join(on) if on else '—'} | OFF: {', '.join(off) if off else '—'}",
            f"Red: {net['state']} ({net['util_pct']:.2f}%) | QoS: {qp['qos']}",
            "Para AI: safe <pregunta>"
        ])}), 200

    if t == "red":
        net = _calc_net_fields(snap)
        qp = net["qos_panel"]
        return jsonify({"reply": "\n".join([
            f"QoS src: {net.get('qos_src') or '—'}",
            f"Uso total: {net['total_mbps']:.3f} Mbps / cap {net['cap_mbps']:.1f} Mbps",
            f"Utilización: {net['util_pct']:.2f}% | Estado: {net['state']}",
            f"QoS: {qp['qos']} | {qp['advice']}",
        ])}), 200

    if t == "tomas":
        tomas = _tomas_detail_from_ttl(snap)
        lines = [f"TTL {int(snap.get('ttl_sec') or TTL_SEC)}s"]
        for x in tomas:
            st = "ONLINE" if x["online"] else "OFFLINE"
            lines.append(f"{x['toma']}: {st} age {x['age_sec']}s | I={x.get('amperaje')}A P={x.get('potencia_w')}W RSSI={x.get('rssi')}")
        return jsonify({"reply": "\n".join(lines)}), 200

    nt = _normalize_toma(t)
    if nt:
        tomas = {x["toma"]: x for x in _tomas_detail_from_ttl(snap)}
        x = tomas.get(nt)
        if not x:
            return jsonify({"reply": f"No veo {nt} en el snapshot aún."}), 200
        st = "ONLINE" if x["online"] else "OFFLINE"
        return jsonify({"reply": "\n".join([
            f"{nt}: {st} age {x['age_sec']}s",
            f"I={x.get('amperaje')} A | P={x.get('potencia_w')} W",
            f"Estado={x.get('estado')} | RSSI={x.get('rssi')}",
            f"Último: {x.get('last_local')}",
        ])}), 200

    return jsonify({"reply": "Estoy en modo DB.\nUsa: estado / red / tomas / toma1\nPara AI: safe <pregunta>"}), 200

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080)


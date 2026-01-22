#!/usr/bin/env python3
# safegrid-dashboard/ingest_tomas.py
# MQTT (telem + alert) -> SQLite (toma_samples + alert_samples)
#
# Fixes:
# - NO usa columna "on" (reservada). Usa is_on.
# - Filtra safegrid/pi/telemetry para NO meterlo como "toma".
# - Asegura schema con ALTER TABLE usando comillas (evita "near on").
# - Inserta seq/ms/sim/reason cuando existan.

import os
import json
import time
import sqlite3
import threading
from typing import Any, Dict, Optional

import paho.mqtt.client as mqtt

DB_PATH = os.getenv("SAFEGRID_DB_PATH", "/home/teleadmin/safegrid-dashboard/safegrid.db")

MQTT_HOST = os.getenv("SAFEGRID_MQTT_HOST", "192.168.4.1")
MQTT_PORT = int(os.getenv("SAFEGRID_MQTT_PORT", "1883"))
MQTT_ALERT_PORT = int(os.getenv("SAFEGRID_MQTT_ALERT_PORT", "1884"))
MQTT_USER = os.getenv("SAFEGRID_MQTT_USER", "control")
MQTT_PASS = os.getenv("SAFEGRID_MQTT_PASS", "user1234")

SUB_TELEM = "safegrid/+/telemetry"
SUB_ALERT = "safegrid/+/alert"

# para evitar que "pi" o "ap" se metan como tomas
RESERVED_IDS = {"pi", "ap", "display", "dashboard"}

PRINT_EVERY = 8  # segundos


def now_ts() -> int:
    return int(time.time())


def connect_db() -> sqlite3.Connection:
    con = sqlite3.connect(DB_PATH, timeout=30, isolation_level=None, check_same_thread=False)
    con.execute("PRAGMA journal_mode=WAL;")
    con.execute("PRAGMA synchronous=NORMAL;")
    con.execute("PRAGMA temp_store=MEMORY;")
    return con


def _cols(con: sqlite3.Connection, table: str) -> Dict[str, str]:
    d: Dict[str, str] = {}
    for row in con.execute(f"PRAGMA table_info({table});"):
        # cid, name, type, notnull, dflt_value, pk
        d[str(row[1])] = str(row[2] or "")
    return d


def ensure_schema(con: sqlite3.Connection) -> None:
    con.execute("""
    CREATE TABLE IF NOT EXISTS toma_samples (
      ts INTEGER NOT NULL,
      toma TEXT NOT NULL,
      seq INTEGER,
      ms INTEGER,
      sim INTEGER,
      is_on INTEGER,
      amperaje REAL,
      potencia_w REAL,
      estado TEXT,
      rssi INTEGER
    );
    """)

    con.execute("""
    CREATE TABLE IF NOT EXISTS alert_samples (
      ts INTEGER NOT NULL,
      toma TEXT NOT NULL,
      seq INTEGER,
      ms INTEGER,
      sim INTEGER,
      is_on INTEGER,
      amperaje REAL,
      potencia_w REAL,
      estado TEXT,
      rssi INTEGER,
      reason TEXT
    );
    """)

    # Si vienes de un schema viejo, agregamos columnas que falten (seguro con comillas)
    need_toma = {
        "seq": "INTEGER",
        "ms": "INTEGER",
        "sim": "INTEGER",
        "is_on": "INTEGER",
        "amperaje": "REAL",
        "potencia_w": "REAL",
        "estado": "TEXT",
        "rssi": "INTEGER",
    }
    need_alert = dict(need_toma)
    need_alert["reason"] = "TEXT"

    have_t = _cols(con, "toma_samples")
    for c, ctype in need_toma.items():
        if c not in have_t:
            con.execute(f'ALTER TABLE toma_samples ADD COLUMN "{c}" {ctype};')

    have_a = _cols(con, "alert_samples")
    for c, ctype in need_alert.items():
        if c not in have_a:
            con.execute(f'ALTER TABLE alert_samples ADD COLUMN "{c}" {ctype};')

    # índices (acelera "último por toma" y series)
    con.execute("CREATE INDEX IF NOT EXISTS idx_toma_ts ON toma_samples(toma, ts);")
    con.execute("CREATE INDEX IF NOT EXISTS idx_alert_ts ON alert_samples(toma, ts);")


def topic_to_id(topic: str) -> Optional[str]:
    # safegrid/<id>/telemetry
    parts = topic.split("/")
    if len(parts) >= 3 and parts[0] == "safegrid":
        return parts[1]
    return None


def safe_float(x: Any) -> Optional[float]:
    try:
        if x is None:
            return None
        return float(x)
    except Exception:
        return None


def safe_int(x: Any) -> Optional[int]:
    try:
        if x is None:
            return None
        return int(x)
    except Exception:
        return None


class Writer:
    def __init__(self) -> None:
        self.con = connect_db()
        ensure_schema(self.con)
        self.lock = threading.Lock()
        self.last_print = 0

    def insert_telem(self, toma: str, doc: Dict[str, Any]) -> None:
        ts = now_ts()
        row = (
            ts,
            toma,
            safe_int(doc.get("seq")),
            safe_int(doc.get("ms")),
            safe_int(doc.get("sim")),
            1 if doc.get("on") is True else 0 if doc.get("on") is False else safe_int(doc.get("is_on")),
            safe_float(doc.get("amperaje")),
            safe_float(doc.get("potencia_w")),
            doc.get("estado"),
            safe_int(doc.get("rssi")),
        )

        with self.lock:
            self.con.execute(
                """INSERT INTO toma_samples(ts,toma,seq,ms,sim,is_on,amperaje,potencia_w,estado,rssi)
                   VALUES(?,?,?,?,?,?,?,?,?,?);""",
                row
            )

        self._maybe_print("telem", toma, ts, doc)

    def insert_alert(self, toma: str, doc: Dict[str, Any]) -> None:
        ts = now_ts()
        row = (
            ts,
            toma,
            safe_int(doc.get("seq")),
            safe_int(doc.get("ms")),
            safe_int(doc.get("sim")),
            1 if doc.get("on") is True else 0 if doc.get("on") is False else safe_int(doc.get("is_on")),
            safe_float(doc.get("amperaje")),
            safe_float(doc.get("potencia_w")),
            doc.get("estado"),
            safe_int(doc.get("rssi")),
            doc.get("reason"),
        )

        with self.lock:
            self.con.execute(
                """INSERT INTO alert_samples(ts,toma,seq,ms,sim,is_on,amperaje,potencia_w,estado,rssi,reason)
                   VALUES(?,?,?,?,?,?,?,?,?,?,?);""",
                row
            )

        self._maybe_print("alert", toma, ts, doc)

    def _maybe_print(self, kind: str, toma: str, ts: int, doc: Dict[str, Any]) -> None:
        now = time.time()
        if now - self.last_print >= PRINT_EVERY:
            self.last_print = now
            print(time.strftime("%Y-%m-%d %H:%M:%S"), "[writer] DB =", DB_PATH)
            print("  last", kind, toma, "ts", ts, "amperaje", doc.get("amperaje"), "P", doc.get("potencia_w"), "estado", doc.get("estado"))


def make_client(name: str, port: int, on_msg) -> mqtt.Client:
    client_id = f"safegrid-{name}-{int(time.time())}"
    c = mqtt.Client(client_id=client_id, clean_session=True)
    if MQTT_USER:
        c.username_pw_set(MQTT_USER, MQTT_PASS)

    def _on_connect(cli, userdata, flags, rc):
        print(time.strftime("%Y-%m-%d %H:%M:%S"), f"[MQTT {name}] connected rc=", rc)
        if rc == 0:
            if name == "telem":
                cli.subscribe(SUB_TELEM, qos=0)
                print(time.strftime("%Y-%m-%d %H:%M:%S"), "[MQTT telem] sub=", SUB_TELEM)
            else:
                cli.subscribe(SUB_ALERT, qos=0)
                print(time.strftime("%Y-%m-%d %H:%M:%S"), "[MQTT alert] sub=", SUB_ALERT)

    c.on_connect = _on_connect
    c.on_message = on_msg
    c.connect(MQTT_HOST, port, keepalive=30)
    return c


def main() -> None:
    print(time.strftime("%Y-%m-%d %H:%M:%S"), "[MQTT] host=", MQTT_HOST, "ports=", MQTT_PORT, MQTT_ALERT_PORT, "user=", MQTT_USER)
    w = Writer()

    def on_telem(cli, userdata, msg):
        toma = topic_to_id(msg.topic) or ""
        if not toma:
            return
        if toma.lower() in RESERVED_IDS:
            # NO metas pi/ap/display a toma_samples
            return
        try:
            doc = json.loads(msg.payload.decode("utf-8", "ignore"))
            if not isinstance(doc, dict):
                return
            # normaliza toma desde payload si viene
            t2 = (doc.get("toma") or doc.get("id") or toma)
            toma_norm = str(t2).strip().lower()
            if toma_norm in RESERVED_IDS:
                return
            w.insert_telem(toma_norm, doc)
        except Exception:
            return

    def on_alert(cli, userdata, msg):
        toma = topic_to_id(msg.topic) or ""
        if not toma:
            return
        try:
            doc = json.loads(msg.payload.decode("utf-8", "ignore"))
            if not isinstance(doc, dict):
                return
            t2 = (doc.get("toma") or doc.get("id") or toma)
            toma_norm = str(t2).strip().lower()
            if toma_norm in RESERVED_IDS:
                # si quieres, podrías guardar alertas del pi, pero normalmente no
                return
            w.insert_alert(toma_norm, doc)
        except Exception:
            return

    c_telem = make_client("telem", MQTT_PORT, on_telem)
    c_alert = make_client("alert", MQTT_ALERT_PORT, on_alert)

    # loops
    c_telem.loop_start()
    c_alert.loop_start()

    while True:
        time.sleep(2)


if __name__ == "__main__":
    main()

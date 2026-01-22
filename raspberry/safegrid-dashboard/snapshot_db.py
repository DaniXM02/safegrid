#!/usr/bin/env python3
import os, sqlite3, time
from typing import Any, Dict, List, Tuple

def _db_path() -> str:
    return os.getenv("SAFEGRID_DB_PATH", "/home/teleadmin/safegrid-dashboard/safegrid.db")

def _connect(path: str) -> sqlite3.Connection:
    con = sqlite3.connect(path, timeout=5.0, isolation_level=None)
    con.row_factory = sqlite3.Row
    con.execute("PRAGMA journal_mode=WAL;")
    con.execute("PRAGMA synchronous=NORMAL;")
    return con

def ensure_schema(con: sqlite3.Connection) -> None:
    con.execute("""
    CREATE TABLE IF NOT EXISTS toma_samples(
      ts INTEGER NOT NULL,
      toma TEXT NOT NULL,
      seq INTEGER,
      is_on INTEGER,
      amperaje REAL,
      potencia_w REAL,
      estado TEXT,
      rssi INTEGER
    );
    """)
    con.execute("CREATE INDEX IF NOT EXISTS idx_toma_ts ON toma_samples(toma, ts);")

    con.execute("""
    CREATE TABLE IF NOT EXISTS alert_samples(
      ts INTEGER NOT NULL,
      toma TEXT,
      seq INTEGER,
      reason TEXT,
      potencia_w REAL,
      rssi INTEGER,
      raw_json TEXT
    );
    """)
    con.execute("CREATE INDEX IF NOT EXISTS idx_alert_ts ON alert_samples(ts);")
    con.execute("CREATE INDEX IF NOT EXISTS idx_alert_toma_ts ON alert_samples(toma, ts);")

    con.execute("""
    CREATE TABLE IF NOT EXISTS pi_samples(
      ts INTEGER NOT NULL,
      cpu_pct REAL,
      ram_used_gb REAL,
      ram_total_gb REAL,
      temp_c REAL,
      uptime_s INTEGER
    );
    """)
    con.execute("CREATE INDEX IF NOT EXISTS idx_pi_ts ON pi_samples(ts);")

    con.execute("""
    CREATE TABLE IF NOT EXISTS net_samples(
      ts INTEGER NOT NULL,
      hi_mbps REAL,
      med_mbps REAL,
      low_mbps REAL,
      cap_mbps REAL,
      qos_src TEXT
    );
    """)
    con.execute("CREATE INDEX IF NOT EXISTS idx_net_ts ON net_samples(ts);")

def _rows(con: sqlite3.Connection, sql: str, params: Tuple[Any,...]=()) -> List[Dict[str,Any]]:
    cur = con.execute(sql, params)
    return [dict(r) for r in cur.fetchall()]

def _one(con: sqlite3.Connection, sql: str, params: Tuple[Any,...]=()) -> Dict[str,Any]:
    cur = con.execute(sql, params)
    r = cur.fetchone()
    return dict(r) if r else {}

def build_snapshot(max_rows_per_table: int = 60, toma_points: int = 60, ttl_sec: int = 10) -> Dict[str, Any]:
    now = int(time.time())
    db = _db_path()
    out: Dict[str, Any] = {
        "now_ts": now,
        "ttl_sec": int(ttl_sec),
        "db_path": db,
        "tables": {},
        "derived": {},
        # ✅ ahora también existirá out["alerts"] cuando haya DB
        "alerts": {"count_120s": 0, "series": []},
    }

    if not os.path.exists(db):
        return out

    con = _connect(db)
    try:
        ensure_schema(con)

        # Latest rows per table
        out["tables"]["pi_samples"] = {
            "latest": _rows(con, "SELECT * FROM pi_samples ORDER BY ts DESC LIMIT 1;")
        }
        out["tables"]["net_samples"] = {
            "latest": _rows(con, "SELECT * FROM net_samples ORDER BY ts DESC LIMIT 1;"),
            "series": _rows(con, "SELECT * FROM net_samples ORDER BY ts DESC LIMIT ?;", (max_rows_per_table,))[::-1]
        }

        # Tomas: latest per toma + series per toma
        tomas = _rows(con, "SELECT DISTINCT toma FROM toma_samples;")
        toma_names = sorted([t["toma"] for t in tomas if t.get("toma")])

        latest_map: Dict[str, Dict[str, Any]] = {}
        for tn in toma_names:
            r = _one(con, """
              SELECT * FROM toma_samples
              WHERE toma=?
              ORDER BY ts DESC
              LIMIT 1;
            """, (tn,))
            if r:
                latest_map[tn] = r

        series_per_toma: Dict[str, List[Dict[str,Any]]] = {}
        for tn in toma_names:
            pts = _rows(con, """
              SELECT ts, toma, amperaje, potencia_w, estado, rssi
              FROM toma_samples
              WHERE toma=?
              ORDER BY ts DESC
              LIMIT ?;
            """, (tn, toma_points))[::-1]
            series_per_toma[tn] = pts

        out["tables"]["toma_samples"] = {
            "latest_per_toma": latest_map,
            "series_per_toma": series_per_toma,
        }

        # Derived TTL ONLINE map
        ttl = int(ttl_sec)
        by_ttl: Dict[str, Dict[str, Any]] = {}
        for tn, r in latest_map.items():
            ts = int(r.get("ts") or 0)
            age = now - ts if ts else 10**9
            by_ttl[tn] = {
                "ts": ts,
                "age_sec": int(age),
                "online": 1 if age <= ttl else 0,
                "amperaje": r.get("amperaje"),
                "potencia_w": r.get("potencia_w"),
                "estado": r.get("estado"),
                "rssi": r.get("rssi"),
            }
        out["derived"]["tomas_by_ttl"] = by_ttl

        # ✅ Alerts: count last 120s + series (alerts/seg)
        win = 120
        c = _one(con, "SELECT COUNT(*) AS c FROM alert_samples WHERE ts >= ?;", (now-win,))
        count_120 = int(c.get("c") or 0)

        series = _rows(con, """
          SELECT ts, COUNT(*) AS n
          FROM alert_samples
          WHERE ts >= ?
          GROUP BY ts
          ORDER BY ts ASC;
        """, (now-win,))

        # ✅ lo que esperan dashboard_server + HTML
        out["alerts"] = {"count_120s": count_120, "series": series}

        # (opcional) mantener también por tablas
        out["tables"]["alert_samples"] = {"count_120s": count_120, "series_120s": series}

    finally:
        con.close()

    return out

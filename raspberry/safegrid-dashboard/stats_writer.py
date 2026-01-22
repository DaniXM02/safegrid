#!/usr/bin/env python3
import os, re, time, sqlite3, subprocess
from typing import Dict, Tuple

DB = os.getenv("SAFEGRID_DB_PATH", "/home/teleadmin/safegrid-dashboard/safegrid.db")
CAP = float(os.getenv("SAFEGRID_CAP_MBPS", "20"))
DEV = os.getenv("SAFEGRID_TC_DEV", os.getenv("SAFEGRID_WLAN_IFACE", "wlan1"))
CLASS_HIGH = os.getenv("SAFEGRID_CLASS_HIGH", "1:10")
CLASS_MED  = os.getenv("SAFEGRID_CLASS_MED",  "1:20")
CLASS_LOW  = os.getenv("SAFEGRID_CLASS_LOW",  "1:30")
QOS_SRC = "mqtt"

def sh(cmd, timeout=1.5) -> str:
    try:
        p = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
        return (p.stdout or "") if p.returncode == 0 else ""
    except Exception:
        return ""

def temp_c() -> float:
    # prefer sysfs
    try:
        with open("/sys/class/thermal/thermal_zone0/temp","r") as f:
            return float(f.read().strip())/1000.0
    except Exception:
        pass
    out = sh(["vcgencmd","measure_temp"])
    m = re.search(r"temp=([0-9.]+)", out)
    return float(m.group(1)) if m else 0.0

def mem_gb() -> Tuple[float,float]:
    mt = mu = 0.0
    try:
        info = {}
        with open("/proc/meminfo","r") as f:
            for line in f:
                k,v = line.split(":",1)
                info[k.strip()] = int(v.strip().split()[0]) # kB
        mt = info.get("MemTotal",0)/1024/1024
        free = info.get("MemAvailable",0)/1024/1024
        mu = mt - free
    except Exception:
        pass
    return (mu, mt)

_prev_cpu = None
def cpu_pct() -> float:
    global _prev_cpu
    try:
        with open("/proc/stat","r") as f:
            parts = f.readline().split()
        vals = list(map(int, parts[1:]))  # user nice system idle iowait irq softirq steal ...
        idle = vals[3] + (vals[4] if len(vals)>4 else 0)
        total = sum(vals)
        if _prev_cpu is None:
            _prev_cpu = (total, idle)
            return 0.0
        pt, pi = _prev_cpu
        dt = total - pt
        di = idle - pi
        _prev_cpu = (total, idle)
        if dt <= 0: return 0.0
        return max(0.0, min(100.0, (dt - di) / dt * 100.0))
    except Exception:
        return 0.0

def uptime_s() -> int:
    try:
        with open("/proc/uptime","r") as f:
            return int(float(f.read().split()[0]))
    except Exception:
        return 0

def ensure_schema(con: sqlite3.Connection) -> None:
    con.execute("PRAGMA journal_mode=WAL;")
    con.execute("PRAGMA synchronous=NORMAL;")
    con.execute("""
    CREATE TABLE IF NOT EXISTS pi_samples(
      ts INTEGER NOT NULL,
      cpu_pct REAL,
      ram_used_gb REAL,
      ram_total_gb REAL,
      temp_c REAL,
      uptime_s INTEGER
    );""")
    con.execute("CREATE INDEX IF NOT EXISTS idx_pi_ts ON pi_samples(ts);")
    con.execute("""
    CREATE TABLE IF NOT EXISTS net_samples(
      ts INTEGER NOT NULL,
      hi_mbps REAL,
      med_mbps REAL,
      low_mbps REAL,
      cap_mbps REAL,
      qos_src TEXT
    );""")
    con.execute("CREATE INDEX IF NOT EXISTS idx_net_ts ON net_samples(ts);")

_rx = {}  # classid -> last_bytes
_last_t = None

def _parse_tc_bytes(tc_out: str) -> Dict[str,int]:
    # finds: class htb 1:10 ... Sent 123456 bytes 789 pkt ...
    m = {}
    for line in tc_out.splitlines():
        if "class htb" in line:
            cid = line.split()[2].strip()
        if "Sent " in line and " bytes" in line:
            mm = re.search(r"Sent\s+(\d+)\s+bytes", line)
            if mm:
                m[cid] = int(mm.group(1))
    return m

def net_mbps() -> Tuple[float,float,float]:
    global _rx, _last_t
    now = time.time()
    tc = sh(["tc","-s","class","show","dev",DEV])
    if not tc.strip():
        # No tc: fake split (all in low)
        return (0.0,0.0,0.0)
    b = _parse_tc_bytes(tc)
    if _last_t is None:
        _last_t = now
        _rx = b
        return (0.0,0.0,0.0)
    dt = max(0.2, now - _last_t)
    _last_t = now

    def calc(cid: str) -> float:
        cur = b.get(cid, 0)
        prev = _rx.get(cid, cur)
        _rx[cid] = cur
        dbytes = max(0, cur - prev)
        return (dbytes * 8.0 / dt) / 1e6  # Mbps

    return (calc(CLASS_HIGH), calc(CLASS_MED), calc(CLASS_LOW))

def main():
    os.makedirs(os.path.dirname(DB), exist_ok=True)
    con = sqlite3.connect(DB, timeout=5.0, isolation_level=None)
    try:
        ensure_schema(con)
        while True:
            ts = int(time.time())

            # pi
            c = cpu_pct()
            ru, rt = mem_gb()
            t = temp_c()
            up = uptime_s()
            con.execute(
                "INSERT INTO pi_samples(ts,cpu_pct,ram_used_gb,ram_total_gb,temp_c,uptime_s) VALUES (?,?,?,?,?,?);",
                (ts, float(c), float(ru), float(rt), float(t), int(up))
            )

            # net
            hi, med, low = net_mbps()
            con.execute(
                "INSERT INTO net_samples(ts,hi_mbps,med_mbps,low_mbps,cap_mbps,qos_src) VALUES (?,?,?,?,?,?);",
                (ts, float(hi), float(med), float(low), float(CAP), QOS_SRC)
            )

            # limpieza simple (30 min)
            cutoff = ts - 1800
            con.execute("DELETE FROM pi_samples WHERE ts < ?;", (cutoff,))
            con.execute("DELETE FROM net_samples WHERE ts < ?;", (cutoff,))

            time.sleep(1.0)
    finally:
        con.close()

if __name__ == "__main__":
    main()

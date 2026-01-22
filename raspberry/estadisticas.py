#!/usr/bin/env python3
import time
import subprocess
import json
import os
import re
import threading

import paho.mqtt.client as mqtt

BROKER = "192.168.4.1"

# Publicación (lo que ya tenías)
PUB_PORT = "1883"
MQTT_USER = "control"
MQTT_PASS = "user1234"

TOPIC_PI  = "safegrid/pi/telemetry"
TOPIC_NET = "safegrid/ap/net"

# Suscripción (para escuchar a tu XIAO)
SUB_PORT = 1883
SUB_USER = "control"
SUB_PASS = "user1234"

TOPIC_TOMA1_TELEM  = "safegrid/toma1/telemetry"
TOPIC_TOMA1_ALERT  = "safegrid/toma1/alert"
TOPIC_TOMA1_STATUS = "safegrid/toma1/status"

IFACE = "wlan1"
INTERVAL = 1.0

# Capacidad (para el %).
CAP_Mbps = 20.0

# Si usas tc/htb, pon aquí tus classid reales:
CLASS_HIGH = "1:10"
CLASS_MED  = "1:20"
CLASS_LOW  = "1:30"

LEASE_FILES = [
    "/var/lib/misc/dnsmasq.leases",
    "/var/lib/dnsmasq/dnsmasq.leases",
]

# ========== Estado “último dato” + contadores (XIAO) ==========
_latest_lock = threading.Lock()
latest_toma1 = {
    "telemetry": None,
    "alert": None,
    "status": None,
    "ts": None,   # time.time() cuando llegó algo

    # bytes por ventana (para graficar ALTA/MEDIA aunque no tengas tc)
    "bytes_telem": 0,
    "bytes_alert": 0,
    "bytes_status": 0,
}

def _on_connect(client, userdata, flags, rc):
    if rc == 0:
        print("[MQTT-SUB] ✅ conectado")
        client.subscribe(TOPIC_TOMA1_TELEM, qos=0)
        client.subscribe(TOPIC_TOMA1_ALERT, qos=0)
        client.subscribe(TOPIC_TOMA1_STATUS, qos=0)
        print(f"[MQTT-SUB] 📌 subscribed {TOPIC_TOMA1_TELEM}")
        print(f"[MQTT-SUB] 📌 subscribed {TOPIC_TOMA1_ALERT}")
        print(f"[MQTT-SUB] 📌 subscribed {TOPIC_TOMA1_STATUS}")
    else:
        print(f"[MQTT-SUB] ❌ connect rc={rc}")

def _on_message(client, userdata, msg):
    raw = msg.payload  # bytes
    payload = raw.decode("utf-8", errors="replace")

    # intenta JSON si aplica
    try:
        data = json.loads(payload)
    except Exception:
        data = {"raw": payload}

    with _latest_lock:
        # contabilidad para gráfica (bytes/seg aprox)
        nbytes = len(raw)

        if msg.topic == TOPIC_TOMA1_TELEM:
            latest_toma1["telemetry"] = data
            latest_toma1["bytes_telem"] += nbytes
        elif msg.topic == TOPIC_TOMA1_ALERT:
            latest_toma1["alert"] = data
            latest_toma1["bytes_alert"] += nbytes
        elif msg.topic == TOPIC_TOMA1_STATUS:
            latest_toma1["status"] = payload.strip()
            latest_toma1["bytes_status"] += nbytes

        latest_toma1["ts"] = time.time()

def start_mqtt_subscriber():
    c = mqtt.Client(client_id=f"estadisticas-sub-{int(time.time())}")
    c.username_pw_set(SUB_USER, SUB_PASS)
    c.on_connect = _on_connect
    c.on_message = _on_message
    c.reconnect_delay_set(min_delay=1, max_delay=10)
    c.connect(BROKER, SUB_PORT, keepalive=30)
    c.loop_start()
    return c

# ========== utilidades sistema ==========
def read_float(path, scale=1.0):
    with open(path, "r") as f:
        return float(f.read().strip()) / scale

def cpu_temp_c():
    return read_float("/sys/class/thermal/thermal_zone0/temp", 1000.0)

def uptime_s():
    with open("/proc/uptime", "r") as f:
        return int(float(f.read().split()[0]))

def cpu_usage_total_idle():
    with open("/proc/stat","r") as f:
        parts = f.readline().split()
    vals = list(map(int, parts[1:8]))
    idle = vals[3] + vals[4]
    total = sum(vals)
    return total, idle

def mem_used_total_gb():
    meminfo = {}
    with open("/proc/meminfo","r") as f:
        for line in f:
            k, v, _ = line.split()
            meminfo[k.rstrip(":")] = int(v)  # kB
    total_kb = meminfo.get("MemTotal", 0)
    avail_kb = meminfo.get("MemAvailable", meminfo.get("MemFree", 0))
    used_kb = max(total_kb - avail_kb, 0)
    total_gb = total_kb / (1024*1024)
    used_gb  = used_kb  / (1024*1024)
    return used_gb, total_gb

def net_bytes(iface):
    rx = read_float(f"/sys/class/net/{iface}/statistics/rx_bytes")
    tx = read_float(f"/sys/class/net/{iface}/statistics/tx_bytes")
    return rx, tx

def iw_macs(iface):
    try:
        out = subprocess.check_output(
            ["bash","-lc", f"iw dev {iface} station dump | grep '^Station' | awk '{{print $2}}'"],
            text=True
        )
        return [x.strip().lower() for x in out.splitlines() if x.strip()]
    except Exception:
        return []

def read_dnsmasq_leases():
    for lf in LEASE_FILES:
        if os.path.exists(lf):
            leases = {}
            with open(lf, "r") as f:
                for line in f:
                    parts = line.strip().split()
                    if len(parts) >= 4:
                        mac = parts[1].lower()
                        host = parts[3]
                        leases[mac] = host
            return leases
    return {}

def clients_list(iface, maxn=9):
    macs = iw_macs(iface)
    leases = read_dnsmasq_leases()
    names = []
    for mac in macs[:maxn]:
        host = leases.get(mac, "")
        if host and host != "*":
            names.append(host[:15])
        else:
            names.append("MAC-" + mac.replace(":","")[-4:])
    return names

def mosquitto_pub(topic, payload):
    cmd = [
        "mosquitto_pub",
        "-h", BROKER, "-p", PUB_PORT,
        "-u", MQTT_USER, "-P", MQTT_PASS,
        "-t", topic, "-m", payload
    ]
    subprocess.run(cmd, capture_output=True, text=True)

# -------- tc parsing (bytes por clase) --------
_tc_re_class = re.compile(r"^class\s+\S+\s+(\d+:\d+)")
_tc_re_sent  = re.compile(r"Sent\s+(\d+)\s+bytes")

def tc_class_bytes(dev):
    """
    Retorna dict {classid: bytes_sent} leyendo `tc -s class show dev <dev>`
    Si no hay tc o no existen clases, retorna {}.
    """
    try:
        out = subprocess.check_output(["bash","-lc", f"tc -s class show dev {dev}"], text=True)
    except Exception:
        return {}

    cur = None
    sent = {}
    for line in out.splitlines():
        m = _tc_re_class.search(line)
        if m:
            cur = m.group(1)
            continue
        m2 = _tc_re_sent.search(line)
        if m2 and cur:
            sent[cur] = int(m2.group(1))
            cur = None
    return sent

def main():
    _sub = start_mqtt_subscriber()

    # init CPU%
    t0_total, t0_idle = cpu_usage_total_idle()

    # init NET
    rx0, tx0 = net_bytes(IFACE)

    # init tc bytes
    last_tc = tc_class_bytes(IFACE)
    last_hi = last_tc.get(CLASS_HIGH, None)
    last_med = last_tc.get(CLASS_MED, None)
    last_low = last_tc.get(CLASS_LOW, None)

    while True:
        time.sleep(INTERVAL)

        # CPU%
        t1_total, t1_idle = cpu_usage_total_idle()
        dt_total = (t1_total - t0_total)
        dt_idle  = (t1_idle  - t0_idle)
        cpu_pct = 0.0 if dt_total <= 0 else (100.0 * (1.0 - (dt_idle / dt_total)))
        t0_total, t0_idle = t1_total, t1_idle

        # RAM
        used_gb, total_gb = mem_used_total_gb()

        # NET total (rx+tx)
        rx1, tx1 = net_bytes(IFACE)
        drx = rx1 - rx0
        dtx = tx1 - tx0
        rx0, tx0 = rx1, tx1
        down_mbps = (drx * 8.0) / (INTERVAL * 1_000_000.0)
        up_mbps   = (dtx * 8.0) / (INTERVAL * 1_000_000.0)
        total_mbps = max(down_mbps + up_mbps, 0.0)

        # === 1) Intento por tc (si existe) ===
        cur_tc = tc_class_bytes(IFACE)
        hi_tc = med_tc = low_tc = None

        def rate_mbps(cur, prev):
            if cur is None or prev is None:
                return None
            d = cur - prev
            if d < 0:
                return None
            return (d * 8.0) / (INTERVAL * 1_000_000.0)

        if cur_tc:
            cur_hi  = cur_tc.get(CLASS_HIGH, None)
            cur_med = cur_tc.get(CLASS_MED, None)
            cur_low = cur_tc.get(CLASS_LOW, None)

            hi_tc  = rate_mbps(cur_hi,  last_hi)
            med_tc = rate_mbps(cur_med, last_med)
            low_tc = rate_mbps(cur_low, last_low)

            last_hi, last_med, last_low = cur_hi, cur_med, cur_low

        # === 2) Siempre calculo por MQTT (para tu gráfica ALTA/MEDIA) ===
        with _latest_lock:
            t = latest_toma1["telemetry"] or {}
            a = latest_toma1["alert"] or {}
            st = latest_toma1["status"]
            ts = latest_toma1["ts"]

            b_telem = latest_toma1["bytes_telem"]
            b_alert = latest_toma1["bytes_alert"]

            # reset ventana
            latest_toma1["bytes_telem"] = 0
            latest_toma1["bytes_alert"] = 0

        telem_mbps = (b_telem * 8.0) / (INTERVAL * 1_000_000.0)
        alert_mbps = (b_alert * 8.0) / (INTERVAL * 1_000_000.0)

        # === Selección de fuente para la gráfica ===
        # Si tc está realmente midiendo (valores no-None), úsalo.
        # Si no, usa MQTT (lo que tú quieres visualizar).
        use_tc = (hi_tc is not None) or (med_tc is not None) or (low_tc is not None)

        if use_tc:
            hi_mbps  = hi_tc  if hi_tc  is not None else 0.0
            med_mbps = med_tc if med_tc is not None else 0.0
            low_mbps = low_tc if low_tc is not None else max(total_mbps - hi_mbps - med_mbps, 0.0)
            qos_src = "tc"
        else:
            hi_mbps  = alert_mbps
            med_mbps = telem_mbps
            low_mbps = max(total_mbps - hi_mbps - med_mbps, 0.0)
            qos_src = "mqtt"

        clist = clients_list(IFACE, maxn=9)

        age_s = None
        if ts is not None:
            age_s = round(time.time() - ts, 1)

        # Resumen toma1 (debug, por si luego quieres mostrarlo en net)
        toma1_summary = {
            "age_s": age_s,
            "status": st,
            "telem": {
                "id": t.get("id"),
                "toma": t.get("toma"),
                "on": t.get("on"),
                "seq": t.get("seq"),
                "ms": t.get("ms"),
                "amperaje": t.get("amperaje"),
                "potencia_w": t.get("potencia_w"),
                "estado": t.get("estado"),
                "rssi": t.get("rssi"),
                "sim": t.get("sim"),
            },
            "alert": {
                "id": a.get("id"),
                "toma": a.get("toma"),
                "on": a.get("on"),
                "seq": a.get("seq"),
                "ms": a.get("ms"),
                "amperaje": a.get("amperaje"),
                "potencia_w": a.get("potencia_w"),
                "estado": a.get("estado"),
                "rssi": a.get("rssi"),
                "reason": a.get("reason"),
                "sim": a.get("sim"),
            },
            "rates_mbps": {
                "telem_mbps": round(telem_mbps, 4),
                "alert_mbps": round(alert_mbps, 4),
            }
        }

        payload_pi = {
            "temp_c": round(cpu_temp_c(), 2),
            "cpu_pct": round(cpu_pct, 1),
            "ram_used_gb": round(used_gb, 2),
            "ram_total_gb": round(total_gb, 2),
            "uptime_s": uptime_s(),
        }

        payload_net = {
            "cap_mbps": CAP_Mbps,
            "hi_mbps": round(hi_mbps, 4),
            "med_mbps": round(med_mbps, 4),
            "low_mbps": round(low_mbps, 4),
            "qos_src": qos_src,              # "mqtt" o "tc"
            "clients_list": clist,
            "toma1": toma1_summary,
        }

        mosquitto_pub(TOPIC_PI, json.dumps(payload_pi))
        mosquitto_pub(TOPIC_NET, json.dumps(payload_net))

if __name__ == "__main__":
    main()


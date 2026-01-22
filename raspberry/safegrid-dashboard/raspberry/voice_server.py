#!/usr/bin/env python3
import io
import os
import json
import time
import wave
import subprocess
import threading
import unicodedata
import tempfile
from datetime import datetime
from typing import Dict, Any, Optional

from flask import Flask, request, jsonify
from vosk import Model, KaldiRecognizer
import paho.mqtt.client as mqtt

# =========================
# OpenAI (Responses API)
# =========================
try:
    from openai import OpenAI
except Exception:
    OpenAI = None  # type: ignore

# =========================
# CONFIG
# =========================
MODEL_PATH = "/home/teleadmin/vosk-model-es-0.42"
SAMPLE_RATE = 16000

# Mosquitto (Pi AP)
BROKER = "192.168.4.1"
MQTT_USER = "control"
MQTT_PASS = "user1234"

# Puertos (si quieres 2 listeners)
MQTT_TELEM_PORT = 1883
MQTT_ALERT_PORT = 1884

# Topics base
TOPIC_PI = "safegrid/pi/telemetry"
TOPIC_NET = "safegrid/ap/net"
TOPIC_TOMA_TEL = "safegrid/+/telemetry"
TOPIC_TOMA_ALERT = "safegrid/+/alert"
TOPIC_TOMA_STATUS = "safegrid/+/status"

# Assistant topics (para logs/visualización)
TOPIC_ASSIST_Q = "safegrid/assistant/q"
TOPIC_ASSIST_A = "safegrid/assistant/a"

# Bluetooth (Echo Dot)
ECHO_MAC = "B4:B7:42:42:F1:B3"
BT_CONNECT_SCRIPT = "/usr/local/sbin/bt-connect-echo.sh"
ALSA_BLUEALSA_DEV = f"bluealsa:DEV={ECHO_MAC},PROFILE=a2dp"
APLAY_BIN = "/usr/bin/aplay"

# TTS (espeak-ng) - hombre latino
TTS_VOICE = "es-419+m3"
TTS_SPEED = 165
TTS_PITCH = 45
TTS_GAIN = 120
TTS_GAP_MS = 8

# Debug
DEBUG = os.getenv("SAFEGRID_DEBUG", "0") == "1"

# OpenAI (NO pegues la key real aquí)
# Ponla por env: OPENAI_API_KEY=...
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "danielKey")  # <-- reemplaza por env, no por código
OPENAI_MODEL = os.getenv("OPENAI_MODEL", "gpt-5.2")

_ai_client: Optional[Any] = None
if OpenAI and OPENAI_API_KEY and OPENAI_API_KEY != "danielKey":
    try:
        _ai_client = OpenAI(api_key=OPENAI_API_KEY)
    except Exception:
        _ai_client = None

# =========================
# ESTADO (cache MQTT)
# =========================
state_lock = threading.Lock()
state: Dict[str, Any] = {
    "tomas": {
        "toma1": {"online": False, "amperaje": None, "potencia_w": None, "estado": "SIN_DATO", "last_alert": None, "ts": 0},
        "toma2": {"online": False, "amperaje": None, "potencia_w": None, "estado": "SIN_DATO", "last_alert": None, "ts": 0},
        "toma3": {"online": False, "amperaje": None, "potencia_w": None, "estado": "SIN_DATO", "last_alert": None, "ts": 0},
    },
    "pi": {"temp_c": None, "cpu_pct": None, "ram_used_gb": None, "ram_total_gb": None, "ram_pct": None, "uptime_s": None, "ts": 0},
    "net": {"hi_mbps": None, "med_mbps": None, "low_mbps": None, "cap_mbps": None, "clients_list": [], "qos_src": None, "ts": 0},
}

# =========================
# UTILS
# =========================
def log(msg: str) -> None:
    if not DEBUG:
        return
    ts = datetime.now().strftime("%H:%M:%S")
    print(f"[{ts}] {msg}", flush=True)

def _run_quiet(cmd: list[str]) -> int:
    try:
        p = subprocess.run(cmd, check=False, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        return int(p.returncode or 0)
    except Exception:
        return 1

def mosquitto_pub(topic: str, payload: str, port: int = MQTT_TELEM_PORT) -> None:
    """Publica usando mosquitto_pub (rápido y simple)."""
    try:
        subprocess.run(
            [
                "mosquitto_pub",
                "-h", BROKER,
                "-p", str(port),
                "-u", MQTT_USER,
                "-P", MQTT_PASS,
                "-t", topic,
                "-m", payload,
            ],
            check=False,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            timeout=4,
        )
    except Exception:
        pass

# =========================
# Bluetooth TTS por BlueALSA (ALSA directo) con interrupción
# =========================
_tts_lock = threading.Lock()
_tts_proc: Optional[subprocess.Popen] = None

def bt_is_connected() -> bool:
    try:
        out = subprocess.check_output(
            ["bluetoothctl", "info", ECHO_MAC],
            text=True,
            stderr=subprocess.DEVNULL
        )
        return "Connected: yes" in out
    except Exception:
        return False

def ensure_bt_connected(retries: int = 6, wait_s: float = 2.0) -> bool:
    """Asegura conexión BT para que BlueALSA tenga el PCM."""
    if bt_is_connected():
        return True

    for i in range(retries):
        log(f"BT: no conectado, intento {i+1}/{retries}")

        if os.path.exists(BT_CONNECT_SCRIPT) and os.access(BT_CONNECT_SCRIPT, os.X_OK):
            rc = _run_quiet([BT_CONNECT_SCRIPT])
            if rc != 0:
                _run_quiet(["sudo", "-n", BT_CONNECT_SCRIPT])
        else:
            rc = _run_quiet(["timeout", "8", "bluetoothctl", "connect", ECHO_MAC])
            if rc != 0:
                _run_quiet(["sudo", "-n", "timeout", "8", "bluetoothctl", "connect", ECHO_MAC])

        time.sleep(wait_s)

        if bt_is_connected():
            log("BT: conectado OK")
            time.sleep(0.8)
            return True

        _run_quiet(["timeout", "5", "bluetoothctl", "disconnect", ECHO_MAC])
        time.sleep(0.5)

    log("BT: NO se pudo conectar")
    return False

def tts_stop_now():
    global _tts_proc
    with _tts_lock:
        if _tts_proc and _tts_proc.poll() is None:
            try:
                log("TTS: terminando audio anterior")
                _tts_proc.terminate()
            except Exception:
                pass
        _tts_proc = None

def _cleanup_when_done(proc: subprocess.Popen, wav_path: str) -> None:
    try:
        proc.wait(timeout=30)
    except Exception:
        pass
    try:
        os.remove(wav_path)
    except Exception:
        pass

def speak_es(text: str) -> None:
    """Habla SOLO por el Echo Dot usando BlueALSA (aplay). Interrumpible."""
    if not text:
        return

    tts_stop_now()

    if not ensure_bt_connected():
        return

    fd, wav_path = tempfile.mkstemp(prefix="safegrid_tts_", suffix=".wav")
    os.close(fd)

    try:
        subprocess.run(
            [
                "espeak-ng",
                "-v", TTS_VOICE,
                "-s", str(TTS_SPEED),
                "-p", str(TTS_PITCH),
                "-a", str(TTS_GAIN),
                "-g", str(TTS_GAP_MS),
                "-w", wav_path,
                "--",
                text
            ],
            check=False,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            timeout=20
        )
    except Exception:
        try:
            os.remove(wav_path)
        except Exception:
            pass
        return

    cmd = [APLAY_BIN, "-q", "-D", ALSA_BLUEALSA_DEV, wav_path]

    global _tts_proc
    try:
        proc = subprocess.Popen(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        with _tts_lock:
            _tts_proc = proc
        threading.Thread(target=_cleanup_when_done, args=(proc, wav_path), daemon=True).start()

        time.sleep(0.18)
        if proc.poll() is not None and (proc.returncode or 0) != 0:
            log(f"TTS: aplay falló rápido rc={proc.returncode}, reintentando")
            ensure_bt_connected(retries=3, wait_s=2.0)
            proc2 = subprocess.Popen(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            with _tts_lock:
                _tts_proc = proc2
            threading.Thread(target=_cleanup_when_done, args=(proc2, wav_path), daemon=True).start()

    except Exception:
        try:
            os.remove(wav_path)
        except Exception:
            pass

# =========================
# MQTT subscriber threads
# =========================
def extract_toma(topic: str) -> Optional[str]:
    parts = topic.split("/")
    if len(parts) >= 3 and parts[0] == "safegrid":
        name = parts[1].strip()
        if name in ("toma1", "toma2", "toma3"):
            return name
    return None

def on_connect_factory(subs: list[tuple[str, int]]):
    def _on_connect(client, userdata, flags, rc):
        if rc == 0:
            for t, qos in subs:
                client.subscribe(t, qos=qos)
            log(f"MQTT: conectado OK, subs={len(subs)}")
        else:
            log(f"MQTT: connect rc={rc}")
    return _on_connect

def on_message(client, userdata, msg):
    t = msg.topic
    payload = msg.payload.decode("utf-8", errors="ignore").strip()
    now = int(time.time())

    with state_lock:
        if t == TOPIC_PI:
            try:
                d = json.loads(payload)
            except Exception:
                return
            s = state["pi"]
            for k in ("temp_c", "cpu_pct", "ram_used_gb", "ram_total_gb", "ram_pct", "uptime_s"):
                if k in d:
                    s[k] = d[k]
            s["ts"] = now
            return

        if t == TOPIC_NET:
            try:
                d = json.loads(payload)
            except Exception:
                return
            s = state["net"]
            for k in ("hi_mbps", "med_mbps", "low_mbps", "cap_mbps", "qos_src"):
                if k in d:
                    s[k] = d[k]
            if "clients_list" in d and isinstance(d["clients_list"], list):
                s["clients_list"] = d["clients_list"][:12]
            s["ts"] = now
            return

        toma = extract_toma(t)
        if not toma:
            return

        if t.endswith("/status"):
            st = state["tomas"][toma]
            if payload == "online":
                st["online"] = True
            elif payload == "offline":
                st["online"] = False
            st["ts"] = now
            return

        if t.endswith("/telemetry"):
            try:
                d = json.loads(payload)
            except Exception:
                return
            st = state["tomas"][toma]
            if "amperaje" in d:
                st["amperaje"] = d["amperaje"]
            if "potencia_w" in d:
                st["potencia_w"] = d["potencia_w"]
            if "estado" in d:
                st["estado"] = d["estado"]
            st["ts"] = now
            return

        if t.endswith("/alert"):
            try:
                d = json.loads(payload)
            except Exception:
                d = {"raw": payload}
            st = state["tomas"][toma]
            st["last_alert"] = d
            st["ts"] = now
            return

def mqtt_worker(name: str, port: int, subs: list[tuple[str, int]]):
    """Hilo MQTT con reconnect."""
    cid = f"safegrid-voice-{name}-{int(time.time())}"
    c = mqtt.Client(client_id=cid)
    c.username_pw_set(MQTT_USER, MQTT_PASS)
    c.on_connect = on_connect_factory(subs)
    c.on_message = on_message
    c.reconnect_delay_set(min_delay=1, max_delay=10)

    while True:
        try:
            c.connect(BROKER, port, keepalive=30)
            c.loop_forever()
        except Exception:
            time.sleep(1.5)

# =========================
# NLU por keywords
# =========================
def norm(s: str) -> str:
    s = (s or "").lower().strip()
    s = "".join(ch for ch in unicodedata.normalize("NFD", s) if unicodedata.category(ch) != "Mn")
    return s

def has_wake(t: str) -> bool:
    toks = t.split()
    if "safe" in toks:
        return True
    for alt in ("seif", "saf", "seifgrid", "safegrid"):
        if alt in toks or alt in t:
            return True
    return False

def pick_intent(text: str) -> str:
    t = norm(text)

    if not has_wake(t):
        return "no_wake"

    # Diagnóstico (AI)
    if any(k in t for k in ("jitter", "latencia", "delay", "cola", "colas", "saturada", "saturado", "perdida", "perdidas", "loss", "drop", "drops")):
        return "diagnostico"

    if any(k in t for k in ("temperatura", "temp", "cpu", "ram", "raspberry", "procesador", "memoria")):
        return "temperatura"
    if any(k in t for k in ("red", "wifi", "clientes", "mbps", "qos", "trafico", "velocidad")):
        return "red"
    if any(k in t for k in ("toma", "tomas", "enchufe", "estado", "corriente", "potencia", "amper", "vatios", "alerta")):
        return "estado"

    return "help"

def reply_for(intent: str) -> str:
    if intent == "no_wake":
        return "No escuché la palabra Safe. Di: hola Safe, y tu pregunta."

    if intent == "estado":
        with state_lock:
            tomas = {k: v.copy() for k, v in state["tomas"].items()}

        parts = []
        for k in ("toma1", "toma2", "toma3"):
            d = tomas[k]
            online = "en linea" if d["online"] else "fuera de linea"
            est = d.get("estado") or "SIN_DATO"
            amp = d.get("amperaje")
            pw = d.get("potencia_w")
            if d["online"] and amp is not None and pw is not None:
                parts.append(f"{k}: {online}, {amp:.2f} amperios, {pw:.0f} vatios, {est}.")
            else:
                parts.append(f"{k}: {online}, {est}.")
        return "Resumen. " + " ".join(parts)

    if intent == "red":
        with state_lock:
            net = state["net"].copy()

        n = len(net.get("clients_list") or [])
        hi = float(net.get("hi_mbps") or 0.0)
        med = float(net.get("med_mbps") or 0.0)
        low = float(net.get("low_mbps") or 0.0)
        total = hi + med + low
        cap = float(net.get("cap_mbps") or 20.0)
        pct = (total * 100.0 / cap) if cap > 0 else 0.0
        src = net.get("qos_src") or "desconocido"
        return f"Red. Hay {n} dispositivos. Uso total {total:.1f} megabits por segundo, {pct:.0f} por ciento del canal. Fuente {src}."

    if intent == "temperatura":
        with state_lock:
            pi = state["pi"].copy()

        temp = pi.get("temp_c")
        cpu = pi.get("cpu_pct")
        ru = pi.get("ram_used_gb")
        rt = pi.get("ram_total_gb")
        rp = pi.get("ram_pct")

        if temp is None or cpu is None:
            return "Aún no tengo datos de la Raspberry. Revisa el publicador MQTT de telemetría."

        ram_txt = ""
        if ru is not None and rt is not None:
            ram_txt = f" RAM {ru:.1f} de {rt:.1f} gigas."
        elif rp is not None:
            ram_txt = f" RAM {float(rp):.0f} por ciento."

        return f"Raspberry. Temperatura {float(temp):.1f} grados. CPU {float(cpu):.0f} por ciento.{ram_txt}"

    if intent == "help":
        return "Dime: temperatura, red, estado de tomas, o diagnóstico. Ejemplo: SafeGrid jitter."

    # diagnostico se maneja aparte
    return "Listo."

# =========================
# AI diagnóstico
# =========================
def ai_diagnose(question: str) -> str:
    if not _ai_client:
        return "La inteligencia no está activa. Revisa OPENAI_API_KEY y la librería openai."

    with state_lock:
        snapshot = {
            "pi": state["pi"].copy(),
            "net": state["net"].copy(),
            "tomas": {k: v.copy() for k, v in state["tomas"].items()},
            "ts": int(time.time())
        }

    instructions = (
        "Eres un asistente NOC para SafeGrid (red Wi-Fi AP + MQTT + QoS). "
        "Responde en español, muy concreto: 1) diagnóstico probable, 2) qué métrica lo evidencia, 3) 2 acciones inmediatas. "
        "Si propones comandos, que sean cortos (tc, ss, ping, iperf, mosquitto_sub). Máximo 6 líneas."
    )

    user_input = (
        f"Pregunta del operador: {question}\n\n"
        f"Snapshot de métricas (JSON):\n{json.dumps(snapshot, ensure_ascii=False)}"
    )

    try:
        resp = _ai_client.responses.create(
            model=OPENAI_MODEL,
            instructions=instructions,
            input=user_input,
            temperature=0.2,
            max_output_tokens=220,
            store=False
        )
        out = (getattr(resp, "output_text", "") or "").strip()
        return out if out else "No pude generar diagnóstico. Intenta otra frase."
    except Exception as e:
        return f"No pude consultar la inteligencia: {type(e).__name__}"

# =========================
# Flask STT API
# =========================
app = Flask(__name__)
model = Model(MODEL_PATH)

@app.get("/health")
def health():
    return jsonify(ok=True, model=MODEL_PATH, sr=SAMPLE_RATE, ai=bool(_ai_client), ai_model=OPENAI_MODEL)

@app.post("/tts_stop")
def tts_stop():
    tts_stop_now()
    return jsonify(ok=True)

@app.get("/wake")
def wake():
    speak_es("Hola. Soy SafeGrid. Dime: temperatura, red, tomas, o diagnóstico. Por ejemplo: SafeGrid jitter.")
    return jsonify(ok=True)

# ---- NUEVO: entrada por texto (sin micrófono) ----
@app.post("/text")
def text_in():
    d = request.get_json(silent=True) or {}
    text = (d.get("text") or "").strip().lower()
    if not text:
        return jsonify(ok=False, error="no_text"), 400

    mosquitto_pub(TOPIC_ASSIST_Q, text)

    intent = pick_intent(text)

    if intent == "diagnostico":
        speak_es("Dame un segundo. Estoy revisando las métricas.")

        def _job():
            ans = ai_diagnose(text)
            mosquitto_pub(TOPIC_ASSIST_A, ans)
            speak_es(ans)

        threading.Thread(target=_job, daemon=True).start()
        return jsonify(ok=True, text=text, intent=intent, reply="procesando", ts=int(time.time()))

    reply = reply_for(intent)
    mosquitto_pub(TOPIC_ASSIST_A, reply)
    speak_es(reply)
    return jsonify(ok=True, text=text, intent=intent, reply=reply, ts=int(time.time()))

@app.post("/stt")
def stt():
    data = request.get_data()
    if not data:
        speak_es("No recibí audio. Intenta de nuevo.")
        return jsonify(ok=False, error="no_audio"), 400

    try:
        wf = wave.open(io.BytesIO(data), "rb")
    except Exception:
        speak_es("Ese audio no lo pude leer. Intenta de nuevo.")
        return jsonify(ok=False, error="bad_wav"), 400

    if wf.getnchannels() != 1 or wf.getsampwidth() != 2:
        speak_es("Formato de audio incorrecto. Necesito mono, dieciséis bits.")
        return jsonify(ok=False, error="wav_format",
                       channels=wf.getnchannels(), sampwidth=wf.getsampwidth()), 400

    sr = wf.getframerate()
    if sr != SAMPLE_RATE:
        speak_es("Frecuencia incorrecta. Necesito dieciséis kilohercios.")
        return jsonify(ok=False, error="sample_rate", sr=sr, expected=SAMPLE_RATE), 400

    frames_total = wf.getnframes()
    duration_s = frames_total / float(SAMPLE_RATE) if SAMPLE_RATE else 0.0

    rec = KaldiRecognizer(model, SAMPLE_RATE)
    rec.SetWords(False)

    while True:
        buf = wf.readframes(4000)
        if not buf:
            break
        rec.AcceptWaveform(buf)

    res = json.loads(rec.FinalResult())
    text = (res.get("text") or "").strip().lower()

    if duration_s < 0.45:
        reply = "Te escuché muy corto. Mantén el botón un poquito más y repite."
        speak_es(reply)
        return jsonify(ok=True, text=text, intent="too_short", reply=reply, dur_s=duration_s, ts=int(time.time()))

    mosquitto_pub(TOPIC_ASSIST_Q, text)

    intent = pick_intent(text)

    if intent == "diagnostico":
        speak_es("Dame un segundo. Estoy revisando las métricas.")

        def _job():
            ans = ai_diagnose(text)
            mosquitto_pub(TOPIC_ASSIST_A, ans)
            speak_es(ans)

        threading.Thread(target=_job, daemon=True).start()
        return jsonify(ok=True, text=text, intent=intent, reply="procesando", dur_s=duration_s, ts=int(time.time()))

    reply = reply_for(intent)
    mosquitto_pub(TOPIC_ASSIST_A, reply)
    speak_es(reply)
    return jsonify(ok=True, text=text, intent=intent, reply=reply, dur_s=duration_s, ts=int(time.time()))

if __name__ == "__main__":
    # Thread MQTT telemetría (1883)
    threading.Thread(
        target=mqtt_worker,
        args=("telem", MQTT_TELEM_PORT, [(TOPIC_PI, 0), (TOPIC_NET, 0), (TOPIC_TOMA_TEL, 0), (TOPIC_TOMA_STATUS, 0)]),
        daemon=True
    ).start()

    # Thread MQTT alertas (1884)
    threading.Thread(
        target=mqtt_worker,
        args=("alert", MQTT_ALERT_PORT, [(TOPIC_TOMA_ALERT, 0)]),
        daemon=True
    ).start()

    app.run(host="0.0.0.0", port=7000)

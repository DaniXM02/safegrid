#!/usr/bin/env python3
import socket
import selectors
import time

# Puertos de demo (UDP)
PORTS = {
    5060: "SIP_demo",       # SIP típicamente UDP 5060
    7000: "VOICE_demo",     # RTP demo (puerto fijo para lab)
    7001: "VIDEO_demo",     # video demo
    1883: "MQTT_norm_demo", # demo (no MQTT real)
    1884: "MQTT_alert_demo" # demo (alerta)
}

# Mapeo DSCP para mostrar bonito
DSCP_NAMES = {
    48: "CS6",
    46: "EF",
    34: "AF41",
    24: "CS3",
    18: "AF21",
    8:  "CS1",
    0:  "BE"
}

def dscp_name(dscp: int) -> str:
    return DSCP_NAMES.get(dscp, f"DSCP{dscp}")

def make_sock(port: int) -> socket.socket:
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.bind(("0.0.0.0", port))

    # Pedir que el kernel nos entregue el campo TOS (donde va DSCP/ECN)
    # En Linux suele existir socket.IP_RECVTOS
    try:
        s.setsockopt(socket.IPPROTO_IP, socket.IP_RECVTOS, 1)
    except AttributeError:
        # fallback: en Linux suele ser 13, pero mejor avisar
        print("⚠️ Tu Python no expone socket.IP_RECVTOS; no podré leer TOS/DSCP.")
    return s

def recv_with_tos(s: socket.socket):
    # recvmsg permite leer control messages (cmsg) donde llega TOS
    data, ancdata, flags, addr = s.recvmsg(2048, 1024)

    tos = None
    for cmsg_level, cmsg_type, cmsg_data in ancdata:
        if cmsg_level == socket.IPPROTO_IP:
            # IP_TOS suele traer 1 byte
            if cmsg_type in (socket.IP_TOS, getattr(socket, "IP_RECVTOS", socket.IP_TOS)):
                tos = cmsg_data[0]
                break

    # Si no llegó por ancdata, intentamos getsockopt IP_TOS (no siempre sirve por paquete)
    if tos is None:
        try:
            tos = s.getsockopt(socket.IPPROTO_IP, socket.IP_TOS)
        except OSError:
            tos = -1

    return data, addr, tos

def main():
    sel = selectors.DefaultSelector()

    socks = []
    for port in PORTS:
        s = make_sock(port)
        s.setblocking(False)
        sel.register(s, selectors.EVENT_READ, data=port)
        socks.append(s)

    print("✅ DSCP demo server escuchando UDP en:")
    for p, name in PORTS.items():
        print(f"  - {p:4d}  {name}")

    print("\nTip Wireshark: filtro: ip.dsfield.dscp == 48 (CS6), 46 (EF), 34 (AF41), 24 (CS3)\n")

    while True:
        for key, _ in sel.select(timeout=1.0):
            s = key.fileobj
            port = key.data
            name = PORTS[port]

            data, addr, tos = recv_with_tos(s)
            now = time.strftime("%H:%M:%S")

            if tos >= 0:
                dscp = (tos & 0xFC) >> 2
                ecn  = (tos & 0x03)
                print(f"[{now}] {name:<14} from {addr[0]}:{addr[1]}  len={len(data):4d}  "
                      f"TOS=0x{tos:02X}  DSCP={dscp:2d}({dscp_name(dscp)})  ECN={ecn}  payload={data[:60]!r}")
            else:
                print(f"[{now}] {name:<14} from {addr[0]}:{addr[1]}  len={len(data):4d}  payload={data[:60]!r}")

if __name__ == "__main__":
    main()

#!/usr/bin/env bash
python3 - <<'PY'
import json, urllib.request
for port in (4040,4041,4042):
    try:
        with urllib.request.urlopen(f"http://127.0.0.1:{port}/api/tunnels", timeout=1.5) as r:
            d=json.loads(r.read().decode())
        t=d.get("tunnels") or []
        print(t[0]["public_url"] if t else "NO_TUNNEL")
        break
    except Exception:
        continue
else:
    print("NO_API")
PY

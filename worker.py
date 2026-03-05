import os
import time
import requests
from datetime import datetime, UTC

API_BASE = os.environ.get("API_BASE", "https://polymarket-backend-production-f363.up.railway.app").rstrip("/")

SCAN_URL = f"{API_BASE}/signals/scan?cooldown_minutes=5&repeat_boost=1.3&min_move=0.4&limit_markets=300"
ALERT_URL = f"{API_BASE}/alerts/run?minutes=20&limit=10"

# Se você quiser desligar alertas sem apagar código:
ENABLE_ALERTS = os.environ.get("ENABLE_ALERTS", "1")  # "1" ou "0"
SLEEP_SECONDS = int(os.environ.get("WORKER_SLEEP_SECONDS", "120"))  # 120 = 2 min

def hit(url: str) -> dict:
    try:
        r = requests.get(url, timeout=30)
        try:
            data = r.json()
        except Exception:
            data = {"non_json": r.text[:200]}
        return {"ok": r.status_code == 200, "status": r.status_code, "data": data}
    except Exception as e:
        return {"ok": False, "error": str(e)}

def main():
    print("✅ PolySignal Worker started")
    print("API_BASE:", API_BASE)
    print("SCAN_URL:", SCAN_URL)
    print("ALERT_URL:", ALERT_URL)
    print("ENABLE_ALERTS:", ENABLE_ALERTS)
    print("SLEEP_SECONDS:", SLEEP_SECONDS)

    while True:
        start = datetime.now(UTC)

        # 1) Rodar scan
        scan_res = hit(SCAN_URL)
        print(f"\n[{start}] SCAN:", scan_res.get("status"), scan_res.get("data"))

        # 2) Rodar alerta (se habilitado)
        if ENABLE_ALERTS == "1":
            alert_res = hit(ALERT_URL)
            print(f"[{start}] ALERT:", alert_res.get("status"), alert_res.get("data"))

        # 3) Dorme
        time.sleep(SLEEP_SECONDS)

if __name__ == "__main__":
    main()
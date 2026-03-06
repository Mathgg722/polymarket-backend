# ============================================================
# PolySignal — Worker Principal (dynamic-rejoicing)
# Roda: scan de sinais + alertas Telegram a cada ciclo
# Railway start command: python worker.py
# ============================================================

import os
import time
import requests
from datetime import datetime, timezone

API_BASE        = os.environ.get("API_BASE", "https://polymarket-backend-production-f363.up.railway.app").rstrip("/")
ENABLE_ALERTS   = os.environ.get("ENABLE_ALERTS", "1")
SLEEP_SECONDS   = int(os.environ.get("WORKER_SLEEP_SECONDS", "120"))  # 2 min padrão

# Parâmetros do scan (ajustáveis via env)
MIN_MOVE        = os.environ.get("MIN_MOVE", "0.3")
COOLDOWN        = os.environ.get("COOLDOWN_MINUTES", "8")
LIMIT_MARKETS   = os.environ.get("LIMIT_MARKETS", "300")

SCAN_URL  = f"{API_BASE}/signals/scan?min_move={MIN_MOVE}&cooldown_minutes={COOLDOWN}&limit_markets={LIMIT_MARKETS}&repeat_boost=1.0"
ALERT_URL = f"{API_BASE}/alerts/run?minutes=15&limit=10"
STATUS_URL = f"{API_BASE}/debug/last_snapshot"


def hit(url: str, method: str = "GET") -> dict:
    try:
        fn = requests.get if method == "GET" else requests.post
        r = fn(url, timeout=30)
        try:
            data = r.json()
        except Exception:
            data = {"non_json": r.text[:300]}
        return {"ok": r.status_code == 200, "status": r.status_code, "data": data}
    except Exception as e:
        return {"ok": False, "error": str(e)}


def main():
    now = datetime.now(timezone.utc)
    print("=" * 60)
    print("✅ PolySignal Worker v3 iniciado")
    print(f"🌐 API_BASE: {API_BASE}")
    print(f"⏱  SLEEP: {SLEEP_SECONDS}s | ALERTS: {ENABLE_ALERTS} | MIN_MOVE: {MIN_MOVE}")
    print("=" * 60)

    cycle = 0
    consecutive_errors = 0

    while True:
        cycle += 1
        ts = datetime.now(timezone.utc).strftime("%H:%M:%S UTC")

        print(f"\n[{ts}] ── Ciclo #{cycle} ──")

        # 1) Checar saúde do worker
        if cycle % 10 == 1:  # a cada 10 ciclos (~20min)
            status_res = hit(STATUS_URL)
            if status_res["ok"]:
                age = status_res["data"].get("age_minutes")
                healthy = status_res["data"].get("worker_healthy", False)
                health_icon = "✅" if healthy else "⚠️"
                print(f"  {health_icon} Snapshot age: {age}min | Healthy: {healthy}")
            else:
                print(f"  ❌ Status check falhou: {status_res.get('error','?')}")

        # 2) Scan de sinais
        scan_res = hit(SCAN_URL)
        if scan_res["ok"]:
            d = scan_res["data"]
            created = d.get("created_signals", 0)
            scanned = d.get("scanned_markets", 0)
            errors  = d.get("errors", 0)
            print(f"  🔍 Scan: {scanned} mercados → {created} sinais criados | erros: {errors}")
            if created > 0 and d.get("preview"):
                for p in d["preview"][:3]:
                    print(f"     → {p.get('tipo','?')} | {p.get('slug','?')} | move: {p.get('move', p.get('err_pts','?'))}")
            consecutive_errors = 0
        else:
            print(f"  ❌ Scan falhou: {scan_res.get('error', scan_res.get('status','?'))}")
            consecutive_errors += 1

        # 3) Alertas Telegram
        if ENABLE_ALERTS == "1":
            alert_res = hit(ALERT_URL)
            if alert_res["ok"]:
                d = alert_res["data"]
                status = d.get("status", "?")
                sent = d.get("sent", 0)
                mode = d.get("mode", "?")
                reason = d.get("reason", "")
                if status == "skip":
                    print(f"  📵 Alert: skip ({reason})")
                elif status == "ok":
                    print(f"  📱 Alert: {sent} sinais enviados | mode: {mode}")
                else:
                    print(f"  ⚠️  Alert status: {status}")
            else:
                print(f"  ❌ Alert falhou: {alert_res.get('error','?')}")

        # 4) Backoff se muitos erros consecutivos
        if consecutive_errors >= 5:
            backoff = SLEEP_SECONDS * 2
            print(f"  ⏸  Muitos erros consecutivos. Pausando {backoff}s...")
            time.sleep(backoff)
            consecutive_errors = 0
        else:
            time.sleep(SLEEP_SECONDS)


if __name__ == "__main__":
    main()

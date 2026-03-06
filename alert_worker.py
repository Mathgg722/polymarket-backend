# ============================================================
# PolySignal — Alert Worker (adaptable-love)
# Monitora anomalias e oportunidades → Telegram + Email
# Railway start command: python alert_worker.py
# ============================================================

import os
import time
import smtplib
import requests
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime

API_URL          = os.environ.get("API_BASE", "https://polymarket-backend-production-f363.up.railway.app")
TELEGRAM_TOKEN   = os.environ.get("TELEGRAM_TOKEN", "") or os.environ.get("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")
GMAIL_USER       = os.environ.get("GMAIL_USER", "")
GMAIL_PASSWORD   = os.environ.get("GMAIL_APP_PASSWORD", "")
EMAIL_DESTINOS   = [e.strip() for e in os.environ.get("EMAIL_DESTINOS", "").split(",") if e.strip()]
CHECK_INTERVAL   = int(os.environ.get("CHECK_INTERVAL", "60"))

alerted_anomalies = set()
alerted_opps = set()


# ── Telegram ──────────────────────────────────────────────────

def send_telegram(message: str):
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        return
    try:
        requests.post(
            f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
            json={"chat_id": TELEGRAM_CHAT_ID, "text": message, "parse_mode": "HTML",
                  "disable_web_page_preview": True},
            timeout=10
        )
        print("✅ Telegram enviado!")
    except Exception as e:
        print(f"❌ Telegram: {e}")


# ── Email ─────────────────────────────────────────────────────

def send_email(subject: str, body_html: str):
    if not GMAIL_USER or not GMAIL_PASSWORD or not EMAIL_DESTINOS:
        return
    try:
        msg = MIMEMultipart("alternative")
        msg["Subject"] = subject
        msg["From"] = GMAIL_USER
        msg["To"] = ", ".join(EMAIL_DESTINOS)
        msg.attach(MIMEText(body_html, "html"))
        with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
            server.login(GMAIL_USER, GMAIL_PASSWORD)
            server.sendmail(GMAIL_USER, EMAIL_DESTINOS, msg.as_string())
        print("✅ Email enviado!")
    except Exception as e:
        print(f"❌ Email: {e}")


# ── Formatadores ──────────────────────────────────────────────

def format_anomaly(a: dict) -> str:
    tipo = a.get("tipo", "")
    emoji = {"SPIKE": "📈", "DUMP": "📉", "REVERSAL_UP": "🔄⬆️",
             "REVERSAL_DOWN": "🔄⬇️", "EXTREME": "💥"}.get(tipo, "⚠️")
    change = a.get("change_5m", 0)
    sign = "+" if change > 0 else ""
    price = a.get("current_price", 0)
    score = a.get("confianca_score", 0)
    url = a.get("polymarket_url", "https://polymarket.com")

    return f"""{emoji} <b>{tipo} — {a.get('alert_level','?')}</b>

📊 <b>{a.get('market','')[:100]}</b>
🎯 {a.get('outcome','?')} | {sign}{change}% em 5min
💰 Preço atual: {price}%
⚡ Score: {score}/100

🔗 <a href="{url}">Ver no Polymarket</a>
⏰ {datetime.utcnow().strftime('%H:%M')} UTC""".strip()


def format_opportunity(op: dict) -> str:
    direcao = op.get("direcao", "?")
    preco = op.get("preco_entrada", 0)
    potencial = op.get("potencial_lucro_10usd", 0)
    score = op.get("score_final", 0)
    change = op.get("change_5m", 0)
    fontes = op.get("num_fontes_confirmando", 0)
    resumo = op.get("resumo_ia", "")
    baleias = op.get("baleias", [])
    trending = op.get("trending", False)
    noticias = op.get("noticias_titulos", [])
    url = op.get("polymarket_url", "https://polymarket.com")

    extras = []
    if noticias:
        extras.append(f"📰 {len(noticias)} notícia(s) confirmando")
    if trending:
        extras.append("🔥 Trending no Google")
    if baleias:
        maior = baleias[0]
        extras.append(f"🌊 Baleia: ${maior.get('valor',0)} {maior.get('outcome','')}")
    extras_text = "\n".join(extras)

    sign = "+" if change > 0 else ""
    sinal_icon = "🟢" if score >= 65 else "🟡"

    return f"""{sinal_icon} <b>OPORTUNIDADE DETECTADA</b>

📊 <b>{op.get('market','')[:100]}</b>
🎯 APOSTE: <b>{direcao} @ {preco}%</b>
💰 $10 → lucro potencial <b>${potencial}</b>

📈 Movimento: <b>{sign}{change}% em 5min</b>
{extras_text}

🤖 {resumo}
⚡ Score: <b>{score}/100</b> ({fontes} fontes)

🔗 <a href="{url}">Ver no Polymarket</a>
⏰ {datetime.utcnow().strftime('%H:%M')} UTC""".strip()


# ── Checkers ──────────────────────────────────────────────────

def check_anomalies() -> list:
    try:
        resp = requests.get(f"{API_URL}/anomalies", timeout=15)
        if resp.status_code == 200:
            return resp.json()
    except Exception as e:
        print(f"[anomalies] erro: {e}")
    return []


def check_opportunities() -> list:
    try:
        resp = requests.get(f"{API_URL}/inefficiencies", timeout=20)
        if resp.status_code == 200:
            return resp.json().get("top_10", [])
    except Exception as e:
        print(f"[opps] erro: {e}")
    return []


# ── Loop principal ────────────────────────────────────────────

def run():
    print("🚀 PolySignal Alert Worker v3 iniciado!")
    print(f"📱 Telegram: {'✅' if TELEGRAM_TOKEN else '❌'}")
    print(f"📧 Email: {'✅ ' + GMAIL_USER if GMAIL_USER else '❌'}")
    print(f"⏱  Intervalo: {CHECK_INTERVAL}s")

    send_telegram("""🚀 <b>PolySignal Alert Worker v3 Ativo!</b>

🔍 Monitorando anomalias e ineficiências
⚡ EXTREME = alerta imediato
📊 Ineficiências calculadas com base histórica

Você só recebe alertas quando há edge real! 🎯""")

    cycle = 0
    while True:
        cycle += 1
        ts = datetime.utcnow().strftime("%H:%M:%S")
        print(f"\n[{ts}] Ciclo #{cycle}")

        # ── Anomalias a cada ciclo ──
        anomalies = check_anomalies()
        extreme_count = 0
        for a in anomalies:
            level = a.get("alert_level", "")
            if level in ["EXTREME", "HIGH"]:
                key = f"anomaly_{a.get('slug')}_{a.get('outcome')}_{round(a.get('change_5m',0))}"
                if key not in alerted_anomalies:
                    alerted_anomalies.add(key)
                    send_telegram(format_anomaly(a))
                    extreme_count += 1
                    time.sleep(1)

        if extreme_count:
            print(f"  🚨 {extreme_count} anomalias EXTREME/HIGH enviadas")
        else:
            print(f"  ℹ️  {len(anomalies)} anomalias verificadas | sem EXTREME/HIGH novo")

        # ── Ineficiências a cada 5 ciclos (~5min) ──
        if cycle % 5 == 0:
            opps = check_opportunities()
            sent_opps = 0
            for op in opps:
                score = op.get("ineficiencia_score", 0)
                if score < 50:
                    continue
                key = f"opp_{op.get('slug')}_{op.get('apostar')}_{round(score)}"
                if key not in alerted_opps:
                    alerted_opps.add(key)
                    # Formata como oportunidade
                    msg_data = {
                        "market": op.get("market"),
                        "direcao": op.get("apostar", "?"),
                        "preco_entrada": op.get("current_price_pct", 0),
                        "potencial_lucro_10usd": round((100 - op.get("current_price_pct",50)) / max(op.get("current_price_pct",50),1) * 10, 2),
                        "score_final": score,
                        "change_5m": op.get("change_5m", 0),
                        "num_fontes_confirmando": 1,
                        "resumo_ia": f"Ineficiência: {op.get('edge_tipo','?')} | {op.get('conviction','?')}",
                        "polymarket_url": op.get("polymarket_url"),
                        "noticias_titulos": [],
                        "baleias": [],
                        "trending": False,
                    }
                    send_telegram(format_opportunity(msg_data))
                    # Email para scores altos
                    if score >= 65 and GMAIL_USER:
                        subject = f"🟢 PolySignal — Aposte {msg_data['direcao']} | Score {score}"
                        body = f"""<html><body style='background:#0a0a1a;color:#eee;font-family:Arial;padding:20px'>
<h2 style='color:#00ff88'>🟢 Ineficiência Detectada</h2>
<p><b>{op.get('market','')}</b></p>
<p>Direção: <b>{msg_data['direcao']}</b> @ {msg_data['preco_entrada']}%</p>
<p>Score: <b>{score}/100</b></p>
<p>Edge: {op.get('edge_tipo','?')}</p>
<p>Conviction: {op.get('conviction','?')}</p>
<a href='{op.get('polymarket_url','')}' style='color:#00ff88'>Ver no Polymarket</a>
</body></html>"""
                        send_email(subject, body)
                    sent_opps += 1
                    time.sleep(1)

            if sent_opps:
                print(f"  📊 {sent_opps} ineficiências alertadas")

        # ── Limpa cache ──
        if len(alerted_anomalies) > 500:
            alerted_anomalies.clear()
        if len(alerted_opps) > 500:
            alerted_opps.clear()

        time.sleep(CHECK_INTERVAL)


if __name__ == "__main__":
    run()
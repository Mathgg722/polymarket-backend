import os
import time
import requests
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime

API_URL = "https://polymarket-backend-production-f363.up.railway.app"
TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN", "")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")
GMAIL_USER = os.environ.get("GMAIL_USER", "")
GMAIL_PASSWORD = os.environ.get("GMAIL_APP_PASSWORD", "")
EMAIL_DESTINOS = os.environ.get("EMAIL_DESTINOS", "").split(",")
CHECK_INTERVAL = 60

alerted_best = set()
alerted_anomalies = set()


def send_telegram(message: str):
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        return
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
        requests.post(url, json={
            "chat_id": TELEGRAM_CHAT_ID,
            "text": message,
            "parse_mode": "HTML"
        }, timeout=10)
        print("✅ Telegram enviado!")
    except Exception as e:
        print(f"❌ Telegram: {e}")


def send_email(subject: str, body: str):
    if not GMAIL_USER or not GMAIL_PASSWORD:
        return
    try:
        msg = MIMEMultipart("alternative")
        msg["Subject"] = subject
        msg["From"] = GMAIL_USER
        msg["To"] = ", ".join(EMAIL_DESTINOS)
        msg.attach(MIMEText(body, "html"))
        with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
            server.login(GMAIL_USER, GMAIL_PASSWORD)
            server.sendmail(GMAIL_USER, EMAIL_DESTINOS, msg.as_string())
        print("✅ Email enviado!")
    except Exception as e:
        print(f"❌ Email: {e}")


def format_best_opportunity(op: dict) -> str:
    """Mensagem essencial e rápida para oportunidade confirmada."""
    sinal = op.get("sinal", "🟡")
    direcao = op.get("direcao", "")
    preco = op.get("preco_entrada", 0)
    potencial = op.get("potencial_lucro_10usd", 0)
    score = op.get("score_final", 0)
    change = op.get("change_5m", 0)
    change_1h = op.get("change_1h")
    fontes = op.get("num_fontes_confirmando", 0)
    baleias = op.get("baleias", [])
    trending = op.get("trending", False)
    resumo = op.get("resumo_ia", "")
    noticias = op.get("noticias_titulos", [])
    reddit = op.get("reddit_posts", [])

    sinal_change = "+" if change > 0 else ""

    extras = []
    if noticias:
        extras.append(f"📰 {len(noticias)} notícia(s) confirmando")
    if reddit:
        extras.append(f"💬 Reddit: {reddit[0][:50]}...")
    if trending:
        extras.append(f"🔥 Google Trends: EM ALTA")
    if baleias:
        maior = baleias[0]
        extras.append(f"🌊 Baleia apostou ${maior.get('valor',0)} {maior.get('outcome','')}")

    extras_text = "\n".join(extras)

    return f"""{sinal} <b>OPORTUNIDADE DETECTADA</b>

📊 <b>{op.get('market')}</b>
🎯 APOSTE: <b>{direcao} @ {preco}%</b>
💰 $10 → lucro potencial <b>${potencial}</b>

📈 Movimento: <b>{sinal_change}{change}% em 5min</b>
{"📅 1h: <b>" + str(change_1h) + "%</b>" if change_1h else ""}

{extras_text}

🤖 IA: <i>{resumo}</i>
⚡ Score: <b>{score}/100</b> ({fontes} fontes confirmando)

🔗 <a href="{op.get('polymarket_url')}">Ver no Polymarket</a>
⏰ {datetime.utcnow().strftime('%H:%M')} UTC""".strip()


def format_anomaly_alert(a: dict) -> str:
    """Alerta rápido de anomalia de preço."""
    tipo = a.get("tipo", "")
    emoji = {"SPIKE":"📈","DUMP":"📉","REVERSAL_UP":"🔄⬆️","REVERSAL_DOWN":"🔄⬇️","EXTREME":"💥"}.get(tipo,"⚠️")
    change = a.get("change_5m", 0)
    sinal = "+" if change > 0 else ""
    return f"""{emoji} <b>MOVIMENTO DETECTADO — {a.get('alert_level')}</b>

📊 {a.get('market')}
🎯 {a.get('outcome')} | {sinal}{change}% em 5min
💰 Preço atual: {a.get('current_price')}%

🔗 <a href="{a.get('polymarket_url')}">Ver no Polymarket</a>""".strip()


def check_best():
    try:
        resp = requests.get(f"{API_URL}/best/v2", timeout=30)
        if resp.status_code == 200:
            return resp.json().get("top_apostas", [])
    except Exception as e:
        print(f"❌ Erro /best/v2: {e}")
    return []


def check_anomalies():
    try:
        resp = requests.get(f"{API_URL}/anomalies", timeout=15)
        if resp.status_code == 200:
            return resp.json()
    except Exception as e:
        print(f"❌ Erro /anomalies: {e}")
    return []


def run():
    print("🚀 PolySignal Alert Worker v2 iniciado!")
    print(f"📱 Telegram: {'✅' if TELEGRAM_TOKEN else '❌'}")
    print(f"📧 Email: {'✅' if GMAIL_USER else '❌'}")

    send_telegram("""🚀 <b>PolySignal v2 Ativado!</b>

Sistema multi-fonte ativo:
📰 NewsAPI + Google News
💬 Reddit
🔥 Google Trends  
🌊 Detecção de Baleias
🤖 IA Claude

Você só receberá alertas quando múltiplas fontes confirmarem a oportunidade! 🎯""")

    cycle = 0
    while True:
        now = datetime.utcnow().strftime("%H:%M:%S")
        cycle += 1
        print(f"\n[{now}] Ciclo #{cycle}")

        # A cada ciclo — checa anomalias rápidas
        print(f"[{now}] Checando anomalias...")
        anomalies = check_anomalies()
        for a in anomalies:
            if a.get("alert_level") == "EXTREME":
                key = f"anomaly_{a.get('slug')}_{a.get('outcome')}_{round(a.get('change_5m',0))}"
                if key not in alerted_anomalies:
                    alerted_anomalies.add(key)
                    send_telegram(format_anomaly_alert(a))
                    time.sleep(1)

        # A cada 5 ciclos (5min) — checa oportunidades multi-fonte
        if cycle % 5 == 0:
            print(f"[{now}] Checando oportunidades multi-fonte...")
            opportunities = check_best()
            for op in opportunities:
                key = f"best_{op.get('slug')}_{op.get('direcao')}_{round(op.get('preco_entrada',0))}"
                if key not in alerted_best:
                    alerted_best.add(key)
                    if op.get("sinal", "").startswith("🟢"):
                        send_telegram(format_best_opportunity(op))
                        # Email também para oportunidades verdes
                        subject = f"🟢 PolySignal — Aposte {op.get('direcao')} em {op.get('market','')[:40]}"
                        body = f"<html><body style='background:#1a1a2e;color:#eee;font-family:Arial;padding:20px'><h2 style='color:#00ff88'>🟢 Oportunidade Detectada</h2><p><b>{op.get('market')}</b></p><p>Direção: <b>{op.get('direcao')}</b> @ {op.get('preco_entrada')}%</p><p>Score: <b>{op.get('score_final')}/100</b></p><p>{op.get('resumo_ia','')}</p><a href='{op.get('polymarket_url')}' style='color:#00ff88'>Ver no Polymarket</a></body></html>"
                        send_email(subject, body)
                        time.sleep(1)

        # Limpa cache
        if len(alerted_best) > 200:
            alerted_best.clear()
        if len(alerted_anomalies) > 200:
            alerted_anomalies.clear()

        time.sleep(CHECK_INTERVAL)


if __name__ == "__main__":
    run()
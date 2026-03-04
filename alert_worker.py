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

alerted = set()

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
        print(f"❌ Telegram erro: {e}")

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
        print(f"✅ Email enviado!")
    except Exception as e:
        print(f"❌ Email erro: {e}")

def format_telegram(a: dict) -> str:
    tipo = a.get("tipo", "")
    emoji = {"SPIKE":"📈","DUMP":"📉","REVERSAL_UP":"🔄⬆️","REVERSAL_DOWN":"🔄⬇️","EXTREME":"💥","MOVE":"⚡"}.get(tipo,"⚠️")
    op = a.get("oportunidade","")
    op_emoji = {"POSSIVEL_YES":"🟢","POSSIVEL_NO":"🔴","AGUARDAR":"🟡"}.get(op,"🟡")
    change = a.get("change_5m", 0)
    sinal = "+" if change > 0 else ""
    change_1h = a.get("change_1h")
    return f"""{emoji} <b>ANOMALIA — {a.get('alert_level')}</b>

📊 <b>{a.get('market')}</b>
🎯 Outcome: <b>{a.get('outcome')}</b> | Tipo: <b>{tipo}</b>

💰 Preço atual: <b>{a.get('current_price')}%</b>
⚡ Variação 5min: <b>{sinal}{change}%</b>
{"📅 Variação 1h: <b>" + str(change_1h) + "%</b>" if change_1h else ""}

{op_emoji} <b>{op.replace('_',' ')}</b>
🎯 Score: <b>{a.get('confianca_score')}/100</b>

🔗 <a href="{a.get('polymarket_url')}">Ver no Polymarket</a>
⏰ {datetime.utcnow().strftime('%H:%M:%S')} UTC""".strip()

def format_email(anomalies: list) -> str:
    rows = ""
    for a in anomalies:
        change = a.get("change_5m", 0)
        sinal = "+" if change > 0 else ""
        op = a.get("oportunidade","").replace("_"," ")
        cor = "#28a745" if "YES" in op else "#dc3545" if "NO" in op else "#ffc107"
        rows += f"<tr><td style='padding:8px;border-bottom:1px solid #333'>{a.get('market')}</td><td style='padding:8px'>{a.get('outcome')}</td><td style='padding:8px'>{a.get('tipo')}</td><td style='padding:8px'>{a.get('current_price')}%</td><td style='padding:8px'>{sinal}{change}%</td><td style='padding:8px;color:{cor}'><b>{op}</b></td><td style='padding:8px'>{a.get('confianca_score')}/100</td></tr>"
    return f"""<html><body style='background:#1a1a2e;color:#eee;font-family:Arial'>
<h2 style='color:#00ff88'>🚨 PolySignal — {len(anomalies)} Anomalia(s)</h2>
<p style='color:#aaa'>{datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')} UTC</p>
<table style='width:100%;border-collapse:collapse;background:#16213e'>
<tr style='background:#0f3460;color:#00ff88'><th style='padding:10px'>Mercado</th><th>Outcome</th><th>Tipo</th><th>Preço</th><th>Var 5min</th><th>Oportunidade</th><th>Score</th></tr>
{rows}
</table></body></html>"""

def run():
    print("🚀 PolySignal Alert Worker iniciado!")
    print(f"📱 Telegram: {'✅' if TELEGRAM_TOKEN else '❌'}")
    print(f"📧 Email: {'✅' if GMAIL_USER else '❌'}")

    send_telegram("🚀 <b>PolySignal Ativado!</b>\n\nMonitorando mercados em tempo real.\nVocê receberá alertas de anomalias automaticamente! 📊")

    while True:
        now = datetime.utcnow().strftime("%H:%M:%S")
        print(f"[{now}] Checando anomalias...")

        try:
            resp = requests.get(f"{API_URL}/anomalies", timeout=15)
            anomalies = resp.json() if resp.status_code == 200 else []
        except:
            anomalies = []

        new_anomalies = []
        for a in anomalies:
            key = f"{a.get('slug')}_{a.get('outcome')}_{round(a.get('change_5m',0))}"
            if key not in alerted:
                alerted.add(key)
                new_anomalies.append(a)

        if new_anomalies:
            print(f"🚨 {len(new_anomalies)} nova(s) anomalia(s)!")
            for a in new_anomalies:
                if a.get("alert_level") in ["HIGH", "EXTREME"]:
                    send_telegram(format_telegram(a))
                    time.sleep(1)
            send_email(f"🚨 PolySignal — {len(new_anomalies)} anomalia(s)", format_email(new_anomalies))
        else:
            print(f"[{now}] Sem novas anomalias.")

        if len(alerted) > 500:
            alerted.clear()

        time.sleep(CHECK_INTERVAL)

if __name__ == "__main__":
    run()

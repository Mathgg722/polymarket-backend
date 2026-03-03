import time
import threading
import requests
from fastapi import FastAPI

app = FastAPI()

def collect_markets():
    try:
        response = requests.get("https://clob.polymarket.com/markets")
        data = response.json()

        print("========== DEBUG START ==========")
        print(type(data))
        print(data)
        print("========== DEBUG END ==========")

    except Exception as e:
        print("ERRO:", e)

def background_collector():
    while True:
        collect_markets()
        time.sleep(60)

@app.on_event("startup")
def start_background_task():
    thread = threading.Thread(target=background_collector)
    thread.daemon = True
    thread.start()

@app.get("/")
def health():
    return {"status": "running"}
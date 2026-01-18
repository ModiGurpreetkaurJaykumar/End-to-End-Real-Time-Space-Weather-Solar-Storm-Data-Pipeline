import requests
from kafka import KafkaProducer
import json
import time

# Correct NOAA API URLs
PLASMA_URL = "https://services.swpc.noaa.gov/products/solar-wind/plasma-1-day.json"
MAG_URL = "https://services.swpc.noaa.gov/products/solar-wind/mag-1-day.json"

producer = KafkaProducer(
    bootstrap_servers=['10.0.0.97:9092'],
    value_serializer=lambda x: json.dumps(x).encode('utf-8')
)

def fetch_latest(url):
    try:
        r = requests.get(url, timeout=10)
        data = r.json()
        # NOAA format: [ [header], [row1], [row2], ... ]
        header = data[0]
        last_row = data[-1]
        return dict(zip(header, last_row))
    except Exception as e:
        print("API fetch error:", url, e)
        return None

while True:
    plasma = fetch_latest(PLASMA_URL)
    mag = fetch_latest(MAG_URL)

    if plasma and mag:
        payload = {
            "timestamp": plasma.get("time_tag"),
            "speed": plasma.get("speed"),
            "density": plasma.get("density"),
            "temperature": plasma.get("temperature"),
            "bx": mag.get("bx_gsm"),
            "by": mag.get("by_gsm"),
            "bz": mag.get("bz_gsm"),
            "bt": mag.get("bt"),
        }

        producer.send("solar-wind-raw", value=payload)
        print("Sent:", payload)
    else:
        print("Skipping send — NOAA API not available.")

    time.sleep(10)

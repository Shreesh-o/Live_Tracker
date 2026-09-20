import json
import os
import time
from datetime import datetime, timezone

import requests
from confluent_kafka import Producer
from dotenv import load_dotenv

load_dotenv()

config = {
    "bootstrap.servers":os.getenv('KAFKA_BOOTSTRAP_SERVERS'),
    'security.protocol':'SASL_SSL',
    'sasl.mechanisms':'PLAIN',
    'sasl.username':os.getenv('KAFKA_API_KEY'),
    'sasl.password':os.getenv('KAFKA_API_SECRET'),
}

producer = Producer(config)

topic = os.getenv('KAFKA_TOPIC')

def delivery_report(err, message):
    if err is not None:
        print(f'❌ Delivery failed: {err}')
    else:
        print(
            f"✅ Sent BTC price to "
            f"{message.topic()} [{message.partition()}]"
        )

while True:
    try:
        response = requests.get(
            "https://api.coingecko.com/api/v3/simple/price",
            params={
                "ids": "bitcoin",
                "vs_currencies": "usd",
            },
            timeout=10,
        )

        response.raise_for_status()

        data = response.json()

        price = data['bitcoin']['usd']

        price_event = {
            "symbol": "BTC",
            "price": price,
            "currency": "USD",
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }

        message = json.dumps(price_event)

        producer.produce(
            topic=topic,
            key='BTC',
            value=message,
            callback=delivery_report,
        )

        producer.flush()

        print(f"📈 BTC: ${price}")

        time.sleep(5)

    except Exception as e:
        print(f"❌ Error: {e}")
        time.sleep(5)
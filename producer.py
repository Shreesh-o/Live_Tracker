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
            f"✅ Sent {message.key().decode()} price to "
            f"{message.topic()} [{message.partition()}]"
        )

while True:
    try:
        response = requests.get(
            "https://api.coingecko.com/api/v3/simple/price",
            params={
                "ids": "bitcoin,ethereum,solana",
                "vs_currencies": "usd",
            },
            timeout=10,
        )

        response.raise_for_status()

        data = response.json()

        timestamp = datetime.now(timezone.utc).isoformat()

        coins = {
            'bitcoin':'BTC',
            'ethereum':'ETH',
            'solana':'SOL',
        }

        for coin_id,symbol in coins.items():
            price = data[coin_id]['usd']

            price_event = {
                "symbol": symbol,
                "price": price,
                "currency": "USD",
                "timestamp": timestamp,
            }

            message = json.dumps(price_event)

            producer.produce(
                topic=topic,
                key=symbol,
                value=message,
                callback=delivery_report,
            )

        producer.flush()

        for coin_id, symbol in coins.items():
            print(f"📈 {symbol}: ${data[coin_id]['usd']:,.2f}")

        time.sleep(30)

    except Exception as e:
        print(f"❌ Error: {e}")
        time.sleep(5)
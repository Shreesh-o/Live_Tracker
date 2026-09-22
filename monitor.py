import json
import os

from dotenv import load_dotenv
from confluent_kafka import Consumer

load_dotenv()

config = {
    "bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP_SERVERS"),
    "security.protocol": "SASL_SSL",
    "sasl.mechanisms": "PLAIN",
    "sasl.username": os.getenv("KAFKA_API_KEY"),
    "sasl.password": os.getenv("KAFKA_API_SECRET"),
    "group.id": "live-price-monitor",
    "auto.offset.reset": "earliest",
}

consumer = Consumer(config)

topic = os.getenv("KAFKA_TOPIC")

consumer.subscribe([topic])

print(f"📡 Consumer started | PID: {os.getpid()} | Topic: {topic}")

try:
    while True:
        message = consumer.poll(1.0)

        if message is None:
            continue

        if message.error():
            print(f"❌ Consumer error: {message.error()}")
            continue

        try:
            data = json.loads(message.value().decode("utf-8"))

            print(
                f"📈 PID {os.getpid()} | "
                f"{data['symbol']} | "
                f"${data['price']:,.2f} {data['currency']} | "
                f"partition={message.partition()} | "
                f"offset={message.offset()}"
            )

        except json.JSONDecodeError:
            print(
                f"⚠️ Skipping non-JSON message | "
                f"partition={message.partition()} | "
                f"offset={message.offset()} | "
                f"value={message.value()!r}"
            )

except KeyboardInterrupt:
    print("\n🛑 Stopping Consumer...")

finally:
    consumer.close()
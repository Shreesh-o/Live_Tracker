import json
import os

from dotenv import load_dotenv
from confluent_kafka import Consumer, Producer

load_dotenv()

# Kafka Consumer configuration
consumer_config = {
    "bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP_SERVERS"),
    "security.protocol": "SASL_SSL",
    "sasl.mechanisms": "PLAIN",
    "sasl.username": os.getenv("KAFKA_API_KEY"),
    "sasl.password": os.getenv("KAFKA_API_SECRET"),
    "group.id": "live-price-consumer",
    "auto.offset.reset": "earliest",
    "enable.auto.commit": False,
}

consumer = Consumer(consumer_config)


# Kafka DLT Producer configuration
dlt_producer_config = {
    "bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP_SERVERS"),
    "security.protocol": "SASL_SSL",
    "sasl.mechanisms": "PLAIN",
    "sasl.username": os.getenv("KAFKA_API_KEY"),
    "sasl.password": os.getenv("KAFKA_API_SECRET"),
    "acks": "all",
    "enable.idempotence": True,
}

dlt_producer = Producer(dlt_producer_config)


topic = os.getenv("KAFKA_TOPIC")
dlt_topic = os.getenv("KAFKA_DLT_TOPIC")

consumer.subscribe([topic])

print(
    f"📡 Consumer started | "
    f"PID: {os.getpid()} | "
    f"Topic: {topic}"
)


try:
    while True:
        message = consumer.poll(1.0)

        if message is None:
            continue

        if message.error():
            print(f"❌ Consumer error: {message.error()}")
            continue

        try:
            # Convert Kafka message to JSON
            data = json.loads(
                message.value().decode("utf-8")
            )

            # Process the message
            print(
                f"📈 PID {os.getpid()} | "
                f"{data['symbol']} | "
                f"${data['price']:,.2f} {data['currency']} | "
                f"partition={message.partition()} | "
                f"offset={message.offset()}"
            )

            # Commit only after successful processing
            consumer.commit(message=message)

        except (json.JSONDecodeError, KeyError, TypeError, ValueError) as e:
            print(
                f"⚠️ Bad message | "
                f"partition={message.partition()} | "
                f"offset={message.offset()} | "
                f"error={e}"
            )

            # Send bad message to Dead Letter Topic
            dlt_producer.produce(
                topic=dlt_topic,
                key=message.key(),
                value=message.value(),
            )

            # Make sure DLT message is delivered
            dlt_producer.flush()

            print(
                f"☠️ Sent message to DLT: {dlt_topic}"
            )

            # Commit original message only after
            # successfully sending it to the DLT
            consumer.commit(message=message)

except KeyboardInterrupt:
    print("\n🛑 Stopping Consumer...")

finally:
    consumer.close()
    dlt_producer.flush()
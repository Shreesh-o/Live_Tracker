import os

from dotenv import load_dotenv
from confluent_kafka import Producer

load_dotenv()

config = {
    "bootstrap.servers":os.getenv('KAFKA_BOOTSTRAP_SERVERS'),
    "security.protocol":"SASL_SSL",
    "sasl.mechanism":"PLAIN",
    "sasl.username":os.getenv('KAFKA_API_KEY'),
    "sasl.password":os.getenv('KAFKA_API_SECRET'),
}

producer = Producer(config)

topic = os.getenv('KAFKA_TOPIC')

def delivery_report(err,message):
    if err is not None:
        print(f"❌ Message delivery failed: {err}")
    else:
        print(
            f"✅ Message delivered to "
            f"{message.topic()} [{message.partition()}]"
        )

message = 'Yellow'

producer.produce(
    topic=topic,
    value=message,
    callback=delivery_report,
)

producer.flush()
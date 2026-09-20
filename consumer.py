import json 
import os

from dotenv import load_dotenv
from confluent_kafka import Consumer

load_dotenv()

config = {
    "bootstrap.servers":os.getenv('KAFKA_BOOTSTRAP_SERVERS'),
    "security.protocol":"SASL_SSL",
    "sasl.mechanisms":"PLAIN",
    "sasl.username":os.getenv('KAFKA_API_KEY'),
    'sasl.password':os.getenv('KAFKA_API_SECRET'),
    'group.id':'live-price-consumer',
    'auto.offset.reset':'earliest',
}   

consumer = Consumer(config)

topic = os.getenv('KAFKA_TOPIC')

consumer.subscribe([topic])

print(f'Listening to topic: {topic}')

try:
    while True:
        message = consumer.poll(1.0)

        if message is None:
            continue

        if message.error():
            print(f"❌ Consumer error: {message.error()}")
            continue

        data = json.loads(message.value().decode('utf-8'))

        print(
            f"📈 {data['symbol']} | "
            f"${data['price']:,.2f} {data['currency']} | "
            f"{data['timestamp']}"
        )
        
except KeyboardInterrupt:
    print('\n Stopping Consumer....')

finally:
    consumer.close()
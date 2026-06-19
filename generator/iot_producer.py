import json
import random
import time
from datetime import datetime, timezone

from kafka import KafkaProducer


KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC_NAME = "iot_events"


def create_event():
    return {
        "device_type_id": random.randint(1, 5),
        "event_time": datetime.now(timezone.utc).isoformat(),
        "temperature": round(random.uniform(15.0, 35.0), 2),
        "humidity": round(random.uniform(30.0, 90.0), 2),
    }


def main():
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
    )

    print(f"Sending IoT events to Kafka topic: {TOPIC_NAME}")

    while True:
        event = create_event()
        producer.send(TOPIC_NAME, value=event)
        producer.flush()

        print(event)

        time.sleep(1)


if __name__ == "__main__":
    main()

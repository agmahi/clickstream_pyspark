from pydantic import BaseModel
import random
import uuid
import time
from datetime import datetime, timedelta
from faker import Faker
import json
from confluent_kafka import Producer


class ClickEvent(BaseModel):
    user_id: str
    event_id: str
    session_id: str
    event_type: str
    timestamp: str
    page_url: str
    referrer_url: str
    user_agent: str
    ip_address: str
    product_id: str
    dwell_time_ms: int

    
EVENT_TYPES = [
    "page_view",
    "click",
    "add_to_cart",
    "remove_from_cart",
    "purchase",
    "search",
    "login",
    "logout",
    "signup",
    "view_product",
]

def generate_event():
    fake = Faker()
    return ClickEvent(
        user_id=fake.uuid4(),
        event_id=fake.uuid4(),
        session_id=fake.uuid4(),
        event_type=random.choice(EVENT_TYPES),
        timestamp=(datetime.utcnow() - timedelta(seconds=random.randint(0, 3600))).isoformat() + "Z",
        page_url=fake.url(),
        referrer_url=fake.url(),
        user_agent=fake.user_agent(),
        ip_address=fake.ipv4_public(),
        product_id=fake.uuid4(),
        dwell_time_ms=random.randint(100, 10000),
    )

def delivery_report(err, msg):
    if err is not None:
        print(f"Delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}]")

def main():
    fake = Faker()
    producer = Producer({'bootstrap.servers': 'localhost:9092'})
    num_events = 500  # Number of events to generate
    while num_events > 0:
        events = [generate_event().dict() for _ in range(100)]
        for event in events:
            producer.produce('raw_clickstream',
                                key=event['user_id'],
                                value=json.dumps(event), 
                                callback=delivery_report)
        producer.flush()
        print(f"Published 100 events to raw_clickstream topic at {datetime.utcnow().isoformat()}")
        num_events -= 100
        time.sleep(3)  # Simulate a small delay between events


if __name__ == "__main__":
    main()
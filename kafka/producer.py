import json
import requests
from confluent_kafka import Producer


# ========================================================
# Kafka Producer Configuration
# ========================================================

producer = Producer({
    "bootstrap.servers": "localhost:9092"
})


# ========================================================
# Fetch data from FastAPI
# ========================================================

API_URL = "http://127.0.0.1:8000/sales"

response = requests.get(API_URL, timeout=30)
response.raise_for_status()

sales_data = response.json()

print(f"Fetched {len(sales_data)} records from API")


# ========================================================
# Send records to Kafka
# ========================================================

for record in sales_data:
    producer.produce(
        "online_sales",
        value=json.dumps(record).encode("utf-8")
    )


# ========================================================
# Wait for all messages to be delivered
# ========================================================

remaining = producer.flush()

if remaining > 0:
    raise RuntimeError(
        f"{remaining} Kafka messages were not delivered."
    )

print("All records successfully sent to Kafka.")
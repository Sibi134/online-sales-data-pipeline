import json
import os
import time
import pandas as pd
from confluent_kafka import Consumer


# ========================================================
# Kafka Consumer Configuration
# ========================================================

consumer = Consumer({
    "bootstrap.servers": "localhost:9092",
    "group.id": "online_sales_group_demo",
    "auto.offset.reset": "earliest"
})


consumer.subscribe(["online_sales"])

print("Waiting for Kafka messages...")


# ========================================================
# Consumer Settings
# ========================================================

records = []

MAX_EMPTY_POLLS = 15
empty_polls = 0


# ========================================================
# Consume Kafka Messages
# ========================================================

while True:

    msg = consumer.poll(1.0)

    # ----------------------------------------------------
    # No message received
    # ----------------------------------------------------

    if msg is None:

        empty_polls += 1

        print(
            f"No message received "
            f"({empty_polls}/{MAX_EMPTY_POLLS})"
        )

        if empty_polls >= MAX_EMPTY_POLLS:
            break

        continue

    # Reset empty poll counter
    empty_polls = 0


    # ----------------------------------------------------
    # Kafka Error
    # ----------------------------------------------------

    if msg.error():

        print(f"Kafka error: {msg.error()}")

        continue


    # ----------------------------------------------------
    # Read Message
    # ----------------------------------------------------

    value = msg.value()

    if value is None:
        continue


    # ----------------------------------------------------
    # Convert JSON → Python Dictionary
    # ----------------------------------------------------

    try:

        record = json.loads(
            value.decode("utf-8")
        )

        records.append(record)

    except json.JSONDecodeError:

        print("Skipping invalid JSON")


# ========================================================
# Close Kafka Consumer
# ========================================================

consumer.close()


# ========================================================
# Staging File Path
# ========================================================

staging_path = os.path.join(
    os.path.dirname(__file__),
    "..",
    "staging",
    "staging.csv"
)


# ========================================================
# Handle No New Records
# ========================================================

if not records:

    print("WARNING: No new Kafka records received.")

    if os.path.exists(staging_path):

        print(
            "Existing staging.csv will be preserved."
        )

    else:

        raise RuntimeError(
            "No Kafka records received and "
            "staging.csv does not exist."
        )

else:

    # ----------------------------------------------------
    # Convert Records to DataFrame
    # ----------------------------------------------------

    df = pd.DataFrame(records)


    # ----------------------------------------------------
    # Save to Staging
    # ----------------------------------------------------

    df.to_csv(
        staging_path,
        index=False
    )


    print(
        f"Saved {len(df)} records to "
        f"{staging_path}"
    )
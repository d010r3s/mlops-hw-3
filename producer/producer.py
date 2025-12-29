import os
import json
import pandas as pd
from kafka import KafkaProducer

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "transactions")
CSV_PATH = os.getenv("CSV_PATH", "/data/train.csv")

REQUIRED_COLUMNS = ["transaction_time", "us_state", "cat_id", "amount"]

def wait_for_kafka(bootstrap: str, timeout_s: int = 90) -> None:
    host, port_str = bootstrap.split(":")
    port = int(port_str)
    start = time.time()
    while time.time() - start < timeout_s:
        try:
            with socket.create_connection((host, port), timeout=3):
                return
        except OSError:
            time.sleep(1)
    raise RuntimeError(f"Kafka not available at {bootstrap} after {timeout_s}s")

def main() -> None:

    df = pd.read_csv(CSV_PATH)
    missing = [c for c in REQUIRED_COLUMNS if c not in df.columns]

    wait_for_kafka(BOOTSTRAP)
    
    dt = pd.to_datetime(df["transaction_time"], errors="coerce")
    df["transaction_time"] = dt.dt.strftime("%Y-%m-%d %H:%M:%S")

    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
        acks="all",
        linger_ms=20,
        retries=10,
    )

    sent = 0
    for _, row in df.iterrows():
        msg = {
            "transaction_time": row["transaction_time"],
            "us_state": str(row["us_state"]),
            "cat_id": str(row["cat_id"]),
            "amount": float(row["amount"]),
        }
        producer.send(TOPIC, msg)
        sent += 1
        if sent % 5000 == 0:
            producer.flush()
            print(f"Sent {sent} messages...")

    producer.flush()
    print(f"Done. Sent {sent} messages to topic '{TOPIC}'")

if __name__ == "__main__":
    main()

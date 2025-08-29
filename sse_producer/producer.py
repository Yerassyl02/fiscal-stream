import requests
import time
import json
from kafka import KafkaProducer, errors

SSE_URL = "http://fiscal-source:8000/stream"
TOPIC = "fiscal_row"

# подключаемся к Kafka
producer = None
for i in range(20):
    try:
        producer = KafkaProducer(
            bootstrap_servers="kafka:9092",
            value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
            linger_ms=50,
            acks="all",   # ждём подтверждения от всех ISR реплик
            retries=5     # пробуем переслать, если не получилось
        )
        print("✅ Connected to Kafka")
        break
    except errors.NoBrokersAvailable:
        print(f"⚠️ Kafka not ready, retry {i+1}/20...")
        time.sleep(5)

if not producer:
    raise RuntimeError("❌ Could not connect to Kafka after retries")

def is_json_line(b: bytes) -> bool:
    b = b.strip()
    return b.startswith(b"{") and b.endswith(b"}")

with requests.get(SSE_URL, stream=True) as r:
    for line in r.iter_lines():
        if not line:
            continue

        if line.startswith(b"data:"):
            payload = line[5:].strip()
        else:
            payload = line
        
        if is_json_line(payload):
            obj = json.loads(payload.decode("utf-8"))
            print("📤 Sending:", obj)

            future = producer.send(TOPIC, value=obj)

            try:
                record_metadata = future.get(timeout=10)
                print(f"✅ Sent to topic={record_metadata.topic}, "
                      f"partition={record_metadata.partition}, "
                      f"offset={record_metadata.offset}")
            except Exception as e:
                print("❌ Failed to send:", e)

            producer.flush()
            time.sleep(0.5)
        else:
            print('❌❌❌❌❌')

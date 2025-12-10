# producer.py
import json
import time
from confluent_kafka import Producer
import pandas as pd

BOOTSTRAP_SERVERS = "localhost:9092"   # Kafka broker
TOPIC_REQUESTS = "premium_requests"    # producer gửi dữ liệu raw vào đây

conf = {"bootstrap.servers": BOOTSTRAP_SERVERS}
producer = Producer(conf)

# danh sách feature để gửi đi, có thể dùng chung cho cả binom/gamma
features_all = [
    "Agent_Age", "Agent_Rating",
    "Efficiency",
    "Expected_Speed", "Expected_Time_min",
    "Traffic_Weather", "Vehicle_Area",
    "Order_Hour", "Area",
    "Goods_Value", "Distance", "Traffic"
]

def delivery_report(err, msg):
    if err is not None:
        print(f"Delivery failed: {err}")
    # else: thành công, không cần print

def make_payload(row_dict, feature_list, meta=None):
    payload = {k: row_dict.get(k) for k in feature_list}
    # đảm bảo không có numpy types gây lỗi JSON
    for k, v in list(payload.items()):
        if v is None:
            payload[k] = None
        elif hasattr(v, "item"):
            try:
                payload[k] = v.item()
            except Exception:
                payload[k] = str(v)
    if meta:
        payload["_meta"] = meta
    return payload

def produce_from_dataframe(df: pd.DataFrame):
    for idx, row in df.iterrows():
        rowd = row.to_dict()
        meta = {"source_idx": int(idx), "ts": time.time()}

        payload = make_payload(rowd, features_all, meta=meta)

        
        key = str(rowd.get("OrderID", idx))

        producer.produce(
            topic=TOPIC_REQUESTS,
            key=key,
            value=json.dumps(payload),
            callback=delivery_report
        )

        # poll để phục vụ callback
        producer.poll(0)

    # block cho đến khi tất cả message được gửi
    producer.flush()

if __name__ == "__main__":
    df = pd.read_csv("/Users/hoangvupham/Desktop/KLTN_VU/Data/final_cleaned_data.csv")
    produce_from_dataframe(df)

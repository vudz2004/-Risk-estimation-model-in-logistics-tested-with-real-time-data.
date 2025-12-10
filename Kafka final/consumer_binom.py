import json
import pickle
import time
import logging
from confluent_kafka import Consumer, Producer
import pandas as pd
import numpy as np

# Nếu dùng statsmodels fallback
import statsmodels.api as sm

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

# ====== CONFIG ======
BOOTSTRAP_SERVERS = "localhost:9092"
IN_TOPIC = "premium_requests"   # đọc từ topic của producer
OUT_TOPIC = "binom_results"     # gửi kết quả binom
GROUP_ID = "consumer_binom_group"

BINOM_MODEL_PATH = "/Users/hoangvupham/Desktop/KLTN_VU/Kafka/binom_model.pkl"
ENCODERS_PATH = "/Users/hoangvupham/Desktop/KLTN_VU/Kafka/encoders.pkl"
SCALER_PATH = "/Users/hoangvupham/Desktop/KLTN_VU/Kafka/scalers.pkl"
# =====================

features_binom = [
    "Agent_Age", "Agent_Rating",
    "Efficiency",
    "Expected_Speed", "Expected_Time_min",
    "Traffic_Weather", "Vehicle_Area",
    "Order_Hour", "Area"
]

# ---- load model and transforms ----
def safe_load(path):
    try:
        return pickle.load(open(path, "rb"))
    except Exception as e:
        logging.warning(f"Không thể load {path}: {e}")
        return None

binom_model = safe_load(BINOM_MODEL_PATH)
encoders_binom = safe_load(ENCODERS_PATH)
scaler_binom = safe_load(SCALER_PATH)

# ---- kafka clients ----
consumer_conf = {
    "bootstrap.servers": BOOTSTRAP_SERVERS,
    "group.id": GROUP_ID,
    "auto.offset.reset": "earliest"
}
producer_conf = {"bootstrap.servers": BOOTSTRAP_SERVERS}

consumer = Consumer(consumer_conf)
producer = Producer(producer_conf)

consumer.subscribe([IN_TOPIC])

# ---- helpers ----
def get_message_key(msg):
    if msg.key() is not None:
        try:
            return msg.key().decode("utf-8")
        except Exception:
            return str(msg.key())
    return None

def prepare_row_for_model(row_dict, feature_list, encoders=None, scaler=None):
    df = pd.DataFrame([row_dict])
    if encoders:
        for col, enc in encoders.items():
            if col in df.columns:
                try:
                    df[col] = enc.transform(df[col].astype(str))
                except Exception:
                    df[col] = -1
    for f in feature_list:
        if f not in df.columns:
            df[f] = 0
    X = df[feature_list].astype(float).values
    if scaler is not None:
        try:
            X = scaler.transform(X)
        except Exception:
            logging.warning("Scaler transform failed; passing raw features")
    return X

def predict_proba_for_binom(X):
    try:
        proba = binom_model.predict_proba(X)[0, 1]
        return float(proba)
    except Exception:
        try:
            Xc = sm.add_constant(X, has_constant='add')
            return float(binom_model.predict(Xc)[0])
        except Exception as e:
            logging.error(f"Predict failed: {e}")
            raise

def publish_prediction(key, meta, p):
    payload = {"probability": float(p), "_meta": meta}
    try:
        producer.produce(topic=OUT_TOPIC, key=key, value=json.dumps(payload))
        producer.poll(0)
    except Exception as e:
        logging.error(f"Publish failed: {e}")

# ---- main loop ----
def main():
    logging.info("Starting consumer_binom...")
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                logging.error(f"Consumer error: {msg.error()}")
                continue

            try:
                raw = msg.value().decode("utf-8")
                payload = json.loads(raw)
            except Exception as e:
                logging.error(f"Failed decode json: {e} raw={msg.value()}")
                continue

            key = get_message_key(msg)
            meta = payload.get("_meta", {})
            try:
                X = prepare_row_for_model(payload, features_binom, encoders_binom, scaler_binom)
                p = predict_proba_for_binom(X)
                logging.info(f"[BINOM] key={key} p={p:.6f} meta={meta}")
                use_key = key if key is not None else str(meta.get("source_idx", meta.get("id", "no_key")))
                publish_prediction(use_key, meta, p)
            except Exception as e:
                logging.exception(f"Error processing message: {e}")
    except KeyboardInterrupt:
        logging.info("Shutting down consumer_binom (KeyboardInterrupt)")
    finally:
        consumer.close()

if __name__ == "__main__":
    main()

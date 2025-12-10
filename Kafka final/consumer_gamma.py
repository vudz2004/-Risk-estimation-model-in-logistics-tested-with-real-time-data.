import json
import pickle
import time
import logging
from confluent_kafka import Consumer, Producer
import pandas as pd
import numpy as np
import statsmodels.api as sm

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

# ====== CONFIG ======
BOOTSTRAP_SERVERS = "localhost:9092"
IN_TOPIC = "premium_requests"   # đọc từ topic producer gửi
OUT_TOPIC = "gamma_results"     # gửi kết quả gamma
GROUP_ID = "consumer_gamma_group"

GAMMA_MODEL_PATH = "/Users/hoangvupham/Desktop/KLTN_VU/Kafka/gamma_model.pkl"
ENCODERS_PATH = "/Users/hoangvupham/Desktop/KLTN_VU/Kafka/encoders_sev.pkl"
SCALER_PATH = "/Users/hoangvupham/Desktop/KLTN_VU/Kafka/scalers_sev.pkl"
# =====================

features_gamma = ["Goods_Value", "Distance", "Traffic", "Agent_Rating"]

# ---- load model and transforms ----
def safe_load(path):
    try:
        return pickle.load(open(path, "rb"))
    except Exception as e:
        logging.warning(f"Không thể load {path}: {e}")
        return None

gamma_model = safe_load(GAMMA_MODEL_PATH)
encoders_gamma = safe_load(ENCODERS_PATH)
scaler_gamma = safe_load(SCALER_PATH)

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

def predict_gamma(X):
    try:
        val = gamma_model.predict(X)[0]
        return float(val)
    except Exception:
        try:
            Xc = sm.add_constant(X, has_constant='add')
            return float(gamma_model.predict(Xc)[0])
        except Exception as e:
            logging.error(f"Predict failed: {e}")
            raise

def publish_prediction(key, meta, sev):
    payload = {"severity": float(sev), "_meta": meta}
    try:
        producer.produce(topic=OUT_TOPIC, key=key, value=json.dumps(payload))
        producer.poll(0)
    except Exception as e:
        logging.error(f"Publish failed: {e}")

# ---- main loop ----
def main():
    logging.info("Starting consumer_gamma...")
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
                X = prepare_row_for_model(payload, features_gamma, encoders_gamma, scaler_gamma)
                sev = predict_gamma(X)
                logging.info(f"[GAMMA] key={key} sev={sev:.6f} meta={meta}")
                use_key = key if key is not None else str(meta.get("source_idx", meta.get("id", "no_key")))
                publish_prediction(use_key, meta, sev)
            except Exception as e:
                logging.exception(f"Error processing message: {e}")
    except KeyboardInterrupt:
        logging.info("Shutting down consumer_gamma (KeyboardInterrupt)")
    finally:
        consumer.close()

if __name__ == "__main__":
    main()

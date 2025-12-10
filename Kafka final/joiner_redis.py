import json
import time
import redis
from confluent_kafka import Consumer, Producer

# ----- CONFIG -----
BOOTSTRAP_SERVERS = "localhost:9092"
GROUP_ID = "joiner_redis_group"
TOPICS = ["binom_results", "gamma_results"]  # topic mới

REDIS_HOST = "localhost"
REDIS_PORT = 6379
REDIS_DB = 0
TTL_SECONDS = 60 * 5  # TTL cho các key tạm: 5 phút

OUT_TOPIC = "premium_out"
# -------------------

# Kafka clients
producer = Producer({"bootstrap.servers": BOOTSTRAP_SERVERS})

consumer_conf = {
    "bootstrap.servers": BOOTSTRAP_SERVERS,
    "group.id": GROUP_ID,
    "auto.offset.reset": "earliest"
}
consumer = Consumer(consumer_conf)
consumer.subscribe(TOPICS)

# Redis client
r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB, decode_responses=True)

def publish_premium(key, meta, premium):
    payload = {"premium": float(premium), "_meta": meta}
    try:
        producer.produce(OUT_TOPIC, key=key, value=json.dumps(payload))
        producer.poll(0)
        print(f"[PREMIUM] key={key} premium={premium:.6f} meta={meta}")
    except Exception as e:
        print(f"[ERROR] publish premium failed: {e}")

def handle_pred_binom(key, data):
    p = float(data.get("probability"))
    r.hset(key, "p", p)
    if data.get("_meta") is not None:
        r.hset(key, "meta", json.dumps(data.get("_meta")))
    r.expire(key, TTL_SECONDS)

    sev = r.hget(key, "sev")
    if sev is not None:
        try:
            sev_f = float(sev)
            meta = json.loads(r.hget(key, "meta") or "{}")
            publish_premium(key, meta, p * sev_f)
        except Exception as e:
            print(f"[ERROR] computing premium for key={key}: {e}")
        finally:
            r.delete(key)

def handle_pred_gamma(key, data):
    sev = float(data.get("severity"))
    r.hset(key, "sev", sev)
    if data.get("_meta") is not None:
        r.hset(key, "meta", json.dumps(data.get("_meta")))
    r.expire(key, TTL_SECONDS)

    p = r.hget(key, "p")
    if p is not None:
        try:
            p_f = float(p)
            meta = json.loads(r.hget(key, "meta") or "{}")
            publish_premium(key, meta, p_f * sev)
        except Exception as e:
            print(f"[ERROR] computing premium for key={key}: {e}")
        finally:
            r.delete(key)

def get_key_from_msg(msg, payload):
    if msg.key() is not None:
        try:
            return msg.key().decode("utf-8")
        except Exception:
            return str(msg.key())
    meta = payload.get("_meta", {})
    return str(meta.get("source_idx", meta.get("id", "no_key")))

def main():
    print("Joiner (Redis) started. Subscribed to:", TOPICS)
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                print("Consumer error:", msg.error())
                continue

            try:
                payload = json.loads(msg.value().decode("utf-8"))
            except Exception as e:
                print("Failed to parse message value:", e, "raw:", msg.value())
                continue

            key = get_key_from_msg(msg, payload)
            topic = msg.topic()
            if topic == "binom_results":
                handle_pred_binom(key, payload)
            elif topic == "gamma_results":
                handle_pred_gamma(key, payload)
            else:
                print("Unknown topic:", topic)
    except KeyboardInterrupt:
        print("Joiner interrupted by user")
    finally:
        consumer.close()

if __name__ == "__main__":
    main()

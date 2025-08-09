from flask import Flask, jsonify
from flask_cors import CORS 
from kafka import KafkaConsumer
from kafka.errors import KafkaError
from threading import Thread
import os
import json
from sqlalchemy import create_engine, Column, Integer, String
from sqlalchemy.orm import sessionmaker, declarative_base
from dotenv import load_dotenv
from dataclasses import dataclass
from datetime import datetime
import json
import time
import base64
import requests
import logging

load_dotenv()

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "user-login-events")
FLASK_PORT = int(os.getenv("FLASK_PORT", 5002))
DATABASE_URL = os.getenv("DATABASE_URL")
ELASTICSEARCH_URL = os.getenv("ELASTICSEARCH_URL")

logging.basicConfig(level=logging.WARNING)
logger = logging.getLogger(__name__)

logging.getLogger('kafka').setLevel(logging.WARNING)
logging.getLogger('kafka.conn').setLevel(logging.WARNING)
logging.getLogger('kafka.coordinator').setLevel(logging.WARNING)
logging.getLogger('kafka.protocol.parser').setLevel(logging.WARNING)
logging.getLogger('kafka.consumer.fetcher').setLevel(logging.WARNING)

# ================= SERVICE DISCOVERY =================
ETCD_URL = os.getenv("ETCD_URL")

# tbd move this into a shared library and import it in all services
def etcd_register(service_name, service_host, service_port):
    key = f"/services/{service_name}"
    value = f"{service_host}:{service_port}"
    url = f"{ETCD_URL}/v3/kv/put"
    data = {
        "key": base64.b64encode(key.encode()).decode(),
        "value": base64.b64encode(value.encode()).decode()
    }
    resp = requests.post(url, json=data)
    if resp.status_code == 200:
        logger.debug(f"Registered {service_name} at {value} in etcd")
    else:
        logger.debug(f"Failed to register service in etcd: {resp.text}")
# ================= SERVICE DISCOVERY =================

MAX_RETRIES = 5
RETRY_BACKOFF = 5

app = Flask(__name__)
CORS(app)

#tbd move to a separate file
@dataclass
class EventLog:
    user_id: str
    session_id: str
    timestamp: datetime
    event_type: str
    details: dict

engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(bind=engine)
Base = declarative_base()

class LoginEvent(Base):
    __tablename__ = "login_events"
    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(String, index=True)
    session_id = Column(String, unique=True, index=True)

Base.metadata.create_all(bind=engine)

def consume_kafka():
    retries = 0
    while retries < MAX_RETRIES:
        try:
            consumer = KafkaConsumer(
                KAFKA_TOPIC,
                bootstrap_servers=KAFKA_BROKER,
                auto_offset_reset="earliest",
                group_id="event-writer-group",
                max_poll_records=100,
                max_poll_interval_ms=300000,
                value_deserializer=lambda m: json.loads(m.decode("utf-8")),
            )
            logger.debug("Connected to Kafka, starting consuming events...")

            for msg in consumer:
                data = msg.value
                user_id = data.get("userId")
                session_id = data.get("sessionId")
                if user_id and session_id:
                    db = SessionLocal()
                    try:
                        event = LoginEvent(user_id=user_id, session_id=session_id)
                        db.add(event)
                        db.commit()
                        logger.debug(f"[Kafka] Saved login event for user {user_id}, session {session_id}")
                        send_to_es(user_id, session_id, "login")
                    except Exception as e:
                        db.rollback()
                        logger.debug(f"[Kafka] DB error: {e}")
                    finally:
                        db.close()
                else:
                    logger.debug(f"[Kafka] Invalid data: {data}")

        except KafkaError as e:
            logger.debug(f"Kafka connection error: {e}. Retry {retries+1}/{MAX_RETRIES}")
            retries += 1
            time.sleep(RETRY_BACKOFF * retries)

    if retries == MAX_RETRIES:
        logger.debug("Failed to connect to Kafka after several retries. Exiting.")

 # Send to Elasticsearch
def send_to_es(user_id, session_id, event_type):
    index = "myapp-logs"
    url = f"{ELASTICSEARCH_URL}/{index}/_doc"
    headers = {"Content-Type": "application/json"}

    event = EventLog(
        user_id=user_id,
        session_id=session_id,
        timestamp=datetime.utcnow(),
        event_type=event_type
    )
    data = json.dumps(event.__dict__, default=str)
    resp = requests.post(url, headers=headers, json=data)


@app.route("/health")
def health():
    return jsonify({"status": "ok"})

if __name__ == "__main__":
    etcd_register("event-writer-service", "event-writer-service", FLASK_PORT)
    Thread(target=consume_kafka, daemon=True).start()
    app.run(host="0.0.0.0", port=FLASK_PORT)

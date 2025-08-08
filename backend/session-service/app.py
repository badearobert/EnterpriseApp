from flask import Flask, request, jsonify
from flask_cors import CORS 
from kafka import KafkaProducer
import uuid
import os
import json
from dotenv import load_dotenv
import base64
import requests
import logging

load_dotenv()

KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "user-login-events")
FLASK_PORT = int(os.getenv("FLASK_PORT", 5001))

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)
producer = KafkaProducer(
    bootstrap_servers=os.getenv("KAFKA_BROKER", "kafka:9092"),
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# tbd add in-memory store for session management
session_map = {}  # user_id -> session data

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

app = Flask(__name__)
CORS(app)

@app.route("/get-session/<string:user_id>", methods=["GET"])
def get_session(user_id):
    if user_id in session_map:
        return jsonify(session_map[user_id])

    session_id = str(uuid.uuid4())
    session_data = {
        "userId": user_id,
        "sessionId": session_id,
        "event": "login"
    }
    session_map[user_id] = session_data
    try:
        producer.send(KAFKA_TOPIC, session_data)
        producer.flush()
    except Exception as e:
        return jsonify({"error": f"Kafka error: {str(e)}"}), 500

    return jsonify(session_data)

@app.route("/health")
def health():
    return jsonify({"status": "ok"})

if __name__ == "__main__":
    etcd_register("session-service", "session-service", FLASK_PORT)
    app.run(host="0.0.0.0", port=FLASK_PORT)

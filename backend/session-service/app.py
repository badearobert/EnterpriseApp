from flask import Flask, request, jsonify
from flask_cors import CORS 
from kafka import KafkaProducer
import uuid
import os
import json
from dotenv import load_dotenv

load_dotenv()

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "user-login-events")
FLASK_PORT = int(os.getenv("FLASK_PORT", 5001))

# tbd add in-memory store for session management
session_map = {}  # user_id -> session data

app = Flask(__name__)
CORS(app)

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

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
    app.run(host="0.0.0.0", port=FLASK_PORT)

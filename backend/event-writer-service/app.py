from flask import Flask, jsonify
from flask_cors import CORS 
from kafka import KafkaConsumer
from threading import Thread
import os
import json
from sqlalchemy import create_engine, Column, Integer, String
from sqlalchemy.orm import sessionmaker, declarative_base
from dotenv import load_dotenv

load_dotenv()

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "user-login-events")
FLASK_PORT = int(os.getenv("FLASK_PORT", 5002))
DATABASE_URL = os.getenv("DATABASE_URL")

app = Flask(__name__)
CORS(app)

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
    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BROKER,
        auto_offset_reset="earliest",
        group_id="login-event-writer",
        value_deserializer=lambda m: json.loads(m.decode("utf-8"))
    )
    db = SessionLocal()
    for msg in consumer:
        data = msg.value
        user_id = data.get("userId")
        session_id = data.get("sessionId")
        if user_id and session_id:
            event = LoginEvent(user_id=user_id, session_id=session_id)
            try:
                db.add(event)
                db.commit()
                print(f"[Kafka] Saved login event for user {user_id}, session {session_id}")
            except Exception as e:
                db.rollback()
                print(f"[Kafka] DB error: {e}")
        else:
            print(f"[Kafka] Invalid data: {data}")

@app.route("/health")
def health():
    return jsonify({"status": "ok"})

if __name__ == "__main__":
    Thread(target=consume_kafka, daemon=True).start()
    app.run(host="0.0.0.0", port=FLASK_PORT)

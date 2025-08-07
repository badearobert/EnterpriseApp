from flask import Flask, jsonify, request
from flask_cors import CORS
import psycopg2
import os
import requests
from dotenv import load_dotenv

app = Flask(__name__)
CORS(app, origins=["http://localhost:8080"])
load_dotenv()

SESSION_SERVICE_URL = os.getenv("SESSION_SERVICE_URL")


@app.route("/health", methods=["GET"])
def health():
    return jsonify({"status": "ok"}), 200


@app.route("/session/<int:user_id>", methods=["GET"])
def get_session(user_id):
    try:
        resp = requests.get(f"{SESSION_SERVICE_URL}/get-session/{user_id}")
        resp.raise_for_status()
        data = resp.json()
        dto = {
            "sessionid": data.get("sessionId")
        }
        return jsonify(dto), resp.status_code
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    
if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)

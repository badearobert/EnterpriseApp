from flask import Flask, jsonify, request, Response
from flask_cors import CORS
import psycopg2
import os
import requests
from dotenv import load_dotenv
from prometheus_client import Counter, generate_latest, CONTENT_TYPE_LATEST

app = Flask(__name__)
CORS(app, origins=["http://localhost:8080"])
load_dotenv()

SESSION_SERVICE_URL = os.getenv("SESSION_SERVICE_URL")
REQUEST_COUNT = Counter(
    'app_requests_total',
    'Total HTTP Requests'
)

@app.route("/health", methods=["GET"])
def health():
    return jsonify({"status": "ok"}), 200


# ===================== MONITORING =====================
@app.before_request
def before_request():
    REQUEST_COUNT.inc()

@app.route("/metrics")
def metrics():
    return Response(generate_latest(), mimetype=CONTENT_TYPE_LATEST)
#========================================================
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

from flask import Flask, jsonify, request, Response
from flask_cors import CORS
import psycopg2
import os
import requests
from dotenv import load_dotenv
from prometheus_client import Counter, generate_latest, CONTENT_TYPE_LATEST
import base64
import logging
import uuid


app = Flask(__name__)
CORS(app, origins=["http://localhost:8080"])
load_dotenv()

REQUEST_COUNT = Counter(
    'app_requests_total',
    'Total HTTP Requests'
)

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)
# ================= SERVICE DISCOVERY =================
ETCD_URL = os.getenv("ETCD_URL", "http://etcd:2379")

# tbd move this into a shared library and import it in all services
def get_service_address(service_name):
    url = f"{ETCD_URL}/v3/kv/range"
    key = f"/services/{service_name}"
    data = {
        "key": base64.b64encode(key.encode()).decode()
    }
    resp = requests.post(url, json=data)
    resp.raise_for_status()
    res_json = resp.json()
    if "kvs" in res_json and len(res_json["kvs"]) > 0:
        value = base64.b64decode(res_json["kvs"][0]["value"]).decode("utf-8")
        return value
    return None
# ===================== MONITORING =====================
@app.before_request
def before_request():
    REQUEST_COUNT.inc()
    request.correlation_id = request.headers.get('X-Correlation-ID') or str(uuid.uuid4())

@app.after_request
def add_correlation_id_to_response(response):
    response.headers['X-Correlation-ID'] = getattr(request, 'correlation_id', '')
    return response

@app.route("/metrics")
def metrics():
    return Response(generate_latest(), mimetype=CONTENT_TYPE_LATEST)
# =======================================================
@app.route("/health", methods=["GET"])
def health():
    return jsonify({"status": "ok"}), 200
#========================================================
@app.route("/session/<string:user_id>", methods=["GET"])
def get_session(user_id):
    try:
        logger.info(f"[{request.correlation_id}] Received request for session of user {user_id}")
        session_service = get_session_service_address()
        resp = requests.get(f"{session_service}/get-session/{user_id}")
        resp.raise_for_status()
        data = resp.json()
        dto = {
            "sessionid": data.get("sessionId")
        }
        return jsonify(dto), resp.status_code
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/user/<string:user_id>', methods=['GET'])
def get_user_data(user_id):
    logger.info(f"[{request.correlation_id}] Received request for user data of user {user_id}")
    user_data_service = get_user_data_service_address()
    resp = requests.get(f"{user_data_service}/user/{user_id}")
    return (resp.content, resp.status_code, resp.headers.items())

@app.route('/user/<user_id>', methods=['POST'])
def add_user_data(user_id):
    logger.info(f"[{request.correlation_id}] Received request to add user data of user {user_id}")
    user_data_service = get_user_data_service_address()
    frontend_data = request.json or {}
    
    user_data_model = {
        "user_id": user_id,
        "data": frontend_data
    }
    
    print(f"Received from frontend: {frontend_data}")
    print(f"Sending to user service: {user_data_model}")
    
    resp = requests.post(f"{user_data_service}/user", json=user_data_model)
    return (resp.content, resp.status_code, resp.headers.items())

#========================================================
def get_session_service_address():
    session_service = get_service_address("session-service")
    if not session_service.startswith("http"):
        session_service = "http://" + session_service
    logger.debug(f"[DEBUG] session_service address: {session_service}")
    return session_service

def get_user_data_service_address():
    user_data_service = get_service_address("user-data-service")
    if not user_data_service.startswith("http"):
        user_data_service = "http://" + user_data_service
    return user_data_service

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)

import sys
import os
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
import grpc
sys.path.append(os.path.join(os.path.dirname(__file__), 'proto'))
from proto import user_pb2
from proto import user_pb2_grpc

load_dotenv()

FLASK_PORT = int(os.getenv("FLASK_PORT", 5004))

DATA_SERVICE_GRPC_HOST = os.getenv("DATA_SERVICE_GRPC_HOST")
DATA_SERVICE_GRPC_PORT = os.getenv("DATA_SERVICE_GRPC_PORT")

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

app = Flask(__name__)
CORS(app)
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
def get_grpc_channel():
    return grpc.insecure_channel(f"{DATA_SERVICE_GRPC_HOST}:{DATA_SERVICE_GRPC_PORT}")

@app.route('/user/<user_id>', methods=['GET'])
def get_user(user_id):
    channel = get_grpc_channel()
    stub = user_pb2_grpc.UserServiceStub(channel)
    try:
        response = stub.GetUser(user_pb2.UserRequest(user_id=user_id))
        return jsonify({"user_id": response.user_id, "data": response.data}), 200
        
    except grpc.RpcError as e:
        print(f"gRPC error: {e}")
        return jsonify({"error": "User not found"}), 404

@app.route('/user', methods=['POST'])
def add_user():
    request_data = request.json
    user_id = request_data.get("user_id")
    data = request_data.get("data")
    if not user_id or not data:
        return jsonify({"error": "Missing user_id or data"}), 400

    channel = get_grpc_channel()
    stub = user_pb2_grpc.UserServiceStub(channel)
    try:
        data_str = json.dumps(data) if isinstance(data, dict) else str(data)
        
        logger.debug(f"Sending user_id: {user_id}")
        logger.debug(f"Sending data (as string): {data_str}")
        
        response = stub.AddUser(user_pb2.AddUserRequest(user_id=user_id, data=data_str))
        
        if response.success:
            return jsonify({"message": response.message}), 201
        else:
            return jsonify({"error": response.message}), 400
            
    except grpc.RpcError as e:
        logger.debug(f"gRPC error: {e}")
        return jsonify({"error": "Failed to add user"}), 500
    except Exception as e:
        logger.debug(f"Unexpected error: {e}")
        return jsonify({"error": "Internal server error"}), 500

@app.route("/health")
def health():
    return jsonify({"status": "ok"})

if __name__ == "__main__":
    etcd_register("user-data-service", "user-data-service", FLASK_PORT)
    app.run(host="0.0.0.0", port=FLASK_PORT)

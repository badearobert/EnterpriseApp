import sys
import os
from flask import Flask, jsonify
import time
from cassandra.cluster import Cluster, NoHostAvailable
from dotenv import load_dotenv
import os
from flask_cors import CORS
import base64
import requests
import logging
from concurrent import futures
import grpc
import threading
sys.path.append(os.path.join(os.path.dirname(__file__), 'proto'))
from proto import user_pb2
from proto import user_pb2_grpc

load_dotenv()

CASSANDRA_CLUSTER_NAME = os.getenv("CASSANDRA_CLUSTER_NAME")
FLASK_PORT = int(os.getenv("DATA_SERVICE_PORT", 5003))
ETCD_URL = os.getenv("ETCD_URL")

app = Flask(__name__)
CORS(app)

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

# ================= CASSANDRA CONNECTION =================
def connect(retries=10, delay=5):
    for attempt in range(1, retries + 1):
        try:
            cluster = Cluster([CASSANDRA_CLUSTER_NAME])
            session = cluster.connect()
            logger.debug(f"Successfully connected to Cassandra on attempt {attempt}.")
            return cluster, session
        except NoHostAvailable:
            logger.debug(f"Attempt {attempt} failed, retrying in {delay} seconds...")
            time.sleep(delay)
    raise Exception("Failed to connect to Cassandra after all retry attempts.")

cluster, session = connect()
# ================= SERVICE DISCOVERY =================
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
        logging.debug(f"Registered {service_name} at {value} in etcd")
    else:
        logging.debug(f"Failed to register service in etcd: {resp.text}")
# ================= gRPC SERVER =================
class UserService(user_pb2_grpc.UserServiceServicer):
    def GetUser(self, request, context):
        user_id = request.user_id
        query = "SELECT data FROM users WHERE user_id=%s"
        result = session.execute(query, (user_id,))
        row = result.one()
        if row:
            return user_pb2.UserResponse(user_id=user_id, data=row.data)
        else:
            context.set_code(grpc.StatusCode.NOT_FOUND)
            context.set_details('User not found')
            return user_pb2.UserResponse()
            
    def AddUser(self, request, context):
        user_id = request.user_id
        data = request.data
        query = "INSERT INTO users (user_id, data) VALUES (%s, %s)"
        try:
            session.execute(query, (user_id, data))
            return user_pb2.AddUserResponse(success=True, message="User added successfully")
        except Exception as e:
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(f'Failed to add user: {str(e)}')
            return user_pb2.AddUserResponse(success=False, message=str(e))

def serve_grpc():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    user_pb2_grpc.add_UserServiceServicer_to_server(UserService(), server)
    server.add_insecure_port('[::]:50051')
    server.start()
    logger.info("gRPC server running on port 50051")
    try:
        while True:
            time.sleep(86400)
    except KeyboardInterrupt:
        server.stop(0)

@app.route("/health")
def health():
    return jsonify({"status": "ok"})

if __name__ == '__main__':
    etcd_register("data-service", "data-service", FLASK_PORT)
        
    # start gRPC server in a background thread
    grpc_thread = threading.Thread(target=serve_grpc)
    grpc_thread.daemon = True
    grpc_thread.start()

    app.run(host='0.0.0.0', port=FLASK_PORT)

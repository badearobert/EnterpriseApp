from flask import Flask, jsonify
import time
from cassandra.cluster import Cluster, NoHostAvailable
from dotenv import load_dotenv
import os
from flask_cors import CORS
import base64
import requests
import logging

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

# tbd move this to cql and load on startup
def create_keyspace_and_table(session):
    session.execute("""
        CREATE KEYSPACE IF NOT EXISTS mykeyspace
        WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};
    """)
    session.set_keyspace('mykeyspace')
    session.execute("""
        CREATE TABLE IF NOT EXISTS users (
            user_id text PRIMARY KEY,
            data text
        );
    """)
    logger.debug("Keyspace loaded!")


cluster, session = connect()
create_keyspace_and_table(session)
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
# ================= SERVICE DISCOVERY =================
@app.route('/user/<user_id>', methods=['GET'])
def get_user(user_id):
    query = "SELECT data FROM users WHERE user_id=%s"
    result = session.execute(query, (user_id,))
    row = result.one()
    if row:
        return jsonify({"user_id": user_id, "data": row.data})
    return jsonify({"error": "User not found"}), 404

@app.route("/health")
def health():
    return jsonify({"status": "ok"})

if __name__ == '__main__':
    etcd_register("data-service", "data-service", FLASK_PORT)
    app.run(host='0.0.0.0', port=FLASK_PORT)

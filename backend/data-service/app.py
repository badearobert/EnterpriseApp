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
from opentelemetry import trace
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.instrumentation.flask import FlaskInstrumentor

app = Flask(__name__)
CORS(app)
load_dotenv()

# Debug environment variables
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.DEBUG)

CASSANDRA_HOST = os.getenv('CASSANDRA_HOST', 'cassandra')
FLASK_PORT = int(os.getenv("DATA_SERVICE_PORT", 5003))
ETCD_URL = os.getenv("ETCD_URL")

logger.info(f"Environment Variables:")
logger.info(f"CASSANDRA_HOST: '{CASSANDRA_HOST}'")
logger.info(f"FLASK_PORT: {FLASK_PORT}")
logger.info(f"ETCD_URL: '{ETCD_URL}'")



logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

# ================= OPEN TELEMETRY  =================
open_telemetry_endpoint = os.getenv("OTEL_COLLECTOR_ENDPOINT")
resource = Resource(attributes={"service.name": "data-service"})
trace.set_tracer_provider(TracerProvider(resource=resource))
tracer = trace.get_tracer(__name__)

otlp_exporter = OTLPSpanExporter(endpoint=open_telemetry_endpoint, insecure=True)
span_processor = BatchSpanProcessor(otlp_exporter)
trace.get_tracer_provider().add_span_processor(span_processor)

FlaskInstrumentor().instrument_app(app)
# ================= CASSANDRA CONNECTION =================
def connect(retries=10, delay=5):
    logger.info(f"Attempting to connect to Cassandra at host: {CASSANDRA_HOST}")
    
    for attempt in range(1, retries + 1):
        try:
            logger.debug(f"Connection attempt {attempt}/{retries} to {CASSANDRA_HOST}:9042")
            cluster = Cluster(
                [CASSANDRA_HOST],
                port=9042,
                connect_timeout=10,
                control_connection_timeout=10
            )
            session = cluster.connect()
            logger.info(f"Successfully connected to Cassandra on attempt {attempt}")
            
            try:
                session.execute("SELECT release_version FROM system.local")
                logger.info("Cassandra connection verified")
            
                logger.info("🔧 Setting up database schema...")
                setup_database_schema(session)
                return cluster, session
            except Exception as e:
                logger.error(f"Connection test failed: {e}")
                cluster.shutdown()
                raise e
                
        except NoHostAvailable as e:
            logger.warning(f"Attempt {attempt}/{retries} failed - No hosts available: {e}")
            if attempt < retries:
                logger.info(f"Retrying in {delay} seconds...")
                time.sleep(delay)
            else:
                logger.error(f"All connection attempts failed. Last error: {e}")
                
        except Exception as e:
            logger.error(f"Unexpected error on attempt {attempt}/{retries}: {type(e).__name__}: {e}")
            if attempt < retries:
                logger.info(f"Retrying in {delay} seconds...")
                time.sleep(delay)
            else:
                logger.error(f"All connection attempts failed. Last error: {e}")
    
    raise Exception(f"Failed to connect to Cassandra at {CASSANDRA_HOST} after {retries} attempts")

# Initialize Cassandra connection
cluster = None
session = None

def initialize_cassandra():
    global cluster, session
    try:
        cluster, session = connect()
        logger.info("Cassandra initialized successfully")
    except Exception as e:
        logger.error(f"Failed to initialize Cassandra: {e}")
        raise e

def setup_database_schema(session):
    """Setup keyspace and tables if they don't exist"""
    try:
        # Create keyspace
        keyspace_query = """
        CREATE KEYSPACE IF NOT EXISTS testks 
        WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}
        """
        session.execute(keyspace_query)
        logger.info("Keyspace 'testks' created/verified")
        
        # Use keyspace
        session.set_keyspace('testks')
        logger.info("Using keyspace 'testks'")
        
        # Create table
        table_query = """
        CREATE TABLE IF NOT EXISTS users (
            user_id TEXT PRIMARY KEY,
            data TEXT
        )
        """
        session.execute(table_query)
        logger.info("Table 'users' created/verified")
        
    except Exception as e:
        logger.error(f"Failed to setup database schema: {e}")
        raise e

# ================= SERVICE DISCOVERY =================
def etcd_register(service_name, service_host, service_port):
    if not ETCD_URL:
        logger.warning("ETCD_URL not configured, skipping service registration")
        return
        
    key = f"/services/{service_name}"
    value = f"{service_host}:{service_port}"
    url = f"{ETCD_URL}/v3/kv/put"
    data = {
        "key": base64.b64encode(key.encode()).decode(),
        "value": base64.b64encode(value.encode()).decode()
    }
    try:
        resp = requests.post(url, json=data, timeout=10)
        if resp.status_code == 200:
            logger.info(f"Registered {service_name} at {value} in etcd")
        else:
            logger.error(f"Failed to register service in etcd: {resp.status_code} - {resp.text}")
    except Exception as e:
        logger.error(f"Error registering service in etcd: {e}")

# ================= gRPC SERVER =================
class UserService(user_pb2_grpc.UserServiceServicer):
    def GetUser(self, request, context):
        if not session:
            context.set_code(grpc.StatusCode.UNAVAILABLE)
            context.set_details('Database connection not available')
            return user_pb2.UserResponse()
            
        user_id = request.user_id
        query = "SELECT data FROM testks.users WHERE user_id=%s"
        try:
            result = session.execute(query, (user_id,))
            row = result.one()
            if row:
                return user_pb2.UserResponse(user_id=user_id, data=row.data)
            else:
                context.set_code(grpc.StatusCode.NOT_FOUND)
                context.set_details('User not found')
                return user_pb2.UserResponse()
        except Exception as e:
            logger.error(f"Error getting user {user_id}: {e}")
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(f'Database error: {str(e)}')
            return user_pb2.UserResponse()
            
    def AddUser(self, request, context):
        if not session:
            context.set_code(grpc.StatusCode.UNAVAILABLE)
            context.set_details('Database connection not available')
            return user_pb2.AddUserResponse(success=False, message="Database not available")
            
        user_id = request.user_id
        data = request.data
        query = "INSERT INTO testks.users (user_id, data) VALUES (%s, %s)"
        try:
            logger.debug(f"Inserting user {user_id} with data: {data}")
            session.execute(query, (user_id, data))
            logger.info(f"User {user_id} added successfully")
            return user_pb2.AddUserResponse(success=True, message="User added successfully")
        except Exception as e:
            logger.error(f"Error adding user {user_id}: {e}")
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
        logger.info("gRPC server stopping...")
        server.stop(0)

@app.route("/health")
def health():
    try:
        if session:
            # Test database connection
            session.execute("SELECT release_version FROM system.local", timeout=5)
            return jsonify({"status": "ok", "cassandra": "connected"})
        else:
            return jsonify({"status": "degraded", "cassandra": "disconnected"}), 503
    except Exception as e:
        logger.error(f"Health check failed: {e}")
        return jsonify({"status": "error", "cassandra": "error", "message": str(e)}), 503

@app.route("/debug")
def debug():
    """Debug endpoint to check configuration"""
    return jsonify({
        "cassandra_host": CASSANDRA_HOST,
        "flask_port": FLASK_PORT,
        "etcd_url": ETCD_URL,
        "cassandra_connected": session is not None
    })

if __name__ == '__main__':
    logger.info("Starting Data Service...")
    logger.info(f"Configuration: CASSANDRA_HOST={CASSANDRA_HOST}, FLASK_PORT={FLASK_PORT}")
    
    # Initialize Cassandra connection
    try:
        initialize_cassandra()

    except Exception as e:
        logger.error(f"Failed to start service due to Cassandra connection error: {e}")
        sys.exit(1)
    
    # Register service with etcd
    etcd_register("data-service", "data-service", FLASK_PORT)
    
    # Start gRPC server in background thread
    grpc_thread = threading.Thread(target=serve_grpc)
    grpc_thread.daemon = True
    grpc_thread.start()

    logger.info(f"Starting Flask server on port {FLASK_PORT}")
    app.run(host='0.0.0.0', port=FLASK_PORT, debug=False)
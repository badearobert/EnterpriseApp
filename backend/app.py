from flask import Flask, jsonify, request
from flask_cors import CORS
import psycopg2
import os

app = Flask(__name__)
CORS(app, origins=["http://localhost:8080"])

def get_conn():
    return psycopg2.connect(
        dbname=os.getenv("POSTGRES_DB"),
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD"),
        host="db",
        port="5432"
    )
    
@app.route('/')
def home():
    return "Salut!"

@app.route("/health", methods=["GET"])
def health():
    return jsonify({"status": "ok"}), 200
    
@app.route('/items', methods=['GET'])
def get_items():
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT id, name FROM items ORDER BY id;")
    items = cur.fetchall()
    cur.close()
    conn.close()
    return jsonify([{"id": i[0], "name": i[1]} for i in items])

@app.route('/items', methods=['POST'])
def add_item():
    data = request.get_json()
    name = data.get("name", "no_name")
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("INSERT INTO items (name) VALUES (%s) RETURNING id;", (name,))
    new_id = cur.fetchone()[0]
    conn.commit()
    cur.close()
    conn.close()
    return jsonify({"id": new_id, "name": name})

@app.route('/items/<int:item_id>', methods=['DELETE'])
def delete_item(item_id):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("DELETE FROM items WHERE id = %s;", (item_id,))
    conn.commit()
    cur.close()
    conn.close()
    return jsonify({"deleted": item_id})

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)

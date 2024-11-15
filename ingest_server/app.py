from flask import Flask, request
from json import loads
import psycopg2
import logging
import os


logging.basicConfig(
    level=logging.INFO,
    handlers=[logging.StreamHandler(), logging.FileHandler("/app/ingest_server.log")],
)
logger = logging.getLogger(__name__)

app = Flask("ingest_server")


def get_db_connection():
    conn = psycopg2.connect(
        f"dbname={os.getenv('POSTGRES_DB')} user={os.getenv('POSTGRES_USER')} password={os.getenv('POSTGRES_PASSWORD')} host={os.getenv('POSTGRES_HOST')}"
    )
    return conn


@app.route("/data", methods=["POST"])
def save_data():
    data = request.json
    logger.info("Received data: %s", data)
    logger.info("Connecting to database")
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("SELECT 1;")
    result = cur.fetchone()
    logger.info(f"Connection successful: {result[0] == 1}")
    # Insert data into a table
    cur.execute(
        "INSERT INTO data_table (field1, field2) VALUES (%s, %s)",
        (data["field1"], data["field2"]),
    )
    conn.commit()
    cur.close()
    conn.close()
    print(data)
    return {"status": "success"}, 201


@app.route("/ping", methods=["GET"])
def ping():
    return "pong", 200


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)

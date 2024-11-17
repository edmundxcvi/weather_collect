from flask import Flask, request
from json import loads
import psycopg2
import logging
import os
from models import WeatherObservation
from sqlalchemy.orm import Session
from sqlalchemy import create_engine


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

def get_db_engine():
    return create_engine(f"postgresql+psycopg2://{os.getenv('POSTGRES_USER')}:{os.getenv('POSTGRES_PASSWORD')}@{os.getenv('POSTGRES_HOST')}/{os.getenv('POSTGRES_DB')}")


@app.route("/data", methods=["POST"])
def save_data():
    data = request.json
    logger.info("Received data: %s", data)
    engine = get_db_engine()
    with Session(engine) as session:
        for var in ["temperature"]:
            obs = WeatherObservation(
                station_id=1,
                observation_datetime=data["time"],
                variable=var,
                value=data[var],
            )
            session.add(obs)
        session.commit()
    logger.info("Successfully inserted data into database")
    return {"status": "success"}, 201


@app.route("/ping", methods=["GET"])
def ping():
    return "pong", 200


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)

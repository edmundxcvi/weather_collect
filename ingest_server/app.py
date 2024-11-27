"""
Weather data collection server
"""

from loguru import logger
import os
import sys
from typing import Dict, Tuple

from flask import Flask, request
from models import WeatherObservation, WeatherStation
from sqlalchemy import Engine, create_engine, select
from sqlalchemy.exc import MultipleResultsFound, NoResultFound
from sqlalchemy.orm import Session
import pandas as pd
import altair as alt
from dotenv import load_dotenv

load_dotenv()

# Start logging
logger.remove()
logger.add(os.getenv('LOG_FILE_PATH'), level='WARNING', retention="2 days")
logger.add(sys.stdout, level='DEBUG')

# Create app
app = Flask("ingest_server")


def get_db_engine() -> Engine:
    """
    Create database engine from environment variables

    Retuns:
        Engine
    """
    return create_engine(
        f"postgresql+psycopg2://{os.getenv('POSTGRES_USER')}:"
        f"{os.getenv('POSTGRES_PASSWORD')}@{os.getenv('POSTGRES_HOST')}/"
        f"{os.getenv('POSTGRES_DB')}"
    )


def validate_station(api_key: str, session: Session) -> WeatherStation:
    """
    Checks that the post request was made by a valid weather station

    Returns weather station if valid

    Returns:
        WeatherStation
    """
    stations = session.scalars(
        select(WeatherStation).where(WeatherStation.station_post_key == api_key)
    )
    return stations.one()


@app.route("/ping", methods=["GET"])
def ping() -> Tuple[str, int]:
    """Test server connection is running

    Returns:
        Tuple[str, int]
    """
    return "pong", 200


@app.route("/data", methods=["POST"])
def save_data() -> Tuple[Dict[str, str], int]:
    """
    API endpoint for data ingestion

    Returns:
        Tuple[Dict[str, str], int]:  `{"status": "success"}, 201`
    """

    # Wrap function into a session
    with Session(get_db_engine()) as session:

        # Check that post request has an API key
        try:
            request.root_url
            api_key = request.headers["Authorization"]
        except KeyError:
            logger.warning(f"Received post request from {request.remote_addr} without authorisation")
            return {"status": "error", "message": "Post requests require header {'Authorization': API_KEY}"}, 401

        # Check that the API key is valid (and get associated station)
        logger.debug("Validating weather station")
        try:
            weather_station = validate_station(api_key, session)
        except (NoResultFound, MultipleResultsFound) as err:
            logger.warning(f"Received post request from {request.remote_addr} with invalid API key")
            return {"status": "error", "message": "Invalid API key"}, 403
        logger.info("Weather station authenticated successfully")

        # Get time of reading
        data = request.json
        try:
            obs_time = data["time"]
        except KeyError:
            logger.warning(f"Received post request from {request.remote_addr} without 'time' key in data")
            return {"status": "error", "message": "Data missing time"}, 400

        # Read values
        for var_name in ["temperature", "pressure", "humidity"]:
            # Get observation
            try:
                obs_value = data[var_name]
            except KeyError:
                logger.warning(f"Received post request from {request.remote_addr} without '{var_name}' key in data")

            # Create new observation
            obs = WeatherObservation(
                weather_station=weather_station,
                observation_datetime=obs_time,
                variable=var_name,
                value=obs_value,
            )
            session.add(obs)

        # Add all observations
        session.commit()

    # Report success
    logger.info(f"Successfully handled post request from {request.remote_addr}")
    return {"status": "success"}, 201


@app.route("/", methods=["GET"])
def plot() -> Tuple[str, int]:
    """Create plot of temperature

    Returns:
        Tuple[str, int]
    """

    # Get data
    with Session(get_db_engine()) as session:
        data = session.scalars(
            select(WeatherObservation).filter(
                WeatherObservation.observation_datetime
                >= pd.Timestamp.now() - pd.Timedelta(hours=8)
            )
        ).all()
    data = pd.DataFrame([obj.__dict__ for obj in data])
    data = data.drop("_sa_instance_state", axis="columns", errors="ignore")

    # Create chart
    chart = (
        alt.Chart(data[["observation_datetime", "variable", "value"]])
        .mark_line()
        .encode(
            x="observation_datetime", y=alt.Y("value").scale(zero=False), row="variable"
        )
        .resolve_scale(y="independent")
    )
    return chart.to_html(), 200


# If file run directly then run debug server
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)

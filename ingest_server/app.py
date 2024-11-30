"""
Weather data collection server
"""

import os
import sys
from typing import Dict, Tuple

import altair as alt
import pandas as pd
from dotenv import load_dotenv
from flask import Flask, request
from loguru import logger
from models import WeatherObservation, WeatherStation
from sqlalchemy import Engine, create_engine, select
from sqlalchemy.exc import MultipleResultsFound, NoResultFound
from sqlalchemy.orm import Session

load_dotenv()

# Start logging
logger.remove()
logger.add(os.getenv("LOG_FILE_PATH"), level="WARNING", retention="2 days")
logger.add(sys.stdout, level="DEBUG")

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
            api_key = request.headers["Authorization"]
        except KeyError:
            logger.warning(
                f"Received post request from {request.remote_addr} without authorisation"
            )
            return {
                "status": "error",
                "message": "Post requests require header {'Authorization': API_KEY}",
            }, 401

        # Check that the API key is valid (and get associated station)
        logger.debug("Validating weather station")
        try:
            weather_station = validate_station(api_key, session)
        except (NoResultFound, MultipleResultsFound):
            logger.warning(
                f"Received post request from {request.remote_addr} with invalid API key"
            )
            return {"status": "error", "message": "Invalid API key"}, 403
        logger.info("Weather station authenticated successfully")

        # Get time of reading
        data = request.json
        try:
            obs_time = data["time"]
        except KeyError:
            logger.warning(
                f"Received post request from {request.remote_addr} without 'time' key in data"
            )
            return {"status": "error", "message": "Data missing time"}, 400

        # Read values
        for var_name in ["temperature", "pressure", "humidity"]:
            # Get observation
            try:
                obs_value = data[var_name]
            except KeyError:
                logger.warning(
                    f"Received post request from {request.remote_addr} "
                    f"without '{var_name}' key in data"
                )

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


var_name_map = {
    "temperature": {"main_title": "Temperature", "y_axis_title": "Temperature / ºC"},
    "pressure": {"main_title": "Pressure", "y_axis_title": "Pressure / mbar"},
    "humidity": {"main_title": "Humidity", "y_axis_title": "Humidity / ??"},
}


def plot_variable(var_name: str) -> str:

    # Get data
    with Session(get_db_engine()) as session:
        data = session.scalars(
            select(WeatherObservation).where(
                (
                    WeatherObservation.observation_datetime
                    >= pd.Timestamp.now() - pd.DateOffset(hours=24)
                )
                & (WeatherObservation.variable == var_name)
            )
        ).all()
    data = pd.DataFrame([obj.__dict__ for obj in data])
    data = data.drop("_sa_instance_state", axis="columns", errors="ignore")

    # Create chart
    interval = alt.selection_interval(encodings=["x"])
    base = (
        alt.Chart(data[["observation_datetime", "variable", "value"]])
        .mark_line()
        .encode(
            x=alt.X("observation_datetime", title="Time"),
            y=alt.Y("value", title=var_name_map[var_name]["y_axis_title"]).scale(
                zero=False
            ),
        )
    )
    # Main chart
    chart = base.encode(
        x=alt.X("observation_datetime", title="Time").scale(domain=interval)
    ).properties(height=500, width=800, title=var_name_map[var_name]["main_title"])
    # Range selector
    view = base.add_params(interval).properties(height=200, width=800)
    return (chart & view).to_html()


@app.route("/temperature", methods=["GET"])
def plot_temperature() -> Tuple[str, int]:
    """Create plot of temperature

    Returns:
        Tuple[str, int]
    """
    return plot_variable("temperature"), 200


@app.route("/pressure", methods=["GET"])
def plot_pressure() -> Tuple[str, int]:
    """Create plot of pressure

    Returns:
        Tuple[str, int]
    """
    return plot_variable("pressure"), 200


@app.route("/humidity", methods=["GET"])
def plot_humidity() -> Tuple[str, int]:
    """Create plot of humidity

    Returns:
        Tuple[str, int]
    """
    return plot_variable("humidity"), 200


# If file run directly then run debug server
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)

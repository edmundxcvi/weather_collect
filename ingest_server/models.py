from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship
from sqlalchemy import ForeignKey, func, Uuid
from typing import List, Optional
from datetime import datetime


class Base(DeclarativeBase):
    pass


class WeatherObservation(Base):
    __tablename__ = "weather_observation"

    observation_id: Mapped[int] = mapped_column(primary_key=True)
    station_id: Mapped[int] = mapped_column(ForeignKey("weather_station.station_id"))
    observation_datetime: Mapped[datetime]
    variable: Mapped[str]
    value: Mapped[float]
    weather_station: Mapped["WeatherStation"] = relationship(
        back_populates="weather_observations"
    )


class WeatherStation(Base):
    __tablename__ = "weather_station"

    station_id: Mapped[int] = mapped_column(primary_key=True)
    station_name: Mapped[str]
    station_post_key: Mapped[str] = mapped_column(Uuid, server_default=func.gen_random_uuid())
    weather_observations: Mapped[List["WeatherObservation"]] = relationship(
        back_populates="weather_station"
    )
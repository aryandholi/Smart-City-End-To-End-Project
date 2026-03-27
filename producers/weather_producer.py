"""
producers/weather_producer.py
─────────────────────────────────────────────────────────────────
Fetches real-time weather and air quality data from OpenWeather
API every WEATHER_UPDATE_INTERVAL_S seconds, using the current
GPS coordinates from shared_state.

Falls back to realistic simulated data if API key is not set.
Publishes to Kafka topic: weather_data
"""

import json
import logging
import time
import random
from datetime import datetime, timezone

import requests
from confluent_kafka import Producer, KafkaException

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from config.config import (
    KAFKA_BOOTSTRAP_SERVERS,
    TOPIC_WEATHER,
    WEATHER_UPDATE_INTERVAL_S,
    VEHICLE_ID,
    OPENWEATHER_API_KEY,
    OPENWEATHER_BASE_URL,
    OPENWEATHER_AQI_URL,
)
import producers.shared_state as shared_state

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [WEATHER_PRODUCER] %(levelname)s: %(message)s",
)
logger = logging.getLogger(__name__)

API_KEY_MISSING = (not OPENWEATHER_API_KEY or OPENWEATHER_API_KEY == "YOUR_API_KEY_HERE")


# ── Delivery callback ──────────────────────────────────────────

def delivery_report(err, msg):
    if err:
        logger.error(f"Message delivery failed: {err}")
    else:
        logger.debug(f"Weather message delivered to {msg.topic()} [{msg.partition()}]")


# ── Real API fetch ─────────────────────────────────────────────

def fetch_weather_api(lat: float, lon: float) -> dict:
    """Call OpenWeather Current Weather + Air Pollution endpoints."""
    weather_url = f"{OPENWEATHER_BASE_URL}/weather"
    aqi_url = OPENWEATHER_AQI_URL

    params = {"lat": lat, "lon": lon, "appid": OPENWEATHER_API_KEY, "units": "metric"}

    weather_resp = requests.get(weather_url, params=params, timeout=10)
    weather_resp.raise_for_status()
    w = weather_resp.json()

    aqi_resp = requests.get(aqi_url, params=params, timeout=10)
    aqi_resp.raise_for_status()
    a = aqi_resp.json()

    components = a["list"][0]["components"]
    aqi_index = a["list"][0]["main"]["aqi"]

    return {
        "temperature_c": round(w["main"]["temp"], 1),
        "feels_like_c": round(w["main"]["feels_like"], 1),
        "humidity_pct": w["main"]["humidity"],
        "pressure_hpa": w["main"]["pressure"],
        "wind_speed_mps": round(w["wind"]["speed"], 2),
        "wind_direction_deg": w["wind"].get("deg", 0),
        "wind_gust_mps": round(w["wind"].get("gust", 0.0), 2),
        "visibility_m": w.get("visibility", 10000),
        "description": w["weather"][0]["description"],
        "icon": w["weather"][0]["icon"],
        "cloud_coverage_pct": w["clouds"]["all"],
        "rain_1h_mm": w.get("rain", {}).get("1h", 0.0),
        "snow_1h_mm": w.get("snow", {}).get("1h", 0.0),
        "aqi": aqi_index,
        "co_ugm3": components.get("co", 0.0),
        "no2_ugm3": components.get("no2", 0.0),
        "o3_ugm3": components.get("o3", 0.0),
        "pm2_5_ugm3": components.get("pm2_5", 0.0),
        "pm10_ugm3": components.get("pm10", 0.0),
        "so2_ugm3": components.get("so2", 0.0),
    }


# ── Simulated fallback ─────────────────────────────────────────

def fetch_weather_simulated(lat: float, lon: float) -> dict:
    """Return realistic simulated weather data when API key is absent."""
    return {
        "temperature_c": round(random.uniform(5.0, 22.0), 1),
        "feels_like_c": round(random.uniform(3.0, 20.0), 1),
        "humidity_pct": random.randint(45, 90),
        "pressure_hpa": random.randint(990, 1025),
        "wind_speed_mps": round(random.uniform(0.5, 12.0), 2),
        "wind_direction_deg": random.randint(0, 359),
        "wind_gust_mps": round(random.uniform(1.0, 18.0), 2),
        "visibility_m": random.randint(4000, 10000),
        "description": random.choice(
            ["clear sky", "few clouds", "scattered clouds", "light rain", "overcast clouds", "mist"]
        ),
        "icon": "04d",
        "cloud_coverage_pct": random.randint(0, 100),
        "rain_1h_mm": round(random.uniform(0.0, 5.0), 2),
        "snow_1h_mm": 0.0,
        "aqi": random.randint(1, 4),
        "co_ugm3": round(random.uniform(200.0, 600.0), 2),
        "no2_ugm3": round(random.uniform(5.0, 80.0), 2),
        "o3_ugm3": round(random.uniform(20.0, 120.0), 2),
        "pm2_5_ugm3": round(random.uniform(2.0, 35.0), 2),
        "pm10_ugm3": round(random.uniform(5.0, 50.0), 2),
        "so2_ugm3": round(random.uniform(1.0, 20.0), 2),
    }


# ── Main producer loop ─────────────────────────────────────────

def run():
    producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS})

    if API_KEY_MISSING:
        logger.warning("OPENWEATHER_API_KEY not set — using simulated weather data.")

    logger.info(f"Starting weather producer — publishing to '{TOPIC_WEATHER}' every {WEATHER_UPDATE_INTERVAL_S}s")

    try:
        while True:
            lat = shared_state.current_location.get("lat", 52.4862)
            lon = shared_state.current_location.get("lon", -1.8904)

            try:
                if API_KEY_MISSING:
                    weather_data = fetch_weather_simulated(lat, lon)
                else:
                    weather_data = fetch_weather_api(lat, lon)
            except requests.RequestException as exc:
                logger.error(f"OpenWeather API error: {exc} — using simulated data.")
                weather_data = fetch_weather_simulated(lat, lon)

            message = {
                "vehicle_id": VEHICLE_ID,
                "event_type": "WEATHER",
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "lat": lat,
                "lon": lon,
                **weather_data,
            }

            try:
                producer.produce(
                    TOPIC_WEATHER,
                    key=VEHICLE_ID,
                    value=json.dumps(message),
                    callback=delivery_report,
                )
                producer.poll(0)
            except KafkaException as exc:
                logger.error(f"Kafka error: {exc}")

            logger.info(
                f"[WEATHER] temp={weather_data['temperature_c']}°C  "
                f"wind={weather_data['wind_speed_mps']}m/s  "
                f"AQI={weather_data['aqi']}  "
                f"PM2.5={weather_data['pm2_5_ugm3']}µg/m³"
            )

            time.sleep(WEATHER_UPDATE_INTERVAL_S)

    except KeyboardInterrupt:
        logger.info("Weather producer stopped by user.")
    finally:
        producer.flush()


if __name__ == "__main__":
    run()

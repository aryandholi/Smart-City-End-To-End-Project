"""
producers/gps_producer.py
─────────────────────────────────────────────────────────────────
Parses data/smart_city_route.gpx and publishes GPS events to the
'gps_data' Kafka topic at GPS_UPDATE_INTERVAL_S intervals.

Also keeps `shared_state.current_location` up-to-date so that
weather_producer.py can call the correct OpenWeather coordinates.
"""

import json
import logging
import time
import math
from datetime import datetime, timezone
from pathlib import Path

import gpxpy
from confluent_kafka import Producer, KafkaException

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from config.config import (
    KAFKA_BOOTSTRAP_SERVERS,
    TOPIC_GPS,
    GPS_UPDATE_INTERVAL_S,
    VEHICLE_ID,
    GPX_FILE_PATH,
)
import producers.shared_state as shared_state

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [GPS_PRODUCER] %(levelname)s: %(message)s",
)
logger = logging.getLogger(__name__)


# ── Kafka delivery callback ────────────────────────────────────

def delivery_report(err, msg):
    if err:
        logger.error(f"Message delivery failed: {err}")
    else:
        logger.debug(f"GPS message delivered to {msg.topic()} [{msg.partition()}] @ offset {msg.offset()}")


# ── GPX loader ─────────────────────────────────────────────────

def load_gpx_track(gpx_path: str) -> list[dict]:
    """Parse a GPX file and return a list of track point dicts."""
    path = Path(gpx_path)
    if not path.exists():
        raise FileNotFoundError(f"GPX file not found: {gpx_path}")

    with open(path, "r", encoding="utf-8") as fh:
        gpx = gpxpy.parse(fh)

    points = []
    for track in gpx.tracks:
        for segment in track.segments:
            for pt in segment.points:
                speed_mps = float(pt.speed) if pt.speed is not None else 0.0
                points.append(
                    {
                        "lat": pt.latitude,
                        "lon": pt.longitude,
                        "elevation_m": round(pt.elevation or 0.0, 1),
                        "speed_mps": round(speed_mps, 2),
                        "speed_kmh": round(speed_mps * 3.6, 2),
                        "gpx_timestamp": pt.time.isoformat() if pt.time else None,
                    }
                )
    logger.info(f"Loaded {len(points)} track points from {path.name}")
    return points


# ── Compass bearing helper ─────────────────────────────────────

def calculate_bearing(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Return compass bearing (degrees) from point 1 → point 2."""
    lat1_r, lat2_r = math.radians(lat1), math.radians(lat2)
    d_lon = math.radians(lon2 - lon1)
    x = math.sin(d_lon) * math.cos(lat2_r)
    y = math.cos(lat1_r) * math.sin(lat2_r) - math.sin(lat1_r) * math.cos(lat2_r) * math.cos(d_lon)
    bearing = (math.degrees(math.atan2(x, y)) + 360) % 360
    return round(bearing, 1)


# ── Main producer loop ─────────────────────────────────────────

def run():
    """Continuously replay the GPX route and publish GPS events."""
    producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS})

    points = load_gpx_track(GPX_FILE_PATH)
    if not points:
        raise ValueError("GPX file contained no track points.")

    logger.info(f"Starting GPS producer — publishing to topic '{TOPIC_GPS}'")
    loop_count = 0

    try:
        while True:
            loop_count += 1
            logger.info(f"Starting route loop #{loop_count}")

            for i, pt in enumerate(points):
                # Calculate heading to next point
                if i < len(points) - 1:
                    nxt = points[i + 1]
                    bearing = calculate_bearing(pt["lat"], pt["lon"], nxt["lat"], nxt["lon"])
                else:
                    bearing = 0.0

                now_utc = datetime.now(timezone.utc).isoformat()
                message = {
                    "vehicle_id": VEHICLE_ID,
                    "event_type": "GPS",
                    "timestamp": now_utc,
                    "lat": pt["lat"],
                    "lon": pt["lon"],
                    "elevation_m": pt["elevation_m"],
                    "speed_mps": pt["speed_mps"],
                    "speed_kmh": pt["speed_kmh"],
                    "heading_deg": bearing,
                    "gpx_timestamp": pt["gpx_timestamp"],
                }

                # Update shared state so weather producer can read current position
                shared_state.current_location["lat"] = pt["lat"]
                shared_state.current_location["lon"] = pt["lon"]

                try:
                    producer.produce(
                        TOPIC_GPS,
                        key=VEHICLE_ID,
                        value=json.dumps(message),
                        callback=delivery_report,
                    )
                    producer.poll(0)
                except KafkaException as exc:
                    logger.error(f"Kafka error: {exc}")

                logger.info(
                    f"[GPS] lat={pt['lat']:.4f}  lon={pt['lon']:.4f}  "
                    f"speed={pt['speed_kmh']:.1f} km/h  heading={bearing}°"
                )
                time.sleep(GPS_UPDATE_INTERVAL_S)

            producer.flush()

    except KeyboardInterrupt:
        logger.info("GPS producer stopped by user.")
    finally:
        producer.flush()


if __name__ == "__main__":
    run()

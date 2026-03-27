"""
producers/telemetry_producer.py
─────────────────────────────────────────────────────────────────
Simulates vehicle engine telemetry and publishes to:
  • telemetry_data   — per-second engine metrics
  • vehicle_data     — every-5-second consolidated summary
  • emergency_data   — test failure events when within 1 km of
                       the traffic office coordinate

'Practical Test Failure' simulator:
  If the vehicle is within TEST_FAILURE_RADIUS_KM of
  TRAFFIC_OFFICE_LAT/LON, there is a 15% chance each cycle of
  generating a random driving failure event.
"""

import json
import logging
import math
import random
import time
from datetime import datetime, timezone

from confluent_kafka import Producer, KafkaException

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from config.config import (
    KAFKA_BOOTSTRAP_SERVERS,
    TOPIC_TELEMETRY,
    TOPIC_VEHICLE,
    TOPIC_EMERGENCY,
    TELEMETRY_UPDATE_INTERVAL_S,
    VEHICLE_SUMMARY_INTERVAL_S,
    VEHICLE_ID,
    TRAFFIC_OFFICE_LAT,
    TRAFFIC_OFFICE_LON,
    TEST_FAILURE_RADIUS_KM,
    TEST_FAILURE_EVENTS,
)
import producers.shared_state as shared_state

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [TELEMETRY_PRODUCER] %(levelname)s: %(message)s",
)
logger = logging.getLogger(__name__)

# ── Haversine distance ─────────────────────────────────────────

EARTH_RADIUS_KM = 6371.0

def haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Return great-circle distance in kilometres."""
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlam = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlam / 2) ** 2
    return 2 * EARTH_RADIUS_KM * math.asin(math.sqrt(a))


# ── Engine state simulation ────────────────────────────────────

class EngineState:
    """Maintains a realistic, slowly-drifting engine state."""

    def __init__(self):
        self.odometer_km = random.uniform(12000, 45000)
        self.fuel_level_pct = random.uniform(60.0, 95.0)
        self.engine_temp_c = 20.0       # cold start
        self.oil_pressure_psi = 45.0
        self.battery_voltage_v = 12.6
        self.throttle_pct = 0.0
        self.gear = 1
        self.trip_km = 0.0

    def update(self, speed_kmh: float) -> dict:
        """Advance state by one second at the given speed."""
        # Distance covered this tick
        dist_km = speed_kmh / 3600.0
        self.odometer_km += dist_km
        self.trip_km += dist_km

        # Fuel consumption (simplified Fc model)
        fc_lt_per_100km = random.uniform(6.5, 12.0)
        self.fuel_level_pct -= (fc_lt_per_100km / 100.0) * dist_km * (100.0 / 45.0)
        self.fuel_level_pct = max(0.0, min(100.0, self.fuel_level_pct))

        # Engine warm-up
        target_temp = 90.0 if speed_kmh > 5 else 60.0
        self.engine_temp_c += (target_temp - self.engine_temp_c) * 0.005
        self.engine_temp_c += random.gauss(0, 0.3)
        self.engine_temp_c = round(self.engine_temp_c, 1)

        # Oil pressure decreases slightly as temp rises
        self.oil_pressure_psi = round(45.0 - (self.engine_temp_c - 20.0) * 0.05 + random.gauss(0, 0.5), 1)

        # Throttle proportional to speed
        self.throttle_pct = round(min(100.0, speed_kmh * 1.2 + random.gauss(0, 3)), 1)

        # RPM: rough approximation
        rpm = 800 + (self.throttle_pct / 100) * 5200 + random.gauss(0, 50)
        rpm = round(max(700, min(7000, rpm)))

        # Battery voltage — fluctuates with load
        self.battery_voltage_v = round(13.8 - (self.throttle_pct / 100) * 0.5 + random.gauss(0, 0.05), 2)

        # Gear estimation
        if speed_kmh < 20:
            self.gear = 1
        elif speed_kmh < 40:
            self.gear = 2
        elif speed_kmh < 60:
            self.gear = 3
        elif speed_kmh < 80:
            self.gear = 4
        else:
            self.gear = 5

        return {
            "speed_kmh": round(speed_kmh, 1),
            "rpm": rpm,
            "throttle_pct": self.throttle_pct,
            "fuel_level_pct": round(self.fuel_level_pct, 2),
            "engine_temp_c": self.engine_temp_c,
            "oil_pressure_psi": self.oil_pressure_psi,
            "battery_voltage_v": self.battery_voltage_v,
            "gear": self.gear,
            "odometer_km": round(self.odometer_km, 3),
            "trip_km": round(self.trip_km, 3),
        }


# ── Delivery callback ──────────────────────────────────────────

def delivery_report(err, msg):
    if err:
        logger.error(f"Kafka delivery error: {err}")


# ── Test failure simulator ─────────────────────────────────────

def maybe_emit_test_failure(producer: Producer, lat: float, lon: float,
                             engine: dict, cycle: int) -> None:
    """Emit a random driving test failure event if within radius of office."""
    dist_km = haversine_km(lat, lon, TRAFFIC_OFFICE_LAT, TRAFFIC_OFFICE_LON)
    if dist_km > TEST_FAILURE_RADIUS_KM:
        return
    if random.random() > 0.15:          # 15% chance per cycle
        return

    failure_code = random.choice(TEST_FAILURE_EVENTS)
    severity = random.choice(["MINOR", "SERIOUS", "DANGEROUS"])

    message = {
        "vehicle_id": VEHICLE_ID,
        "event_type": "TEST_FAILURE",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "lat": lat,
        "lon": lon,
        "distance_to_office_km": round(dist_km, 3),
        "failure_code": failure_code,
        "severity": severity,
        "speed_at_event_kmh": engine["speed_kmh"],
        "cycle": cycle,
        "description": f"Practical test failure detected: {failure_code} [{severity}]",
    }

    producer.produce(
        TOPIC_EMERGENCY,
        key=VEHICLE_ID,
        value=json.dumps(message),
        callback=delivery_report,
    )
    producer.poll(0)
    logger.warning(
        f"🚨 TEST FAILURE: {failure_code} ({severity})  "
        f"dist_to_office={dist_km:.3f} km"
    )


# ── Main producer loop ─────────────────────────────────────────

def run():
    producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS})
    engine = EngineState()
    cycle = 0
    last_vehicle_summary = 0.0

    logger.info("Starting telemetry producer...")

    try:
        while True:
            cycle += 1
            now_ts = time.time()

            # Read current GPS speed from shared state (default if GPS not yet running)
            lat = shared_state.current_location.get("lat", TRAFFIC_OFFICE_LAT)
            lon = shared_state.current_location.get("lon", TRAFFIC_OFFICE_LON)
            speed_kmh = random.uniform(0, 60)  # simulated; real integration uses GPS speed

            engine_data = engine.update(speed_kmh)
            timestamp = datetime.now(timezone.utc).isoformat()

            # ── Telemetry message ──────────────────────────────
            telemetry_msg = {
                "vehicle_id": VEHICLE_ID,
                "event_type": "TELEMETRY",
                "timestamp": timestamp,
                "lat": lat,
                "lon": lon,
                **engine_data,
            }
            try:
                producer.produce(
                    TOPIC_TELEMETRY,
                    key=VEHICLE_ID,
                    value=json.dumps(telemetry_msg),
                    callback=delivery_report,
                )
                producer.poll(0)
            except KafkaException as exc:
                logger.error(f"Kafka error (telemetry): {exc}")

            logger.info(
                f"[TELEMETRY] speed={engine_data['speed_kmh']} km/h  "
                f"rpm={engine_data['rpm']}  "
                f"fuel={engine_data['fuel_level_pct']:.1f}%  "
                f"temp={engine_data['engine_temp_c']}°C"
            )

            # ── Consolidated vehicle summary (every 5s) ────────
            if now_ts - last_vehicle_summary >= VEHICLE_SUMMARY_INTERVAL_S:
                vehicle_msg = {
                    "vehicle_id": VEHICLE_ID,
                    "event_type": "VEHICLE",
                    "timestamp": timestamp,
                    "lat": lat,
                    "lon": lon,
                    "make": "Toyota",
                    "model": "Corolla",
                    "year": 2022,
                    "engine_cc": 1800,
                    **engine_data,
                }
                try:
                    producer.produce(
                        TOPIC_VEHICLE,
                        key=VEHICLE_ID,
                        value=json.dumps(vehicle_msg),
                        callback=delivery_report,
                    )
                    producer.poll(0)
                except KafkaException as exc:
                    logger.error(f"Kafka error (vehicle): {exc}")

                logger.info(f"[VEHICLE] Summary published — odometer={engine_data['odometer_km']:.1f} km")
                last_vehicle_summary = now_ts

            # ── Test failure check ─────────────────────────────
            maybe_emit_test_failure(producer, lat, lon, engine_data, cycle)

            time.sleep(TELEMETRY_UPDATE_INTERVAL_S)

    except KeyboardInterrupt:
        logger.info("Telemetry producer stopped by user.")
    finally:
        producer.flush()


if __name__ == "__main__":
    run()

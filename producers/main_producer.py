"""
producers/main_producer.py
─────────────────────────────────────────────────────────────────
Orchestrator: starts GPS, Weather, and Telemetry producers in
separate daemon threads. Blocks until KeyboardInterrupt.

Usage:
    python producers/main_producer.py
"""

import logging
import sys
import threading
import time
from pathlib import Path

# Ensure project root is on the path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from dotenv import load_dotenv
load_dotenv()

import producers.gps_producer as gps_producer
import producers.weather_producer as weather_producer
import producers.telemetry_producer as telemetry_producer

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [MAIN] %(levelname)s: %(message)s",
)
logger = logging.getLogger(__name__)

BANNER = """
╔══════════════════════════════════════════════════════════════╗
║   🚗  Smart City Real-Time Streaming Pipeline  🏙️           ║
║   Starting all IoT producers...                              ║
╚══════════════════════════════════════════════════════════════╝
"""


def _thread(target, name: str) -> threading.Thread:
    t = threading.Thread(target=target, name=name, daemon=True)
    t.start()
    return t


def main():
    print(BANNER)
    logger.info("Launching producers — press Ctrl+C to stop all.")

    threads = [
        _thread(gps_producer.run, "GPSProducer"),
        _thread(weather_producer.run, "WeatherProducer"),
        _thread(telemetry_producer.run, "TelemetryProducer"),
    ]

    logger.info(f"✅  {len(threads)} producer threads started.")

    try:
        while True:
            alive = [t.name for t in threads if t.is_alive()]
            dead  = [t.name for t in threads if not t.is_alive()]
            if dead:
                logger.error(f"Dead threads detected: {dead}. Check logs above.")
            time.sleep(10)
    except KeyboardInterrupt:
        logger.info("Shutdown signal received. Waiting for threads to drain...")
        # Daemon threads will auto-exit when main exits
        time.sleep(2)
        logger.info("All producers stopped. Goodbye.")
        sys.exit(0)


if __name__ == "__main__":
    main()

"""
config/config.py
─────────────────────────────────────────────────────────────────
Central configuration for the Smart City Streaming pipeline.
All producers, consumers, and scripts import from here.
"""
import os
from dotenv import load_dotenv

load_dotenv()

# ── Kafka ──────────────────────────────────────────────────────
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

TOPIC_VEHICLE   = "vehicle_data"
TOPIC_GPS       = "gps_data"
TOPIC_WEATHER   = "weather_data"
TOPIC_TELEMETRY = "telemetry_data"
TOPIC_EMERGENCY = "emergency_data"

ALL_TOPICS = [
    TOPIC_VEHICLE,
    TOPIC_GPS,
    TOPIC_WEATHER,
    TOPIC_TELEMETRY,
    TOPIC_EMERGENCY,
]

# ── OpenWeather API ────────────────────────────────────────────
OPENWEATHER_API_KEY   = os.getenv("OPENWEATHER_API_KEY", "YOUR_API_KEY_HERE")
OPENWEATHER_BASE_URL  = "https://api.openweathermap.org/data/2.5"
OPENWEATHER_AQI_URL   = "https://api.openweathermap.org/data/2.5/air_pollution"

# ── Timing ────────────────────────────────────────────────────
GPS_UPDATE_INTERVAL_S     = 1      # seconds between GPS publishes
WEATHER_UPDATE_INTERVAL_S = 600    # 10 minutes
TELEMETRY_UPDATE_INTERVAL_S = 1
VEHICLE_SUMMARY_INTERVAL_S  = 5

# ── Simulation geography ──────────────────────────────────────
# City: Birmingham, UK
VEHICLE_ID        = "VH-2024-BHAM-001"
GPX_FILE_PATH     = os.path.join(os.path.dirname(__file__), "..", "data", "smart_city_route.gpx")

# Traffic test office coordinate (latitude, longitude)
TRAFFIC_OFFICE_LAT  = 52.4862
TRAFFIC_OFFICE_LON  = -1.8904
TEST_FAILURE_RADIUS_KM = 1.0      # radius in km that triggers potential failure events

# Possible driving test failure events
TEST_FAILURE_EVENTS = [
    "SIGNAL_RIGHT_FAILED",
    "STOP_SIGN_VIOLATION",
    "SPEEDING_IN_SCHOOL_ZONE",
    "UNSAFE_LANE_CHANGE",
    "FAILURE_TO_YIELD",
    "IMPROPER_TURN",
    "TAILGATING",
    "MOBILE_PHONE_USE",
]

# ── AWS / S3 ──────────────────────────────────────────────────
AWS_ACCESS_KEY_ID     = os.getenv("AWS_ACCESS_KEY_ID", "")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY", "")
AWS_DEFAULT_REGION    = os.getenv("AWS_DEFAULT_REGION", "us-east-1")
S3_BUCKET_NAME        = os.getenv("S3_BUCKET_NAME", "smart-city-streaming-bucket")
S3_OUTPUT_PREFIX      = "smart-city"

# Local fallback output path (used when S3 is not configured)
LOCAL_OUTPUT_PATH = os.path.join(os.path.dirname(__file__), "..", "output")

# ── Glue / Redshift ───────────────────────────────────────────
GLUE_DATABASE_NAME  = "smart_city_db"
GLUE_CRAWLER_NAME   = "smart-city-crawler"
GLUE_IAM_ROLE_NAME  = "AWSGlueServiceRole-SmartCity"
REDSHIFT_CLUSTER_ID = os.getenv("REDSHIFT_CLUSTER_ID", "smart-city-cluster")
REDSHIFT_IAM_ROLE   = os.getenv("REDSHIFT_IAM_ROLE", "arn:aws:iam::ACCOUNT_ID:role/RedshiftRole")

-- ══════════════════════════════════════════════════════════════════
-- scripts/redshift_ddl.sql
-- Smart City Streaming — Redshift Spectrum external schema DDL
-- ══════════════════════════════════════════════════════════════════
-- Prerequisites:
--   1. Redshift cluster is in the same AWS region as S3 + Glue.
--   2. Glue Crawler has already run and populated 'smart_city_db'.
--   3. Replace <ACCOUNT_ID> and <REDSHIFT_ROLE> with real values.
--   4. Run these statements in Amazon Redshift Query Editor v2.
-- ══════════════════════════════════════════════════════════════════


-- ── 1. Create external schema pointing at Glue Data Catalog ──────
CREATE EXTERNAL SCHEMA IF NOT EXISTS smart_city
FROM DATA CATALOG
DATABASE 'smart_city_db'
IAM_ROLE 'arn:aws:iam::<ACCOUNT_ID>:role/<REDSHIFT_ROLE>'
CREATE EXTERNAL DATABASE IF NOT EXISTS;


-- ══════════════════════════════════════════════════════════════════
-- ── 2. Preview discovered tables ─────────────────────────────────
-- ══════════════════════════════════════════════════════════════════

SELECT schemaname, tablename
FROM svv_external_tables
WHERE schemaname = 'smart_city'
ORDER BY tablename;


-- ══════════════════════════════════════════════════════════════════
-- ── 3. Example analytical queries ────────────────────────────────
-- ══════════════════════════════════════════════════════════════════

-- GPS: latest 10 positions
SELECT vehicle_id, lat, lon, speed_kmh, heading_deg, timestamp
FROM smart_city.gps_data
ORDER BY timestamp DESC
LIMIT 10;


-- Weather: average temperature and AQI by date
SELECT
    date,
    ROUND(AVG(temperature_c), 2)  AS avg_temp_c,
    ROUND(AVG(aqi), 2)            AS avg_aqi,
    ROUND(AVG(pm2_5_ugm3), 2)    AS avg_pm2_5,
    COUNT(*)                       AS record_count
FROM smart_city.weather_data
GROUP BY date
ORDER BY date DESC;


-- Telemetry: high-RPM events (potential aggressive driving)
SELECT vehicle_id, timestamp, speed_kmh, rpm, fuel_level_pct, engine_temp_c
FROM smart_city.telemetry_data
WHERE rpm > 4500
ORDER BY rpm DESC
LIMIT 50;


-- Emergency / Test Failures: breakdown by failure code and severity
SELECT
    failure_code,
    severity,
    COUNT(*)                        AS occurrences,
    ROUND(AVG(speed_at_event_kmh), 1) AS avg_speed_kmh
FROM smart_city.emergency_data
GROUP BY failure_code, severity
ORDER BY occurrences DESC;


-- Vehicle: fuel consumption trend over time
SELECT
    date,
    vehicle_id,
    ROUND(MIN(fuel_level_pct), 1) AS fuel_start_pct,
    ROUND(MAX(fuel_level_pct), 1) AS fuel_end_pct,
    ROUND(MAX(odometer_km) - MIN(odometer_km), 2) AS distance_km
FROM smart_city.vehicle_data
GROUP BY date, vehicle_id
ORDER BY date DESC;


-- ══════════════════════════════════════════════════════════════════
-- ── 4. Create a Redshift materialized view for dashboard use ─────
-- ══════════════════════════════════════════════════════════════════

CREATE MATERIALIZED VIEW IF NOT EXISTS smart_city_daily_summary AS
SELECT
    g.date                              AS trip_date,
    g.vehicle_id,
    COUNT(DISTINCT g.lat || ',' || g.lon) AS unique_positions,
    ROUND(MAX(g.speed_kmh), 1)          AS max_speed_kmh,
    ROUND(AVG(g.speed_kmh), 1)          AS avg_speed_kmh,
    ROUND(AVG(w.temperature_c), 1)      AS avg_temp_c,
    ROUND(AVG(w.aqi), 1)               AS avg_aqi,
    COUNT(DISTINCT e.failure_code)      AS distinct_failures,
    COUNT(e.failure_code)               AS total_failures
FROM smart_city.gps_data g
LEFT JOIN smart_city.weather_data w
       ON g.date = w.date
LEFT JOIN smart_city.emergency_data e
       ON g.date = e.date AND g.vehicle_id = e.vehicle_id
GROUP BY g.date, g.vehicle_id;


-- Query the materialized view
SELECT * FROM smart_city_daily_summary ORDER BY trip_date DESC;

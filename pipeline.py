import requests
import snowflake.connector
import os
import logging
from datetime import datetime

# ============================================
# LOGGING SETUP
# ============================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("pipeline.log"),
        logging.StreamHandler()
    ]
)
log = logging.getLogger(__name__)

# ============================================
# CONFIGURATION
# Credentials are loaded from environment
# variables — never hardcoded in source code.
# See .env.example for required variables.
# ============================================
SNOWFLAKE_CONFIG = {
    "user":      os.environ.get("SNOWFLAKE_USER"),
    "password":  os.environ.get("SNOWFLAKE_PASSWORD"),
    "account":   os.environ.get("SNOWFLAKE_ACCOUNT"),
    "warehouse": "COMPUTE_WH",
    "database":  "PIPELINE_DB",
    "schema":    "PIPELINE_SCHEMA"
}

# Stand-in data source demonstrating the pipeline pattern.
# In production this would be replaced with the learner
# activity logs API endpoint and credentials.
LOCATIONS = [
    {"city": "New York",   "country": "US", "lat": 40.71, "lon": -74.01},
    {"city": "London",     "country": "GB", "lat": 51.51, "lon": -0.13},
    {"city": "Tokyo",      "country": "JP", "lat": 35.68, "lon": 139.69},
    {"city": "Paris",      "country": "FR", "lat": 48.85, "lon": 2.35},
    {"city": "Sydney",     "country": "AU", "lat": -33.87, "lon": 151.21},
    {"city": "Dubai",      "country": "AE", "lat": 25.20, "lon": 55.27},
    {"city": "Toronto",    "country": "CA", "lat": 43.65, "lon": -79.38},
    {"city": "Berlin",     "country": "DE", "lat": 52.52, "lon": 13.40},
    {"city": "Singapore",  "country": "SG", "lat": 1.35,  "lon": 103.82},
    {"city": "Mumbai",     "country": "IN", "lat": 19.08, "lon": 72.88},
]

# ============================================
# STEP 1 - EXTRACT: Fetch data from API
# ============================================
def fetch_data(locations):
    log.info("Starting data extraction from API...")
    results = []
    for c in locations:
        try:
            url = (
                f"https://api.open-meteo.com/v1/forecast"
                f"?latitude={c['lat']}&longitude={c['lon']}"
                f"&current=temperature_2m,relative_humidity_2m,"
                f"wind_speed_10m,weather_code"
            )
            response = requests.get(url, timeout=10)
            response.raise_for_status()
            data = response.json()["current"]
            results.append({
                "city":         c["city"],
                "country":      c["country"],
                "temperature":  data["temperature_2m"],
                "humidity":     data["relative_humidity_2m"],
                "weather_desc": f"weather code {data['weather_code']}",
                "wind_speed":   data["wind_speed_10m"]
            })
            log.info(f"  [OK] Fetched: {c['city']}")
        except requests.exceptions.Timeout:
            log.error(f"  [FAILED] Timeout fetching {c['city']} - skipping")
        except requests.exceptions.RequestException as e:
            log.error(f"  [FAILED] {c['city']}: {e} - skipping")
    log.info(f"Extraction complete. {len(results)}/{len(locations)} records fetched.")
    return results

# ============================================
# STEP 2 - LOAD: Stage then Merge into Snowflake
# ============================================
def load_to_snowflake(records):
    if not records:
        log.warning("No records to load. Exiting.")
        return

    log.info(f"Loading {len(records)} records into Snowflake...")
    conn = None
    try:
        conn = snowflake.connector.connect(**SNOWFLAKE_CONFIG)
        cursor = conn.cursor()

        # Create staging table if not exists
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS WEATHER_DATA_STAGING (
                CITY         VARCHAR(100),
                COUNTRY      VARCHAR(10),
                TEMPERATURE  FLOAT,
                HUMIDITY     INT,
                WEATHER_DESC VARCHAR(200),
                WIND_SPEED   FLOAT,
                LOADED_AT    TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

        # Clear staging — safe to truncate since it's not consumer-facing
        cursor.execute("TRUNCATE TABLE WEATHER_DATA_STAGING")

        # Insert into staging
        for record in records:
            cursor.execute("""
                INSERT INTO WEATHER_DATA_STAGING
                (CITY, COUNTRY, TEMPERATURE, HUMIDITY, WEATHER_DESC, WIND_SPEED)
                VALUES (%s, %s, %s, %s, %s, %s)
            """, (
                record["city"], record["country"], record["temperature"],
                record["humidity"], record["weather_desc"], record["wind_speed"]
            ))

        # MERGE staging into production
        # This is the key pattern: production table is NEVER wiped.
        # Existing records are updated, new records are inserted.
        # Running this twice produces the same result — fully idempotent.
        cursor.execute("""
            MERGE INTO WEATHER_DATA AS target
            USING WEATHER_DATA_STAGING AS source
            ON target.CITY = source.CITY
            AND target.COUNTRY = source.COUNTRY
            WHEN MATCHED THEN UPDATE SET
                TEMPERATURE  = source.TEMPERATURE,
                HUMIDITY     = source.HUMIDITY,
                WEATHER_DESC = source.WEATHER_DESC,
                WIND_SPEED   = source.WIND_SPEED,
                LOADED_AT    = CURRENT_TIMESTAMP
            WHEN NOT MATCHED THEN INSERT
                (CITY, COUNTRY, TEMPERATURE, HUMIDITY, WEATHER_DESC, WIND_SPEED)
            VALUES
                (source.CITY, source.COUNTRY, source.TEMPERATURE,
                 source.HUMIDITY, source.WEATHER_DESC, source.WIND_SPEED)
        """)

        conn.commit()
        log.info("[OK] Data successfully merged into production table!")

    except snowflake.connector.errors.DatabaseError as e:
        log.error(f"Snowflake error: {e}")
        raise
    finally:
        if conn:
            cursor.close()
            conn.close()
            log.info("Snowflake connection closed.")

# ============================================
# STEP 3 - RUN THE PIPELINE
# ============================================
if __name__ == "__main__":
    log.info("=" * 50)
    log.info(f"PIPELINE STARTED: {datetime.now()}")
    log.info("=" * 50)

    try:
        data = fetch_data(LOCATIONS)
        load_to_snowflake(data)
        log.info("PIPELINE COMPLETED SUCCESSFULLY")
    except Exception as e:
        log.error(f"PIPELINE FAILED: {e}")

    log.info("=" * 50)

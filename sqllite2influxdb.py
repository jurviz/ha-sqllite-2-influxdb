import sqlite3
import json
import pytz
from datetime import datetime
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS
from dotenv import load_dotenv
import logging
import os

# Load environment variables
load_dotenv()

# Setup logging
DEBUG_MODE = os.getenv("DEBUG_MODE", "false").lower() == "true"
logging_level = logging.DEBUG if DEBUG_MODE else logging.INFO
logging.basicConfig(level=logging_level, format='%(asctime)s - %(levelname)s - %(message)s')

# Retrieve configuration from environment variables
sqlite_db = os.getenv("SQLITE_DB")
influx_url = os.getenv("INFLUXDB_URL")
influx_token = os.getenv("INFLUXDB_TOKEN")
influx_org = os.getenv("INFLUXDB_ORG")
influx_bucket = os.getenv("INFLUXDB_BUCKET")
HA_INFLUXDB_SOURCE = os.getenv("HA_INFLUXDB_SOURCE", "HA") # Default to "HA" if not set
logging.info(f"Using InfluxDB source '{HA_INFLUXDB_SOURCE}' from environment variable.")

HA_TIMEZONE_STR = os.getenv("HA_TIMEZONE")
HA_TIMEZONE = None
if HA_TIMEZONE_STR:
    try:
        HA_TIMEZONE = pytz.timezone(HA_TIMEZONE_STR)
        logging.info(f"Using timezone '{HA_TIMEZONE_STR}' from environment variable.")
    except pytz.exceptions.UnknownTimeZoneError:
        logging.error(f"Invalid timezone '{HA_TIMEZONE_STR}' specified in HA_TIMEZONE. Assuming UTC.")
        HA_TIMEZONE = pytz.utc
else:
    logging.warning("HA_TIMEZONE environment variable not set. Assuming UTC (may be incorrect).")
    HA_TIMEZONE = pytz.utc

# Validate environment variables
required_env_vars = [sqlite_db, influx_url, influx_token, influx_org, influx_bucket]
if any(v is None for v in required_env_vars):
    logging.error("One or more required environment variables are not set.")
    exit(1)

BATCH_SIZE = int(os.getenv("BATCH_SIZE", 10000))

def connect_to_sqlite(db_path):
    try:
        # Connect to SQLite database
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        logging.info("Successfully connected to SQLite")
        return conn, cursor
    except sqlite3.Error as e:
        logging.error(f"SQLite error: {e}")
        exit(1)

def connect_to_influxdb(url, token, org):
    try:
        # Connect to InfluxDB and return the client write and query APIs
        client = InfluxDBClient(url=url, token=token, org=org)
        logging.info("Successfully connected to InfluxDB")
        return client.write_api(write_options=SYNCHRONOUS), client.query_api()
    except Exception as e:
        logging.error(f"InfluxDB connection error: {e}")
        exit(1)

def get_oldest_influx_timestamp(query_api):
    try:
        # Query InfluxDB for the oldest timestamp in the specified bucket
        query_string = f'''
        from(bucket: "{influx_bucket}")
          |> range(start: 0)
          |> filter(fn: (r) => r["_measurement"] == "units")
          |> sort(columns: ["_time"], desc: false)
          |> limit(n: 1)
        '''
        result = query_api.query(org=influx_org, query=query_string)
        if result and len(result) > 0 and len(result[0].records) > 0:
            return result[0].records[0].get_time().isoformat()
    except Exception as e:
        logging.error(f"Error querying InfluxDB for the oldest timestamp: {e}")
    return None

def format_timestamp(oldest_timestamp):
    try:
        # Convert ISO format timestamp to a string format compatible with SQLite
        dt_obj = datetime.fromisoformat(oldest_timestamp.replace('Z', ''))
        return dt_obj.strftime("%Y-%m-%d %H:%M:%S")
    except ValueError as e:
        logging.error(f"Error parsing timestamp: {e}")
        exit(1)

def build_sqlite_query(formatted_timestamp):
    # Build the SQLite query with an optional timestamp filter
    base_query = """
    SELECT s.state, sm.entity_id, s.last_updated_ts, sa.shared_attrs
    FROM states s
    LEFT JOIN state_attributes sa ON sa.attributes_id = s.attributes_id
    JOIN states_meta sm ON sm.metadata_id = s.metadata_id
    """
    if formatted_timestamp:
        return f"{base_query} WHERE s.last_updated_ts < '{formatted_timestamp}' ORDER BY s.last_updated_ts ASC"
    return f"{base_query} ORDER BY s.last_updated_ts ASC"

def parse_attributes(shared_attrs):
    try:
        # Parse the shared attributes JSON
        return json.loads(shared_attrs)
    except (TypeError, json.JSONDecodeError) as e:
        logging.warning(f"Failed to parse attributes: {e}")
        return {}

def batch_insert_to_influx(write_api, rows):
    points = []
    for row in rows:
        state, entity_id, last_updated_ts, shared_attrs = row
        if state in ["unknown", "unavailable", "None"]:
            continue
        domain, _, entity_id_short = entity_id.partition('.')
        attributes_json = parse_attributes(shared_attrs)

        friendly_name = attributes_json.get('friendly_name', entity_id_short)
        unit_of_measurement = attributes_json.get('unit_of_measurement', 'default_measurement')

        if unit_of_measurement == '':
            unit_of_measurement = 'count'
        try:
            last_updated_ts_float = float(last_updated_ts)
            if HA_TIMEZONE != pytz.utc:
                local_dt = datetime.fromtimestamp(last_updated_ts_float)
                local_dt_aware = HA_TIMEZONE.localize(local_dt)
                utc_dt = local_dt_aware.astimezone(pytz.utc)
                point.time(utc_dt)
            else:
                # If no timezone is provided or invalid, assume UTC (though this might be wrong)
                utc_dt_naive = datetime.utcfromtimestamp(last_updated_ts_float)
                point.time(utc_dt_naive.replace(tzinfo=pytz.utc))

        except ValueError as e:
            logging.warning(f"Error converting timestamp for entity {entity_id}

            # Add additional attributes as fields, ensuring correct type
            for key, value in attributes_json.items():
                if key in ["id", "id_str", "update_available"]:
                    continue
                try:
                    if key in ["temperature", "humidity", "voc", "formaldehyd", "co2", "linkquality"]:
                        point.field(key, float(value))
                    elif isinstance(value, (int, float)) or (isinstance(value, str) and value.replace('.', '', 1).isdigit()):
                        point.field(key, float(value))
                    else:
                        point.field(f"{key}", str(value))
                except Exception as e:
                    logging.warning(f"Skipping field '{key}' for entity '{entity_id}' with value '{value}' due to type conflict: {e}")

            points.append(point)

        except ValueError as e:
            logging.warning(f"Error preparing InfluxDB point for entity {entity_id}: {e}, row: {row}")

    if points:
        # Write points to InfluxDB, writing each point individually in DEBUG mode
        if DEBUG_MODE:
            for point in points:
                try:
                    write_api.write(bucket=influx_bucket, org=influx_org, record=point)
                except Exception as e:
                    logging.error(f"Error writing point to InfluxDB: {e}. Point: {point}")
        else:
            try:
                write_api.write(bucket=influx_bucket, org=influx_org, record=points)
                logging.info(f"Successfully wrote {len(points)} points to InfluxDB")
            except Exception as e:
                logging.error(f"Error writing points to InfluxDB: {e}")
    else:
        logging.info("No points to write in this batch.")

def main():
    # Main execution flow
    conn, cursor = connect_to_sqlite(sqlite_db)
    write_api, query_api = connect_to_influxdb(influx_url, influx_token, influx_org)

    # Get the oldest timestamp from InfluxDB to determine how much data to process
    oldest_influx_timestamp = get_oldest_influx_timestamp(query_api)
    logging.info(f"Oldest InfluxDB timestamp: {oldest_influx_timestamp}")

    # Format the timestamp for SQLite and build the query
    formatted_timestamp = format_timestamp(oldest_influx_timestamp) if oldest_influx_timestamp else None
    sqlite_query = build_sqlite_query(formatted_timestamp)
    logging.info(f"Final SQLite query: {sqlite_query}")

    try:
        # Execute the SQLite query and process rows in batches
        logging.info(f"Fetching Data from SQLite.")
        cursor.execute(sqlite_query)
        rows_fetched = 0
        logging.info(f"Started Processing Data from SQLite.")
        while True:
            rows = cursor.fetchmany(BATCH_SIZE)
            if not rows:
                break
            batch_insert_to_influx(write_api, rows)
            rows_fetched += len(rows)
            # logging.info(f"Processed {rows_fetched} rows so far.")
    except sqlite3.Error as e:
        logging.error(f"SQLite query error: {e}")
    finally:
        # Close all connections
        cursor.close()
        conn.close()
        write_api.close()
        logging.info("Closed connections to SQLite and InfluxDB")

    logging.info("Data export complete.")

if __name__ == "__main__":
    main()

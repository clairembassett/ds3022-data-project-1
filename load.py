import duckdb  # Import DuckDB for in-process SQL database
import logging  # Import logging module for logging messages
from urllib.parse import urljoin  # Import urljoin to construct full URLs
import time  # Import time for sleep delays
import requests  # Import requests to check URL accessibility
import numpy as np  # Import numpy (not directly used here, but likely for future calculations)
import pandas as pd  # Import pandas for DataFrame handling
import random  # Import random for generating random sleep intervals

# Configure logging: INFO level, timestamp, log level, message, and log file path
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    filename="logs/load.log"
)
logger = logging.getLogger(__name__)  # Create a logger object

def is_url_accessible(url):
    # Function to check if a URL is reachable by sending a HEAD request
    try:
        response = requests.head(url, timeout=5)  # Send HEAD request with 5s timeout
        return response.status_code == 200  # Return True if status code is 200
    except requests.RequestException as e:  # Catch any request exception
        logger.warning(f"HEAD request failed for {url}: {e}")  # Log warning
        return False  # Return False if request fails
    

def log_table_row_count(con, table_name):
    # Log the number of rows in a DuckDB table
    try:
        result = con.execute(f"SELECT COUNT(*) AS row_count FROM {table_name}").fetchone()  # Get row count
        logger.info(f"Raw row count for table '{table_name}': {result[0]}")  # Log row count
    except Exception as e:
        logger.warning(f"Could not get row count for table '{table_name}': {e}")  # Log warning if fails


def load_parquet_files(start_year=2015, end_year=2024):
    # Function to load multiple taxi trip parquet files into DuckDB
    con = None
    skipped_files = []  # List to track files that cannot be loaded

    try:
        con = duckdb.connect(database="emissions.duckdb", read_only=False)  # Connect to DuckDB
        logger.info("Connected to DuckDB instance")
        con.execute("DROP TABLE IF EXISTS taxi_trips;")  # Drop table if it exists
        logger.info("Dropped table if exists")

        base_url = "https://d37ci6vzurychx.cloudfront.net/trip-data/"  # Base URL for taxi data
        first_file_loaded = False  # Flag to track first file loaded (to CREATE table)

        for year in range(start_year, end_year + 1):  # Loop through years
            for month in range(1, 13):  # Loop through months
                month_str = f"{month:02d}"  # Format month as two digits
                for taxi_type in ["yellow", "green"]:  # Loop through taxi types
                    fname = f"{taxi_type}_tripdata_{year}-{month_str}.parquet"  # Construct filename
                    file_url = urljoin(base_url, fname)  # Construct full file URL
                    logger.info(f"Attempting to load {fname} from {file_url}")  # Log attempt

                    if not is_url_accessible(file_url):  # Check URL accessibility
                        logger.warning(f"Skipping {fname}: URL not accessible")  # Log skipped file
                        skipped_files.append(fname)  # Add to skipped list
                        continue  # Skip to next iteration

                    try:
                        df = con.execute(f"SELECT * FROM read_parquet('{file_url}') LIMIT 0").fetchdf()  # Get columns
                        col_names = df.columns  # Get column names

                        # Determine pickup and dropoff column names
                        pickup_col = 'tpep_pickup_datetime' if 'tpep_pickup_datetime' in col_names else 'lpep_pickup_datetime'
                        dropoff_col = 'tpep_dropoff_datetime' if 'tpep_dropoff_datetime' in col_names else 'lpep_dropoff_datetime'

                        query = f"""
                            SELECT 
                                {pickup_col} AS pickup_datetime,
                                {dropoff_col} AS dropoff_datetime,
                                passenger_count,
                                trip_distance,
                                '{taxi_type}' AS taxi_type
                            FROM read_parquet('{file_url}')
                        """  # Construct SQL query for reading parquet

                        if not first_file_loaded:  # If first file, CREATE table
                            con.execute(f"CREATE TABLE taxi_trips AS {query}")
                            logger.info(f"Created taxi_trips from {fname}")
                            first_file_loaded = True  # Set flag to True
                        else:  # Otherwise, INSERT into existing table
                            con.execute(f"INSERT INTO taxi_trips {query}")
                            logger.info(f"Inserted {fname}")

                        time.sleep(random.uniform(60, 120))  # Random delay to avoid server overload

                    except Exception as e_inner:  # Catch exceptions for individual files
                        logger.warning(f"Could not load {fname}: {e_inner}")  # Log warning
                        skipped_files.append(fname)  # Add to skipped list

        logger.info("Done loading all taxi data into taxi_trips")  # Log completion
        log_table_row_count(con, "taxi_trips")  # Log total row count

        if skipped_files:  # Log any skipped files
            logger.info(f"Skipped {len(skipped_files)} files due to inaccessible URLs:")
            for fname in skipped_files:
                logger.info(f"  - {fname}")

    except Exception as e:  # Catch any fatal errors
        logger.error(f"Fatal error: {e}")  # Log error
        raise
    finally:
        if con:
            con.close()  # Close DuckDB connection
            logger.info("Closed DuckDB connection")


csv_path = 'data/vehicle_emissions.csv'  # Path to vehicle emissions CSV file

def load_csv(csv_path):
    # Load CSV data into DuckDB table vehicle_emissions
    con = None
    try:
        con = duckdb.connect(database="emissions.duckdb", read_only=False)  # Connect to DuckDB
        logger.info("Connected to DuckDB instance for CSV loading")

        con.execute("DROP TABLE IF EXISTS vehicle_emissions;")  # Drop existing table if exists
        logger.info("Dropped existing vehicle_emissions table if it existed")

        con.execute(f"""
            CREATE TABLE vehicle_emissions AS
            SELECT * FROM read_csv_auto('{csv_path}')
        """)  # Create table and load CSV automatically detecting column types
        logger.info(f"Loaded CSV data from {csv_path} into vehicle_emissions table")
        log_table_row_count(con, "vehicle_emissions")  # Log row count

    except Exception as e:  # Catch errors during CSV load
        logger.error(f"Error loading CSV into vehicle_emissions table: {e}")  # Log error
        raise
    finally:
        if con:
            con.close()  # Close DuckDB connection
            logger.info("Closed DuckDB connection after CSV load")


if __name__ == "__main__":
    load_parquet_files(2015, 2024) 
    load_csv(csv_path)  # Load vehicle emissions CSV

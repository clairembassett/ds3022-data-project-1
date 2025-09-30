import os
import duckdb
import logging

# Set up logging to file and console
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    filename="logs/clean.log",
)

console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
console_handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
logger = logging.getLogger(__name__)
logger.addHandler(console_handler)

def clean_parquet():
    con = None

    try:
        logger.info("Connecting to DuckDB database: emissions.duckdb")
        con = duckdb.connect(database='emissions.duckdb', read_only=False)

        # Step 1: Remove duplicates
        logger.info("Removing duplicate rows based on pickup and dropoff timestamps")
        con.execute("DROP TABLE IF EXISTS taxi_trips_clean;")
        con.execute("""
        CREATE TABLE taxi_trips_clean AS
        SELECT *
        FROM (
            SELECT 
                *,
                ROW_NUMBER() OVER (
                    PARTITION BY pickup_datetime, dropoff_datetime
                    ORDER BY pickup_datetime
                ) AS rn
            FROM taxi_trips
        ) t
        WHERE rn = 1;
        """)
        con.execute("DROP TABLE taxi_trips;")
        con.execute("ALTER TABLE taxi_trips_clean RENAME TO taxi_trips;")

        # Verify duplicates removed
        duplicates = con.execute("""
            SELECT COUNT(*) FROM (
                SELECT pickup_datetime, dropoff_datetime, COUNT(*) AS cnt
                FROM taxi_trips
                GROUP BY pickup_datetime, dropoff_datetime
                HAVING cnt > 1
            );
        """).fetchone()[0]
        logger.info(f"Duplicate check: {duplicates} duplicate groups remain")

        # Step 2: Delete zero-passenger trips
        logger.info("Deleting rows with passenger_count = 0")
        con.execute("DELETE FROM taxi_trips WHERE passenger_count = 0;")

        # Verify zero-passenger rows removed
        zero_passengers = con.execute("SELECT COUNT(*) FROM taxi_trips WHERE passenger_count = 0;").fetchone()[0]
        logger.info(f"Verification: {zero_passengers} rows with passenger_count = 0 remain")

        # Step 3: Delete zero-distance trips
        logger.info("Deleting rows with trip_distance = 0")
        con.execute("DELETE FROM taxi_trips WHERE trip_distance = 0;")

        # Verify zero-distance rows removed
        zero_distance = con.execute("SELECT COUNT(*) FROM taxi_trips WHERE trip_distance = 0;").fetchone()[0]
        logger.info(f"Verification: {zero_distance} rows with trip_distance = 0 remain")

        # Step 4: Delete implausibly long trips
        logger.info("Deleting rows with trip_distance > 100 miles")
        con.execute("DELETE FROM taxi_trips WHERE trip_distance > 100.00;")

        # Verify long-distance rows removed
        long_distance = con.execute("SELECT COUNT(*) FROM taxi_trips WHERE trip_distance > 100.00;").fetchone()[0]
        logger.info(f"Verification: {long_distance} rows with trip_distance > 100 miles remain")

        # Step 5: Delete trips longer than 24 hours
        logger.info("Deleting trips longer than 24 hours")
        con.execute("""
        DELETE FROM taxi_trips
        WHERE epoch(dropoff_datetime) - epoch(pickup_datetime) > 86400;
        """)

        # Verify long-duration trips removed
        long_duration = con.execute("""
            SELECT COUNT(*) FROM taxi_trips
            WHERE epoch(dropoff_datetime) - epoch(pickup_datetime) > 86400;
        """).fetchone()[0]
        logger.info(f"Verification: {long_duration} trips longer than 24 hours remain")

        # Final row count
        final_count = con.execute("SELECT COUNT(*) FROM taxi_trips;").fetchone()[0]
        logger.info(f"Cleaning process completed successfully. Final row count: {final_count}")

    except Exception as e:
        logger.error(f"An error occurred during cleaning: {e}")

    finally:
        if con:
            con.close()
            logger.info("Closed DuckDB connection")

if __name__ == "__main__":
    clean_parquet()
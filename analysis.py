import duckdb
import logging
import matplotlib.pyplot as plt
import pandas as pd

# Setting up logging formats and adding a file to store logs for the analysis file
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    filename="logs/analysis.log",
)

logger = logging.getLogger(__name__)

# Connecting to duckdb and defining a function for the analysis
def analysis_parquet():
    con = None
    try:
        logger.info("Connecting to DuckDB database: emissions.duckdb")
        con = duckdb.connect(database="emissions.duckdb", read_only=False)

        # Calculating max co2 levels for both taxi types
        for taxi in ["yellow", "green"]:
            try:
                max_trip = con.execute(f"""
                    SELECT MAX(trip_co2_kgs) AS max_co2
                    FROM transform
                    WHERE taxi_type = '{taxi}'
                """).fetchone()[0]

                logger.info(f"{taxi.capitalize()} taxi max CO₂ trip: {max_trip}")
                print(f"The single most carbon producing trip for {taxi} taxis produced {max_trip} kg of CO₂")
            except Exception as e:
                logger.error(f"Error calculating {taxi} max CO₂ trip: {e}")

        # Calculating heaviest and lightest hours
        for taxi in ["yellow", "green"]:
            try:
                hours = con.execute(f"""
                    SELECT 
                        hour_of_day,
                        AVG(trip_co2_kgs) AS avg_co2
                    FROM transform
                    WHERE taxi_type = '{taxi}'
                    GROUP BY hour_of_day
                    ORDER BY avg_co2 DESC
                """).fetchdf()

                heaviest_hour = hours.iloc[0]['hour_of_day']
                lightest_hour = hours.iloc[-1]['hour_of_day']

                logger.info(f"{taxi.capitalize()} taxis - Heaviest hour: {heaviest_hour}, Lightest hour: {lightest_hour}")
                print(f"For {taxi} taxis - Heaviest hour: {heaviest_hour}, Lightest hour: {lightest_hour}")
            except Exception as e:
                logger.error(f"Error calculating {taxi} taxi hours: {e}")

        # Mapping the numbers representing the days for clarity
        day_map = {
            0: "Sunday",
            1: "Monday",
            2: "Tuesday",
            3: "Wednesday",
            4: "Thursday",
            5: "Friday",
            6: "Saturday"
        }

        # Calculating heaviest day of the week
        for taxi in ["yellow", "green"]:
            try:
                days = con.execute(f"""
                    SELECT 
                        day_of_week,
                        AVG(trip_co2_kgs) AS avg_co2
                    FROM transform
                    WHERE taxi_type = '{taxi}'
                    GROUP BY day_of_week
                    ORDER BY avg_co2 DESC
                """).fetchdf()

                heaviest_day_num = days.iloc[0]['day_of_week']
                lightest_day_num = days.iloc[-1]['day_of_week']
                
                heaviest_day = day_map[heaviest_day_num]
                lightest_day = day_map[lightest_day_num]

                logger.info(f"{taxi.capitalize()} taxis - Heaviest day: {heaviest_day}, Lightest day: {lightest_day}")
                print(f"For {taxi} taxis - Heaviest day: {heaviest_day}, Lightest day: {lightest_day}")
            except Exception as e:
                logger.error(f"Error calculating {taxi} taxi days of week: {e}")

        # Calculating heaviest week of the year (1-52 across all years)
        for taxi in ["yellow", "green"]:
            try:
                weeks = con.execute(f"""
                    SELECT 
                        week_of_year,
                        AVG(trip_co2_kgs) AS avg_co2
                    FROM transform
                    WHERE taxi_type = '{taxi}'
                    GROUP BY week_of_year
                    ORDER BY avg_co2 DESC
                """).fetchdf()

                heaviest_week = weeks.iloc[0]['week_of_year']
                lightest_week = weeks.iloc[-1]['week_of_year']

                logger.info(f"{taxi.capitalize()} taxis - Heaviest week: {heaviest_week}, Lightest week: {lightest_week}")
                print(f"For {taxi} taxis - Heaviest week of year: {heaviest_week}, Lightest week of year: {lightest_week}")
            except Exception as e:
                logger.error(f"Error calculating {taxi} taxi weeks: {e}")

        # Month name mapping
        month_map = {
            1: "January",
            2: "February",
            3: "March",
            4: "April",
            5: "May",
            6: "June",
            7: "July",
            8: "August",
            9: "September",
            10: "October",
            11: "November",
            12: "December"
        }

        # Heaviest/lightest month (Jan-Dec across all years)
        for taxi in ["yellow", "green"]:
            try:
                months = con.execute(f"""
                    SELECT 
                        month_of_year,
                        AVG(trip_co2_kgs) AS avg_co2
                    FROM transform
                    WHERE taxi_type = '{taxi}'
                    GROUP BY month_of_year
                    ORDER BY avg_co2 DESC
                """).fetchdf()

                heaviest_month_num = months.iloc[0]['month_of_year']
                lightest_month_num = months.iloc[-1]['month_of_year']
                
                heaviest_month = month_map[heaviest_month_num]
                lightest_month = month_map[lightest_month_num]

                logger.info(f"{taxi.capitalize()} taxis - Heaviest month: {heaviest_month}, Lightest month: {lightest_month}")
                print(f"For {taxi} taxis - Heaviest month: {heaviest_month}, Lightest month: {lightest_month}")
            except Exception as e:
                logger.error(f"Error calculating {taxi} taxi months: {e}")

        # Plot monthly totals across all years (time series)
        try:
            df = con.execute("""
                SELECT
                    taxi_type,
                    strftime(pickup_datetime, '%Y-%m') AS year_month,
                    SUM(trip_co2_kgs) AS total_co2
                FROM transform
                GROUP BY taxi_type, year_month
                ORDER BY year_month
            """).fetchdf()

            if df.empty:
                logger.warning("No data returned for plotting monthly CO2.")
                print("No data found for plotting.")
            else:
                df_pivot = df.pivot(index="year_month", columns="taxi_type", values="total_co2").fillna(0)

                plt.figure(figsize=(14, 7))
                plt.plot(df_pivot.index, df_pivot.get("yellow", pd.Series()), marker="o", label="Yellow Taxis", color='gold', linewidth=2)
                plt.plot(df_pivot.index, df_pivot.get("green", pd.Series()), marker="o", label="Green Taxis", color='green', linewidth=2)

                plt.title("Monthly Total CO₂ Emissions by Taxi Type", fontsize=16, fontweight='bold')
                plt.xlabel("Month-Year", fontsize=12)
                plt.ylabel("Total CO₂ (kg)", fontsize=12)
                plt.xticks(rotation=45, ha='right')
                plt.legend(fontsize=11)
                plt.grid(True, alpha=0.3)

                plt.tight_layout()
                plt.savefig("monthly_co2.png", dpi=300)
                plt.show()

                logger.info("Generated monthly CO2 plot (monthly_co2.png).")
                print("\nPlot saved as 'monthly_co2.png'")
        except Exception as e:
            logger.error(f"Error generating monthly CO2 plot: {e}")
            print(f"Error: {e}")

    except Exception as e:
        logger.warning(f"Could not connect to DuckDB Instance: {e}")
        return
    finally:
        if con:
            con.close()
            logger.info("Closed DuckDB connection")


if __name__ == "__main__":
    analysis_parquet()
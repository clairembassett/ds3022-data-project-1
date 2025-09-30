-- Create a base CTE for taxi trips to simplify further transformations
WITH taxi_trips_base AS (
    SELECT *
    FROM taxi_trips
),

-- Create a base CTE for vehicle emissions to simplify joins
vehicle_emissions_base AS (
    SELECT *
    FROM vehicle_emissions
),

-- Mapping dictionary to align taxi_trips.taxi_type with vehicle_emissions.vehicle_type
taxi_type_mapping AS (
    SELECT 'yellow' AS taxi_type, 'yellow_taxi' AS vehicle_type
    UNION ALL
    SELECT 'green'  AS taxi_type, 'green_taxi'  AS vehicle_type
    UNION ALL
    SELECT 'uberx'  AS taxi_type, 'uber_x'      AS vehicle_type
    UNION ALL
    SELECT 'uberxl' AS taxi_type, 'uber_xl'     AS vehicle_type
    UNION ALL
    SELECT 'lyft'   AS taxi_type, 'lyft'        AS vehicle_type
    UNION ALL
    SELECT 'lyftxl' AS taxi_type, 'lyft_xl'     AS vehicle_type
    UNION ALL
    SELECT 'via'    AS taxi_type, 'via'         AS vehicle_type
    UNION ALL
    SELECT 'juno'   AS taxi_type, 'juno'        AS vehicle_type
),

-- Join taxi_trips_base with taxi_type_mapping to add standardized vehicle_type
taxi_trips_mapped AS (
    SELECT
        t.*,  -- All original taxi trip columns
        m.vehicle_type  -- Add mapped vehicle type
    FROM taxi_trips_base t
    LEFT JOIN taxi_type_mapping m
      ON lower(t.taxi_type) = lower(m.taxi_type)  -- Case-insensitive join
),

-- Transform trips by calculating CO2, speed, and extracting time components
transform AS (
    SELECT
        tt.*,  -- All columns from mapped trips
        ve.co2_grams_per_mile,  -- Add emissions factor
        tt.trip_distance * ve.co2_grams_per_mile / 1000 AS trip_co2_kgs,  -- Calculate CO2 in kg
        tt.trip_distance / NULLIF(DATEDIFF('second', tt.pickup_datetime, tt.dropoff_datetime) / 3600.0, 0) AS avg_mph,  -- Average speed in mph
        EXTRACT(hour FROM tt.pickup_datetime) AS hour_of_day,  -- Extract hour for analysis
        dayofweek(tt.pickup_datetime) AS day_of_week,  -- Extract day of week (Sunday=1)
        weekofyear(tt.pickup_datetime) AS week_of_year,  -- Extract ISO week number
        month(tt.pickup_datetime) AS month_of_year  -- Extract month
    FROM taxi_trips_mapped tt
    LEFT JOIN vehicle_emissions_base ve
      ON lower(tt.vehicle_type) = lower(ve.vehicle_type)  -- Join to get emissions factor
)

-- Return the final transformed dataset
SELECT *
FROM transform
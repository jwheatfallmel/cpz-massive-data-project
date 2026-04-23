-- 1. SETUP: Configure Snowflake to access S3
USE ROLE ACCOUNTADMIN;

-- Check existing storage integration (connection to AWS)
DESC INTEGRATION s3_int;

-- Create storage integration to allow Snowflake to access S3
CREATE STORAGE INTEGRATION s3_int
TYPE = EXTERNAL_STAGE
STORAGE_PROVIDER = S3
ENABLED = TRUE
STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::291314302118:role/snowflake_s3_role'
STORAGE_ALLOWED_LOCATIONS = ('s3://ppol5206-nyc-did-project/');

-- Verify integration
DESC INTEGRATION s3_int;

-- Update IAM role
ALTER STORAGE INTEGRATION s3_int
SET STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::291314302118:role/snowflake_s3_role';

-- 2. Load Yellow Cab data from S3

-- Create stage pointing to yellow taxi data
CREATE STAGE yellow_stage
URL = 's3://ppol5206-nyc-did-project/yellow/'
STORAGE_INTEGRATION = s3_int
FILE_FORMAT = (TYPE = PARQUET);

-- Preview raw data
SELECT * FROM @yellow_stage LIMIT 10;

-- 3. CREATE Full Yellow Cab dataset
CREATE OR REPLACE TABLE yellow_trips AS
SELECT
    $1:VendorID::INT AS VendorID,
    $1:PULocationID::INT AS PULocationID,
    $1:DOLocationID::INT AS DOLocationID,
    $1:trip_distance::FLOAT AS trip_distance,
    $1:fare_amount::FLOAT AS fare_amount,
    $1:total_amount::FLOAT AS total_amount,
    $1:passenger_count::INT AS passenger_count,
    TO_TIMESTAMP($1:tpep_pickup_datetime::NUMBER/1000000) AS pickup_datetime,
    TO_TIMESTAMP($1:tpep_dropoff_datetime::NUMBER/1000000) AS dropoff_datetime
FROM @yellow_stage;

SELECT CURRENT_DATABASE(), CURRENT_SCHEMA();
SHOW TABLES LIKE 'YELLOW_TRIPS';

SELECT $1
FROM @yellow_stage
LIMIT 1;

SELECT DISTINCT key
FROM @yellow_stage,
LATERAL FLATTEN(input => $1)
ORDER BY key;

-- 4. Create clean Yellow dataset with harmonized features 
CREATE OR REPLACE TABLE yellow_trips_full AS
SELECT
    $1:VendorID::INT AS VendorID,

    -- FIXED timestamps
    TO_TIMESTAMP($1:tpep_pickup_datetime::NUMBER / 1000000) AS pickup_datetime,
    TO_TIMESTAMP($1:tpep_dropoff_datetime::NUMBER / 1000000) AS dropoff_datetime,

    $1:passenger_count::INT AS passenger_count,
    $1:trip_distance::FLOAT AS trip_distance,
    $1:RatecodeID::INT AS RatecodeID,
    $1:store_and_fwd_flag::STRING AS store_and_fwd_flag,
    $1:PULocationID::INT AS PULocationID,
    $1:DOLocationID::INT AS DOLocationID,
    $1:payment_type::INT AS payment_type,
    $1:fare_amount::FLOAT AS fare_amount,
    $1:extra::FLOAT AS extra,
    $1:mta_tax::FLOAT AS mta_tax,
    $1:tip_amount::FLOAT AS tip_amount,
    $1:tolls_amount::FLOAT AS tolls_amount,
    $1:improvement_surcharge::FLOAT AS improvement_surcharge,
    $1:total_amount::FLOAT AS total_amount,
    $1:congestion_surcharge::FLOAT AS congestion_surcharge,
    $1:Airport_fee::FLOAT AS airport_fee,
    $1:cbd_congestion_fee::FLOAT AS cbd_congestion_fee

FROM @yellow_stage;

CREATE OR REPLACE TABLE yellow_trips_clean AS
SELECT
    pickup_datetime,
    dropoff_datetime,
    PULocationID,
    DOLocationID,
    trip_distance,
    tip_amount,
    tolls_amount,
    congestion_surcharge,
    airport_fee,
    cbd_congestion_fee,
    DATE(pickup_datetime) AS trip_date,
    CASE 
        WHEN DATE(pickup_datetime) >= '2025-01-05' THEN 1
        ELSE 0
    END AS post,
    'yellow' AS service_type
    
FROM yellow_trips_full;

-- 5. Validation checks
WITH all_dates AS (
    SELECT DATEADD(day, seq4(), '2024-01-01') AS dt
    FROM TABLE(GENERATOR(ROWCOUNT => 730))
),
data_dates AS (
    SELECT DISTINCT trip_date
    FROM yellow_trips_clean
)
SELECT a.dt
FROM all_dates a
LEFT JOIN data_dates d
ON a.dt = d.trip_date
WHERE d.trip_date IS NULL
ORDER BY a.dt;

SELECT 
    pickup_datetime,
    DATE(pickup_datetime) AS computed_date
FROM yellow_trips_full
LIMIT 10;

-- 6. Load HVFHV Data
CREATE OR REPLACE STAGE hvfhv_stage
URL = 's3://ppol5206-nyc-did-project/hvfhv/'
STORAGE_INTEGRATION = s3_int
FILE_FORMAT = (TYPE = PARQUET);

SELECT $1
FROM @hvfhv_stage
LIMIT 1;

-- 7. Create full HVFHV dataset
CREATE OR REPLACE TABLE hvfhv_trips_full AS
SELECT
    $1:hvfhs_license_num::STRING AS hvfhs_license_num,
    $1:dispatching_base_num::STRING AS dispatching_base_num,
    $1:originating_base_num::STRING AS originating_base_num,

    -- FIXED timestamps
    TO_TIMESTAMP($1:pickup_datetime::NUMBER / 1000000) AS pickup_datetime,
    TO_TIMESTAMP($1:dropoff_datetime::NUMBER / 1000000) AS dropoff_datetime,

    -- leave these as-is (unless they also error)
    $1:request_datetime::NUMBER / 1000000 AS request_datetime,
    $1:on_scene_datetime::NUMBER / 1000000 AS on_scene_datetime,

    $1:PULocationID::INT AS PULocationID,
    $1:DOLocationID::INT AS DOLocationID,

    $1:trip_miles::FLOAT AS trip_miles,
    $1:trip_time::FLOAT AS trip_time,

    $1:base_passenger_fare::FLOAT AS base_passenger_fare,
    $1:tolls::FLOAT AS tolls,
    $1:bcf::FLOAT AS bcf,
    $1:sales_tax::FLOAT AS sales_tax,
    $1:congestion_surcharge::FLOAT AS congestion_surcharge,
    $1:airport_fee::FLOAT AS airport_fee,
    $1:tips::FLOAT AS tips,
    $1:driver_pay::FLOAT AS driver_pay,

    $1:shared_request_flag::STRING AS shared_request_flag,
    $1:shared_match_flag::STRING AS shared_match_flag,
    $1:access_a_ride_flag::STRING AS access_a_ride_flag,
    $1:wav_request_flag::STRING AS wav_request_flag,
    $1:wav_match_flag::STRING AS wav_match_flag,

    $1:cbd_congestion_fee::FLOAT AS cbd_congestion_fee

FROM @hvfhv_stage;

SELECT 
    pickup_datetime,
    DATE(pickup_datetime) AS trip_date
FROM hvfhv_trips_full
LIMIT 10;

-- 8. Create Clean HVFHV dataset
CREATE OR REPLACE TABLE hvfhv_trips_clean AS
SELECT
    pickup_datetime,
    dropoff_datetime,
    PULocationID,
    DOLocationID,

    trip_miles AS trip_distance,

    tips AS tip_amount,
    tolls AS tolls_amount,
    congestion_surcharge,
    airport_fee,
    cbd_congestion_fee,

    DATE(pickup_datetime) AS trip_date,

    CASE 
        WHEN DATE(pickup_datetime) >= '2025-01-05' THEN 1
        ELSE 0
    END AS post,

    'hvfhv' AS service_type

FROM hvfhv_trips_full;

-- 9. Data validation
DESC TABLE yellow_trips_clean;
DESC TABLE hvfhv_trips_clean;

SELECT column_name
FROM information_schema.columns
WHERE table_name = 'YELLOW_TRIPS_CLEAN';

SELECT column_name
FROM information_schema.columns
WHERE table_name = 'HVFHV_TRIPS_CLEAN';

SELECT 
    MIN(trip_distance),
    MAX(trip_distance),
    AVG(trip_distance)
FROM hvfhv_trips_clean;

SELECT 
    MIN(trip_distance),
    MAX(trip_distance),
    AVG(trip_distance)
FROM yellow_trips_clean;

SELECT 
    COUNT(*) AS total,
    COUNT(pickup_datetime) AS pickup_ok,
    COUNT(trip_distance) AS distance_ok,
    COUNT(PULocationID) AS pu_ok,
    COUNT(DOLocationID) AS do_ok
FROM yellow_trips_clean;

SELECT 
    COUNT(*) AS total,
    COUNT(pickup_datetime) AS pickup_ok,
    COUNT(trip_distance) AS distance_ok,
    COUNT(PULocationID) AS pu_ok,
    COUNT(DOLocationID) AS do_ok
FROM hvfhv_trips_clean;

SELECT column_name, data_type
FROM information_schema.columns
WHERE table_name = 'YELLOW_TRIPS_CLEAN'
ORDER BY ordinal_position;

SELECT column_name, data_type
FROM information_schema.columns
WHERE table_name = 'HVFHV_TRIPS_CLEAN'
ORDER BY ordinal_position;

SELECT 
    'yellow' AS dataset,
    MIN(trip_date) AS min_date,
    MAX(trip_date) AS max_date
FROM yellow_trips_clean

UNION ALL

SELECT 
    'hvfhv',
    MIN(trip_date),
    MAX(trip_date)
FROM hvfhv_trips_clean;

SELECT trip_date, COUNT(*)
FROM yellow_trips_clean
GROUP BY trip_date
LIMIT 10;

SELECT 
    pickup_datetime,
    DATE(pickup_datetime),
    trip_date
FROM yellow_trips_clean
LIMIT 10;

SELECT $1:tpep_pickup_datetime
FROM @yellow_stage
LIMIT 10;

SELECT 
    'yellow' AS dataset,
    post,
    COUNT(*) AS trips
FROM yellow_trips_clean
GROUP BY post

UNION ALL

SELECT 
    'hvfhv',
    post,
    COUNT(*)
FROM hvfhv_trips_clean
GROUP BY post;

SELECT COUNT(*) 
FROM yellow_trips_clean
WHERE trip_distance < 0

UNION ALL

SELECT COUNT(*) 
FROM hvfhv_trips_clean
WHERE trip_distance < 0;

-- 10. Combine both datasets
CREATE OR REPLACE TABLE all_trips AS
SELECT 
    pickup_datetime,
    dropoff_datetime,
    PULocationID,
    DOLocationID,
    trip_distance,
    tip_amount,
    tolls_amount,
    congestion_surcharge,
    airport_fee,
    cbd_congestion_fee,
    trip_date,
    post,
    service_type
FROM yellow_trips_clean

UNION ALL

SELECT 
    pickup_datetime,
    dropoff_datetime,
    PULocationID,
    DOLocationID,
    trip_distance,
    tip_amount,
    tolls_amount,
    congestion_surcharge,
    airport_fee,
    cbd_congestion_fee,
    trip_date,
    post,
    service_type
FROM hvfhv_trips_clean;

SELECT service_type, COUNT(*)
FROM all_trips
GROUP BY service_type;

-- 11. Backup to S3 bucket

CREATE OR REPLACE STAGE all_trips_stage
URL='s3://ppol5206-nyc-did-project/all-trips/'
STORAGE_INTEGRATION = s3_int;

COPY INTO @all_trips_stage
FROM all_trips
FILE_FORMAT = (TYPE = PARQUET)
OVERWRITE = TRUE;
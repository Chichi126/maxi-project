CREATE DATABASE amdari_db;
create schema amdari_db.raw;

SHOW STORAGE INTEGRATIONS;

CREATE OR REPLACE STAGE sales_stage
  URL = 'gcs://maxi-sales-bucket001/sales/'
  STORAGE_INTEGRATION = maxi_gcp_integration
  FILE_FORMAT = (TYPE = PARQUET);



CREATE OR REPLACE STAGE weather_stage
  URL = 'gcs://maxi-sales-bucket001/weathers_data/'
  STORAGE_INTEGRATION = maxi_gcp_integration
  FILE_FORMAT = (TYPE = PARQUET);  -- This should be the Snowflake storage integration you've configured

-- List out the files in the path
  list @sales_stage;
  list @weather_stage;


create file format p_maxi_format
type = 'parquet';


    
--- Creating the all weather external table
CREATE OR REPLACE EXTERNAL TABLE  amdari_db.raw.all_loc_weather_stage(
    date_col TIMESTAMP_NTZ AS (TO_TIMESTAMP_NTZ((VALUE:date/1000000000)::INTEGER)),
    latitude FLOAT AS (VALUE:latitude::FLOAT),
    longitude FLOAT AS (VALUE:longitude::FLOAT),
    location_name STRING AS (VALUE:location:: STRING),
    sunset TIMESTAMP_NTZ AS (TO_TIMESTAMP_NTZ(VALUE: sunset:: integer)),
    sunrise TIMESTAMP_NTZ AS (TO_TIMESTAMP_NTZ(VALUE: sunrise:: integer)),
    sunshine_duration FLOAT AS (VALUE:sunshine_duration:: FLOAT),
    temperature_2m_mean FLOAT AS (VALUE:temperature_2m_mean:: FLOAT),
    temperature_2m_max FLOAT AS (VALUE:temperature_2m_max:: FLOAT),
    temperature_2m_min FLOAT AS (VALUE:temperature_2m_min:: FLOAT),
    weather_code FLOAT AS (VALUE:weather_code:: FLOAT)
    )
    WITH LOCATION = @weather_stage
    PATTERN = '.*all_weather.*\.parquet' 
    AUTO_REFRESH = False
    FILE_FORMAT = (
    TYPE = PARQUET
    COMPRESSION = AUTO
    BINARY_AS_TEXT = FALSE
    );
select * from amdari_db.raw.all_loc_weather_stage limit 10;  



--creating external table for customer
CREATE OR REPLACE EXTERNAL TABLE amdari_db.raw.customers_stage (
    customer_id INT AS (VALUE:customer_id::INT),
    address STRING AS (VALUE:address::STRING),
    card_number STRING AS (VALUE:card_number::STRING),
    city STRING AS (VALUE:city::STRING),
    country STRING AS (VALUE:country::STRING),
    postal_code STRING AS (VALUE:postal_code::STRING),
    created_at TIMESTAMP_NTZ AS (TO_TIMESTAMP_NTZ((VALUE:created_at / 1000000000)::INTEGER)),
    updated_at TIMESTAMP_NTZ AS (TO_TIMESTAMP_NTZ((VALUE:updated_at / 1000000000)::INTEGER))
)
LOCATION = @raw.sales_stage
PATTERN = '.*customer.*\.parquet'
AUTO_REFRESH = FALSE 
FILE_FORMAT = (
    TYPE = PARQUET
    COMPRESSION = AUTO
    BINARY_AS_TEXT = FALSE
);


-- crteating sales_transaction external table
CREATE OR REPLACE EXTERNAL TABLE amdari_db.raw.sales_transactions_stage (
    transaction_id INT AS (VALUE:transaction_id::INT), 
    store_id INT AS (VALUE:store_id::INT),
    customer_id INT AS (VALUE:customer_id::INT),
    product_id INT AS (VALUE:product_id::INT),
    quantity INT AS (VALUE:quantity::INT),
    unit_price FLOAT AS (VALUE:unit_price::FLOAT),
    payment_method STRING AS (VALUE:payment_method::STRING),
    sales_channel STRING AS (VALUE:sales_channel::STRING),
    created_at TIMESTAMP_NTZ AS (TO_TIMESTAMP_NTZ((VALUE:created_at / 1000000000)::INTEGER)),
    updated_at TIMESTAMP_NTZ AS (TO_TIMESTAMP_NTZ((VALUE:updated_at / 1000000000)::INTEGER))
    )
LOCATION = @raw.sales_stage  
PATTERN = '.*sale_transactions.*\.parquet'  
AUTO_REFRESH = FALSE 
FILE_FORMAT = (
    TYPE = PARQUET
    COMPRESSION = AUTO
    BINARY_AS_TEXT = FALSE
);

-- creating the store external table
CREATE OR REPLACE EXTERNAL TABLE amdari_db.raw.stores_stage (
    store_id INT AS (VALUE:store_id::INT),
    store_name STRING AS (VALUE:store_name::STRING),
    city STRING AS (VALUE:city::STRING),
    country STRING AS (VALUE:country::STRING),
    latitude FLOAT AS (VALUE:latitude::FLOAT),
    longitude FLOAT AS (VALUE:longitude::FLOAT),
    created_at TIMESTAMP_NTZ AS (TO_TIMESTAMP_NTZ((VALUE:created_at / 1000000000)::INTEGER)),
    updated_at TIMESTAMP_NTZ AS (TO_TIMESTAMP_NTZ((VALUE:updated_at / 1000000000)::INTEGER))
    )
LOCATION = @raw.sales_stage  
PATTERN = '.*stores.*\.parquet'  
AUTO_REFRESH = FALSE 
FILE_FORMAT = (
    TYPE = PARQUET
    COMPRESSION = AUTO
    BINARY_AS_TEXT = FALSE
);


-- creating products external table
CREATE OR REPLACE EXTERNAL TABLE amdari_db.raw.inventory_stage (
    inventory_id INT AS (VALUE:inventory_id::INT),
    product_id INT AS (VALUE:product_id::INT),
    store_id INT AS (VALUE:store_id::INT),
    current_stock INT AS (VALUE:current_stock::INT),
    max_stock INT AS (VALUE:max_stock::INT),
    reorder_level INT AS (VALUE:reorder_level::INT),
    last_restocked TIMESTAMP_NTZ AS (TO_TIMESTAMP_NTZ((VALUE:last_restocked / 1000000000)::INTEGER)),
    created_at TIMESTAMP_NTZ AS (TO_TIMESTAMP_NTZ((VALUE:created_at / 1000000000)::INTEGER)),
    updated_at TIMESTAMP_NTZ AS (TO_TIMESTAMP_NTZ((VALUE:updated_at / 1000000000)::INTEGER))
    )
LOCATION = @raw.sales_stage  
PATTERN = '.*inventory.*\.parquet'  
AUTO_REFRESH = FALSE 
FILE_FORMAT = (
    TYPE = PARQUET
    COMPRESSION = AUTO
    BINARY_AS_TEXT = FALSE
);

select * from amdari_db.raw.inventory_stage limit 10; 


-- creating products external table
CREATE OR REPLACE EXTERNAL TABLE amdari_db.raw.products_stage (
    product_id INT AS (VALUE:product_id::INT),
    product_name STRING AS (VALUE:product_name::STRING),
    sku STRING AS (VALUE:sku::STRING),
    category STRING AS (VALUE:category::STRING),
    price FLOAT AS (VALUE:price::FLOAT),
    cost FLOAT AS (VALUE:cost::FLOAT),
    created_at TIMESTAMP_NTZ AS (TO_TIMESTAMP_NTZ((VALUE:created_at / 1000000000)::INTEGER)),
    updated_at TIMESTAMP_NTZ AS (TO_TIMESTAMP_NTZ((VALUE:updated_at / 1000000000)::INTEGER))
    )
LOCATION = @raw.sales_stage  
PATTERN = '.*products.*\.parquet'  
AUTO_REFRESH = FALSE 
FILE_FORMAT = (
    TYPE = PARQUET
    COMPRESSION = AUTO
    BINARY_AS_TEXT = FALSE
);


-- creating products external table
CREATE OR REPLACE EXTERNAL TABLE amdari_db.raw.managers_stage (
    manager_id INT AS (VALUE:manager_id::INT),
    manager_name STRING AS (VALUE:manager_name::STRING),
    location STRING AS (VALUE:location::STRING),
    store_id INT AS (VALUE:store_id::INT)
    
    )
    
LOCATION = @raw.sales_stage  
PATTERN = '.*sales_managers.*\.parquet'  
AUTO_REFRESH = FALSE 
FILE_FORMAT = (
    TYPE = PARQUET
    COMPRESSION = AUTO
    BINARY_AS_TEXT = FALSE
);

--Uploading a CSV file
CREATE OR REPLACE EXTERNAL TABLE amdari_db.raw.managers_stage (
    manager_id INT AS (VALUE:c1::INT),
    manager_name STRING AS (VALUE:c2::STRING),
    location STRING AS (VALUE:c3::STRING),
    store_id INT AS (VALUE:c4::INT)
    )
WITH LOCATION = @sales_stage
PATTERN = '.*sales_managers.*\.csv'
AUTO_REFRESH = FALSE 
FILE_FORMAT = (TYPE = CSV FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1);


SELECT $1
FROM @sales_stage/inventory.parquet
(FILE_FORMAT => 'p_maxi_format')
LIMIT 1;

drop table amdari_db.raw.paris_stage_table; 

select * from amdari_db.raw.managers_stage limit 10;




--- CREATING THE BUSINESS ANALYTICS TABLES

CREATE OR REPLACE SCHEMA analytics;

CREATE OR REPLACE VIEW amdari_db.analytics.paris_customer_vw AS
SELECT 
    cs.customer_id,
    cs.city,
    cs.country,
    cs.postal_code,
    cs.card_number,
    cs.updated_at,
    ps.date_col,
    ps.location_name, 
    ps.temperature_2m_mean,
    ps.latitude,
    ps.longitude,
    ps.temperature_2m_max,
    ps.temperature_2m_min,
    ps.weather_code,
    ps.sunshine_duration
FROM amdari_db.raw.customers_stage AS cs
INNER JOIN amdari_db.raw.paris_stage_table AS ps
ON DATE(ps.date_col) = DATE(cs.updated_at);


USE SCHEMA analytics;

select * from amdari_db.analytics.paris_customer_vw limit 10;

-------- CREATING A CDC TABLE TO UPSERT LATEST CHANGES
-- Step 1: Create the target table for CDC
-- Step 1: Create the target table for CDC
CREATE OR REPLACE TABLE amdari_db.analytics.customer_weather_cdc (
    customer_id INT,
    city STRING,
    country STRING,
    postal_code STRING,
    card_number STRING,
    updated_at TIMESTAMP_NTZ,  
    -- Weather data
    weather_date TIMESTAMP_NTZ,
    location STRING,
    weather_temp_mean FLOAT,
    weather_temp_max FLOAT,
    weather_temp_min FLOAT,
    weather_code INT,
    weather_sunshine FLOAT,
    -- CDC metadata columns
    cdc_insert_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    cdc_update_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    cdc_operation STRING, -- INSERT, UPDATE, DELETE
    record_hash STRING -- For change detection
);

-- Step 2: Create the MERGE statement for CDC upsert
MERGE INTO amdari_db.analytics.customer_weather_cdc AS target
USING (
    SELECT 
        -- Customer columns
        c.customer_id,
        c.city,
        c.country,
        c.postal_code,
        c.card_number,
        c.updated_at,
        
        -- Weather columns
        al.date_col AS weather_date,
        al.location_name AS location,
        al.temperature_2m_mean AS weather_temp_mean,
        al.temperature_2m_max AS weather_temp_max,
        al.temperature_2m_min AS weather_temp_min,
        al.weather_code,
        al.sunshine_duration AS weather_sunshine,
        
        -- Create hash for change detection
        HASH(
            c.customer_id, c.city, c.country, c.postal_code, c.card_number, c.updated_at,
            al.date_col, al.location_name, al.temperature_2m_mean, al.temperature_2m_max, 
            al.temperature_2m_min, al.weather_code, al.sunshine_duration
        ) AS record_hash
        
    FROM amdari_db.raw.customers_stage c
    INNER JOIN amdari_db.raw.all_loc_weather_stage al
        ON DATE(c.updated_at) = DATE(al.date_col)
) AS source
ON target.customer_id = source.customer_id 
   AND target.weather_date = source.weather_date

-- When record exists but data has changed (UPDATE)
WHEN MATCHED AND target.record_hash != source.record_hash THEN
    UPDATE SET
        city = source.city,
        country = source.country,
        postal_code = source.postal_code,
        card_number = source.card_number,
        updated_at = source.updated_at,
        weather_date = source.weather_date,
        location = source.location,
        weather_temp_mean = source.weather_temp_mean,
        weather_temp_max = source.weather_temp_max,
        weather_temp_min = source.weather_temp_min,
        weather_code = source.weather_code,
        weather_sunshine = source.weather_sunshine,
        cdc_update_timestamp = CURRENT_TIMESTAMP(),
        cdc_operation = 'UPDATE',
        record_hash = source.record_hash

-- When record doesn't exist (INSERT)
WHEN NOT MATCHED THEN
    INSERT (
        customer_id, city, country, postal_code, card_number, updated_at,
        weather_date, location, weather_temp_mean, weather_temp_max, 
        weather_temp_min, weather_code, weather_sunshine,
        cdc_insert_timestamp, cdc_update_timestamp, cdc_operation, record_hash
    )
    VALUES (
        source.customer_id, source.city, source.country, source.postal_code, source.card_number, source.updated_at,
        source.weather_date, source.location, source.weather_temp_mean, source.weather_temp_max,
        source.weather_temp_min, source.weather_code, source.weather_sunshine,
        CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), 'INSERT', source.record_hash
    );
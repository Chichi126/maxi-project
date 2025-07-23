-- CREATING WAREHOUSE
CREATE WAREHOUSE  maxi_wh
warehouse_size = 'small'
auto_suspend = 30
auto_resume = True;

-- CREATING DATABASE
CREATE DATABASE maxistore_db;
-- CREATING SCHEMA
CREATE SCHEMA staging_schema;


CREATE SCHEMA maxistore_schema;
-- DROPING BAD SCHEMA
drop schema staging;


-- CREATING USER

CREATE USER data_analyst
PASSWORD = 'StrongPasword'
MUST_CHANGE_PASSWORD = true;


CREATE ROLE data_analyst_role;

DROP ROLE data_analyst;


use role data_analyst_role;

-- To grant roles to user

GRANT USAGE ON DATABASE MAXISTORE_DB TO ROLE data_analyst_role;
GRANT USAGE ON SCHEMA maxistore_schema TO ROLE data_analyst_role;
GRANT SELECT ON ALL TABLES IN SCHEMA MAXISTORE_SCHEMA TO ROLE data_analyst_role;
GRANT ROLE data_analyst_role to user data_analyst;


-- Creating an integration

CREATE OR REPLACE  STORAGE INTEGRATION maxi_gcp_integration
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = 'GCS'
  ENABLED = TRUE
  STORAGE_ALLOWED_LOCATIONS = ('gcs://maxi-sales-bucket001/' );




-- TO GET INTEGRATION PRINCIPAL
DESCRIBE INTEGRATION maxi_gcp_integration; 

SHOW STORAGE INTEGRATIONS;


CREATE OR REPLACE FILE FORMAT maxi_csv
type = 'CSV'
FIELD_OPTIONALLY_ENCLOSED_BY = '"'
SKIP_HEADER = 1;

DROP FILE FORMAT maxi_csv;

-- CREATE STAGE 
CREATE OR REPLACE STAGE weather_stage
  URL = 'gcs://maxi-sales-bucket001/weather_data/'
  STORAGE_INTEGRATION = maxi_gcp_integration  -- This should be the Snowflake storage integration you've configured
  FILE_FORMAT = (TYPE = CSV FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1);


CREATE OR REPLACE STAGE sales_stage
  URL = 'gcs://maxi-sales-bucket001/sales/'
  STORAGE_INTEGRATION = maxi_gcp_integration  -- This should be the Snowflake storage integration you've configured
  FILE_FORMAT = (TYPE = CSV FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1);

  
-- TO DISPLAY THE STAGES
SHOW STAGES;


LIST @weather_stage;
LIST @sales_stage;




SELECT $1
FROM @weather_stage/berlin_weather_
(FILE_FORMAT => 'maxi_csv')
LIMIT 1


SELECT $1
FROM @sales_stage/customers
(FILE_FORMAT => 'maxi_csv')
LIMIT 1; 


-- Creating Weather tables
CREATE OR REPLACE TABLE maxistore_db.staging_schema.berlin_weather (
    date TIMESTAMP_NTZ,
    location_name STRING,
    latitude FLOAT,
    longitude FLOAT,
    temperature_2m_mean FLOAT,
    weather_code INT,
    sunshine_duration FLOAT,
    temperature_2m_max FLOAT,
    temperature_2m_min FLOAT,
    sunrise TIMESTAMP_NTZ,
    sunset TIMESTAMP_NTZ
)
CLUSTER BY (date);


CREATE OR REPLACE TABLE maxistore_db.staging_schema.london_weather (
    date TIMESTAMP_NTZ,
    location_name STRING,
    latitude FLOAT,
    longitude FLOAT,
    temperature_2m_mean FLOAT,
    weather_code INT,
    sunshine_duration FLOAT,
    temperature_2m_max FLOAT,
    temperature_2m_min FLOAT,
    sunrise TIMESTAMP_NTZ,
    sunset TIMESTAMP_NTZ
)
CLUSTER BY (date);

CREATE OR REPLACE TABLE maxistore_db.staging_schema.los_angeles_weather (
    date TIMESTAMP_NTZ,
    location_name STRING,
    latitude FLOAT,
    longitude FLOAT,
    temperature_2m_mean FLOAT,
    weather_code INT,
    sunshine_duration FLOAT,
    temperature_2m_max FLOAT,
    temperature_2m_min FLOAT,
    sunrise TIMESTAMP_NTZ,
    sunset TIMESTAMP_NTZ
)
CLUSTER BY (date);

CREATE OR REPLACE TABLE maxistore_db.staging_schema.new_york_weather (
    date TIMESTAMP_NTZ,
    location_name STRING,
    latitude FLOAT,
    longitude FLOAT,
    temperature_2m_mean FLOAT,
    weather_code INT,
    sunshine_duration FLOAT,
    temperature_2m_max FLOAT,
    temperature_2m_min FLOAT,
    sunrise TIMESTAMP_NTZ,
    sunset TIMESTAMP_NTZ
)
CLUSTER BY (date);


CREATE OR REPLACE TABLE maxistore_db.staging_schema.paris_weather (
    date TIMESTAMP_NTZ,
    location_name STRING,
    latitude FLOAT,
    longitude FLOAT,
    temperature_2m_mean FLOAT,
    weather_code INT,
    sunshine_duration FLOAT,
    temperature_2m_max FLOAT,
    temperature_2m_min FLOAT,
    sunrise TIMESTAMP_NTZ,
    sunset TIMESTAMP_NTZ
)
CLUSTER BY (date);


-- copying weather data
COPY INTO maxistore_db.staging_schema.new_york_weather
FROM @weather_stage/new_york_weather_data_2025-07-21_21-43-41.csv
FILE_FORMAT = (TYPE = CSV FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1)

select COUNT(*) from maxistore_db.staging_schema.london_weather limit 10;
select * from maxistore_db.staging_schema.customers limit 10;


--- craete maxi_sales Tables
CREATE OR REPLACE TABLE maxistore_db.staging_schema.customers(
    customer_id NUMBER PRIMARY KEY,
    card_number STRING,
    address STRING,
    city STRING, 
    country STRING,
    postal_code STRING,
    created_at TIMESTAMP_NTZ,
    updated_at TIMESTAMP_NTZ
    )
    CLUSTER BY (country);

CREATE OR REPLACE TABLE maxistore_db.staging_schema.stores (
    store_id NUMBER PRIMARY KEY,
    store_name STRING,
    city STRING,
    country STRING,
    latitude NUMBER,
    longitude NUMBER,
    created_at TIMESTAMP_NTZ,
    updated_at TIMESTAMP_NTZ
);

CREATE OR REPLACE TABLE maxistore_db.staging_schema.products (
    product_id NUMBER PRIMARY KEY,
    sku STRING,
    product_name STRING,
    category STRING,
    price NUMBER,
    cost NUMBER,
    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    updated_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);


CREATE OR REPLACE TABLE maxistore_db.staging_schema.sales_transactions (
    transaction_id NUMBER(38,0) PRIMARY KEY, 
    store_id NUMBER,
    customer_id NUMBER,
    product_id NUMBER,
    quantity NUMBER,
    unit_price NUMBER,
    payment_method STRING,
    sales_channel STRING,
    created_at TIMESTAMP_NTZ,
    updated_at TIMESTAMP_NTZ,
    customer_card STRING,
    
    -- Foreign key constraints
    CONSTRAINT fk_sales_store FOREIGN KEY (store_id) REFERENCES maxistore_db.staging_schema.stores(store_id),
    CONSTRAINT fk_sales_customer FOREIGN KEY (customer_id) REFERENCES maxistore_db.staging_schema.customers(customer_id),
    CONSTRAINT fk_sales_product FOREIGN KEY (product_id) REFERENCES maxistore_db.staging_schema.products(product_id)
)
CLUSTER BY (updated_at);

-- Inventory table
CREATE OR REPLACE TABLE maxistore_db.staging_schema.inventory (
    inventory_id NUMBER PRIMARY KEY,
    store_id NUMBER,
    product_id NUMBER,
    current_stock NUMBER,
    reorder_level NUMBER,
    max_stock NUMBER,
    last_restocked TIMESTAMP_NTZ,
    created_at TIMESTAMP_NTZ,
    updated_at TIMESTAMP_NTZ,
    
    -- Foreign key constraints
    CONSTRAINT fk_inventory_store FOREIGN KEY (store_id) REFERENCES maxistore_db.staging_schema.stores(store_id),
    CONSTRAINT fk_inventory_product FOREIGN KEY (product_id) REFERENCES maxistore_db.staging_schema.products(product_id)
) CLUSTER BY (updated_at,store_id );

-- Sales manager table (fixed syntax errors)
CREATE OR REPLACE TABLE maxistore_db.staging_schema.sales_manager (
    manager_id NUMBER PRIMARY KEY,
    manager_name STRING,
    location STRING,
    store_id NUMBER,
    -- Foreign key constraints
    CONSTRAINT fk_manager_store FOREIGN KEY (store_id) REFERENCES maxistore_db.staging_schema.stores(store_id)
);

-- copying into tables
COPY INTO maxistore_db.staging_schema.sales_transactions
FROM @sales_stage/sale_transactions.csv
FILE_FORMAT = (TYPE = CSV FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1)


select Count(*) from maxistore_db.staging_schema.sales_transactions

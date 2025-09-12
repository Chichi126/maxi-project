-- CREATE WAREHOUSE
CREATE WAREHOUSE maxi_wh
warehouse_size = 'small'
auto_suspend = 30
auto_resume = True

-- CREATING DATABASE
CREATE DATABASE maxistore_db;

-- CREATING SCHEMA
CREATE SCHEMA staging_schema;

CREATE SCHEMA maxistore_schema;

-- DROPPING  BAD SCHEMA
DROP SCHEMA staging

-- CREATING USER
CREATE USER data_analyst
PASSWORD = 'Strongpassword'
MUST_CHANGE_PASSWORD = true;

-- CREATE ROLE
CREATE ROLE data_analyst_role

-- DROP ROLE
DROP ROLE data_analyst

use role data_analyst_role

-- To GRANT ROLE TO USER
GRANT USAGE ON DATABASE MAXISTORE_DB to ROLE data_analyst_role
GRANT USAGE ON SCHEMA MAXISTORE_SCHEMA to ROLE data_analyst_role
GRANT SELECT ON ALL TABLES IN SCHEMA MAXISTORE_SCHEMA to ROLE data_analyst_role
GRANT ROLE data_analyst_role to user data_analyst

-- CREATING AN INTEGRATION 
CREATE OR REPLACE STORAGE INTEGRATION maxi_gcp_integration
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = 'GCS'
  ENABLED = TRUE
  STORAGE_ALLOWED_LOCATIONS =('gcs://maxisales_bucket/')

 -- DELETE INTEGRATION
  DROP INTEGRATION maxi_gcp_integration; 

  -- TO GET INTEGRATION FUNCTION
  DESCRIBE INTEGRATION maxi_gcp_integration

  SHOW STORAGE INTEGRATIONS;

  -- CREATE WEATHER STAGING


CREATE OR REPLACE FILE FORMAT maxi_csv
  TYPE = 'CSV'
  FIELD_OPTIONALLY_ENCLOSED_BY = '"'
  SKIP_HEADER = 1;


  DROP FILE FORMAT maxi_csv;

  
  
  -- CREATING WEATHER STAGING
CREATE OR REPLACE STAGE weather_stage
  URL='gcs://maxisales_bucket/weather_data'
  STORAGE_INTEGRATION = maxi_gcp_integration
  FILE_FORMAT = (TYPE = CSV FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1);

-- CREATING SALES STAGING
CREATE OR REPLACE STAGE sales_stage
  URL='gcs://maxisales_bucket/sales'
  STORAGE_INTEGRATION = maxi_gcp_integration
  FILE_FORMAT = (TYPE = CSV FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1);

  
  -- TO DISPLAY STAGE
  SHOW STAGES;
  
  LIST @weather_stage;
  LIST @sales_stage;

--QUERY FOR CSV FILES IN THE WEATHER STAGE
SELECT $1
FROM @weather_stage/berlin_daily_weather_
(FILE_FORMAT => 'maxi_csv')
LIMIT 1;

--QUERY FOR CSV FILES IN THE SALES STAGE
SELECT $1
FROM @sales_stage/customers
(FILE_FORMAT => 'maxi_csv')
LIMIT 1;

-- CREATE TABLES
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
)
CLUSTER BY (country);

CREATE OR REPLACE TABLE maxistore_db.staging_schema.products (
    product_id NUMBER PRIMARY KEY,
    sku STRING,
    product_name STRING,
    category STRING,
    price NUMBER,
    cost NUMBER,
    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    updated_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
)
CLUSTER BY (product_name);

CREATE OR REPLACE TABLE maxistore_db.staging_schema.sales_transactions (
    invoice_id NUMBER(38,0) PRIMARY KEY,
    store_id NUMBER,
    customer_id NUMBER,
    product_id NUMBER,
    quantity NUMBER,
    unit_price NUMBER,
    payment_method STRING,
    sales_channel STRING,
    created_at TIMESTAMP_NTZ,
    updated_at TIMESTAMP_NTZ,
    
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

CREATE OR REPLACE TABLE maxistore_db.staging_schema.berlin_weather (
    date TIMESTAMP_NTZ,
    location_name STRING,
    location_latitude FLOAT,
    location_longitude FLOAT,
    temperature_2m_mean FLOAT,
    weather_code INT,
    sunshine_duration FLOAT,
    temperature_2m_max FLOAT,
    temperature_2m_min FLOAT,
    sunrise TIMESTAMP_NTZ,
    sunset TIMESTAMP_NTZ,
    rain_sum FLOAT,
    snowfall_sum FLOAT
)
CLUSTER BY (date);


CREATE OR REPLACE TABLE maxistore_db.staging_schema.london_weather (
    date TIMESTAMP_NTZ,
    location_name STRING,
    location_latitude FLOAT,
    location_longitude FLOAT,
    temperature_2m_mean FLOAT,
    weather_code INT,
    sunshine_duration FLOAT,
    temperature_2m_max FLOAT,
    temperature_2m_min FLOAT,
    sunrise TIMESTAMP_NTZ,
    sunset TIMESTAMP_NTZ,
    rain_sum FLOAT,
    snowfall_sum FLOAT
)
CLUSTER BY (date);


CREATE OR REPLACE TABLE maxistore_db.staging_schema.los_angeles_weather (
    date TIMESTAMP_NTZ,
    location_name STRING,
    location_latitude FLOAT,
    location_longitude FLOAT,
    temperature_2m_mean FLOAT,
    weather_code INT,
    sunshine_duration FLOAT,
    temperature_2m_max FLOAT,
    temperature_2m_min FLOAT,
    sunrise TIMESTAMP_NTZ,
    sunset TIMESTAMP_NTZ,
    rain_sum FLOAT,
    snowfall_sum FLOAT
)
CLUSTER BY (date);

CREATE OR REPLACE TABLE maxistore_db.staging_schema.new_york_weather (
    date TIMESTAMP_NTZ,
    location_name STRING,
    location_latitude FLOAT,
    location_longitude FLOAT,
    temperature_2m_mean FLOAT,
    weather_code INT,
    sunshine_duration FLOAT,
    temperature_2m_max FLOAT,
    temperature_2m_min FLOAT,
    sunrise TIMESTAMP_NTZ,
    sunset TIMESTAMP_NTZ,
    rain_sum FLOAT,
    snowfall_sum FLOAT
)
CLUSTER BY (date);


CREATE OR REPLACE TABLE maxistore_db.staging_schema.paris_weather (
    date TIMESTAMP_NTZ,
    location_name STRING,
    location_latitude FLOAT,
    location_longitude FLOAT,
    temperature_2m_mean FLOAT,
    weather_code INT,
    sunshine_duration FLOAT,
    temperature_2m_max FLOAT,
    temperature_2m_min FLOAT,
    sunrise TIMESTAMP_NTZ,
    sunset TIMESTAMP_NTZ,
    rain_sum FLOAT,
    snowfall_sum FLOAT
)
CLUSTER BY (date);


-- COPY DATA INTO CUSTOMER TABLE
COPY INTO maxistore_db.staging_schema.customers
FROM @sales_stage/customers.csv
FILE_FORMAT = (TYPE = CSV FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1);

-- VIEW DATA IN CUSTOMER TABLE
SELECT * FROM maxistore_db.staging_schema.customers LIMIT 10;

-- COPY DATA INTO STORE TABLE
COPY INTO maxistore_db.staging_schema.stores
FROM @sales_stage/stores.csv
FILE_FORMAT = (TYPE = CSV FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1);

-- VIEW DATA IN STORE TABLE
SELECT * FROM maxistore_db.staging_schema.stores LIMIT 10;

-- COPY DATA INTO PRODUCTS TABLE
COPY INTO maxistore_db.staging_schema.products
FROM @sales_stage/products.csv
FILE_FORMAT = (TYPE = CSV FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1);

-- VIEW DATA IN PRODUCTS TABLE
SELECT * FROM maxistore_db.staging_schema.products LIMIT 10;

-- COPY DATA INTO SALES_TRANSACTIONS TABLE
COPY INTO maxistore_db.staging_schema.sales_transactions
FROM @sales_stage/sales_transactions.csv
FILE_FORMAT = (TYPE = CSV FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1);

-- VIEW DATA IN SALES_TRANSACTIONS TABLE
SELECT * FROM maxistore_db.staging_schema.sales_transactions LIMIT 10;

-- COPY DATA INTO INVENTORY TABLE
COPY INTO maxistore_db.staging_schema.inventory
FROM @sales_stage/inventory.csv
FILE_FORMAT = (TYPE = CSV FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1);

-- VIEW DATA IN INVENTORY TABLE
SELECT * FROM maxistore_db.staging_schema.inventory LIMIT 10;

-- COPY DATA INTO SALES_MANAGER TABLE
COPY INTO maxistore_db.staging_schema.sales_manager
FROM @sales_stage/sales_managers.csv
FILE_FORMAT = (TYPE = CSV FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1);

-- VIEW DATA IN INVENTORY TABLE
SELECT * FROM maxistore_db.staging_schema.sales_manager LIMIT 10;


  
-- COPY DATA INTO BERLIN_WEATHER TABLE

COPY INTO maxistore_db.staging_schema.berlin_weather
FROM @weather_stage/berlin
FILE_FORMAT = (TYPE = 'CSV' SKIP_HEADER = 1)
PATTERN = '.*\\.csv';


-- VIEW DATA IN BERLIN_DATA TABLE
SELECT * FROM maxistore_db.staging_schema.berlin_weather LIMIT 10;

-- COPY DATA INTO LONDON_WEATHER TABLE

COPY INTO maxistore_db.staging_schema.london_weather
FROM @weather_stage/london
FILE_FORMAT = (TYPE = 'CSV' SKIP_HEADER = 1)
PATTERN = '.*\\.csv';


-- VIEW DATA IN LONDON_DATA TABLE
SELECT * FROM maxistore_db.staging_schema.london_weather LIMIT 10;


-- VIEW DATA IN los angeles_DATA TABLE
COPY INTO maxistore_db.staging_schema.los_angeles_weather
FROM @weather_stage/los_angeles
FILE_FORMAT = (TYPE = 'CSV' SKIP_HEADER = 1)
PATTERN = '.*\\.csv';


-- VIEW DATA IN LOS_ANGELES_DATA TABLE
SELECT * FROM maxistore_db.staging_schema.los_angeles_weather LIMIT 10;


-- VIEW DATA IN NEW_YORK_DATA TABLE
COPY INTO maxistore_db.staging_schema.new_york_weather
FROM @weather_stage/los_angeles
FILE_FORMAT = (TYPE = 'CSV' SKIP_HEADER = 1)
PATTERN = '.*\\.csv';


-- VIEW DATA IN NEW_YORK_DATA TABLE
SELECT * FROM maxistore_db.staging_schema.new_york_weather LIMIT 10;



-- VIEW DATA IN NEW_YORK_DATA TABLE
COPY INTO maxistore_db.staging_schema.paris_weather
FROM @weather_stage/paris
FILE_FORMAT = (TYPE = 'CSV' SKIP_HEADER = 1)
PATTERN = '.*\\.csv';


-- VIEW DATA IN LOS_ANGELES_DATA TABLE
SELECT * FROM maxistore_db.staging_schema.paris_weather LIMIT 10;
CREATE DATABASE maxiexternal_db;

CREATE SCHEMA maxiexternal_db.raw; 

SHOW STORAGE INTEGRATIONS;

-- CREATING WEATHER STAGING
CREATE OR REPLACE STAGE weather_stage
  URL='gcs://maxisales_bucket/weather_data'
  STORAGE_INTEGRATION = maxi_gcp_integration
  FILE_FORMAT = (TYPE = PARQUET);


-- CREATING SALES STAGING
CREATE OR REPLACE STAGE sales_stage
  URL='gcs://maxisales_bucket/sales'
  STORAGE_INTEGRATION = maxi_gcp_integration
  FILE_FORMAT = (TYPE = PARQUET);

  -- CREATING WEATHER STAGING
CREATE OR REPLACE STAGE weathers_stage
  URL='gcs://maxisales_bucket/weathers_data'
  STORAGE_INTEGRATION = maxi_gcp_integration
  FILE_FORMAT = (TYPE = PARQUET);

  LIST @sales_stage;
  LIST @weather_stage;
  LIST @weathers_stage;

 CREATE FILE FORMAT p_maxi_format
  type = 'parquet';

  -- Preview parquet file from GCS stage
SELECT *
FROM @weather_stage/berlin/berlin_daily_weather_20250815_174531.parquet
(FILE_FORMAT => p_maxi_format)
LIMIT 10;


  SELECT $1
  FROM @sales_stage/customers.parquet
  (FILE_FORMAT => 'p_maxi_format')
  LIMIT 1;


--- Creating the all weather external table
CREATE OR REPLACE EXTERNAL TABLE  maxiexternal_db.raw.all_loc_weather_stage(
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
    WITH LOCATION = @weathers_stage
    PATTERN = '.*all_weather.*\.parquet' 
    AUTO_REFRESH = False
    FILE_FORMAT = (
    TYPE = PARQUET
    COMPRESSION = AUTO
    BINARY_AS_TEXT = FALSE
    );
select * from maxiexternal_db.raw.all_loc_weather_stage limit 10; 


--creating external table for customer
CREATE OR REPLACE EXTERNAL TABLE maxiexternal_db.raw.customers_stage (
    customer_id INT AS (VALUE:customer_id::INT),
    address STRING AS (VALUE:address::STRING),
    phone STRING AS (VALUE:phone::STRING),
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

select * from maxiexternal_db.raw.customers_stage limit 10; 

-- crteating sales_transaction external table
CREATE OR REPLACE EXTERNAL TABLE maxiexternal_db.raw.sales_transactions_stage (
    invoice_id INT AS (VALUE:invoice_id::INT), 
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
PATTERN = '.*sales_transactions.*\.parquet'  
AUTO_REFRESH = FALSE 
FILE_FORMAT = (
    TYPE = PARQUET
    COMPRESSION = AUTO
    BINARY_AS_TEXT = FALSE
);

select * from maxiexternal_db.raw.sales_transactions_stage limit 100; 


-- creating the store external table
CREATE OR REPLACE EXTERNAL TABLE maxiexternal_db.raw.stores_stage (
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

select * from maxiexternal_db.raw.stores_stage limit 10;

-- creating products external table
CREATE OR REPLACE EXTERNAL TABLE maxiexternal_db.raw.inventory_stage (
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

select * from maxiexternal_db.raw.inventory_stage limit 10; 

-- creating products external table
CREATE OR REPLACE EXTERNAL TABLE maxiexternal_db.raw.products_stage (
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

select * from maxiexternal_db.raw.products_stage limit 10; 


-- creating products external table
CREATE OR REPLACE EXTERNAL TABLE maxiexternal_db.raw.managers_stage (
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

select * from maxiexternal_db.raw.managers_stage limit 10;


CREATE OR REPLACE VIEW maxiexternal_db.analytics.customer_weather AS
SELECT 
    cs.customer_id,
    cs.city,
    cs.country,
    cs.postal_code,
    cs.phone,
    cs.updated_at,
    al.date_col AS weather_date,
    al.location_name, 
    al.temperature_2m_mean,
    al.latitude,
    al.longitude,
    al.temperature_2m_max,
    al.temperature_2m_min,
    al.weather_code,
    al.sunshine_duration
FROM maxiexternal_db.raw.customers_stage AS cs
JOIN maxiexternal_db.raw.all_loc_weather_stage AS al
    ON TO_DATE(cs.updated_at) = TO_DATE(al.date_col)
WHERE al.latitude = '52.52'
  AND al.longitude = '13.41';



select COUNT(*) from maxiexternal_db.analytics.customer_weather limit 10;

select * from maxiexternal_db.analytics.customer_weather limit 10;

DROP VIEW CUSTOMER_WEATHER_BERLIN;

USE SCHEMA analytics;


CREATE OR REPLACE VIEW maxiexternal_db.analytics.store_weather_view AS
WITH ranked_weather AS (
    SELECT 
        s.store_id,
        s.store_name,
        s.city,
        s.country,
        s.latitude AS store_lat,
        s.longitude AS store_lon,
        w.date_col,
        w.location_name AS weather_location,
        w.sunset,
        w.sunrise,
        w.sunshine_duration,
        w.temperature_2m_mean,
        w.temperature_2m_max,
        w.temperature_2m_min,
        w.weather_code,
        ROW_NUMBER() OVER (
            PARTITION BY s.store_id 
            ORDER BY ABS(DATEDIFF(DAY, TO_DATE(s.updated_at), TO_DATE(w.date_col))), 
                     SQRT(POWER(s.latitude - w.latitude, 2) + POWER(s.longitude - w.longitude, 2))
        ) AS rn
    FROM maxiexternal_db.raw.stores_stage s
    JOIN maxiexternal_db.raw.all_loc_weather_stage w
        ON w.date_col IS NOT NULL
)
SELECT *
FROM ranked_weather
WHERE rn = 1;



SELECT COUNT(*) FROM maxiexternal_db.raw.stores_stage;

SELECT COUNT(*) FROM maxiexternal_db.raw.all_loc_weather_stage;

select * from maxiexternal_db.analytics.store_weather_view limit 10;


CREATE OR REPLACE VIEW analytics.sales_weather_inventory_view AS
WITH sales_enriched AS (
    SELECT 
        s.invoice_id,
        s.store_id,
        st.store_name,
        st.city,
        st.country,
        st.latitude,
        st.longitude,
        s.product_id,
        p.product_name,
        p.category,
        s.customer_id,
        c.city AS customer_city,
        c.country AS customer_country,
        s.quantity,
        s.unit_price,
        (s.quantity * s.unit_price) AS revenue,
        s.sales_channel,
        s.payment_method,
        DATE_TRUNC('day', s.created_at) AS sales_date,
        -- join inventory
        i.current_stock,
        i.max_stock,
        i.reorder_level,
        -- join weather (by date and closest lat/long)
        w.temperature_2m_mean,
        w.temperature_2m_max,
        w.temperature_2m_min,
        w.sunshine_duration,
        w.weather_code
    FROM maxiexternal_db.raw.sales_transactions_stage s
    JOIN maxiexternal_db.raw.stores_stage st 
        ON s.store_id = st.store_id
    JOIN maxiexternal_db.raw.products_stage p 
        ON s.product_id = p.product_id
    LEFT JOIN maxiexternal_db.raw.customers_stage c 
        ON s.customer_id = c.customer_id
    LEFT JOIN maxiexternal_db.raw.inventory_stage i 
        ON s.store_id = i.store_id 
       AND s.product_id = i.product_id
    LEFT JOIN maxiexternal_db.raw.all_loc_weather_stage w 
        ON DATE_TRUNC('day', s.created_at) = DATE_TRUNC('day', w.date_col)
       AND ABS(st.latitude - w.latitude) < 0.5   -- fuzzy join (approx 50km)
       AND ABS(st.longitude - w.longitude) < 0.5
)
SELECT 
    store_id,
    store_name,
    city,
    country,
    product_id,
    product_name,
    category,
    sales_date,
    SUM(quantity) AS total_units_sold,
    SUM(revenue) AS total_revenue,
    AVG(unit_price) AS avg_unit_price,
    AVG(current_stock) AS avg_stock_level,
    MIN(reorder_level) AS reorder_level,
    AVG(temperature_2m_mean) AS avg_temp,
    MAX(temperature_2m_max) AS max_temp,
    MIN(temperature_2m_min) AS min_temp,
    AVG(sunshine_duration) AS avg_sunshine,
    COUNT(DISTINCT customer_id) AS unique_customers,
    SUM(CASE WHEN current_stock <= reorder_level THEN 1 ELSE 0 END) AS stockout_risk_days
FROM sales_enriched
GROUP BY store_id, store_name, city, country, product_id, product_name, category, sales_date;


select * from maxiexternal_db.analytics.sales_weather_view;


-- Sales + Weather Demand View
-- Tackles: Inaccurate Forecasting due to lack of weather signals
-- Use case: See how sales trend with temperature, rainfall, and sunshine (input for ML models).

CREATE OR REPLACE VIEW analytics.sales_weather_view AS
SELECT 
    s.invoice_id,
    s.customer_id,
    s.store_id,
    st.store_name,
    st.city,
    st.country,
    s.product_id,
    p.product_name,
    p.category,
    DATE_TRUNC('day', s.created_at) AS sales_date,
    s.quantity,
    s.unit_price,
    (s.quantity * s.unit_price) AS revenue,
    w.temperature_2m_mean,
    w.temperature_2m_max,
    w.temperature_2m_min,
    w.sunshine_duration,
    w.weather_code
FROM maxiexternal_db.raw.sales_transactions_stage s
JOIN maxiexternal_db.raw.stores_stage st 
    ON s.store_id = st.store_id
JOIN maxiexternal_db.raw.products_stage p 
    ON s.product_id = p.product_id
LEFT JOIN maxiexternal_db.raw.all_loc_weather_stage w 
    ON DATE_TRUNC('day', s.created_at) = DATE_TRUNC('day', w.date_col)
   AND ABS(st.latitude - w.latitude) < 0.5
   AND ABS(st.longitude - w.longitude) < 0.5;


-- Inventory Risk View
-- Tackles: Stockouts, Overstocks, and Emergency Restocking Costs
-- Use case: Ops team gets alerts for restocking or reducing excess stock.

CREATE OR REPLACE VIEW analytics.inventory_risk_view AS
SELECT 
    i.store_id,
    st.store_name,
    i.product_id,
    p.product_name,
    i.current_stock,
    i.max_stock,
    i.reorder_level,
    CASE 
        WHEN i.current_stock <= i.reorder_level THEN 'AT RISK OF STOCKOUT'
        WHEN i.current_stock > i.max_stock * 0.9 THEN 'POTENTIAL OVERSTOCK'
        ELSE 'OPTIMAL'
    END AS inventory_status,
    i.last_restocked
FROM maxiexternal_db.raw.inventory_stage i
JOIN maxiexternal_db.raw.stores_stage st 
    ON i.store_id = st.store_id
JOIN maxiexternal_db.raw.products_stage p 
    ON i.product_id = p.product_id;

select * from maxiexternal_db.analytics.inventory_risk_view;

-- Customer Experience View
-- Tackles: Customer Dissatisfaction due to stockouts of weather-sensitive products
-- Use case: Identify customers who might face dissatisfaction because products they buy often are at stockout risk.

CREATE OR REPLACE VIEW analytics.customer_experience_view AS
SELECT 
    s.customer_id,
    c.city AS customer_city,
    c.country AS customer_country,
    s.store_id,
    st.store_name,
    s.product_id,
    p.product_name,
    SUM(s.quantity) AS total_units_bought,
    COUNT(DISTINCT s.invoice_id) AS purchase_frequency,
    MAX(i.inventory_status) AS last_inventory_status
FROM analytics.sales_weather_view s
JOIN maxiexternal_db.raw.customers_stage c 
    ON s.customer_id = c.customer_id
JOIN analytics.inventory_risk_view i 
    ON s.product_id = i.product_id
   AND s.store_id = i.store_id
JOIN maxiexternal_db.raw.stores_stage st 
    ON s.store_id = st.store_id
JOIN maxiexternal_db.raw.products_stage p 
    ON s.product_id = p.product_id
GROUP BY s.customer_id, c.city, c.country, s.store_id, st.store_name, s.product_id, p.product_name;


select * from maxiexternal_db.analytics.customer_experience_view;

-- Operational Efficiency View
-- Tackles: Operational Costs & Missed Sales due to inefficient supply chain
-- Use case: Store-level view of efficiency → helps management cut storage costs & avoid missed sales.

CREATE OR REPLACE VIEW analytics.operational_efficiency_view AS
SELECT 
    st.store_id,
    st.store_name,
    st.city,
    st.country,
    SUM(s.revenue) AS total_revenue,
    SUM(i.current_stock) AS total_stock,
    COUNT(DISTINCT s.customer_id) AS unique_customers,
    SUM(CASE WHEN i.inventory_status = 'AT RISK OF STOCKOUT' THEN 1 ELSE 0 END) AS stockout_days,
    SUM(CASE WHEN i.inventory_status = 'POTENTIAL OVERSTOCK' THEN 1 ELSE 0 END) AS overstock_days
FROM analytics.sales_weather_view s
JOIN analytics.inventory_risk_view i 
    ON s.store_id = i.store_id
   AND s.product_id = i.product_id
JOIN maxiexternal_db.raw.stores_stage st 
    ON s.store_id = st.store_id
GROUP BY st.store_id, st.store_name, st.city, st.country;

select * from maxiexternal_db.analytics.operational_efficiency_view;


--Competitive Advantage View
--Tackles: Leveraging weather to predict future demand better than competitors
--Use case: Marketing & supply chain teams can anticipate spikes in demand based on weather-sensitive products.

CREATE OR REPLACE VIEW analytics.weather_demand_signal_view AS
SELECT 
    product_id,
    product_name,
    category,
    sales_date,
    SUM(quantity) AS total_units_sold,
    AVG(temperature_2m_mean) AS avg_temp,
    AVG(sunshine_duration) AS avg_sunshine,
    CASE 
        WHEN category ILIKE '%drink%' AND AVG(temperature_2m_mean) > 30 THEN 'HIGH WEATHER DEMAND'
        WHEN category ILIKE '%umbrella%' AND AVG(weather_code) BETWEEN 50 AND 70 THEN 'HIGH WEATHER DEMAND'
        ELSE 'NORMAL DEMAND'
    END AS weather_demand_signal
FROM analytics.sales_weather_view
GROUP BY product_id, product_name, category, sales_date;

select * from maxiexternal_db.analytics.weather_demand_signal_view;

-- Sales + Weather Forecasting View (with rolling averages & lag features)
-- This view is ML-ready: You get sales + weather + rolling windows + lags for time-series forecasting.

CREATE OR REPLACE VIEW analytics.sales_weather_forecast_features AS
WITH base AS (
    SELECT 
        s.store_id,
        st.store_name,
        st.city,
        st.country,
        s.product_id,
        p.product_name,
        p.category,
        DATE_TRUNC('day', s.created_at) AS sales_date,
        SUM(s.quantity) AS total_units_sold,
        SUM(s.quantity * s.unit_price) AS total_revenue,
        AVG(s.unit_price) AS avg_unit_price,
        AVG(w.temperature_2m_mean) AS avg_temp,
        MAX(w.temperature_2m_max) AS max_temp,
        MIN(w.temperature_2m_min) AS min_temp,
        AVG(w.sunshine_duration) AS avg_sunshine,
        AVG(w.weather_code) AS avg_weather_code
    FROM maxiexternal_db.raw.sales_transactions_stage s
    JOIN maxiexternal_db.raw.stores_stage st 
        ON s.store_id = st.store_id
    JOIN maxiexternal_db.raw.products_stage p 
        ON s.product_id = p.product_id
    LEFT JOIN maxiexternal_db.raw.all_loc_weather_stage w 
        ON DATE_TRUNC('day', s.created_at) = DATE_TRUNC('day', w.date_col)
       AND ABS(st.latitude - w.latitude) < 0.5
       AND ABS(st.longitude - w.longitude) < 0.5
    GROUP BY s.store_id, st.store_name, st.city, st.country, s.product_id, p.product_name, p.category, DATE_TRUNC('day', s.created_at)
)
SELECT 
    *,
    -- Rolling features
    AVG(total_units_sold) OVER (PARTITION BY store_id, product_id ORDER BY sales_date ROWS BETWEEN 7 PRECEDING AND CURRENT ROW) AS rolling_avg_7d_units,
    AVG(total_units_sold) OVER (PARTITION BY store_id, product_id ORDER BY sales_date ROWS BETWEEN 30 PRECEDING AND CURRENT ROW) AS rolling_avg_30d_units,
    SUM(total_units_sold) OVER (PARTITION BY store_id, product_id ORDER BY sales_date ROWS BETWEEN 7 PRECEDING AND CURRENT ROW) AS rolling_sum_7d_units,
    SUM(total_units_sold) OVER (PARTITION BY store_id, product_id ORDER BY sales_date ROWS BETWEEN 30 PRECEDING AND CURRENT ROW) AS rolling_sum_30d_units,

    -- Lag features (previous days)
    LAG(total_units_sold, 1) OVER (PARTITION BY store_id, product_id ORDER BY sales_date) AS lag_1d_units,
    LAG(total_units_sold, 7) OVER (PARTITION BY store_id, product_id ORDER BY sales_date) AS lag_7d_units,
    LAG(total_units_sold, 30) OVER (PARTITION BY store_id, product_id ORDER BY sales_date) AS lag_30d_units
FROM base;

select * from maxiexternal_db.analytics.sales_weather_forecast_features;

-- Inventory Forecast View (with demand risk signals)
-- This view helps supply chain: Predict stockout/overstock risk using both inventory levels + demand features + weather context.
CREATE OR REPLACE VIEW analytics.inventory_forecast_features AS
SELECT 
    i.store_id,
    st.store_name,
    i.product_id,
    p.product_name,
    p.category,
    i.current_stock,
    i.max_stock,
    i.reorder_level,
    i.last_restocked,
    -- Stock risk signal
    CASE 
        WHEN i.current_stock <= i.reorder_level THEN 1 ELSE 0 
    END AS stockout_risk_flag,
    CASE 
        WHEN i.current_stock > (i.max_stock * 0.9) THEN 1 ELSE 0 
    END AS overstock_risk_flag,
    -- Historical demand features from sales
    swf.rolling_avg_7d_units,
    swf.rolling_avg_30d_units,
    swf.lag_1d_units,
    swf.lag_7d_units,
    swf.lag_30d_units,
    swf.avg_temp,
    swf.avg_sunshine,
    swf.avg_weather_code
FROM maxiexternal_db.raw.inventory_stage i
JOIN maxiexternal_db.raw.stores_stage st 
    ON i.store_id = st.store_id
JOIN maxiexternal_db.raw.products_stage p 
    ON i.product_id = p.product_id
LEFT JOIN analytics.sales_weather_forecast_features swf
    ON i.store_id = swf.store_id
   AND i.product_id = swf.product_id
   AND swf.sales_date = CURRENT_DATE;  -- align inventory with latest sales features


select * from maxiexternal_db.analytics.inventory_forecast_features;

-- Customer Demand Sensitivity View (weather-driven patterns)
-- This view helps marketing & product teams: Detect weather-sensitive demand spikes for targeted promotions or stocking.

CREATE OR REPLACE VIEW analytics.customer_weather_demand_view AS
SELECT 
    swf.store_id,
    swf.store_name,
    swf.city,
    swf.country,
    swf.product_id,
    swf.product_name,
    swf.category,
    swf.sales_date,
    swf.total_units_sold,
    swf.total_revenue,
    swf.avg_temp,
    swf.avg_sunshine,
    CASE 
        WHEN swf.category ILIKE '%drink%' AND swf.avg_temp > 30 THEN 'HOT WEATHER SPIKE'
        WHEN swf.category ILIKE '%umbrella%' AND swf.avg_weather_code BETWEEN 50 AND 70 THEN 'RAIN DEMAND SPIKE'
        ELSE 'NORMAL'
    END AS weather_demand_signal
FROM analytics.sales_weather_forecast_features swf;

select * from maxiexternal_db.analytics.customer_weather_demand_view;

SELECT CURRENT_REGION();


SELECT SYSTEM$GET_ACCOUNT_LOCATOR(), CURRENT_REGION();


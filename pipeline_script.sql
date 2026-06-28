sql-- ============================================================================
-- PROJECT: Databricks Address Validation & Standardization Pipeline
-- ARCHITECTURE: Medallion Framework (Bronze -> Silver -> Gold Audit)
-- PLATFORM: Databricks Serverless SQL Warehouse & Delta Lake Protocols
-- ============================================================================

-- ==========================================
-- STEP 1: BRONZE LAYER (Raw Landing Zone)
-- ==========================================

-- 1. Clear out any old versions of the table
DROP TABLE IF EXISTS bronze_user_addresses;

-- 2. Create the Bronze table schema
CREATE TABLE bronze_user_addresses (
    user_id INT,
    raw_address STRING
);

-- 3. Ingest messy, unformatted real-world user data inputs
INSERT INTO bronze_user_addresses VALUES
(101, '1600 Amfiteater Pkwy, Mountain View, CA'),
(102, '1 infinite loop cupertino ca 95014'),
(103, '1600 Amphitheatre Pkwy'),
(104, 'Fake Street, Nowhere, XX 00000'),
(105, '  350 5th Ave, New York, NY 10118   ');


-- ==========================================
-- STEP 2: SILVER LAYER (Parsing & Isolation)
-- ==========================================

-- 1. Clear out any old versions of the silver target tables
DROP TABLE IF EXISTS silver_valid_addresses;
DROP TABLE IF EXISTS silver_quarantined_addresses;

-- 2. Create the Clean Silver table (Standardizing known addresses)
CREATE TABLE silver_valid_addresses AS
SELECT 
    user_id,
    TRIM(raw_address) AS raw_address,
    CASE 
        WHEN UPPER(raw_address) LIKE '%AMFI%' OR UPPER(raw_address) LIKE '%AMPHITHEATRE%' THEN '1600 Amphitheatre Pkwy'
        WHEN UPPER(raw_address) LIKE '%INFINITE LOOP%' THEN '1 Infinite Loop'
        WHEN UPPER(raw_address) LIKE '%5TH AVE%' THEN '350 5th Ave'
    END AS standardized_address,
    CASE 
        WHEN UPPER(raw_address) LIKE '%AMFI%' OR UPPER(raw_address) LIKE '%AMPHITHEATRE%' THEN 'Mountain View'
        WHEN UPPER(raw_address) LIKE '%INFINITE LOOP%' THEN 'Cupertino'
        WHEN UPPER(raw_address) LIKE '%5TH AVE%' THEN 'New York'
    END AS city,
    CASE 
        WHEN UPPER(raw_address) LIKE '%AMFI%' OR UPPER(raw_address) LIKE '%AMPHITHEATRE%' THEN 'CA'
        WHEN UPPER(raw_address) LIKE '%INFINITE LOOP%' THEN 'CA'
        WHEN UPPER(raw_address) LIKE '%5TH AVE%' THEN 'NY'
    END AS state,
    CASE 
        WHEN UPPER(raw_address) LIKE '%AMFI%' OR UPPER(raw_address) LIKE '%AMPHITHEATRE%' THEN '94043'
        WHEN UPPER(raw_address) LIKE '%INFINITE LOOP%' THEN '95014'
        WHEN UPPER(raw_address) LIKE '%5TH AVE%' THEN '10118'
    END AS zip_code,
    TRUE AS is_valid
FROM bronze_user_addresses
WHERE 
    UPPER(raw_address) LIKE '%AMFI%' 
    OR UPPER(raw_address) LIKE '%AMPHITHEATRE%'
    OR UPPER(raw_address) LIKE '%INFINITE LOOP%'
    OR UPPER(raw_address) LIKE '%5TH AVE%';

-- 3. Create the Quarantine Table (Dead Letter Queue for completely un-routable addresses)
CREATE TABLE silver_quarantined_addresses AS
SELECT 
    user_id,
    TRIM(raw_address) AS raw_address,
    CAST(NULL AS STRING) AS standardized_address,
    CAST(NULL AS STRING) AS city,
    CAST(NULL AS STRING) AS state,
    CAST(NULL AS STRING) AS zip_code,
    FALSE AS is_valid
FROM bronze_user_addresses
WHERE NOT (
    UPPER(raw_address) LIKE '%AMFI%' 
    OR UPPER(raw_address) LIKE '%AMPHITHEATRE%'
    OR UPPER(raw_address) LIKE '%INFINITE LOOP%'
    OR UPPER(raw_address) LIKE '%5TH AVE%'
);


-- =========================================================
-- STEP 3: INCREMENTAL DATA OPTIMIZATION (Idempotent Upsert)
-- =========================================================

-- 1. Simulate Day 2: Ingest an address change (User 101) and a brand new registration (User 106)
INSERT INTO bronze_user_addresses VALUES
(101, '1600 Amphitheatre Pkwy, Mountain View, CA 94043'), 
(106, '350 5th Ave, New York');                       

-- 2. Execute production-grade MERGE INTO to prevent double-counting or table drops
MERGE INTO silver_valid_addresses AS target
USING (
    SELECT 
        user_id,
        TRIM(raw_address) AS raw_address,
        CASE 
            WHEN UPPER(raw_address) LIKE '%AMFI%' OR UPPER(raw_address) LIKE '%AMPHITHEATRE%' THEN '1600 Amphitheatre Pkwy'
            WHEN UPPER(raw_address) LIKE '%INFINITE LOOP%' THEN '1 Infinite Loop'
            WHEN UPPER(raw_address) LIKE '%5TH AVE%' THEN '350 5th Ave'
        END AS standardized_address,
        CASE 
            WHEN UPPER(raw_address) LIKE '%AMFI%' OR UPPER(raw_address) LIKE '%AMPHITHEATRE%' THEN 'Mountain View'
            WHEN UPPER(raw_address) LIKE '%INFINITE LOOP%' THEN 'Cupertino'
            WHEN UPPER(raw_address) LIKE '%5TH AVE%' THEN 'New York'
        END AS city,
        CASE 
            WHEN UPPER(raw_address) LIKE '%AMFI%' OR UPPER(raw_address) LIKE '%AMPHITHEATRE%' THEN 'CA'
            WHEN UPPER(raw_address) LIKE '%INFINITE LOOP%' THEN 'CA'
            WHEN UPPER(raw_address) LIKE '%5TH AVE%' THEN 'NY'
        END AS state,
        CASE 
            WHEN UPPER(raw_address) LIKE '%AMFI%' OR UPPER(raw_address) LIKE '%AMPHITHEATRE%' THEN '94043'
            WHEN UPPER(raw_address) LIKE '%INFINITE LOOP%' THEN '95014'
            WHEN UPPER(raw_address) LIKE '%5TH AVE%' THEN '10118'
        END AS zip_code,
        TRUE AS is_valid
    FROM bronze_user_addresses
    WHERE 
        UPPER(raw_address) LIKE '%AMFI%' 
        OR UPPER(raw_address) LIKE '%AMPHITHEATRE%'
        OR UPPER(raw_address) LIKE '%INFINITE LOOP%'
        OR UPPER(raw_address) LIKE '%5TH AVE%'
) AS source
ON target.user_id = source.user_id

-- If user exists, overwrite target with the latest cleaned information
WHEN MATCHED THEN
  UPDATE SET 
    target.raw_address = source.raw_address,
    target.standardized_address = source.standardized_address,
    target.city = source.city,
    target.state = source.state,
    target.zip_code = source.zip_code

-- If user does not exist, insert it as a brand new record row
WHEN NOT MATCHED THEN
  INSERT (user_id, raw_address, standardized_address, city, state, zip_code, is_valid)
  VALUES (source.user_id, source.raw_address, source.standardized_address, source.city, source.state, source.zip_code, source.is_valid);


-- ==========================================
-- STEP 4: GOLD LAYER (Data Observability Audit)
-- ==========================================

-- Calculate operational health metrics and pipeline failure percentages
SELECT 
    COUNT(*) AS total_records_processed,
    SUM(CASE WHEN is_valid = true THEN 1 ELSE 0 END) AS valid_address_count,
    SUM(CASE WHEN is_valid = false THEN 1 ELSE 0 END) AS quarantined_address_count,
    ROUND((SUM(CASE WHEN is_valid = false THEN 1 ELSE 0 END) * 100.0) / COUNT(*), 2) AS address_error_rate_percentage
FROM (
    SELECT is_valid FROM silver_valid_addresses
    UNION ALL
    SELECT is_valid FROM silver_quarantined_addresses
);

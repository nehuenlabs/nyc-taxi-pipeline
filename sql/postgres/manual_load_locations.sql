-- ============================================
-- Manual: Load Location Dimension from CSV
-- ============================================
-- Run this AFTER downloading the taxi zone lookup file
-- 
-- Prerequisites:
--   1. Download data: make download-data MONTHS=2023-01
--   2. Copy CSV to container: 
--      docker cp data/raw/taxi_zone_lookup.csv postgres:/tmp/
--   3. Connect to psql: make db-shell
--   4. Run this script: \i /tmp/manual_load_locations.sql
-- ============================================

SET search_path TO nyc_taxi_dw, public;

-- Create temporary table for loading
CREATE TEMP TABLE IF NOT EXISTS tmp_zones (
    LocationID INTEGER,
    Borough VARCHAR(50),
    Zone VARCHAR(100),
    service_zone VARCHAR(50)
);

-- Truncate in case of re-run
TRUNCATE tmp_zones;

-- Copy from CSV
\COPY tmp_zones FROM '/tmp/taxi_zone_lookup.csv' WITH (FORMAT CSV, HEADER TRUE);

-- Insert into dim_location
INSERT INTO dim_location (location_key, location_id, borough, zone_name, service_zone)
SELECT 
    LocationID AS location_key,
    LocationID AS location_id,
    Borough AS borough,
    Zone AS zone_name,
    service_zone
FROM tmp_zones
ON CONFLICT (location_key) DO UPDATE SET
    borough = EXCLUDED.borough,
    zone_name = EXCLUDED.zone_name,
    service_zone = EXCLUDED.service_zone;

-- Verify
SELECT COUNT(*) AS location_count FROM dim_location;

-- Show sample
SELECT * FROM dim_location LIMIT 5;

-- Cleanup
DROP TABLE tmp_zones;

DO $$
BEGIN
    RAISE NOTICE 'Location dimension loaded successfully!';
END $$;

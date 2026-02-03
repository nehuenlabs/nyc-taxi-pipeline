-- ============================================
-- Load Location Dimension from CSV
-- ============================================
-- THIS SCRIPT IS FOR MANUAL EXECUTION ONLY
-- It is NOT run automatically during initialization
-- 
-- Usage:
--   1. Download data: make download-data MONTHS=2023-01
--   2. Copy CSV to container: docker cp data/raw/taxi_zone_lookup.csv postgres:/tmp/
--   3. Connect to psql: make db-shell
--   4. Run: \i /docker-entrypoint-initdb.d/manual_load_locations.sql
-- 
-- OR use the Python job which loads locations automatically
-- ============================================

-- This file is intentionally empty for auto-init
-- See manual_load_locations.sql for the actual load script

DO $$
BEGIN
    RAISE NOTICE 'Location data will be loaded by the pipeline or manually';
END $$;

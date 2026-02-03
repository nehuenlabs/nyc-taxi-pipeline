-- ============================================
-- NYC Taxi Pipeline - Schema Initialization
-- ============================================
-- This script creates the main schema for the data warehouse
-- ============================================

-- Create schema if not exists
CREATE SCHEMA IF NOT EXISTS nyc_taxi_dw;

-- Set search path
SET search_path TO nyc_taxi_dw, public;

-- Grant permissions
GRANT ALL ON SCHEMA nyc_taxi_dw TO pipeline;

-- Log
DO $$
BEGIN
    RAISE NOTICE 'Schema nyc_taxi_dw created successfully';
END $$;

-- ============================================
-- Dimension: Rate Code
-- ============================================
-- Taxi fare rate codes
-- Grain: One row per rate code
-- ============================================

SET search_path TO nyc_taxi_dw, public;

DROP TABLE IF EXISTS dim_ratecode CASCADE;

CREATE TABLE dim_ratecode (
    -- Primary Key
    ratecode_key        INTEGER PRIMARY KEY,
    
    -- Natural Key
    ratecode_id         SMALLINT NOT NULL UNIQUE,
    
    -- Rate code attributes
    ratecode_name       VARCHAR(50) NOT NULL,
    ratecode_description VARCHAR(200)
);

COMMENT ON TABLE dim_ratecode IS 'Rate code dimension - fare calculation methods';
COMMENT ON COLUMN dim_ratecode.ratecode_id IS 'NYC TLC Rate Code ID';
COMMENT ON COLUMN dim_ratecode.ratecode_name IS 'Short name (Standard, JFK, Newark, etc.)';

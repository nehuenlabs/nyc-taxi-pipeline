-- ============================================
-- Dimension: Location (Taxi Zones)
-- ============================================
-- NYC Taxi zones dimension
-- Grain: One row per taxi zone
-- ============================================

SET search_path TO nyc_taxi_dw, public;

DROP TABLE IF EXISTS dim_location CASCADE;

CREATE TABLE dim_location (
    -- Primary Key
    location_key    INTEGER PRIMARY KEY,
    
    -- Natural Key
    location_id     INTEGER NOT NULL UNIQUE,
    
    -- Location attributes
    borough         VARCHAR(50),
    zone_name       VARCHAR(100),
    service_zone    VARCHAR(50)
);

-- Indexes for common queries
CREATE INDEX idx_dim_location_borough ON dim_location(borough);
CREATE INDEX idx_dim_location_service_zone ON dim_location(service_zone);

COMMENT ON TABLE dim_location IS 'NYC Taxi Zone dimension - one row per zone';
COMMENT ON COLUMN dim_location.location_key IS 'Surrogate key (same as location_id for simplicity)';
COMMENT ON COLUMN dim_location.location_id IS 'NYC TLC Location ID';
COMMENT ON COLUMN dim_location.borough IS 'NYC Borough (Manhattan, Brooklyn, Queens, Bronx, Staten Island, EWR)';
COMMENT ON COLUMN dim_location.service_zone IS 'Service zone classification (Yellow Zone, Boro Zone, etc.)';

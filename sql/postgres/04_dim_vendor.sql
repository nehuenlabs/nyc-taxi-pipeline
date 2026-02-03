-- ============================================
-- Dimension: Vendor
-- ============================================
-- Taxi technology vendors
-- Grain: One row per vendor
-- ============================================

SET search_path TO nyc_taxi_dw, public;

DROP TABLE IF EXISTS dim_vendor CASCADE;

CREATE TABLE dim_vendor (
    -- Primary Key
    vendor_key      INTEGER PRIMARY KEY,
    
    -- Natural Key
    vendor_id       SMALLINT NOT NULL UNIQUE,
    
    -- Vendor attributes
    vendor_name     VARCHAR(100) NOT NULL
);

COMMENT ON TABLE dim_vendor IS 'Taxi vendor dimension - technology providers';
COMMENT ON COLUMN dim_vendor.vendor_id IS 'NYC TLC Vendor ID (1=CMT, 2=VeriFone)';

-- ============================================
-- Dimension: Payment Type
-- ============================================
-- Payment methods
-- Grain: One row per payment type
-- ============================================

SET search_path TO nyc_taxi_dw, public;

DROP TABLE IF EXISTS dim_payment_type CASCADE;

CREATE TABLE dim_payment_type (
    -- Primary Key
    payment_type_key    INTEGER PRIMARY KEY,
    
    -- Natural Key
    payment_type_id     SMALLINT NOT NULL UNIQUE,
    
    -- Payment type attributes
    payment_type_name   VARCHAR(50) NOT NULL
);

COMMENT ON TABLE dim_payment_type IS 'Payment type dimension - payment methods';
COMMENT ON COLUMN dim_payment_type.payment_type_id IS 'NYC TLC Payment Type ID';

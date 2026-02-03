-- ============================================
-- Dimension: Date
-- ============================================
-- Calendar dimension for date analysis
-- Grain: One row per calendar day
-- ============================================

SET search_path TO nyc_taxi_dw, public;

DROP TABLE IF EXISTS dim_date CASCADE;

CREATE TABLE dim_date (
    -- Primary Key (format: YYYYMMDD)
    date_key        INTEGER PRIMARY KEY,
    
    -- Date attributes
    full_date       DATE NOT NULL UNIQUE,
    year            SMALLINT NOT NULL,
    month           SMALLINT NOT NULL,
    day             SMALLINT NOT NULL,
    
    -- Week attributes
    day_of_week     SMALLINT NOT NULL,  -- 1=Sunday, 7=Saturday (PostgreSQL default)
    day_name        VARCHAR(10) NOT NULL,
    week_of_year    SMALLINT NOT NULL,
    
    -- Flags
    is_weekend      BOOLEAN NOT NULL,
    is_holiday      BOOLEAN NOT NULL DEFAULT FALSE,
    
    -- Quarter
    quarter         SMALLINT NOT NULL,
    
    -- Constraints
    CONSTRAINT chk_month CHECK (month BETWEEN 1 AND 12),
    CONSTRAINT chk_day CHECK (day BETWEEN 1 AND 31),
    CONSTRAINT chk_day_of_week CHECK (day_of_week BETWEEN 1 AND 7),
    CONSTRAINT chk_quarter CHECK (quarter BETWEEN 1 AND 4)
);

-- Index for common queries
CREATE INDEX idx_dim_date_full_date ON dim_date(full_date);
CREATE INDEX idx_dim_date_year_month ON dim_date(year, month);

COMMENT ON TABLE dim_date IS 'Calendar dimension - one row per day';
COMMENT ON COLUMN dim_date.date_key IS 'Surrogate key in YYYYMMDD format';
COMMENT ON COLUMN dim_date.day_of_week IS '1=Sunday, 7=Saturday';

-- ============================================
-- Dimension: Time
-- ============================================
-- Time of day dimension for temporal analysis
-- Grain: One row per minute (1440 rows total)
-- ============================================

SET search_path TO nyc_taxi_dw, public;

DROP TABLE IF EXISTS dim_time CASCADE;

CREATE TABLE dim_time (
    -- Primary Key (format: HHMM)
    time_key        INTEGER PRIMARY KEY,
    
    -- Time attributes
    hour            SMALLINT NOT NULL,
    minute          SMALLINT NOT NULL,
    
    -- Time of day classification
    time_of_day     VARCHAR(20) NOT NULL,  -- Morning, Afternoon, Evening, Night
    
    -- Flags
    is_rush_hour    BOOLEAN NOT NULL,
    is_night        BOOLEAN NOT NULL,
    
    -- Constraints
    CONSTRAINT chk_hour CHECK (hour BETWEEN 0 AND 23),
    CONSTRAINT chk_minute CHECK (minute BETWEEN 0 AND 59),
    CONSTRAINT chk_time_of_day CHECK (time_of_day IN ('Morning', 'Afternoon', 'Evening', 'Night'))
);

-- Index for time of day queries
CREATE INDEX idx_dim_time_hour ON dim_time(hour);
CREATE INDEX idx_dim_time_time_of_day ON dim_time(time_of_day);

COMMENT ON TABLE dim_time IS 'Time dimension - one row per minute of the day';
COMMENT ON COLUMN dim_time.time_key IS 'Surrogate key in HHMM format (e.g., 1430 = 2:30 PM)';
COMMENT ON COLUMN dim_time.time_of_day IS 'Morning (6-12), Afternoon (12-17), Evening (17-21), Night (21-6)';
COMMENT ON COLUMN dim_time.is_rush_hour IS 'True for 7-10 AM and 4-7 PM';

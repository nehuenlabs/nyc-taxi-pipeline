-- ============================================
-- Fact Table: Trips
-- ============================================
-- Main fact table for taxi trips
-- Grain: One row per individual taxi trip
-- ============================================

SET search_path TO nyc_taxi_dw, public;

DROP TABLE IF EXISTS fact_trips CASCADE;

CREATE TABLE fact_trips (
    -- Surrogate Key
    trip_key                BIGSERIAL PRIMARY KEY,
    
    -- ============================================
    -- Foreign Keys (Dimensions)
    -- ============================================
    date_key                INTEGER NOT NULL,
    pickup_time_key         INTEGER NOT NULL,
    dropoff_time_key        INTEGER NOT NULL,
    pickup_location_key     INTEGER NOT NULL,
    dropoff_location_key    INTEGER NOT NULL,
    vendor_key              INTEGER NOT NULL,
    ratecode_key            INTEGER NOT NULL,
    payment_type_key        INTEGER NOT NULL,
    
    -- ============================================
    -- Measures (Facts)
    -- ============================================
    -- Trip metrics
    passenger_count         SMALLINT,
    trip_distance_miles     DECIMAL(10,2),
    trip_duration_minutes   DECIMAL(10,2),
    
    -- Fare components
    fare_amount             DECIMAL(10,2),
    extra_amount            DECIMAL(10,2),
    mta_tax                 DECIMAL(10,2),
    tip_amount              DECIMAL(10,2),
    tolls_amount            DECIMAL(10,2),
    improvement_surcharge   DECIMAL(10,2),
    congestion_surcharge    DECIMAL(10,2),
    airport_fee             DECIMAL(10,2),
    total_amount            DECIMAL(10,2),
    
    -- ============================================
    -- Audit Columns
    -- ============================================
    _source_file            VARCHAR(255),
    _ingestion_ts           TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    -- ============================================
    -- Foreign Key Constraints
    -- ============================================
    CONSTRAINT fk_fact_trips_date 
        FOREIGN KEY (date_key) REFERENCES dim_date(date_key),
    CONSTRAINT fk_fact_trips_pickup_time 
        FOREIGN KEY (pickup_time_key) REFERENCES dim_time(time_key),
    CONSTRAINT fk_fact_trips_dropoff_time 
        FOREIGN KEY (dropoff_time_key) REFERENCES dim_time(time_key),
    CONSTRAINT fk_fact_trips_pickup_location 
        FOREIGN KEY (pickup_location_key) REFERENCES dim_location(location_key),
    CONSTRAINT fk_fact_trips_dropoff_location 
        FOREIGN KEY (dropoff_location_key) REFERENCES dim_location(location_key),
    CONSTRAINT fk_fact_trips_vendor 
        FOREIGN KEY (vendor_key) REFERENCES dim_vendor(vendor_key),
    CONSTRAINT fk_fact_trips_ratecode 
        FOREIGN KEY (ratecode_key) REFERENCES dim_ratecode(ratecode_key),
    CONSTRAINT fk_fact_trips_payment_type 
        FOREIGN KEY (payment_type_key) REFERENCES dim_payment_type(payment_type_key)
);

-- ============================================
-- Comments
-- ============================================
COMMENT ON TABLE fact_trips IS 'Fact table for NYC Yellow Taxi trips - one row per trip';
COMMENT ON COLUMN fact_trips.trip_key IS 'Surrogate key (auto-generated)';
COMMENT ON COLUMN fact_trips.date_key IS 'FK to dim_date based on pickup date';
COMMENT ON COLUMN fact_trips.trip_duration_minutes IS 'Calculated: dropoff_time - pickup_time in minutes';
COMMENT ON COLUMN fact_trips._source_file IS 'Source parquet file name for lineage';
COMMENT ON COLUMN fact_trips._ingestion_ts IS 'Timestamp when record was loaded';

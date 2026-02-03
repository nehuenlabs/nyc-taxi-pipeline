-- ============================================
-- Indexes for fact_trips
-- ============================================
-- Optimized for common query patterns
-- ============================================

SET search_path TO nyc_taxi_dw, public;

-- ============================================
-- Single Column Indexes (Foreign Keys)
-- ============================================

-- Date dimension (most common filter)
CREATE INDEX IF NOT EXISTS idx_fact_trips_date_key 
    ON fact_trips(date_key);

-- Location dimensions (for geographic analysis)
CREATE INDEX IF NOT EXISTS idx_fact_trips_pickup_location 
    ON fact_trips(pickup_location_key);

CREATE INDEX IF NOT EXISTS idx_fact_trips_dropoff_location 
    ON fact_trips(dropoff_location_key);

-- Vendor (for vendor performance analysis)
CREATE INDEX IF NOT EXISTS idx_fact_trips_vendor 
    ON fact_trips(vendor_key);

-- Payment type (for payment analysis)
CREATE INDEX IF NOT EXISTS idx_fact_trips_payment_type 
    ON fact_trips(payment_type_key);

-- ============================================
-- Composite Indexes (Common Query Patterns)
-- ============================================

-- Date + Pickup Location (most common combination)
CREATE INDEX IF NOT EXISTS idx_fact_trips_date_pickup 
    ON fact_trips(date_key, pickup_location_key);

-- Date + Dropoff Location
CREATE INDEX IF NOT EXISTS idx_fact_trips_date_dropoff 
    ON fact_trips(date_key, dropoff_location_key);

-- Date + Vendor (for vendor daily analysis)
CREATE INDEX IF NOT EXISTS idx_fact_trips_date_vendor 
    ON fact_trips(date_key, vendor_key);

-- Pickup + Dropoff (for route analysis)
CREATE INDEX IF NOT EXISTS idx_fact_trips_route 
    ON fact_trips(pickup_location_key, dropoff_location_key);

-- ============================================
-- Partial Indexes (Filtered Queries)
-- ============================================

-- High value trips (total > $50)
CREATE INDEX IF NOT EXISTS idx_fact_trips_high_value 
    ON fact_trips(date_key, total_amount) 
    WHERE total_amount > 50;

-- Airport trips (common analysis)
-- Note: Airport location IDs are 1 (Newark), 132 (JFK), 138 (LaGuardia)
CREATE INDEX IF NOT EXISTS idx_fact_trips_airport_pickup 
    ON fact_trips(date_key, pickup_location_key) 
    WHERE pickup_location_key IN (1, 132, 138);

CREATE INDEX IF NOT EXISTS idx_fact_trips_airport_dropoff 
    ON fact_trips(date_key, dropoff_location_key) 
    WHERE dropoff_location_key IN (1, 132, 138);

-- ============================================
-- Log completion
-- ============================================
DO $$
BEGIN
    RAISE NOTICE 'Indexes created successfully for fact_trips';
END $$;

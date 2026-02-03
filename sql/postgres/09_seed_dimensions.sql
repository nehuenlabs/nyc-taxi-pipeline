-- ============================================
-- Seed Data for Static Dimensions
-- ============================================
-- Populates reference data that doesn't change
-- ============================================

SET search_path TO nyc_taxi_dw, public;

-- ============================================
-- Dim Vendor
-- ============================================
INSERT INTO dim_vendor (vendor_key, vendor_id, vendor_name)
VALUES 
    (1, 1, 'Creative Mobile Technologies, LLC'),
    (2, 2, 'VeriFone Inc.')
ON CONFLICT (vendor_key) DO UPDATE SET
    vendor_name = EXCLUDED.vendor_name;

-- ============================================
-- Dim Rate Code
-- ============================================
INSERT INTO dim_ratecode (ratecode_key, ratecode_id, ratecode_name, ratecode_description)
VALUES 
    (1, 1, 'Standard rate', 'Standard metered fare'),
    (2, 2, 'JFK', 'JFK Airport flat fare ($52 + tolls)'),
    (3, 3, 'Newark', 'Newark Airport negotiated fare'),
    (4, 4, 'Nassau/Westchester', 'Nassau or Westchester fare'),
    (5, 5, 'Negotiated fare', 'Negotiated flat fare'),
    (6, 6, 'Group ride', 'Group ride fare'),
    (99, 99, 'Unknown', 'Unknown or invalid rate code')
ON CONFLICT (ratecode_key) DO UPDATE SET
    ratecode_name = EXCLUDED.ratecode_name,
    ratecode_description = EXCLUDED.ratecode_description;

-- ============================================
-- Dim Payment Type
-- ============================================
INSERT INTO dim_payment_type (payment_type_key, payment_type_id, payment_type_name)
VALUES 
    (1, 1, 'Credit card'),
    (2, 2, 'Cash'),
    (3, 3, 'No charge'),
    (4, 4, 'Dispute'),
    (5, 5, 'Unknown'),
    (6, 6, 'Voided trip')
ON CONFLICT (payment_type_key) DO UPDATE SET
    payment_type_name = EXCLUDED.payment_type_name;

-- ============================================
-- Dim Time (1440 rows - one per minute)
-- ============================================
INSERT INTO dim_time (time_key, hour, minute, time_of_day, is_rush_hour, is_night)
SELECT 
    (h * 100 + m) AS time_key,
    h AS hour,
    m AS minute,
    CASE 
        WHEN h >= 6 AND h < 12 THEN 'Morning'
        WHEN h >= 12 AND h < 17 THEN 'Afternoon'
        WHEN h >= 17 AND h < 21 THEN 'Evening'
        ELSE 'Night'
    END AS time_of_day,
    CASE 
        WHEN (h >= 7 AND h < 10) OR (h >= 16 AND h < 19) THEN TRUE
        ELSE FALSE
    END AS is_rush_hour,
    CASE 
        WHEN h < 6 OR h >= 21 THEN TRUE
        ELSE FALSE
    END AS is_night
FROM 
    generate_series(0, 23) AS h,
    generate_series(0, 59) AS m
ON CONFLICT (time_key) DO NOTHING;

-- ============================================
-- Dim Date (Generate 5 years: 2020-2025)
-- ============================================
INSERT INTO dim_date (
    date_key, 
    full_date, 
    year, 
    month, 
    day, 
    day_of_week, 
    day_name, 
    week_of_year, 
    is_weekend, 
    is_holiday, 
    quarter
)
SELECT 
    TO_CHAR(d, 'YYYYMMDD')::INTEGER AS date_key,
    d AS full_date,
    EXTRACT(YEAR FROM d)::SMALLINT AS year,
    EXTRACT(MONTH FROM d)::SMALLINT AS month,
    EXTRACT(DAY FROM d)::SMALLINT AS day,
    EXTRACT(DOW FROM d)::SMALLINT + 1 AS day_of_week,  -- 1=Sunday, 7=Saturday
    TO_CHAR(d, 'Day') AS day_name,
    EXTRACT(WEEK FROM d)::SMALLINT AS week_of_year,
    CASE WHEN EXTRACT(DOW FROM d) IN (0, 6) THEN TRUE ELSE FALSE END AS is_weekend,
    FALSE AS is_holiday,  -- Can be enhanced with holiday calendar
    EXTRACT(QUARTER FROM d)::SMALLINT AS quarter
FROM 
    generate_series('2020-01-01'::DATE, '2025-12-31'::DATE, '1 day'::INTERVAL) AS d
ON CONFLICT (date_key) DO NOTHING;

-- ============================================
-- Dim Location (Placeholder - will be loaded from CSV)
-- ============================================
-- Insert placeholder for unknown location
INSERT INTO dim_location (location_key, location_id, borough, zone_name, service_zone)
VALUES 
    (264, 264, 'Unknown', 'Unknown', 'Unknown'),
    (265, 265, 'Unknown', 'Unknown', 'Unknown')
ON CONFLICT (location_key) DO NOTHING;

-- ============================================
-- Verification Queries
-- ============================================
DO $$
DECLARE
    v_vendor_count INTEGER;
    v_ratecode_count INTEGER;
    v_payment_count INTEGER;
    v_time_count INTEGER;
    v_date_count INTEGER;
BEGIN
    SELECT COUNT(*) INTO v_vendor_count FROM dim_vendor;
    SELECT COUNT(*) INTO v_ratecode_count FROM dim_ratecode;
    SELECT COUNT(*) INTO v_payment_count FROM dim_payment_type;
    SELECT COUNT(*) INTO v_time_count FROM dim_time;
    SELECT COUNT(*) INTO v_date_count FROM dim_date;
    
    RAISE NOTICE 'Seed data loaded successfully:';
    RAISE NOTICE '  - dim_vendor: % rows', v_vendor_count;
    RAISE NOTICE '  - dim_ratecode: % rows', v_ratecode_count;
    RAISE NOTICE '  - dim_payment_type: % rows', v_payment_count;
    RAISE NOTICE '  - dim_time: % rows', v_time_count;
    RAISE NOTICE '  - dim_date: % rows', v_date_count;
END $$;

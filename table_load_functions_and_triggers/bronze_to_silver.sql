CREATE OR REPLACE FUNCTION silver_load()                 --function transforms data from bronze to silver
RETURNS TRIGGER AS $$
DECLARE
    m_last_load_time TIMESTAMP;
BEGIN
    SELECT last_load_time
    INTO m_last_load_time
    FROM metadata.last_load_period
    WHERE schema_type = 'bronze';

    INSERT INTO silver.silver_taxi
    SELECT DISTINCT
        vendorID,
        CASE vendorID
            WHEN 1 THEN 'Creative Mobile Technologies, LLC'
            WHEN 2 THEN 'Curb Mobility, LLC'
            WHEN 6 THEN 'Myle Technologies Inc'
            WHEN 7 THEN 'Helix'
            ELSE 'Unknown'
        END AS vendor_name,
        CASE RatecodeID
            WHEN 1 THEN 'Standard rate'
            WHEN 2 THEN 'JFK'
            WHEN 3 THEN 'Newark'
            WHEN 4 THEN 'Nassau or Westchester'
            WHEN 5 THEN 'Negotiated fare'
            WHEN 6 THEN 'Group ride'
            ELSE 'Null/unknown'
        END AS rate_description,
        CASE payment_type
            WHEN 0 THEN 'Flex Fare trip'
            WHEN 1 THEN 'Credit card'
            WHEN 2 THEN 'Cash'
            WHEN 3 THEN 'No charge'
            WHEN 4 THEN 'Dispute'
            WHEN 5 THEN 'Unknown'
            WHEN 6 THEN 'Voided trip'
        END AS payment_description,
        tpep_pickup_datetime,
        tpep_dropoff_datetime,
        ROUND(EXTRACT(EPOCH FROM (tpep_dropoff_datetime - tpep_pickup_datetime)) / 60, 2) AS tripduration,
        passenger_count,
        trip_distance,
        store_and_fwd_flag,
        PULocationID,
        DOLocationID,
        ABS(fare_amount),
        ABS(extra),
        ABS(mta_tax),
        ABS(tip_amount),
        ABS(tolls_amount),
        ABS(improvement_surcharge),
        ABS(fare_amount + extra + mta_tax + tip_amount + tolls_amount + improvement_surcharge 
            + congestion_surcharge + airport_fee),
        ABS(congestion_surcharge),
        ABS(airport_fee)
    FROM bronze.bronze_taxi
    WHERE (m_last_load_time IS NULL OR time_uploaded > m_last_load_time)
      AND trip_distance > 0
	  AND tpep_dropoff_datetime > '2023-12-31' 
	  AND tpep_pickup_datetime > '2023-12-31';
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

--copies data from bronze into silver
CREATE TRIGGER trigger_silver_load
AFTER INSERT ON bronze.bronze_taxi
FOR EACH STATEMENT
EXECUTE FUNCTION silver_load();
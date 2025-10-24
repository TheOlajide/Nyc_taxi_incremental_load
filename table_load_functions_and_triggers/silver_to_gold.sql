CREATE OR REPLACE FUNCTION gold_load()                  --function transforms data from silver to gold
RETURNS TRIGGER AS $$
BEGIN
    WITH new_data AS (
        SELECT 
            DATE(tpep_pickup_datetime) AS trip_date,
            COUNT(*) AS total_trips,
            SUM(passenger_count) AS total_passengers,
            SUM(trip_distance) AS total_distance_miles,
            SUM(total_amount) AS total_revenue,
            SUM(tip_amount) AS total_tips
        FROM new_silver_rows
        GROUP BY DATE(tpep_pickup_datetime)
    )
    INSERT INTO gold.daily_summary (
        trip_date, total_trips, total_passengers, total_distance_miles,
        total_revenue, total_tips, avg_fare, avg_trip_distance
    )
    SELECT
        trip_date, total_trips, total_passengers, total_distance_miles,
        total_revenue, total_tips,
        total_revenue / total_trips,
        total_distance_miles / total_trips
    FROM new_data
    ON CONFLICT (trip_date)
    DO UPDATE SET
        total_trips = gold.daily_summary.total_trips + EXCLUDED.total_trips,
        total_passengers = gold.daily_summary.total_passengers + EXCLUDED.total_passengers,
        total_distance_miles = gold.daily_summary.total_distance_miles + EXCLUDED.total_distance_miles,
        total_revenue = gold.daily_summary.total_revenue + EXCLUDED.total_revenue,
        total_tips = gold.daily_summary.total_tips + EXCLUDED.total_tips,
        avg_fare = (gold.daily_summary.total_revenue + EXCLUDED.total_revenue) / 
                   (gold.daily_summary.total_trips + EXCLUDED.total_trips),
        avg_trip_distance = (gold.daily_summary.total_distance_miles + EXCLUDED.total_distance_miles) / 
                            (gold.daily_summary.total_trips + EXCLUDED.total_trips);

    WITH new_data AS (
        SELECT 
            DATE_TRUNC('month', tpep_pickup_datetime)::DATE AS month,
            COUNT(*) AS total_trips,
            SUM(total_amount) AS total_revenue,
            SUM(tip_amount) AS total_tips,
            SUM(trip_distance) AS total_distance
        FROM new_silver_rows
        GROUP BY month
    )
    INSERT INTO gold.monthly_summary (month, total_trips, total_revenue, total_tips, total_distance)
    SELECT month, total_trips, total_revenue, total_tips, total_distance FROM new_data
    ON CONFLICT (month)
    DO UPDATE SET
        total_trips = gold.monthly_summary.total_trips + EXCLUDED.total_trips,
        total_revenue = gold.monthly_summary.total_revenue + EXCLUDED.total_revenue,
        total_tips = gold.monthly_summary.total_tips + EXCLUDED.total_tips,
        total_distance = gold.monthly_summary.total_distance + EXCLUDED.total_distance;

    WITH new_data AS (
        SELECT 
            payment_description,
            COUNT(*) AS trip_count,
            SUM(total_amount) AS total_revenue,
            SUM(tip_amount) AS total_tips
        FROM new_silver_rows
        GROUP BY payment_description
    )
    INSERT INTO gold.payment_summary (payment_description, trip_count, total_revenue, total_tips, avg_tip_percent)
    SELECT 
        payment_description, trip_count, total_revenue, total_tips,
        ROUND((total_tips / NULLIF(total_revenue, 0))::numeric * 100, 2)
    FROM new_data
    ON CONFLICT (payment_description)
    DO UPDATE SET
        trip_count = gold.payment_summary.trip_count + EXCLUDED.trip_count,
        total_revenue = gold.payment_summary.total_revenue + EXCLUDED.total_revenue,
        total_tips = gold.payment_summary.total_tips + EXCLUDED.total_tips,
        avg_tip_percent = ROUND(
            ((gold.payment_summary.total_tips + EXCLUDED.total_tips) /
             NULLIF(gold.payment_summary.total_revenue + EXCLUDED.total_revenue, 0))::numeric * 100, 2
        );

    WITH new_data AS (
        SELECT 
            vendor_name,
            COUNT(*) AS total_trips,
            SUM(total_amount) AS total_revenue,
            SUM(trip_distance) AS total_distance
        FROM new_silver_rows
        GROUP BY vendor_name
    )
    INSERT INTO gold.vendor_summary (vendor_name, total_trips, total_revenue, total_distance, avg_trip_distance, avg_fare)
    SELECT
        vendor_name, total_trips, total_revenue, total_distance,
        total_distance / total_trips,
        total_revenue / total_trips
    FROM new_data
    ON CONFLICT (vendor_name)
    DO UPDATE SET
        total_trips = gold.vendor_summary.total_trips + EXCLUDED.total_trips,
        total_revenue = gold.vendor_summary.total_revenue + EXCLUDED.total_revenue,
        total_distance = gold.vendor_summary.total_distance + EXCLUDED.total_distance,
        avg_trip_distance = (gold.vendor_summary.total_distance + EXCLUDED.total_distance) / 
                            (gold.vendor_summary.total_trips + EXCLUDED.total_trips),
        avg_fare = (gold.vendor_summary.total_revenue + EXCLUDED.total_revenue) /
                   (gold.vendor_summary.total_trips + EXCLUDED.total_trips);

    WITH new_data AS (
        SELECT 
            PULocationID,
            COUNT(*) AS pickups,
            SUM(total_amount) AS revenue_from_pickups,
            SUM(tip_amount) AS total_tips
        FROM new_silver_rows
        GROUP BY PULocationID
    )
    INSERT INTO gold.zone_summary (PULocationID, pickups, revenue_from_pickups, total_tips)
    SELECT PULocationID, pickups, revenue_from_pickups, total_tips FROM new_data
    ON CONFLICT (PULocationID)
    DO UPDATE SET
        pickups = gold.zone_summary.pickups + EXCLUDED.pickups,
        revenue_from_pickups = gold.zone_summary.revenue_from_pickups + EXCLUDED.revenue_from_pickups,
        total_tips = gold.zone_summary.total_tips + EXCLUDED.total_tips;
     
     --update metadata table for gold
    UPDATE metadata.last_load_period
    SET last_load_time = CURRENT_TIMESTAMP
    WHERE schema_type = 'gold';

    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

-- --copies data from silver into gold
CREATE TRIGGER trigger_gold_load                   
AFTER INSERT ON silver.taxi
REFERENCING NEW TABLE AS new_silver_rows
FOR EACH STATEMENT
EXECUTE FUNCTION gold_load();
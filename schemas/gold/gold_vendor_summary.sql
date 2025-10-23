CREATE TABLE gold.vendor_summary (
    vendor_name VARCHAR PRIMARY KEY,
    total_trips BIGINT,
    total_revenue FLOAT,
    total_distance FLOAT,
    avg_trip_distance FLOAT,
    avg_fare FLOAT
);
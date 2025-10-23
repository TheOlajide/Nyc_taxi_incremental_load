

CREATE TABLE gold.daily_summary (
    trip_date DATE PRIMARY KEY,
    total_trips BIGINT,
    total_passengers FLOAT,
    total_distance_miles FLOAT,
    total_revenue FLOAT,
    total_tips FLOAT,
    avg_fare FLOAT,
    avg_trip_distance FLOAT
);


CREATE TABLE gold.monthly_summary (
    month DATE PRIMARY KEY,
    total_trips BIGINT,
    total_revenue FLOAT,
    total_tips FLOAT,
    total_distance FLOAT
);

CREATE TABLE gold.payment_summary (
    payment_description VARCHAR PRIMARY KEY,
    trip_count BIGINT,
    total_revenue FLOAT,
    total_tips FLOAT,
    avg_tip_percent NUMERIC
);
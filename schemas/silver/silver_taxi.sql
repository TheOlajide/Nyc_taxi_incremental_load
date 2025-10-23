

CREATE TABLE silver.taxi (
    vendorID INT,
    vendor_name VARCHAR,
    rate_description VARCHAR,
    payment_description VARCHAR,
    tpep_pickup_datetime TIMESTAMP,
    tpep_dropoff_datetime TIMESTAMP,
    tripduration FLOAT,
    passenger_count FLOAT,
    trip_distance FLOAT,
    store_and_fwd_flag VARCHAR(5),
    PULocationID INT,
    DOLocationID INT,
    fare_amount FLOAT,
    extra FLOAT,
    mta_tax FLOAT,
    tip_amount FLOAT,
    tolls_amount FLOAT,
    improvement_surcharge FLOAT,
    total_amount FLOAT,
    congestion_surcharge FLOAT,
    airport_fee FLOAT
);

CREATE TABLE bronze.taxi (
    VendorID INT,
    tpep_pickup_datetime TIMESTAMP,
    tpep_dropoff_datetime TIMESTAMP,
    passenger_count FLOAT,
    trip_distance FLOAT,
    RatecodeID FLOAT,
    store_and_fwd_flag VARCHAR(5),
    PULocationID INT,
    DOLocationID INT,
    payment_type INT,
    fare_amount FLOAT, 
    extra FLOAT,
    mta_tax FLOAT,
    tip_amount FLOAT,
    tolls_amount FLOAT,
    improvement_surcharge FLOAT,
    total_amount FLOAT,
    congestion_surcharge FLOAT,
    Airport_fee FLOAT,
    time_uploaded TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT unique_trip UNIQUE (VendorID, tpep_pickup_datetime, tpep_dropoff_datetime, PULocationID, DOLocationID)
);
CREATE TABLE metadata.last_load_period(
    last_load_time TIMESTAMP,
    schema_type VARCHAR PRIMARY KEY
);

INSERT INTO metadata.last_load_period(schema_type, last_load_time)
VALUES
    ('bronze', NULL),
    ('silver', NULL),
    ('gold', NULL);
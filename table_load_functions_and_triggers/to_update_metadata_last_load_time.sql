-- this function updates last_load_time for each schema_type whenever a trigger hits it

CREATE OR REPLACE FUNCTION metadata.update_last_load()
RETURNS TRIGGER AS $$
BEGIN
    UPDATE metadata.last_load_period
    SET last_load_time = CURRENT_TIMESTAMP
    WHERE schema_type = TG_TABLE_SCHEMA;
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;
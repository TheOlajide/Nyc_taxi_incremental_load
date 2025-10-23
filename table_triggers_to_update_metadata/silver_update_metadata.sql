--trigger metadata function to update last_load_time for "silver" schema_type
--(ascribed to silver_taxi table) in metadata table
CREATE TRIGGER trigger_update_metadata_silver
AFTER INSERT ON silver.silver_taxi
FOR EACH STATEMENT
EXECUTE FUNCTION metadata.update_last_load();
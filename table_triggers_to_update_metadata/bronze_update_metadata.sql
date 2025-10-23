----trigger metadata function to update last_load_time for "bronze" schema_type
----(ascribed to bronze_taxi table) in metadata table

CREATE TRIGGER trigger_update_metadata_bronze
AFTER INSERT ON bronze.bronze_taxi
FOR EACH STATEMENT
EXECUTE FUNCTION metadata.update_last_load();
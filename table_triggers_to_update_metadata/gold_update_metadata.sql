

----trigger metadata function to update last_load_time for "gold" schema_type
----(ascribed to gold_taxi table) in metadata table

CREATE TRIGGER trigger_update_metadata_gold
AFTER INSERT ON gold.gold_taxi
FOR EACH STATEMENT
EXECUTE FUNCTION metadata.update_last_load();
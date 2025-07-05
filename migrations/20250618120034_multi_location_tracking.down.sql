-- Rollback multi-location storage tracking

-- Drop functions
DROP FUNCTION IF EXISTS get_storage_health_summary();
DROP FUNCTION IF EXISTS cleanup_old_health_alerts();
DROP FUNCTION IF EXISTS update_location_status_timestamp();

-- Drop tables (in reverse dependency order)
DROP TABLE IF EXISTS sinex_schemas.storage_metrics;
DROP TABLE IF EXISTS sinex_schemas.health_alerts;
DROP TABLE IF EXISTS sinex_schemas.sync_errors;
DROP TABLE IF EXISTS sinex_schemas.location_status;
DROP TABLE IF EXISTS sinex_schemas.storage_locations;
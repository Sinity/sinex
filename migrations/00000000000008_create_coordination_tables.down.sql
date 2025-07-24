-- Rollback coordination tables
-- Migration: 00000000000008_create_coordination_tables.down.sql

-- Drop functions
DROP FUNCTION IF EXISTS core.cleanup_processed_signals();
DROP FUNCTION IF EXISTS core.cleanup_old_satellite_instances();

-- Drop indexes
DROP INDEX IF EXISTS idx_service_leadership_heartbeat;
DROP INDEX IF EXISTS idx_satellite_signals_target_unprocessed;
DROP INDEX IF EXISTS idx_satellite_instances_service_version;

-- Drop tables (order matters due to foreign keys)
DROP TABLE IF EXISTS core.service_leadership;
DROP TABLE IF EXISTS core.satellite_signals;
DROP TABLE IF EXISTS core.satellite_instances;
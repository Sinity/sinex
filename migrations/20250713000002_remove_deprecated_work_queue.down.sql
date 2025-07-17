-- This is a destructive migration that removes deprecated components
-- Rollback is not supported as the work queue system has been replaced
-- by the satellite architecture with Redis Streams

-- The rollback would require recreating the entire work_queue schema
-- which is complex and not recommended. Instead, users should:
-- 1. Restore from backup if needed
-- 2. Use the old codebase version if rollback is required

SELECT 'WARNING: This migration removes deprecated work_queue components. Rollback requires backup restoration.' AS notice;
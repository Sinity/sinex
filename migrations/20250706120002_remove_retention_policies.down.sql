-- Migration: Re-add retention policies (not recommended for personal exocortex)
-- Down Migration

-- This migration intentionally does NOT re-add retention policies
-- Personal exocortex systems should retain all data forever for complete digital memory

RAISE NOTICE 'Down migration for retention policy removal - no action taken';
RAISE NOTICE 'Personal exocortex systems should keep all data forever';
RAISE NOTICE 'If you really need retention policies, add them manually with:';
RAISE NOTICE 'SELECT add_retention_policy(''table_name'', INTERVAL ''duration'');';
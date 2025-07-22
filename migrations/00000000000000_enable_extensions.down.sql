-- Note: Dropping extensions is generally not recommended as they may be used by other databases
-- This is provided for completeness but should be used with caution

-- Drop extensions in reverse order of dependencies
-- Note: CASCADE will drop all dependent objects!
-- DROP EXTENSION IF EXISTS vector CASCADE;
-- DROP EXTENSION IF EXISTS pg_jsonschema CASCADE;
-- DROP EXTENSION IF EXISTS timescaledb CASCADE;
-- DROP EXTENSION IF EXISTS ulid CASCADE;

-- Safer approach: just log that we would drop them
DO $$
BEGIN
    RAISE NOTICE 'Down migration for extensions - extensions are not dropped to prevent data loss';
    RAISE NOTICE 'To manually drop extensions, use DROP EXTENSION <name> CASCADE';
END;
$$;
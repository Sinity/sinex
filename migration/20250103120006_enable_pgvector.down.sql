-- Down migration for 20250103120006_enable_pgvector

-- Note: We don't drop the extension as tables might depend on it
-- DROP EXTENSION IF EXISTS vector CASCADE;
-- Migration rollback: Restore artifact_contents from revisions
-- This migration reverses the rename from core.revisions back to core.artifact_contents

BEGIN;

-- Step 1: Rename the table back
ALTER TABLE core.revisions RENAME TO artifact_contents;

-- Step 2: Rename indexes back to original names (5 total: 3 manual + 2 automatic)
ALTER INDEX core.idx_revisions_artifact_id RENAME TO idx_artifact_contents_artifact_id;
ALTER INDEX core.idx_revisions_content_search RENAME TO idx_artifact_contents_content_search;
ALTER INDEX core.idx_revisions_extracted_search RENAME TO idx_artifact_contents_extracted_search;
ALTER INDEX core.revisions_pkey RENAME TO artifact_contents_pkey;
ALTER INDEX core.revisions_artifact_id_version_key RENAME TO artifact_contents_artifact_id_version_key;

-- Step 3: Restore original table comment
COMMENT ON TABLE core.artifact_contents IS 'Versioned content storage for artifacts';

COMMIT;
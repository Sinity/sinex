-- Migration: Rename artifact_contents table to revisions
-- This migration renames core.artifact_contents to core.revisions to reflect
-- its purpose as a versioned content storage system for artifacts.

BEGIN;

-- Step 1: Rename the table
ALTER TABLE core.artifact_contents RENAME TO revisions;

-- Step 2: Rename indexes (5 total: 3 manual + 2 automatic)
ALTER INDEX core.idx_artifact_contents_artifact_id RENAME TO idx_revisions_artifact_id;
ALTER INDEX core.idx_artifact_contents_content_search RENAME TO idx_revisions_content_search;
ALTER INDEX core.idx_artifact_contents_extracted_search RENAME TO idx_revisions_extracted_search;
ALTER INDEX core.artifact_contents_pkey RENAME TO revisions_pkey;
ALTER INDEX core.artifact_contents_artifact_id_version_key RENAME TO revisions_artifact_id_version_key;

-- Step 3: Update table comment
COMMENT ON TABLE core.revisions IS 'Versioned content storage for artifacts';

COMMIT;
# TIM-CoreArtifactsSchema: DDL for `core.artifacts` and `core.artifact_contents`

## Status Dashboard
**Maturity Level**: L4 - Implemented
**Implementation**: 85% (Complete database schema with versioning, missing Rust models and CRUD API layer)
**Dependencies**: `pgx_ulid` extension, `core.blobs` table, `core.set_updated_at_trigger_func_generic()` trigger function
**Blocks**: PKM note management, content versioning, artifact-based workflows, content search and discovery

## Implementation Checklist
- [x] Database migration to create artifact tables (implemented as `km.artifacts` and `km.artifact_revisions`)
- [ ] Artifact management API (create, update, version, delete)
- [ ] PKM note Yjs integration (per ADR-004)
- [ ] Tests for artifact operations and content versioning
- [ ] Full-text search index setup (tsvector generation)

*   **Purpose:** Provides the canonical Data Definition Language (DDL) for `core.artifacts` (representing conceptual documents/items like PKM notes, web pages, emails) and `core.artifact_contents` (storing their versioned textual content or references to content blobs).
*   **Source:** Derived from original Vision Document Appendix A and Part II.2, refined by ADR-004 and specific content needs.
*   **Dependencies:** `pgx_ulid` extension. `core.blobs` for optional blob FKs. The `core.set_updated_at_trigger_func_generic()` from `TIM-EventSubstrateDDL.md` is assumed.

## 1. `core.artifacts` Table

Represents conceptual documents or items. This table was also previously outlined; this version aligns with Vision's original intent for these artifacts.

```sql
CREATE TABLE IF NOT EXISTS core.artifacts (
    artifact_id             ULID PRIMARY KEY DEFAULT gen_ulid(), -- pgx_ulid
    artifact_type           TEXT NOT NULL, 
                            -- Examples from Vision: 'pkm_note', 'webpage_archive', 'email_message', 
                            -- 'pdf_document' (if primarily a blob), 'task_item', 
                            -- 'project_definition', 'ocr_result_text', 'audio_transcript_text', 
                            -- 'narrative_summary', 'planning_document_live' (like Living Document parts), 'image_artifact'
    canonical_identifier    TEXT UNIQUE NOT NULL, 
                            -- User/system chosen unique, stable identifier for this artifact concept.
                            -- For 'pkm_note': often a unique title or a path-like identifier (e.g., "my_project/notes/design_choices").
                            -- For 'webpage_archive': normalized URL of the archived page.
                            -- For 'email_message': immutable Message-ID header value.
                            -- For 'task_item': a user-defined task ID or a generated unique task name.
    current_title           TEXT NULLABLE,       -- The current human-readable title of the artifact; can change across versions.
    tags_denormalized       TEXT[] NULLABLE,     -- Denormalized array of current tag names (from core.tags via artifact_tags) for quick filtering. Updated by agent/trigger.
    properties              JSONB NULLABLE,      -- Type-specific properties. Examples:
                                            -- For 'task_item': {"status": "open", "priority": "high", "due_date_iso": "YYYY-MM-DD", "project_entity_id": "ULID"}
                                            -- For 'webpage_archive': {"original_url": "http://...", "capture_tool_name": "browsertrix_v1", "archived_ts_orig": "..."}
                                            -- For 'email_message': {"from_address": "...", "to_addresses": ["..."], "subject_original": "...", "received_ts_orig": "..."}
                                            -- For 'image_artifact': {"dimensions_px": {"width":1920, "height":1080}, "format_detected":"jpeg"}
    created_at_ts_orig      TIMESTAMPTZ NULLABLE, -- Timestamp from original source (e.g., PKM file mtime, email date, web page publication date).
    last_event_ts_orig      TIMESTAMPTZ NULLABLE, -- Timestamp of the last raw.event directly related to changes or significant interactions with this artifact concept.
    
    current_content_id      ULID NULLABLE,        -- FK to core.artifact_contents.content_id, pointing to the current textual content version.
                                                  -- Can be NULL if the artifact is primarily non-textual (e.g., an image_artifact primarily described by its primary_blob_id and properties).
    
    primary_blob_id         ULID NULLABLE,        -- REFERENCES core.blobs(blob_id) ON DELETE SET NULL, -- Add FK after core.blobs is defined
                                                  -- If the artifact's primary representation IS a blob (e.g., for 'pdf_document', 'image_artifact', original WARC for 'webpage_archive').
                                                  -- Textual content in artifact_contents might then be an extraction from this blob.

    created_at              TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at              TIMESTAMPTZ NOT NULL DEFAULT now()
);

COMMENT ON TABLE core.artifacts IS 'Canonical entities for conceptual documents, notes, web pages, tasks, emails, etc., representing the item itself, distinct from its versioned content.';
COMMENT ON COLUMN core.artifacts.artifact_type IS 'Primary type of the artifact, e.g., pkm_note, webpage_archive, email_message.';
COMMENT ON COLUMN core.artifacts.canonical_identifier IS 'A stable, unique textual identifier for the artifact concept (e.g., normalized URL, unique note path string).';
COMMENT ON COLUMN core.artifacts.current_title IS 'The current human-readable title of the artifact, may be version-dependent.';
COMMENT ON COLUMN core.artifacts.tags_denormalized IS 'Denormalized list of current tag names for faster search. Maintained by triggers/agents.';
COMMENT ON COLUMN core.artifacts.properties IS 'JSONB store for artifact-type-specific structured metadata not fitting dedicated columns.';
COMMENT ON COLUMN core.artifacts.created_at_ts_orig IS 'Original creation/publication timestamp from the source system.';
COMMENT ON COLUMN core.artifacts.last_event_ts_orig IS 'Timestamp of the last significant raw.event concerning this artifact.';
COMMENT ON COLUMN core.artifacts.current_content_id IS 'FK to core.artifact_contents, pointing to the current/latest textual content version of this artifact.';
COMMENT ON COLUMN core.artifacts.primary_blob_id IS 'FK to core.blobs, if the artifact is primarily represented by a binary object (e.g., a PDF, an image, a WARC file).';

-- Indexes
CREATE INDEX IF NOT EXISTS idx_core_artifacts_type_identifier ON core.artifacts (artifact_type, canonical_identifier);
CREATE INDEX IF NOT EXISTS idx_core_artifacts_type_title_fts ON core.artifacts USING GIN (artifact_type, to_tsvector('english', current_title)) WHERE current_title IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_core_artifacts_tags_gin ON core.artifacts USING GIN (tags_denormalized) WHERE tags_denormalized IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_core_artifacts_properties_gin ON core.artifacts USING GIN (properties jsonb_path_ops);
CREATE INDEX IF NOT EXISTS idx_core_artifacts_primary_blob_id ON core.artifacts (primary_blob_id) WHERE primary_blob_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_core_artifacts_created_at_ts_orig ON core.artifacts (created_at_ts_orig DESC NULLS LAST) WHERE created_at_ts_orig IS NOT NULL;

-- Trigger for updated_at
CREATE TRIGGER trg_core_artifacts_set_updated_at
BEFORE UPDATE ON core.artifacts
FOR EACH ROW EXECUTE FUNCTION core.set_updated_at_trigger_func_generic();
```

## 2. `core.artifact_contents` Table

Stores actual versioned textual content (or references to textual content blobs) for artifacts. For PKM notes using Yjs (ADR-004), this table stores Markdown snapshots derived from the Yjs document.

```sql
CREATE TABLE IF NOT EXISTS core.artifact_contents (
    content_id              ULID PRIMARY KEY DEFAULT gen_ulid(), -- pgx_ulid
    artifact_id             ULID NOT NULL, -- REFERENCES core.artifacts(artifact_id) ON DELETE CASCADE, -- Add FK after core.artifacts is defined
    version_identifier      TEXT NOT NULL,         -- A version identifier, could be a sequence number, a Yjs clock, a content hash, or ts_orig.
                                                   -- For Yjs snapshots: ULID of last Yjs delta incorporated or snapshot timestamp.
                                                   -- For file versions: Could be file mtime or commit hash if from Git.
    
    content_text            TEXT NULLABLE,         -- Actual textual content (e.g., Markdown, plain text). NULL if content_blob_id is used.
    content_blob_id         ULID NULLABLE,         -- REFERENCES core.blobs(blob_id) ON DELETE SET NULL, -- Add FK after core.blobs
                                                   -- If the textual content itself is large and stored as a git-annexed blob.
    content_hash_blake3     TEXT NOT NULL,         -- BLAKE3 hash of the actual textual content (derived from content_text OR by resolving content_blob_id).
    content_format          TEXT NOT NULL DEFAULT 'text/markdown', -- e.g., 'text/markdown', 'text/plain', 'application/json', 'text/html'
    
    captured_at_ts_orig     TIMESTAMPTZ NOT NULL,  -- Timestamp of when this specific content version was captured/created/saved.
    capture_method          TEXT NULLABLE,         -- How this content version was obtained (e.g., 'yjs_snapshot_from_neovim_v1.2', 'trafilatura_extract_v0.9', 'file_import_v1')
    
    word_count              INT NULLABLE,          -- Calculated word count of content_text.
    char_count              INT NULLABLE,          -- Calculated character count of content_text.
    metadata                JSONB NULLABLE,        -- e.g., For web archive Markdown: original URL of page, extracted author. 
                                                   -- For Yjs snapshot: Yjs state vector info or reference to last delta_id in pkm_note_yjs_deltas.
                                                   -- For email: key headers not in artifact properties.
    
    created_at              TIMESTAMPTZ NOT NULL DEFAULT now(),
    -- No updated_at here as content versions are conceptually immutable. New version = new row.

    CONSTRAINT uq_artifact_contents_artifact_version_id UNIQUE (artifact_id, version_identifier), -- Ensure unique versions per artifact
    CONSTRAINT uq_artifact_contents_hash_format_combo UNIQUE (artifact_id, content_hash_blake3, content_format), -- Prevent storing identical content version for same artifact
    CONSTRAINT chk_artifact_contents_source_defined CHECK (content_text IS NOT NULL OR content_blob_id IS NOT NULL)
);

COMMENT ON TABLE core.artifact_contents IS 'Stores versioned textual content for artifacts (e.g., PKM notes, webpage markdown). Content versions are immutable.';
COMMENT ON COLUMN core.artifact_contents.artifact_id IS 'FK to the core.artifact this content version belongs to.';
COMMENT ON COLUMN core.artifact_contents.version_identifier IS 'Identifier for this specific version of the content (e.g., sequence, timestamp, content hash of Yjs delta).';
COMMENT ON COLUMN core.artifact_contents.content_text IS 'The textual content, if stored directly in the table.';
COMMENT ON COLUMN core.artifact_contents.content_blob_id IS 'FK to core.blobs if the textual content is stored as a large blob.';
COMMENT ON COLUMN core.artifact_contents.content_hash_blake3 IS 'BLAKE3 hash of the actual textual content (from content_text or resolved from content_blob_id). Used for deduplication and integrity.';
COMMENT ON COLUMN core.artifact_contents.content_format IS 'MIME type or format descriptor of the content_text (e.g., text/markdown).';
COMMENT ON COLUMN core.artifact_contents.captured_at_ts_orig IS 'Timestamp of when this specific content version was captured or saved.';
COMMENT ON COLUMN core.artifact_contents.metadata IS 'JSONB store for metadata specific to this content version.';

-- Indexes
CREATE INDEX IF NOT EXISTS idx_core_artifact_contents_artifact_id_version_desc ON core.artifact_contents (artifact_id, version_identifier DESC);
CREATE INDEX IF NOT EXISTS idx_core_artifact_contents_artifact_id_ts_orig_desc ON core.artifact_contents (artifact_id, captured_at_ts_orig DESC);
CREATE INDEX IF NOT EXISTS idx_core_artifact_contents_hash ON core.artifact_contents (content_hash_blake3); -- For finding all artifacts with identical content version
CREATE INDEX IF NOT EXISTS idx_core_artifact_contents_blob_id ON core.artifact_contents (content_blob_id) WHERE content_blob_id IS NOT NULL;

-- Full-text search index on content_text (example, from TIM-HybridSearchPostgreSQL.md)
-- Ensure this is added after the column exists and is populated if table has data.
-- ALTER TABLE core.artifact_contents
-- ADD COLUMN IF NOT EXISTS content_text_tsvector tsvector
-- GENERATED ALWAYS AS (to_tsvector('english', coalesce(content_text, ''))) STORED;
-- CREATE INDEX IF NOT EXISTS idx_core_artifact_contents_fts_gin ON core.artifact_contents
-- USING GIN (content_text_tsvector);

-- Foreign Key constraints (add after all referenced tables are defined)
-- ALTER TABLE core.artifacts ADD CONSTRAINT fk_artifacts_current_content FOREIGN KEY (current_content_id) REFERENCES core.artifact_contents(content_id) ON DELETE SET NULL;
-- ALTER TABLE core.artifacts ADD CONSTRAINT fk_artifacts_primary_blob FOREIGN KEY (primary_blob_id) REFERENCES core.blobs(blob_id) ON DELETE SET NULL;

-- ALTER TABLE core.artifact_contents ADD CONSTRAINT fk_artifact_contents_artifact FOREIGN KEY (artifact_id) REFERENCES core.artifacts(artifact_id) ON DELETE CASCADE;
-- ALTER TABLE core.artifact_contents ADD CONSTRAINT fk_artifact_contents_blob FOREIGN KEY (content_blob_id) REFERENCES core.blobs(blob_id) ON DELETE SET NULL;

```
*Note: The `version_identifier` column is crucial for distinguishing different content states. For Yjs-managed content like PKM notes, this might be a ULID corresponding to the `delta_id` or `ts_created` of the last Yjs update that formed this snapshot, or the ULID of the snapshot `content_id` itself if it provides enough ordering. A simple integer sequence per `artifact_id` is also robust.*


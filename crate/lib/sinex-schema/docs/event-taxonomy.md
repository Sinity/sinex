Status: canonical  
Last Verified: 2025-12-02 (manual review)
> **Purpose:** Provide canonical event families, naming rules, and minimal payload keys so producers stay consistent.
# Event Taxonomy

Principles
- Naming: `domain.category.action` (dot‑scoped). Keep domains stable and payloads minimal; add optional fields as needed.
- Time: Producers may include `ts_client`; canonical `ts_orig` is derived per precedence from temporal ledger or intrinsic timestamps.
- Evidence: Prefer hashes/ids for large content; store blobs separately.

Families (canonical event_type and minimal payload)
- Input
  - `input.key`: key, action (down|up|repeat), modifiers?, device?, ts_client?
  - `input.mouse`: kind (move|click|scroll), button?, delta?, position?, device?, ts_client?

- Focus/Window
  - `focus.window`: window_class, window_title, pid?, workspace?, app_id?, ts_client?

- Browser
  - `browser.page_visit`: url, transition_type?, referrer?, tab_id?, window_id?, title?, ts_client?
  - `browser.dom_event`: event_name, selector?, value_hash?, url?, tab_id?, ts_client?
  - `browser.media_event`: media_kind (audio|video), action (play|pause|seek|end), url?, tab_id?, position_s?, ts_client?
  - `browser.bookmark_added`: url, title?, folder_path?, source (manual|import), ts_client?

- Webpage Processing
  - `webpage.snapshot_html`: source_url, blob_sha256, size_bytes?, mime?, charset?, title?, ts_client?
  - `webpage.text_extracted`: source_blob_sha256, extractor_id, extractor_version, text_hash, length_chars, ts_client?
  - `webpage.summary`: source_blob_sha256|source_text_hash, model_name, model_version, summary_hash, tokens_in?, tokens_out?, ts_client?

- Audio
  - `audio.segment_raw`: blob_sha256, mime?, duration_s?, sample_rate_hz?, channels?, ts_client?
  - `audio.transcript`: origin_blob_sha256, model_name, model_version, language?, text_hash, duration_s?, ts_client?

- Screen/OCR
  - `screen.text_ocr`: region_hash, text_hash, bbox?, page?, confidence?, ts_client?

- Terminal
  - `terminal.session_cast`: blob_sha256, tty?, shell?, duration_s?, cmds_count?, ts_client?
  - `terminal.command`: argv_norm_hash, argv?, cwd?, exit_code?, duration_ms?, tty?, session_id?, ts_client?

- Bookmarks/Reading
  - `bookmark.raindrop`: raindrop_id, url, title?, tags?, collection?, created_at?, ts_client?

- Chats
  - `chat.conversation_import`: platform, conversation_id_platform, title?, participants[], created_at?
  - `chat.message_import`: platform, conversation_id_platform, message_id_platform, role (user|assistant|system|tool), content_hash, parent_id?, attachments?, ts_client?

- Self‑tracking
  - `self.mood_event`: mood, context?, note?, ts_client?
  - `self.task_event`: task_id?, title?, status (created|started|done|blocked), project?, tags?, ts_client?
  - `self.substance_event`: substance, dose?, unit?, route?, note?, ts_client?

- Living Document (LD)
  - `ld.input`: section?, intent?, text_hash, cursor?, ts_client?
  - `ld.delta`: target_note_id, patch_hash|full_text_hash, model_name?, model_version?, rationale_hash?, ts_client?

- Metrics/Diagnostics (internal)
  - `system.heartbeat`: node, version?, uptime_s?
  - `ingestion.anchor_mismatch`: node, material_id, anchor_byte, rule_id, expected?, observed?
  - `annex.probe`: sample_size, failures, bytes_missing, duration_ms

Notes
- Minimal payloads prioritize identifiers/hashes; put large content in blobs and reference by hash.
- See `crate/lib/sinex-schema/docs/overview.md` for database columns and provenance rules; schemas live under `crate/lib/sinex-schema/src/schema/`.

Relations (planned)
- Purpose: capture causality, context, and workflows between events.
- Core relation types: causal (causes, triggers, enables), temporal (precedes, follows, concurrent_with),
  contextual (references, derived_from, part_of), workflow (workflow_step, retry_of, alternative_to).
- Detection sources: temporal proximity, explicit references, content similarity, manual annotation, and ML-assisted discovery.
- Queries: support tracing effects forward and causes backward; clusters for sessions/workflows/topics.

Tags (planned)
- Hierarchical dot-scoped tags (e.g., `project.sinex.docs`, `status.in-progress`, `topic.rust.async`).
- Polymorphic tagging across events/entities/blobs; unique per (tag, kind, id).
- Aliases for discoverability and simple UI properties (color/icon) as metadata.

This taxonomy is summarised for quick reference in `docs/misc-including-high-level-overviews-and-plans/_data_models_event_taxonomy_analysis.md`. Keep both artefacts in sync when event schemas evolve.

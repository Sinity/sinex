//! Each ingress frame's `outcome` is one typed result tagged by `result`:
//! `admitted` with its counts and commit range, `kinds_registered` with the
//! registered declarations, `replayed` with the replay's operation, or
//! `refused` with the refusal kind. The content hash sits beside the tag.
//! The upgrade rewrites the untyped outcomes stored before, keeping every
//! fact a reader uses: counts absent from an older outcome were zero, and a
//! content hash absent from it stays unknown.

/// The counts an admitted frame records, each zero when an older outcome
/// lacks it.
const COUNTS: [&str; 10] = [
    "admitted",
    "repeats",
    "replaced",
    "older",
    "superseded",
    "superseded_input",
    "failures",
    "dirty_keys",
    "replay_archived",
    "removed_at_source",
];

pub(crate) fn statements() -> Vec<String> {
    let counts = COUNTS
        .iter()
        .map(|name| format!("'{name}', coalesce((outcome->>'{name}')::bigint, 0)"))
        .collect::<Vec<_>>()
        .join(", ");
    vec![
        // Hidden-record suppression left the lifecycle; a count of it has no
        // place in the typed outcome.
        "DO $$ BEGIN IF EXISTS (SELECT 1 FROM core.ingress_frames WHERE coalesce((outcome->>'hidden')::bigint, 0) <> 0) THEN RAISE EXCEPTION 'ingress frames record suppressed hidden readings; decide where that count goes before upgrading'; END IF; END $$".into(),
        "DO $$ BEGIN IF EXISTS (SELECT 1 FROM core.ingress_frames WHERE refusal IS NOT NULL AND outcome->>'refusal_kind' IS NULL AND NOT outcome ? 'result') THEN RAISE EXCEPTION 'ingress frames record refusals without a refusal kind; decide what their producers must do before upgrading'; END IF; END $$".into(),
        format!(
            "UPDATE core.ingress_frames SET outcome = jsonb_build_object('content_hash', coalesce(outcome->'frame_content_hash', 'null'::jsonb)) || CASE
               WHEN outcome ? 'replay' THEN jsonb_build_object('result', 'replayed', 'operation', outcome->'replay')
               WHEN refusal IS NOT NULL THEN jsonb_build_object('result', 'refused', 'kind', outcome->'refusal_kind',
                 'failures', coalesce((outcome->>'failures')::bigint, 0),
                 'source', coalesce(outcome->'source', 'null'::jsonb),
                 'event_types', coalesce(outcome->'event_types', '[]'::jsonb))
               WHEN jsonb_array_length(coalesce(outcome->'registered_kinds', '[]'::jsonb)) > 0
                 THEN jsonb_build_object('result', 'kinds_registered', 'kinds', outcome->'registered_kinds')
               ELSE jsonb_build_object('result', 'admitted', 'counts', jsonb_build_object({counts}),
                 'commit_range', coalesce(outcome->'commit_range', 'null'::jsonb))
             END
             WHERE NOT outcome ? 'result'"
        ),
    ]
}

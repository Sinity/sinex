use super::*;
use sinex_primitives::Id;
use sinex_primitives::events::DynamicPayload;

/// `activity.window.summary` is a real production payload registered with
/// `revision_policy = "supersede_on_change"` (`sinex_primitives::events::
/// payloads::automaton::ActivityWindowSummaryPayload`) — using its real
/// event_type string routes these tests through `classify_live_match`'s
/// actual `RevisionPolicy::SupersedeOnChange` arm via the real
/// `revision_policy_for_event_type` registry lookup, not a reimplementation.
const SUPERSEDE_ON_CHANGE_EVENT_TYPE: &str = "activity.window.summary";

fn candidate(payload: serde_json::Value) -> Event<JsonValue> {
    DynamicPayload::new("test.source", SUPERSEDE_ON_CHANGE_EVENT_TYPE, payload)
        .from_material(Id::new())
        .build()
        .expect("test candidate should build")
}

/// sinex-w1w7: proves the actual bug and its fix, through the real
/// `classify_live_match`. Simulates a live row whose STORED `content_hash`
/// reflects its ORIGINAL admission-time payload, but whose `payload` column
/// has since been mutated (as `strip_postgres_jsonb_nul_chars` or
/// `redact_batch` would do) — a realistic post-admission state. A fresh
/// candidate that's an identical re-emit of the ORIGINAL (pre-mutation)
/// content must be classified `Suppress`, not `Supersede`.
///
/// Before the fix, `classify_live_match` compared the candidate's hash
/// against a hash recomputed from `live.payload` (the mutated form), which
/// would never match the candidate's hash of the original content — an
/// unchanged re-emission would misclassify as "changed" and churn
/// archive/re-insert forever. Reverting `live_row_content_hash` to always
/// recompute from `live.payload` (ignoring `content_hash`) makes this test
/// fail: `classify_live_match` would return `Supersede` instead of
/// `Suppress`.
#[test]
fn supersede_on_change_prefers_stored_hash_over_mutated_live_payload() {
    let original_payload = serde_json::json!({ "value": "hello\u{0}world" });
    let mutated_payload = serde_json::json!({ "value": "helloworld" }); // NUL stripped

    let original_hash = sinex_primitives::events::payload_content_hash(&original_payload);

    let live = LiveEquivalenceRow {
        equivalence_key: "test-key".to_string(),
        id: Uuid::now_v7(),
        // The live row's CURRENT persisted payload has already been mutated
        // (simulating strip_postgres_jsonb_nul_chars/redact_batch having run
        // on it at ITS admission time) — this is what a naive recompute-from-
        // payload comparison would hash.
        payload: mutated_payload,
        // But its STORED admission-time hash reflects the ORIGINAL candidate
        // content, computed before that mutation — exactly what
        // admitted_to_stream_rows now does.
        content_hash: Some(original_hash.to_vec()),
    };

    // A fresh candidate re-emitting the exact same original content.
    let fresh_candidate = candidate(original_payload);

    let outcome = classify_live_match(&fresh_candidate, &live);
    assert_eq!(
        outcome,
        EquivalenceOutcome::Suppress,
        "an identical re-emit of the live row's ORIGINAL content must suppress, \
         even though the live row's current persisted payload was mutated since \
         its own admission — comparing stored hash to stored hash sidesteps the \
         mutation entirely"
    );
}

/// Sibling case: a genuinely different candidate (not equal to either the
/// live row's original OR mutated content) must still supersede.
#[test]
fn supersede_on_change_supersedes_on_genuine_content_change() {
    let original_payload = serde_json::json!({ "value": "hello\u{0}world" });
    let original_hash = sinex_primitives::events::payload_content_hash(&original_payload);
    let live_id = Uuid::now_v7();

    let live = LiveEquivalenceRow {
        equivalence_key: "test-key".to_string(),
        id: live_id,
        payload: serde_json::json!({ "value": "helloworld" }),
        content_hash: Some(original_hash.to_vec()),
    };

    let different_candidate = candidate(serde_json::json!({ "value": "something else entirely" }));

    let outcome = classify_live_match(&different_candidate, &live);
    assert_eq!(
        outcome,
        EquivalenceOutcome::Supersede {
            superseded_event_id: live_id
        },
        "a genuinely different candidate must supersede the live row"
    );
}

/// Pre-existing rows written before the `content_hash` column existed carry
/// `NULL` — the comparison must fall back to recomputing from `live.payload`
/// (best effort; self-heals once such a row is superseded and rewritten with
/// a stored hash) rather than panicking or always-superseding.
#[test]
fn supersede_on_change_falls_back_to_recomputed_hash_when_stored_hash_is_null() {
    let payload = serde_json::json!({ "value": "unmutated since admission" });

    let live = LiveEquivalenceRow {
        equivalence_key: "test-key".to_string(),
        id: Uuid::now_v7(),
        payload: payload.clone(),
        content_hash: None,
    };

    let identical_candidate = candidate(payload);

    let outcome = classify_live_match(&identical_candidate, &live);
    assert_eq!(
        outcome,
        EquivalenceOutcome::Suppress,
        "NULL content_hash must fall back to recomputing from live.payload, \
         and an identical re-emit against an unmutated live row must suppress"
    );
}

/// A malformed (wrong-length) stored hash — should never occur for a real
/// row given the DB CHECK constraint, but `live_row_content_hash` must not
/// silently compare it as a zeroed/empty hash; it falls back to recomputing
/// from `live.payload` instead.
#[test]
fn supersede_on_change_falls_back_when_stored_hash_is_malformed() {
    let payload = serde_json::json!({ "value": "unmutated since admission" });

    let live = LiveEquivalenceRow {
        equivalence_key: "test-key".to_string(),
        id: Uuid::now_v7(),
        payload: payload.clone(),
        content_hash: Some(vec![1, 2, 3]), // wrong length, not a real 32-byte digest
    };

    let identical_candidate = candidate(payload);

    let outcome = classify_live_match(&identical_candidate, &live);
    assert_eq!(
        outcome,
        EquivalenceOutcome::Suppress,
        "a malformed stored hash must fall back to recomputing from live.payload, \
         not silently mismatch every candidate"
    );
}

/// sinex-naw9: `ValidationResult::SchemaNotFound` is the arm reached when a
/// schema failed to compile (`compile_schemas` in `sinex-db/src/
/// validation.rs` drops it from both cache and lookup, so a corrupt
/// registered schema silently downgrades to `NoSchema`... but the SEPARATE,
/// more direct `SchemaNotFound` path -- reached whenever `schema_lookup` has
/// an entry but `schema_cache` doesn't, e.g. a schema deleted between lookup
/// and fetch -- unconditionally accepts with only a warning, never
/// consulting `strict_mode` the way every other arm here does. This is the
/// REAL production `resolve_validation_result` in this module (not the
/// `#[cfg(test)]`-only duplicate in `jetstream_consumer/prepare.rs`, which
/// has the identical bug and its own non-strict-only coverage in
/// `jetstream_consumer_test.rs::schema_not_found_is_accepted_leniently`).
///
/// Expected to fail today: strict mode must reject an event whose schema
/// went missing after being matched, exactly as it already does for
/// `NoSchema` (see `strict_mode_rejects_missing_schema` below, which passes).
#[test]
fn strict_mode_rejects_schema_not_found() {
    let err = resolve_validation_result(
        ValidationResult::SchemaNotFound {
            schema_id: Uuid::now_v7(),
        },
        true,
        &sinex_primitives::domain::EventSource::from_static("test"),
        &sinex_primitives::domain::EventType::from_static("schema.vanished"),
    )
    .expect_err(
        "sinex-naw9: strict mode must reject an event whose matched schema is missing from \
         the cache, not silently accept it with payload_schema_id=NULL -- \
         ValidationResult::SchemaNotFound currently ignores strict_mode entirely, unlike \
         every other ValidationResult arm in this function",
    );
    assert!(
        err.to_string().contains("Strict validation enabled"),
        "unexpected error: {err}"
    );
}

/// Companion passing test: `NoSchema` already does the right thing under
/// strict mode -- this is the behavior `SchemaNotFound` above is missing.
#[test]
fn strict_mode_rejects_missing_schema() {
    let err = resolve_validation_result(
        ValidationResult::NoSchema,
        true,
        &sinex_primitives::domain::EventSource::from_static("test"),
        &sinex_primitives::domain::EventType::from_static("schema.missing"),
    )
    .expect_err("strict mode must reject events without a registered schema");
    assert!(err.to_string().contains("Strict validation enabled"));
}

/// Non-strict mode: `SchemaNotFound` should still (correctly, both today and
/// after naw9 is fixed) accept leniently -- naw9 only requires strict mode
/// to close the hole, not that SchemaNotFound become universally rejected.
#[test]
fn non_strict_mode_still_accepts_schema_not_found() {
    let accepted = resolve_validation_result(
        ValidationResult::SchemaNotFound {
            schema_id: Uuid::now_v7(),
        },
        false,
        &sinex_primitives::domain::EventSource::from_static("test"),
        &sinex_primitives::domain::EventType::from_static("schema.vanished"),
    )
    .expect("non-strict mode must not reject a missing-schema-cache-entry event");
    assert!(accepted.is_none());
}

/// sinex-tgjw: proves the WRITE side of the fix, through the real
/// `admitted_to_stream_rows` (exercised via the production `AdmittedEvent`
/// type, not a hand-built `StreamBatchRow`). Simulates exactly what
/// `persist_batch_optimized` does in production: construct `AdmittedEvent`
/// (capturing `content_hash` from the payload as it stood at admission --
/// matching the real construction site in
/// `jetstream_consumer/persist.rs::persist_batch_optimized`), THEN mutate
/// `event.payload` in place (simulating the `redact_batch` chokepoint, which
/// runs between `AdmittedEvent` construction and `admitted_to_stream_rows`
/// in the real pipeline). The persisted `content_hash` on the resulting
/// `StreamBatchRow` must equal the hash of the ORIGINAL, pre-mutation
/// payload -- not a hash recomputed from the now-mutated `event.payload`.
///
/// Before the fix, `admitted_to_stream_rows` recomputed
/// `payload_content_hash(&event.payload)` directly, which would hash the
/// MUTATED payload here and diverge from `classify_live_match`'s candidate-
/// side hash (computed earlier in the pipeline, before mutation) -- this
/// test fails under that recompute-here behavior and passes under the fix
/// (reading the hash captured at `AdmittedEvent` construction time).
#[test]
fn admitted_to_stream_rows_persists_the_pre_redaction_hash_not_a_post_mutation_recompute() {
    let original_payload = serde_json::json!({ "value": "hello\u{0}world" });
    let event = DynamicPayload::new("test.source", SUPERSEDE_ON_CHANGE_EVENT_TYPE, original_payload.clone())
        .from_material(Id::new())
        .at_time(sinex_primitives::Timestamp::now())
        .build()
        .expect("test event should build");

    // Mirrors the real construction site in persist.rs::persist_batch_optimized:
    // content_hash is captured from the payload AS IT STOOD at this exact point.
    let original_hash = sinex_primitives::events::payload_content_hash(&event.payload);
    let mut admitted = AdmittedEvent {
        content_hash: original_hash,
        event,
        event_id: Uuid::now_v7(),
        metadata: None,
    };

    // Simulate redact_batch mutating the payload in place, AFTER AdmittedEvent
    // construction -- exactly the real ordering in persist_batch_optimized.
    admitted.event.payload = serde_json::json!({ "value": "[REDACTED]" });

    let rows = admitted_to_stream_rows(&[&admitted]).expect("stream row conversion should succeed");
    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0].content_hash.as_deref(),
        Some(original_hash.as_slice()),
        "the persisted content_hash must be the PRE-redaction hash captured at \
         AdmittedEvent construction, not a hash recomputed from the (now-mutated) \
         event.payload"
    );
}

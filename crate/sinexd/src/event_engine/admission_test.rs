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

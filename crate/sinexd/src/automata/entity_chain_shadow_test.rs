//! Integration tests for the entity-chain `StreamCheckpoint` shadow lane
//! (sinex-0vx.9). All tests run against a real sandboxed Postgres via
//! `#[sinex_test]` (`xtask::sandbox`), matching the pattern
//! `crate::authority_test`/`crate::api::handlers::curation` tests use for
//! DB-integration coverage embedded in `src` rather than `tests/api`.

use super::*;
use sinex_db::repositories::source_materials::material_types;
use sinex_primitives::Id;
use sinex_primitives::derivation::{AdjudicationStatus, SupportLevel};
use sinex_primitives::domain::RelationType;
use sinex_primitives::events::payloads::{CanonicalCommandPayload, CurationJudgmentActorKind};
use sinex_primitives::events::{EntityRelatedPayload, EventPayload};
use sinex_primitives::temporal::Duration;
use xtask::sandbox::prelude::*;
use xtask::sandbox::sinex_test;

fn shadow_event(text: &str, ts_secs: i64) -> StreamCheckpointEventRow {
    StreamCheckpointEventRow {
        event_id: uuid::Uuid::new_v4(),
        payload: json!({"text": text}),
        ts_orig: Timestamp::now() - Duration::seconds(1_000_000) + Duration::seconds(ts_secs),
        source_material_id: None,
    }
}

/// sinex-z3a7 (sub-issue 2): `entity_chain_shadow.rs`'s co-occurrence window
/// tracks `last_seen = Some(now)` unconditionally (line ~378), the exact
/// pattern `sinex-audit-outoforder-pattern` already fixed in the LIVE
/// `relation_extractor.rs` (which tracks max-arrived-at, never regressing on
/// an out-of-order entry) -- just never ported here. It matters more in this
/// lane because events are read `ORDER BY id ASC` (interpretation-time
/// order) while all window arithmetic uses `ts_orig`, so a historical import
/// (exactly what the imminent wipe-and-reimport will do) routinely delivers
/// events out of `ts_orig` order.
///
/// This test constructs exactly that shape: four events processed in
/// interpretation order, where the third (`C`) has a `ts_orig` earlier than
/// the second (`B`)'s. Under the current last-pushed watermark, processing
/// `C` regresses `last_seen` from `B`'s 1000s down to `C`'s 1s, so the
/// legitimately-close-in-time fourth event `D` (1050s, only 50s after `B`'s
/// true 1000s) sees a spurious ~1049s "gap" and force-closes the window
/// early -- splitting `[A,B,C,D]` into `[A,B,C]` + `[D]` instead of keeping
/// them one co-occurrence group. A final, much-later event `E` (5000s)
/// should legitimately start a new window either way.
///
/// Expected (max-arrived-at, not yet implemented): A,B,C,D co-occur as one
/// group (6 pairs; C(1)r's 5001, E gets no pair (window len 1 at freeze).
/// Actual (current last-pushed bug): A,B,C close early (3 pairs) + D,E
/// close together as their own pair (1 pair) = 4 pairs total, with no
/// pairing that includes D and any of A/B/C -- proving D was wrongly
/// evicted from the group it chronologically belongs to.
#[sinex_test]
#[ignore = "sinex-z3a7 open: shadow-lane watermark uses last-pushed not max-arrived-at -- fails until fixed"]
async fn shadow_lane_window_watermark_should_not_regress_on_out_of_order_arrival() -> TestResult<()>
{
    let events = vec![
        shadow_event("check out https://tool-a.example.com", 0),
        shadow_event("check out https://tool-b.example.com", 1000),
        shadow_event("check out https://tool-c.example.com", 1), // out of order vs B
        shadow_event("check out https://tool-d.example.com", 1050),
        shadow_event("check out https://tool-e.example.com", 5000),
    ];

    let (_, rows) = run_entity_chain(&events);
    let relation_pair_count = rows.iter().filter(|r| r.output_kind == "relation").count();

    assert_eq!(
        relation_pair_count, 6,
        "sinex-z3a7: with a correct max-arrived-at watermark, D (1050s) is within \
         WINDOW_GAP_SECS of B's true latest arrival (1000s) and should stay grouped with \
         A/B/C as one 4-member co-occurrence window (6 pairs). Got {relation_pair_count} \
         pairs -- the current last-pushed watermark regresses on the out-of-order C (1s) \
         entry, making D's gap look larger than it is and force-closing the window early.",
    );
    Ok(())
}

/// Seed every `derivation.product_declarations` row this module's writes
/// (and the live entity-chain automata's own declarations, needed by the
/// direct-canonical-write rejection test) foreign-key against.
async fn seed_declarations(pool: &sqlx::PgPool) -> TestResult<()> {
    crate::automata::product_declarations::reconcile_product_declarations(
        pool,
        crate::automata::registry::AUTOMATA,
    )
    .await?;
    crate::automata::product_declarations::reconcile_declarations(
        pool,
        "curation-rpc",
        crate::api::handlers::curation::CURATION_OUTPUT_DECLARATIONS,
    )
    .await?;
    crate::automata::product_declarations::reconcile_declarations(
        pool,
        "entity-chain-shadow",
        ENTITY_CHAIN_SHADOW_OUTPUT_DECLARATIONS,
    )
    .await?;
    Ok(())
}

/// Seed `authority.finalizer_registry` for both the general curation
/// finalizers and this module's entity-chain relation finalizer. Callers
/// exercising the bypass-rejection path must NOT call this.
async fn seed_finalizers(pool: &sqlx::PgPool) -> TestResult<()> {
    crate::authority::reconcile_finalizer_registrations(
        pool,
        crate::api::handlers::curation::CURATION_FINALIZER_DECLARATIONS,
    )
    .await?;
    crate::authority::reconcile_finalizer_registrations(
        pool,
        ENTITY_CHAIN_RELATION_FINALIZER_DECLARATIONS,
    )
    .await?;
    Ok(())
}

/// Insert four `command.canonical` fixture events, each containing text that
/// triggers a DIFFERENT extractor pattern (URL / email / command / file
/// path) so the resulting shadow run resolves four distinct entities and
/// (since the batch always force-drains its co-occurrence window at scope
/// end) at least one `co_occurs_with` relation between them.
async fn insert_fixture_events(pool: &sqlx::PgPool) -> TestResult<Vec<uuid::Uuid>> {
    let material_record = pool
        .source_materials()
        .register_in_flight(
            material_types::STREAM,
            Some("entity-chain-shadow-test"),
            serde_json::json!({"test": true}),
        )
        .await?;
    let material_id = Id::<sinex_primitives::events::SourceMaterial>::from_uuid(material_record.id);

    let texts = [
        "check https://example.com/docs for details",
        "reported by alice@example.org please",
        "run git commit -m msg now",
        "open /home/alice/notes/todo.md file",
    ];
    let base = Timestamp::now();
    let mut ids = Vec::with_capacity(texts.len());
    for (index, text) in texts.iter().enumerate() {
        // Small millisecond spacing, not seconds: `events_check7` requires
        // ts_orig <= ts_coided + 1s, and ts_coided is minted at .build()
        // time (effectively "now"), so pushing ts_orig seconds into the
        // future relative to `base` trips that constraint once the test
        // reaches later fixture indices.
        let ts = base + sinex_primitives::temporal::Duration::milliseconds(index as i64 * 10);
        let payload = CanonicalCommandPayload {
            command: (*text).to_string(),
            working_directory: None,
            exit_code: None,
            duration_ms: None,
            start_time: ts,
            end_time: ts,
            user: None,
            session_id: None,
            environment_hash: None,
            source_events: Vec::new(),
            enrichment_history: Vec::new(),
        };
        let event = payload
            .from_material_at(material_id, index as i64)
            .at_time(ts)
            .build()?;
        let inserted = pool.events().insert(event).await?;
        let id = inserted
            .id
            .ok_or_else(|| color_eyre::eyre::eyre!("fixture event missing id"))?
            .to_uuid();
        ids.push(id);
    }
    Ok(ids)
}

#[sinex_test]
async fn entity_chain_stream_checkpoint_shadow_lane(ctx: TestContext) -> TestResult<()> {
    let pool = ctx.pool();
    seed_declarations(pool).await?;
    insert_fixture_events(pool).await?;

    let scope = freeze_entity_chain_stream_checkpoint(pool, 0).await?;
    let DerivationScope::StreamCheckpoint { end_seq, .. } = &scope else {
        panic!("expected a StreamCheckpoint scope");
    };
    assert!(
        end_seq.unwrap_or(0) >= 4,
        "expected the 4 fixture events to be visible in the frozen scope, got end_seq={end_seq:?}"
    );

    let result = run_entity_chain_shadow_lane(pool, scope, None, None, "shadow-lane").await?;

    // AC: a generic LaneDiffReport vs the empty baseline yields
    // candidate-only (entity_new/relation_added).
    assert_eq!(
        result.outputs.entities.len(),
        4,
        "expected 4 distinct entities: url/person/tool/file"
    );
    assert!(
        !result.outputs.relations.is_empty(),
        "expected at least one co-occurrence relation among 4 entities in one window"
    );
    assert_eq!(result.diff.output_kind, "entity_relation");
    assert_eq!(
        result.diff.summary.added,
        result.outputs.entities.len() + result.outputs.relations.len(),
        "vs an empty baseline every output must be an addition"
    );
    assert_eq!(result.diff.summary.removed, 0);
    assert_eq!(result.diff.summary.changed, 0);
    let counts = result.diff.counts.clone();
    assert_eq!(counts["entity_new"], serde_json::json!(4));
    assert_eq!(
        counts["relation_added"],
        serde_json::json!(result.outputs.relations.len())
    );

    // AC: writes derivation.lane_outputs with product_class=semantic_candidate
    // and an honest ClaimSupport -- never core.events.
    let repo = pool.derivation_lanes();
    let rows = repo
        .list_lane_outputs(result.candidate_lane_id, 100)
        .await?;
    assert_eq!(
        rows.len(),
        result.outputs.entities.len() + result.outputs.relations.len()
    );
    let mut saw_entity = false;
    let mut saw_relation = false;
    for row in &rows {
        assert_eq!(row.product_class, "semantic_candidate");
        match row.output_kind.as_str() {
            "entity" => saw_entity = true,
            "relation" => saw_relation = true,
            other => panic!("unexpected output_kind {other}"),
        }
        let claim_support: sinex_primitives::derivation::ClaimSupport =
            serde_json::from_value(row.claim_support.clone())?;
        assert_eq!(
            claim_support.support_level(),
            SupportLevel::Heuristic,
            "entity-chain candidates are always pattern-heuristic, never claimed as direct fact"
        );
        assert_eq!(
            claim_support.adjudication(),
            AdjudicationStatus::Unreviewed,
            "a freshly-run shadow lane must never carry an adjudicated claim"
        );
        assert!(
            claim_support.adjudication_event_id().is_none(),
            "unreviewed claim support must not carry a judgment event id"
        );
        assert!(
            claim_support.evidence_event_count() >= 1,
            "claim support must be built from real observed evidence counts, not a fabricated \
             1.0/empty-evidence default (mutating honest_claim_support to always report 0 must \
             fail this assertion)"
        );
    }
    assert!(saw_entity && saw_relation);

    // Doctrine guard: nothing lands in core.events for these candidates.
    let canonical_relation_count: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM core.events WHERE event_type = 'entity.related'")
            .fetch_one(pool)
            .await?;
    assert_eq!(
        canonical_relation_count, 0,
        "shadow-lane candidates must never be written as canonical-looking core.events rows"
    );

    Ok(())
}

#[sinex_test]
async fn stream_checkpoint_scope_reproducible(ctx: TestContext) -> TestResult<()> {
    let pool = ctx.pool();
    seed_declarations(pool).await?;
    insert_fixture_events(pool).await?;

    let scope = freeze_entity_chain_stream_checkpoint(pool, 0).await?;

    let first = run_entity_chain_shadow_lane(pool, scope.clone(), None, None, "repro-a").await?;

    // Insert MORE matching events AFTER the freeze. A correct frozen
    // StreamCheckpoint re-run must be blind to them -- this is the actual
    // "not a wall clock" proof: if the ordinal read used "everything
    // currently in the table" instead of the frozen (start_seq, end_seq]
    // range, this second run would see 8 events instead of 4 and diverge.
    insert_fixture_events(pool).await?;

    // Re-run under the SAME epoch (the bead's literal wording) -- reusing
    // `first.epoch_id` rather than minting a second one, since two runs
    // over an identical frozen scope are the same interpretation regime.
    let second = run_entity_chain_shadow_lane(
        pool,
        scope,
        Some(first.epoch_id),
        Some((first.baseline_lane_id, first.candidate_lane_id)),
        "repro-b",
    )
    .await?;

    assert_eq!(
        first.outputs, second.outputs,
        "identical frozen scope + epoch must produce byte-identical candidate outputs"
    );
    assert_eq!(first.diff.counts, second.diff.counts);
    assert_eq!(first.diff.examples, second.diff.examples);
    assert_eq!(first.diff.summary, second.diff.summary);

    // Sanity: prove the second insert really did add new matching rows,
    // so the equality above is a property of the freeze, not an artifact
    // of "there was nothing new to find".
    let total_matching: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM core.events WHERE event_type = 'command.canonical'",
    )
    .fetch_one(pool)
    .await?;
    assert!(
        total_matching >= 8,
        "expected both fixture batches present in core.events, got {total_matching}"
    );

    Ok(())
}

#[sinex_test]
async fn entity_chain_shadow_lane_promotion_flips_status_and_emits_finalized(
    ctx: TestContext,
) -> TestResult<()> {
    let pool = ctx.pool();
    seed_declarations(pool).await?;
    seed_finalizers(pool).await?;
    let event_ids = insert_fixture_events(pool).await?;

    let scope = freeze_entity_chain_stream_checkpoint(pool, 0).await?;
    let result = run_entity_chain_shadow_lane(pool, scope, None, None, "promote").await?;
    let relation = result
        .outputs
        .relations
        .first()
        .expect("expected at least one relation candidate to promote")
        .clone();

    let finalized_event = promote_entity_related_output(
        pool,
        result.candidate_lane_id,
        &relation,
        event_ids,
        CurationJudgmentActorKind::Operator,
        "test-operator",
    )
    .await?;

    assert_eq!(finalized_event.source.as_str(), "curation");
    assert_eq!(finalized_event.event_type.as_str(), "curation.finalized");
    let adjudication_event_id = finalized_event
        .adjudication_event_id
        .expect("promoted output must carry adjudication_event_id");
    assert_ne!(adjudication_event_id, uuid::Uuid::nil());

    let lane = pool
        .derivation_lanes()
        .get_lane(result.candidate_lane_id)
        .await?;
    assert_eq!(
        lane.status, "promoted",
        "promotion through proposal -> judgment -> finalizer must flip the lane to promoted"
    );

    Ok(())
}

/// AC: "a direct canonical entity.related write and a finalizer bypass are
/// both REJECTED at the DB/API gate" -- both rejection paths in one test,
/// matching the bead's literal verify-command test name.
#[sinex_test]
async fn entity_related_direct_canonical_rejected(ctx: TestContext) -> TestResult<()> {
    let pool = ctx.pool();
    seed_declarations(pool).await?;

    // ── Rejection path 1: direct canonical write ──────────────────────
    // The live `relation-extractor` automaton only ever declares
    // entity.related at product_class = semantic_candidate
    // (RELATION_EXTRACTOR_OUTPUT_DECLARATIONS). Claiming the SAME
    // (source, event_type) at CanonicalDerivedEvent has no matching
    // derivation.product_declarations row -- the DB trigger must reject it
    // as an undeclared product write, proving a candidate cannot be
    // relabeled canonical without going through the promotion seam.
    let payload = EntityRelatedPayload {
        source_entity_id: uuid::Uuid::now_v7(),
        target_entity_id: uuid::Uuid::now_v7(),
        relation_type: RelationType::new("co_occurs_with"),
        confidence: 0.9,
    };
    let material_record = pool
        .source_materials()
        .register_in_flight(
            material_types::STREAM,
            Some("direct-canonical-write-test"),
            serde_json::json!({}),
        )
        .await?;
    let material_id = Id::<sinex_primitives::events::SourceMaterial>::from_uuid(material_record.id);
    let mut event = payload.from_material_at(material_id, 0).build()?;
    event.product_class = Some(DerivedProductClass::CanonicalDerivedEvent);
    event.claim_support = Some(sinex_primitives::derivation::ClaimSupport::unreviewed(
        SupportLevel::Direct,
        sinex_primitives::derivation::SourceCoverage::Covered,
        ClaimTemporalQuality::RealtimeCapture,
        1,
        0,
        1,
        0,
    ));
    event.derivation_declaration_id = Some("relation-extractor.entity.related".to_string());

    let insert_result = pool.events().insert(event).await;
    let insert_error =
        insert_result.expect_err("a direct canonical entity.related write must be rejected");
    let message = insert_error.to_string();
    assert!(
        message.contains("undeclared product write"),
        "expected the undeclared-product-write DB trigger rejection, got: {message}"
    );

    // ── Rejection path 2: finalizer bypass ─────────────────────────────
    // No finalizer has been reconciled for this test (seed_finalizers is
    // deliberately NOT called) -- promotion must be refused before any
    // adjudicated write is even attempted.
    let bypass_result = crate::authority::authorize_finalization(
        pool,
        "entity.related",
        "relation-extractor",
        "entity.related",
        CurationJudgmentActorKind::Operator,
    )
    .await;
    let bypass_error = bypass_result.expect_err(
        "finalizing an entity.related proposal must be refused before \
             a finalizer is registered (the curation-bypass rejection)",
    );
    assert!(
        bypass_error.to_string().contains("no registered finalizer"),
        "expected the curation-bypass rejection, got: {bypass_error}"
    );

    Ok(())
}

/// sinex-ee7p (sub-issue 1 of 3, entity_chain_shadow.rs's own slice only):
/// `drain_relation_pairs` hardcodes `SourceCoverage::Covered` on every
/// co_occurs_with relation, regardless of whether the underlying co-occurring
/// events actually had material grounding -- even though
/// `StreamCheckpointEventRow.source_material_id` is captured right there.
/// A relation built entirely from events with `source_material_id: None`
/// (e.g. shell-derived synthetic events) is emitted as fully covered
/// evidence, overstating evidence quality to curation/analytics consumers
/// that trust `source_coverage`. Failing by design until the coverage is
/// computed from the pair's actual `source_material_id` presence.
#[sinex_test]
#[ignore = "sinex-ee7p open: shadow-lane relations claim source_coverage: Covered unconditionally -- fails until fixed"]
async fn shadow_lane_relation_coverage_reflects_actual_material_grounding_sinex_ee7p()
-> TestResult<()> {
    // Both co-occurring events lack material grounding entirely.
    let events = vec![
        shadow_event("check out https://tool-a.example.com", 0),
        shadow_event("check out https://tool-b.example.com", 1),
    ];
    assert!(events.iter().all(|e| e.source_material_id.is_none()));

    let (_, rows) = run_entity_chain(&events);
    let relation_row = rows
        .iter()
        .find(|r| r.output_kind == "relation")
        .expect("A and B are within the co-occurrence window and should produce a relation");

    let coverage = relation_row.claim_support["source_coverage"]
        .as_str()
        .expect("claim_support.source_coverage should be a string");

    assert_ne!(
        coverage, "covered",
        "sinex-ee7p: a relation whose evidence events both have source_material_id: None \
         has no actual material grounding and should not be marked source_coverage: covered \
         -- got 'covered' unconditionally regardless of the pair's real grounding",
    );
    Ok(())
}

/// sinex-u39b (sub-issue 3 of 6): `read_stream_checkpoint_events` computes
/// `limit` via `end_seq.saturating_sub(start_seq)`. For a malformed scope
/// with `start_seq > end_seq`, that saturates to 0 rather than surfacing an
/// error -- the lane silently processes zero events and still reports
/// success instead of rejecting the nonsensical range.
#[sinex_test]
#[ignore = "sinex-u39b open: entity_chain_shadow silently accepts a start_seq > end_seq scope as zero-events-processed success instead of erroring"]
async fn shadow_lane_rejects_start_seq_past_end_seq_sinex_u39b(ctx: TestContext) -> TestResult<()> {
    let pool = ctx.pool();
    seed_declarations(pool).await?;
    insert_fixture_events(pool).await?;

    let scope = DerivationScope::StreamCheckpoint {
        stream: "core-events".to_string(),
        filter_subjects: vec![],
        start_seq: 100,
        end_seq: Some(5),
        coverage_window: None,
        input_set_hash: "malformed-start-past-end".to_string(),
    };

    let result = run_entity_chain_shadow_lane(pool, scope, None, None, "malformed-range").await;
    let succeeded_with_output_count = result.as_ref().ok().map(|r| r.outputs.entities.len());

    assert!(
        result.is_err(),
        "sinex-u39b: a scope with start_seq (100) > end_seq (5) must be rejected, not silently \
         accepted as a zero-event success (entity count if it succeeded: {succeeded_with_output_count:?})"
    );
    Ok(())
}

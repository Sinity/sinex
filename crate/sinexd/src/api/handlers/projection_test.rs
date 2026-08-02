#![allow(clippy::unwrap_used)]

use super::*;
use sinex_db::DbPoolExt;
use sinex_db::repositories::{
    EmailMailboxProjectionEvent, ProjectionCoverageWindow, ProjectionRegistrationInput,
};
use sinex_primitives::views::ReadinessCaveatId;
use uuid::Uuid;
use xtask::sandbox::sinex_test;

fn registration<'a>(kind: &'a str, scope_key: &'a str) -> ProjectionRegistrationInput<'a> {
    ProjectionRegistrationInput {
        projection_kind: kind,
        scope_key,
        semantics_version: "v1",
        input_fingerprint: "fp-test",
        coverage_window: ProjectionCoverageWindow {
            start: OffsetDateTime::now_utc() - Duration::hours(1),
            end: None,
        },
        freshness_class: ProjectionFreshnessClass::Hours,
        acceptable_staleness_secs: 3600,
        verification_command: "xtask test -p sinexd -E 'test(projection_readiness_view)'",
    }
}

/// Real behavior under test: `build_projection_readiness_view` reads
/// `derivation.projection_registry` through `ProjectionRegistryRepository`
/// and renders each row's `status` into the shared `ReadinessCaveatId`
/// vocabulary via `ProjectionReadinessView::from_rows`. Removing either the
/// `registered_projection_rows` query or the status->caveat mapping in
/// `sinex_primitives::views::projection` makes this test fail: with the
/// query removed, `entries_for` returns empty and every `find` below panics;
/// with the mapping removed, the `ready` row would carry a spurious caveat
/// or the degraded rows would carry none.
#[sinex_test]
async fn projection_readiness_view_renders_registry_row_caveats(
    ctx: TestContext,
) -> TestResult<()> {
    let kind = format!("test_kind_{}", Uuid::now_v7());
    let repo = ctx.pool().projection_registry();

    let ready_id = repo.begin_build(&registration(&kind, "ready")).await?;
    repo.mark_ready(ready_id, serde_json::json!({"n": 3})).await?;

    let stale_id = repo.begin_build(&registration(&kind, "stale")).await?;
    repo.mark_ready(stale_id, serde_json::json!({})).await?;
    repo.mark_stale(stale_id, "acceptable staleness exceeded")
        .await?;

    let failed_id = repo.begin_build(&registration(&kind, "failed")).await?;
    repo.mark_failed(failed_id, "connection refused").await?;

    let partial_id = repo.begin_build(&registration(&kind, "partial")).await?;
    repo.mark_partial(partial_id, "half the scope covered", serde_json::json!({}))
        .await?;

    repo.mark_absent(&registration(&kind, "absent")).await?;

    // `building` scope: begin_build and leave it there.
    repo.begin_build(&registration(&kind, "building")).await?;

    let view = build_projection_readiness_view(ctx.pool()).await?;
    let entries: Vec<_> = view
        .projections
        .iter()
        .filter(|entry| entry.projection_kind == kind)
        .collect();
    assert_eq!(entries.len(), 6, "expected one row per seeded scope_key");

    let find = |scope: &str| {
        entries
            .iter()
            .find(|entry| entry.scope_key == scope)
            .unwrap_or_else(|| panic!("missing entry for scope {scope}"))
    };

    let ready = find("ready");
    assert_eq!(ready.status, ProjectionStatus::Ready);
    assert!(ready.caveats.is_empty());
    assert!(!ready.read_time_computed);

    let stale = find("stale");
    assert_eq!(stale.status, ProjectionStatus::Stale);
    assert_eq!(
        stale.caveats[0].id,
        ReadinessCaveatId::ReadmodelStaleBy.as_str()
    );
    assert!(stale.caveats[0].message.contains("acceptable staleness exceeded"));

    let failed = find("failed");
    assert_eq!(failed.status, ProjectionStatus::Failed);
    assert_eq!(
        failed.caveats[0].id,
        ReadinessCaveatId::ReadmodelFailed.as_str()
    );
    assert!(failed.caveats[0].message.contains("connection refused"));

    let partial = find("partial");
    assert_eq!(partial.status, ProjectionStatus::Partial);
    assert_eq!(
        partial.caveats[0].id,
        ReadinessCaveatId::ReadmodelPartial.as_str()
    );

    let absent = find("absent");
    assert_eq!(absent.status, ProjectionStatus::Absent);
    assert_eq!(
        absent.caveats[0].id,
        ReadinessCaveatId::ReadmodelAbsent.as_str()
    );

    let building = find("building");
    assert_eq!(building.status, ProjectionStatus::Building);
    assert_eq!(
        building.caveats[0].id,
        ReadinessCaveatId::ReadmodelBuilding.as_str()
    );

    Ok(())
}

/// Real behavior under test: the first-slice `email_mailbox` entry is
/// computed at READ TIME from `core.email_mailbox_projection` (a real,
/// already-existing read model — not a synthetic placeholder), not from a
/// `derivation.projection_registry` row. Removing `email_mailbox_readiness_rows`
/// (or its call from `build_projection_readiness_view`) makes this test fail:
/// no entry for the seeded mode would be found at all.
#[sinex_test]
async fn projection_readiness_view_includes_read_time_email_mailbox_entry(
    ctx: TestContext,
) -> TestResult<()> {
    let mode_id = format!("test-mode-{}", Uuid::now_v7());
    ctx.pool()
        .email_mailbox_projections()
        .upsert_event(EmailMailboxProjectionEvent {
            source_id: EMAIL_MAILBOX_SOURCE_ID.to_string(),
            mode_id: mode_id.clone(),
            observed_event_id: Uuid::now_v7(),
            event_type: "email.message.received".to_string(),
            payload: serde_json::json!({
                "message_id": format!("msg-{}", Uuid::now_v7()),
                "subject": "test message",
                "body_bytes": 128,
            }),
        })
        .await?;

    let view = build_projection_readiness_view(ctx.pool()).await?;
    let entry = view
        .projections
        .iter()
        .find(|entry| {
            entry.projection_kind == EMAIL_MAILBOX_PROJECTION_KIND && entry.scope_key == mode_id
        })
        .expect("expected a read-time-computed email_mailbox entry for the seeded mode");

    assert!(entry.read_time_computed);
    assert_eq!(entry.status, ProjectionStatus::Ready);
    assert!(entry.caveats.is_empty());
    assert!(entry.built_at.is_some());

    Ok(())
}

fn registration_versioned<'a>(
    kind: &'a str,
    scope_key: &'a str,
    semantics_version: &'a str,
) -> ProjectionRegistrationInput<'a> {
    let mut input = registration(kind, scope_key);
    input.semantics_version = semantics_version;
    input
}

/// Real behavior under test (sinex-68c.4, AC "Semantics-version bumps mark
/// older projection rows stale before read surfaces claim readiness"):
/// `ProjectionRegistryRepository::begin_build` stales the current row for a
/// `(projection_kind, scope_key)` the moment a new build starts under a
/// DIFFERENT `semantics_version` -- it does not wait for the new build to
/// finish. Removing the `stale_superseded_semantics` call from
/// `insert_row` makes this test fail: the v1 row would still read `ready`
/// immediately after the v2 `begin_build`, so `ProjectionReadinessView`
/// would keep serving output built under semantics that are already known
/// to be superseded.
#[sinex_test]
async fn projection_registry_semantics_bump(ctx: TestContext) -> TestResult<()> {
    let kind = format!("test_semantics_bump_{}", Uuid::now_v7());
    let scope_key = "scope-a";
    let repo = ctx.pool().projection_registry();

    let v1_id = repo
        .begin_build(&registration_versioned(&kind, scope_key, "v1"))
        .await?;
    repo.mark_ready(v1_id, serde_json::json!({"n": 1})).await?;

    let v1_after_ready = repo
        .find_latest(&kind, scope_key)
        .await?
        .expect("v1 row must exist after mark_ready");
    assert_eq!(v1_after_ready.status, "ready");
    assert_eq!(v1_after_ready.id, v1_id);

    // Bump to v2. This must immediately stale the v1 row rather than leave
    // it `ready` until the v2 build completes.
    let v2_id = repo
        .begin_build(&registration_versioned(&kind, scope_key, "v2"))
        .await?;

    let latest = repo
        .find_latest(&kind, scope_key)
        .await?
        .expect("row must exist after v2 begin_build");
    assert_eq!(latest.id, v2_id, "the v2 building row must be current");
    assert_eq!(latest.status, "building");
    assert_eq!(latest.semantics_version, "v2");

    // `find_latest` only surfaces the current (most-recently-updated) row
    // per scope, so read the v1 row directly by id to confirm it was
    // actually transitioned rather than merely superseded in ordering.
    let v1_row: (String, Option<String>) = sqlx::query_as(
        "SELECT status, stale_reason FROM derivation.projection_registry WHERE id = $1",
    )
    .bind(v1_id)
    .fetch_one(ctx.pool())
    .await?;
    assert_eq!(v1_row.0, "stale", "v1 row must be staled by the v2 bump");
    let reason = v1_row.1.unwrap_or_default();
    assert!(
        reason.contains("semantics_version") && reason.contains("v2"),
        "stale_reason should explain the semantics version bump, got: {reason:?}"
    );

    Ok(())
}

/// Real behavior under test (sinex-68c.4, AC "Redaction policy changes mark
/// dependent projections stale or partial"): the real
/// `handle_privacy_policy_rule_add` RPC handler -- the same handler
/// `sinexctl privacy rule add` and the gateway RPC surface invoke -- stales
/// every current `derivation.projection_registry` row after a policy
/// mutation lands. Removing the `stale_projections_for_policy_change` call
/// from the handler makes this test fail: the seeded `ready` row would
/// stay `ready` after the rule is added, even though redaction behavior
/// downstream of it just changed.
#[sinex_test]
async fn projection_registry_redaction_invalidation(ctx: TestContext) -> TestResult<()> {
    let kind = format!("test_redaction_{}", Uuid::now_v7());
    let scope_key = "scope-a";
    let repo = ctx.pool().projection_registry();

    let ready_id = repo.begin_build(&registration(&kind, scope_key)).await?;
    repo.mark_ready(ready_id, serde_json::json!({})).await?;

    let before = repo
        .find_latest(&kind, scope_key)
        .await?
        .expect("row must exist before policy change");
    assert_eq!(before.status, "ready");

    let rule_name = format!("test-redaction-rule-{}", Uuid::now_v7());
    crate::api::handlers::privacy::handle_privacy_policy_rule_add(
        ctx.pool(),
        sinex_primitives::rpc::privacy::PrivacyPolicyRuleAddRequest {
            name: rule_name.clone(),
            description: "sinex-68c.4 test rule".to_string(),
            matcher_type: "literal".to_string(),
            matcher_value: "sinex-68c-4-test-secret".to_string(),
            matcher_config: serde_json::json!({}),
            context_words: Vec::new(),
            recognizer_backend_id: None,
            recognizer_kind: "local_pattern".to_string(),
            case_sensitive: false,
            action: "redact".to_string(),
            action_label: None,
            key_namespace: "default".to_string(),
        },
    )
    .await?;

    let after = repo
        .find_latest(&kind, scope_key)
        .await?
        .expect("row must still exist after policy change");
    assert_eq!(
        after.status, "stale",
        "adding a privacy rule must stale existing ready projections"
    );
    let reason = after.stale_reason.unwrap_or_default();
    assert!(
        reason.contains(&rule_name),
        "stale_reason should reference the added rule, got: {reason:?}"
    );

    Ok(())
}

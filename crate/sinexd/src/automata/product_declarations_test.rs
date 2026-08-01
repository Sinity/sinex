use super::reconcile_product_declarations;
use crate::automata::canonicalizer::CANONICALIZER_OUTPUT_DECLARATIONS;
use crate::automata::registry::AUTOMATA;
use sinex_primitives::derivation::DerivedProductClass;
use xtask::sandbox::prelude::*;

/// End-to-end proof of the gap sinex-x79t closes: against a database that
/// only ever went through real `xtask schema apply` (the sandbox template —
/// never manual row seeding), the reconciler is the ONLY thing that makes
/// `derivation.product_declarations` non-empty. Without it, every insert
/// below fails with `derivation.enforce_event_product_declaration()`'s
/// "undeclared product write" `check_violation` — the exact failure mode
/// that made every automaton in the system unable to persist a derived
/// event before this bead. Deleting the call to
/// `reconcile_product_declarations` (or breaking its INSERT) turns this red
/// with that trigger error; deleting the trigger itself would turn it green
/// for the wrong reason, which is why `automata_derivation_declarations_cover_registry`
/// in `registry_test.rs` independently pins the registry shape this test
/// depends on.
#[sinex_test]
async fn reconcile_allows_real_canonicalizer_output_insert(
    ctx: TestContext,
) -> TestResult<()> {
    let inserted = reconcile_product_declarations(ctx.pool(), AUTOMATA).await?;
    assert!(
        inserted > 0,
        "expected at least one declaration to be freshly inserted against a fresh, \
         schema-applied database with no pre-existing product_declarations rows"
    );

    // Running it again must be a pure no-op: every declaration now matches
    // what's already in the DB, so nothing new gets inserted and nothing
    // errors.
    let inserted_again = reconcile_product_declarations(ctx.pool(), AUTOMATA).await?;
    assert_eq!(
        inserted_again, 0,
        "re-running the reconciler against already-reconciled rows must insert nothing"
    );

    let declaration = &CANONICALIZER_OUTPUT_DECLARATIONS[0];
    let output_source = declaration
        .output_source
        .expect("derived_output declaration carries output_source");
    let output_event_type = declaration
        .output_event_type
        .expect("derived_output declaration carries output_event_type");

    // A derived insert requires its `source_event_ids` to reference LIVE
    // parent rows (`derived_insert_rejects_non_live_parent` in
    // `sinex-db::repositories::events::persistence_test`) — insert a real
    // material-provenance parent event first.
    let material_id = ctx
        .create_source_material(Some("sinex-x79t-reconciler-test-material"))
        .await?;
    let parent_event = Event {
        id: Some(Id::new()),
        source: EventSource::new("test.source")?,
        event_type: EventType::new("test.parent")?,
        payload: json!({"ok": true}),
        ts_orig: Some(Timestamp::now()),
        ts_quality: None,
        host: sinex_primitives::HostName::from_static("sinex-x79t-test"),
        module_run_id: None,
        payload_schema_id: None,
        provenance: sinex_primitives::events::Provenance::from_material(material_id, 0, None, None),
        anchor_payload_hash: None,
        associated_blob_ids: None,
        temporal_policy: None,
        semantics_version: None,
        scope_key: None,
        equivalence_key: None,
        created_by_operation_id: None,
        automaton_model: None,
        product_class: None,
        claim_support: None,
        derivation_declaration_id: None,
        derivation_epoch_id: None,
        derivation_lane_id: None,
        adjudication_event_id: None,
    };
    let parent_event = ctx.pool().events().insert(parent_event).await?;
    let parent_id = parent_event.id.expect("inserted parent event should have id");

    let event = Event {
        id: Some(Id::new()),
        source: EventSource::new(output_source)?,
        event_type: EventType::new(output_event_type)?,
        payload: json!({"command": "echo hi"}),
        ts_orig: Some(Timestamp::now()),
        ts_quality: None,
        host: sinex_primitives::HostName::from_static("sinex-x79t-test"),
        module_run_id: None,
        payload_schema_id: None,
        provenance: sinex_primitives::events::Provenance::from_derived([
            sinex_primitives::events::EventId::from_uuid(parent_id.to_uuid()),
        ])
        .expect("single parent id should produce derived provenance"),
        anchor_payload_hash: None,
        associated_blob_ids: None,
        temporal_policy: None,
        semantics_version: Some(declaration.semantics_version.to_string()),
        scope_key: None,
        equivalence_key: None,
        created_by_operation_id: None,
        automaton_model: None,
        product_class: Some(declaration.product_class),
        claim_support: Some(declaration.default_support.instantiate(1, 0, 1, 0)),
        derivation_declaration_id: Some(declaration.declaration_id.to_string()),
        derivation_epoch_id: None,
        derivation_lane_id: None,
        adjudication_event_id: None,
    };

    // This is the real production write path's shape: the runtime adapter's
    // `build_output_event()` (crate::runtime::automaton::adapter::output)
    // stamps exactly these six derivation columns from a `DerivedOutput`
    // before handing the event to `EventRepository::insert`. Going through
    // `EventRepository::insert` directly exercises the same DB trigger
    // without needing a full `AutomatonContext`.
    let inserted_event = ctx.pool().events().insert(event).await?;
    assert_eq!(inserted_event.product_class, Some(declaration.product_class));
    assert_eq!(
        inserted_event.derivation_declaration_id.as_deref(),
        Some(declaration.declaration_id)
    );

    Ok(())
}

/// Named for sinex-0vx.5's original VERIFY line
/// (`xtask test -p sinexd -E 'test(product_declaration_startup_diff)'`,
/// carried over unchanged when the startup-reconcile half of that bead was
/// split out into sinex-x79t). Proves the "startup diff" framing literally:
/// a binary that reconciles successfully once, then observes the DB row for
/// one of its own declarations get changed out from under it (e.g. an
/// older/newer binary version, or a manual edit) between the first and
/// second reconcile call, refuses to start on the second call rather than
/// silently accepting the drifted value. Complements
/// `reconcile_rejects_conflicting_existing_row` (which seeds the conflict
/// before the reconciler ever runs) by proving the diff is caught even when
/// the first run legitimately succeeded.
#[sinex_test]
async fn product_declaration_startup_diff(ctx: TestContext) -> TestResult<()> {
    let declaration = &CANONICALIZER_OUTPUT_DECLARATIONS[0];

    let first_run = reconcile_product_declarations(ctx.pool(), AUTOMATA).await?;
    assert!(
        first_run > 0,
        "first reconcile against a fresh schema-applied database must insert declarations"
    );

    // Simulate drift: something other than the reconciler changed the row
    // for a declaration_id the static registry still declares.
    sqlx::query!(
        "UPDATE derivation.product_declarations SET product_class = $1 WHERE declaration_id = $2",
        DerivedProductClass::AnalysisClaim.as_str(),
        declaration.declaration_id,
    )
    .execute(ctx.pool())
    .await
    .map_err(|error| color_eyre::eyre::eyre!("simulate DB drift: {error}"))?;

    let error = reconcile_product_declarations(ctx.pool(), AUTOMATA)
        .await
        .expect_err("a second reconcile must fail closed on the drifted row, not accept it");
    assert!(
        error.to_string().contains(declaration.declaration_id),
        "startup-diff error should name the drifted declaration_id"
    );

    Ok(())
}

/// Fail-closed AC: a `derivation.product_declarations` row that already
/// exists for a registered automaton's `declaration_id` but disagrees on
/// shape (here: `product_class`) must make the reconciler refuse to start,
/// never silently overwrite the row and never silently accept the DB's
/// stale value.
#[sinex_test]
async fn reconcile_rejects_conflicting_existing_row(ctx: TestContext) -> TestResult<()> {
    let declaration = &CANONICALIZER_OUTPUT_DECLARATIONS[0];
    assert_eq!(declaration.product_class, DerivedProductClass::CanonicalDerivedEvent);

    let conflicting = sinex_primitives::derivation::DerivationOutputDeclaration {
        product_class: DerivedProductClass::AnalysisClaim,
        ..*declaration
    };

    // Seed the conflicting row directly through the repository, bypassing
    // the reconciler entirely — this simulates a DB that drifted from the
    // code (e.g. an older binary's declaration, or a manual edit).
    ctx.pool().product_declarations().insert(&conflicting).await?;

    let error = reconcile_product_declarations(ctx.pool(), AUTOMATA)
        .await
        .expect_err("reconciler must refuse to start on a conflicting existing row");
    let message = error.to_string();
    assert!(
        message.contains(declaration.declaration_id),
        "error should name the conflicting declaration_id, got: {message}"
    );
    assert!(
        message.contains("product_class"),
        "error should name the specific field that disagreed, got: {message}"
    );

    Ok(())
}

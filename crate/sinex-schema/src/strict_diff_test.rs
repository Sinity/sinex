use super::{
    DECLARED_FK_ACTIONS, DECLARED_INLINE_CHECKS, DeclaredForeignKeyAction, DriftCategory,
    HYPERTABLE_CHUNK_INTERVAL_MICROS, StrictDrift, foreign_key_action_drifts,
    hypertable_chunk_interval_drift, hypertable_retention_policy_drift, inline_check_drift,
    orphan_column_drifts_for_table,
};
use xtask::sandbox::prelude::sinex_test;

#[sinex_test]
async fn drift_category_display_round_trip() -> xtask::sandbox::TestResult<()> {
    // The Display impl is what `sinex-schema diff --strict` would surface
    // in operator-friendly output. Pin it so a refactor of the enum
    // names doesn't silently break consumer formatting.
    assert_eq!(format!("{}", DriftCategory::TriggerBody), "trigger_body");
    assert_eq!(
        format!("{}", DriftCategory::ColumnDefault),
        "column_default"
    );
    assert_eq!(
        format!("{}", DriftCategory::ForeignKeyAction),
        "foreign_key_action"
    );
    assert_eq!(
        format!("{}", DriftCategory::InlineCheckExpr),
        "inline_check_expr"
    );
    assert_eq!(
        format!("{}", DriftCategory::HypertableSetting),
        "hypertable_setting"
    );
    assert_eq!(format!("{}", DriftCategory::Comment), "comment");
    assert_eq!(format!("{}", DriftCategory::OrphanColumn), "orphan_column");
    Ok(())
}

#[sinex_test]
async fn strict_drift_display_includes_location_and_summaries() -> xtask::sandbox::TestResult<()>
{
    let drift = StrictDrift {
        category: DriftCategory::ColumnDefault,
        location: "core.events.ts_persisted".to_string(),
        declared_summary: "contains `now()`".to_string(),
        observed_summary: "no DEFAULT set".to_string(),
    };
    let rendered = format!("{drift}");
    assert!(rendered.contains("column_default"));
    assert!(rendered.contains("core.events.ts_persisted"));
    assert!(rendered.contains("now()"));
    assert!(rendered.contains("no DEFAULT set"));
    Ok(())
}

#[sinex_test]
async fn inline_check_drift_reports_when_required_markers_are_split()
-> xtask::sandbox::TestResult<()> {
    let declared = DECLARED_INLINE_CHECKS
        .iter()
        .find(|check| check.label == "xor_provenance")
        .expect("xor provenance strict-diff expectation is declared");
    let partial_definitions = vec![
        "CHECK ((source_material_id IS NOT NULL) AND (source_event_ids IS NULL))".to_string(),
        "CHECK ((source_material_id IS NULL))".to_string(),
    ];

    let drift = inline_check_drift(declared, &partial_definitions)
        .expect("split markers across constraints must not satisfy one inline check");

    assert_eq!(drift.category, DriftCategory::InlineCheckExpr);
    assert_eq!(drift.location, "core.events::xor_provenance");
    assert!(drift.declared_summary.contains("source_material_id"));
    assert_eq!(drift.observed_summary, "2 CHECK constraint(s); none match");

    let matching_definition = vec![declared.expected_markers.join(" AND ")];
    assert!(
        inline_check_drift(declared, &matching_definition).is_none(),
        "one CHECK containing every declared marker is not drift"
    );
    Ok(())
}

#[sinex_test]
async fn inline_check_drift_reports_missing_constraints() -> xtask::sandbox::TestResult<()> {
    let declared = DECLARED_INLINE_CHECKS
        .iter()
        .find(|check| check.label == "anchor_byte_non_negative")
        .expect("anchor-byte strict-diff expectation is declared");

    let drift = inline_check_drift(declared, &[])
        .expect("absence of inline CHECK definitions must be reported");

    assert_eq!(drift.category, DriftCategory::InlineCheckExpr);
    assert_eq!(drift.location, "core.events::anchor_byte_non_negative");
    assert_eq!(drift.observed_summary, "table has no CHECK constraints");
    Ok(())
}

#[sinex_test]
async fn inline_check_drift_reports_partial_marker_subset() -> xtask::sandbox::TestResult<()> {
    let declared = DECLARED_INLINE_CHECKS
        .iter()
        .find(|check| check.label == "offset_kind_enum")
        .expect("offset-kind enum strict-diff expectation is declared");
    let partial_definition =
        vec!["CHECK ((offset_kind = ANY (ARRAY['byte', 'line', 'rowid'])))".to_string()];

    let drift = inline_check_drift(declared, &partial_definition)
        .expect("a CHECK missing one declared enum marker must not satisfy strict diff");

    assert_eq!(drift.category, DriftCategory::InlineCheckExpr);
    assert_eq!(drift.location, "core.events::offset_kind_enum");
    assert!(drift.declared_summary.contains("'logical'"));
    assert_eq!(drift.observed_summary, "1 CHECK constraint(s); none match");
    Ok(())
}

#[sinex_test]
async fn foreign_key_action_drift_reports_missing_delete_action()
-> xtask::sandbox::TestResult<()> {
    let declared = DECLARED_FK_ACTIONS
        .iter()
        .find(|fk| fk.table == "tagged_items")
        .expect("tagged_items FK action strict-diff expectation is declared");
    let definitions =
        vec!["FOREIGN KEY (tag_id) REFERENCES core.tags(id) ON DELETE NO ACTION".to_string()];

    let drifts = foreign_key_action_drifts(declared, &definitions);

    assert_eq!(drifts.len(), 1);
    let drift = &drifts[0];
    assert_eq!(drift.category, DriftCategory::ForeignKeyAction);
    assert_eq!(
        drift.location,
        "core.tagged_items FOREIGN KEY (tag_id) (ON DELETE)"
    );
    assert_eq!(drift.declared_summary, "contains `ON DELETE CASCADE`");
    assert!(drift.observed_summary.contains("ON DELETE NO ACTION"));
    Ok(())
}

#[sinex_test]
async fn foreign_key_action_drift_reports_missing_fk_definition()
-> xtask::sandbox::TestResult<()> {
    let declared = DECLARED_FK_ACTIONS
        .iter()
        .find(|fk| fk.table == "tags")
        .expect("tags self-FK action strict-diff expectation is declared");
    let definitions = vec!["FOREIGN KEY (other_id) REFERENCES core.tags(id)".to_string()];

    let drifts = foreign_key_action_drifts(declared, &definitions);

    assert_eq!(drifts.len(), 1);
    let drift = &drifts[0];
    assert_eq!(drift.category, DriftCategory::ForeignKeyAction);
    assert_eq!(drift.location, "core.tags FOREIGN KEY (parent_tag_id)");
    assert!(drift.declared_summary.contains("ON DELETE SET NULL"));
    assert!(
        drift
            .observed_summary
            .contains("no FK on core.tags matches")
    );
    Ok(())
}

#[sinex_test]
async fn foreign_key_action_drift_reports_missing_update_action()
-> xtask::sandbox::TestResult<()> {
    let declared = DeclaredForeignKeyAction {
        schema: "core",
        table: "child_rows",
        fk_marker: "FOREIGN KEY (parent_id)",
        expected_delete_action_marker: None,
        expected_update_action_marker: Some("ON UPDATE CASCADE"),
    };
    let definitions = vec![
        "FOREIGN KEY (parent_id) REFERENCES core.parent_rows(id) ON DELETE CASCADE".to_string(),
    ];

    let drifts = foreign_key_action_drifts(&declared, &definitions);

    assert_eq!(drifts.len(), 1);
    let drift = &drifts[0];
    assert_eq!(drift.category, DriftCategory::ForeignKeyAction);
    assert_eq!(
        drift.location,
        "core.child_rows FOREIGN KEY (parent_id) (ON UPDATE)"
    );
    assert_eq!(drift.declared_summary, "contains `ON UPDATE CASCADE`");
    assert!(drift.observed_summary.contains("ON DELETE CASCADE"));
    Ok(())
}

#[sinex_test]
async fn orphan_column_drift_ignores_declared_and_drop_allowlisted_columns()
-> xtask::sandbox::TestResult<()> {
    let live_cols = vec![
        "id".to_string(),
        "payload".to_string(),
        "old_name".to_string(),
        "pending_name".to_string(),
    ];
    let declared_names = vec!["id".to_string(), "payload".to_string()];
    let pending_drop = vec!["pending_name".to_string()];

    let drifts = orphan_column_drifts_for_table(
        "core.events",
        &live_cols,
        &declared_names,
        &["old_name"],
        &pending_drop,
    );

    assert!(
        drifts.is_empty(),
        "allow-listed columns are not orphan drift: {drifts:?}"
    );
    Ok(())
}

#[sinex_test]
async fn orphan_column_drift_reports_live_column_outside_source_and_allowlists()
-> xtask::sandbox::TestResult<()> {
    let live_cols = vec!["id".to_string(), "rogue_col".to_string()];
    let declared_names = vec!["id".to_string()];
    let pending_drop = Vec::<String>::new();

    let drifts = orphan_column_drifts_for_table(
        "core.events",
        &live_cols,
        &declared_names,
        &[] as &[&str],
        &pending_drop,
    );

    assert_eq!(drifts.len(), 1);
    assert_eq!(drifts[0].category, DriftCategory::OrphanColumn);
    assert_eq!(drifts[0].location, "core.events.rogue_col");
    assert!(
        drifts[0]
            .observed_summary
            .contains("not declared in source")
    );
    Ok(())
}

#[sinex_test]
async fn hypertable_setting_drift_reports_chunk_interval_states()
-> xtask::sandbox::TestResult<()> {
    assert!(
        hypertable_chunk_interval_drift(Some((Some(HYPERTABLE_CHUNK_INTERVAL_MICROS),)))
            .is_none(),
        "declared 7-day chunk interval is not drift"
    );

    let drift = hypertable_chunk_interval_drift(Some((Some(60_000_000),)))
        .expect("wrong chunk interval must be reported");
    assert_eq!(drift.category, DriftCategory::HypertableSetting);
    assert_eq!(drift.location, "core.events::chunk_interval");
    assert!(drift.declared_summary.contains("7 days"));
    assert_eq!(drift.observed_summary, "interval_length = 60000000");

    let missing =
        hypertable_chunk_interval_drift(None).expect("missing hypertable must be reported");
    assert_eq!(missing.location, "core.events");
    assert_eq!(missing.observed_summary, "core.events is not a hypertable");
    Ok(())
}

#[sinex_test]
async fn hypertable_setting_drift_reports_retention_policy() -> xtask::sandbox::TestResult<()> {
    assert!(
        hypertable_retention_policy_drift(0).is_none(),
        "declared state has no retention-policy drift"
    );

    let drift =
        hypertable_retention_policy_drift(2).expect("retention policy jobs must be reported");
    assert_eq!(drift.category, DriftCategory::HypertableSetting);
    assert_eq!(drift.location, "core.events::retention_policy");
    assert_eq!(drift.declared_summary, "no retention policy");
    assert_eq!(drift.observed_summary, "2 retention policy job(s) present");
    Ok(())
}

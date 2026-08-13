use super::*;

#[test]
fn normalization_removes_catalog_formatting_without_erasing_body_tokens() {
    assert_eq!(normalize_sql("CHECK (x >= 0) NOT VALID"), "check (x >= 0)");
    assert_eq!(
        normalize_sql("CREATE INDEX IF NOT EXISTS \"ix\" ON \"core\".\"events\" (id)"),
        "create index if not exists ix on core.events (id)"
    );
}

#[test]
fn index_comparison_is_not_name_only() {
    assert!(index_definition_matches(
        "CREATE INDEX IF NOT EXISTS ix ON core.events (source, ts_orig DESC)",
        "CREATE INDEX core.ix ON core.events USING btree (source, ts_orig DESC)",
    ));
    assert!(!index_definition_matches(
        "CREATE INDEX IF NOT EXISTS ix ON core.events (source, ts_orig DESC)",
        "CREATE INDEX core.ix ON core.events USING btree (source, ts_coided DESC)",
    ));
}

#[test]
fn inline_check_requires_one_complete_constraint_body() {
    let markers = vec![
        "source_material_id IS NOT NULL".to_string(),
        "source_event_ids IS NULL".to_string(),
    ];
    assert!(!inline_check_matches(
        &markers,
        &[
            "CHECK (source_material_id IS NOT NULL)".to_string(),
            "CHECK (source_event_ids IS NULL)".to_string()
        ],
    ));
    assert!(inline_check_matches(
        &markers,
        &["CHECK (source_material_id IS NOT NULL AND source_event_ids IS NULL)".to_string()],
    ));
}

#[test]
fn trigger_comparison_catches_disabled_and_retargeted_same_name() {
    let expected = TriggerExpectation {
        schema: "core",
        table: "events",
        name: "trg_events_no_update",
        enabled: "O",
        definition_markers: &["before update", "core.fn_events_no_update()"],
    };
    let definition = "CREATE TRIGGER trg_events_no_update BEFORE UPDATE ON core.events FOR EACH ROW EXECUTE FUNCTION core.fn_events_no_update()";
    assert!(trigger_definition_matches(&expected, "O", definition));
    assert!(!trigger_definition_matches(&expected, "D", definition));
    assert!(!trigger_definition_matches(
        &expected,
        "O",
        definition
            .replace("fn_events_no_update", "other_function")
            .as_str()
    ));
}

#[test]
fn function_comparison_is_exact_after_normalization() {
    let expected = body_hash("BEGIN\n  RETURN NEW;\nEND");
    assert!(function_body_matches(&expected, "BEGIN RETURN NEW; END"));
    assert!(!function_body_matches(&expected, "BEGIN RETURN OLD; END"));
}

#[test]
fn expectation_registry_covers_reflection_and_archive_provenance_checks() {
    let checks = inline_check_expectations();
    for table in ["core.events", "reflection.events", "audit.archived_events"] {
        assert!(
            checks
                .iter()
                .any(|check| check.name.as_deref() == Some(&format!("{table}::xor_provenance")))
        );
    }
}

#[test]
fn source_table_definitions_generate_typed_expectations() {
    let tables = table_expectations().expect("table expectation registry must build");
    let events = tables
        .iter()
        .find(|table| table.schema == "core" && table.table == "events")
        .expect("core.events expectation");
    let ts_coided = events
        .columns
        .iter()
        .find(|column| column.name == "ts_coided")
        .expect("ts_coided declaration");
    assert_eq!(ts_coided.type_sql, "timestamp with time zone");
    assert!(ts_coided.generated_sql.is_some());
    assert!(
        events
            .indexes
            .iter()
            .any(|index| index.name == "ix_events_ts_orig")
    );
    assert!(events.constraints.iter().any(|constraint| constraint.name.as_deref() == Some("events_material_anchor_required")));
}

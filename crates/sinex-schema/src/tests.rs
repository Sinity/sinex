use super::*;

/// A payload field named like one of the view's event columns makes the
/// kind's view, and so the whole schema, fail to apply.
#[devkit::test]
fn no_payload_field_shadows_an_event_column() {
    let event_columns = [
        "id",
        "source",
        "ts_orig",
        "ts_quality",
        "ts_coided",
        "record_state",
    ];
    for kind in event_types::ALL {
        let schema = (kind.json_schema)();
        let props = schema.get("properties").and_then(|p| p.as_object());
        for field in props.into_iter().flat_map(|p| p.keys()) {
            assert!(
                !event_columns.contains(&field.as_str()),
                "{} has a payload field {field:?} that shadows an event column",
                kind.event_type
            );
        }
    }
}

#[devkit::test]
fn declarations_are_deterministic_and_named() {
    assert_eq!(declarations(), declarations());
    assert_eq!(declaration_hash().len(), 16);
    let all = declarations().join("\n");
    for table in [
        "core.events",
        "core.live_locations",
        "core.live_equivalence",
        "core.hidden_records",
        "core.commit_log",
        "audit.archived",
        "raw.materials",
        "raw.positions",
        "derivation.dirty",
        "core.admission_stage",
        "core.source_counts",
    ] {
        assert!(all.contains(&format!("TABLE {table} (")), "{table}");
    }
    assert!(all.contains("CREATE VIEW event_types.\"command_executed\""));
}

#[devkit::test]
fn each_version_after_0_has_its_upgrade() {
    assert_eq!(i32::try_from(upgrades::upgrades().len()).unwrap(), VERSION);
}

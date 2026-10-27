use super::*;

#[test]
fn column_lists_keep_nested_commas_and_quotes() {
    assert_eq!(entries("a text CHECK (a IN ('one,two', 'it''s')), b text[] DEFAULT ARRAY['a,b','c']"),
        vec!["a text CHECK (a IN ('one,two', 'it''s'))", "b text[] DEFAULT ARRAY['a,b','c']"]);
}

#[test]
fn kind_upgrade_uses_the_current_column_definitions() {
    let current = Description::current();
    let before = Description::version_three();
    let changes = current.changes_from(&before, &["registry.event_types"]);
    for table in ["core.events", "telemetry.events", "core.admission_stage"] {
        assert!(changes.iter().any(|sql| sql.starts_with(&format!("ALTER TABLE {table} ADD COLUMN kind_version bigint NOT NULL"))));
        assert!(changes.iter().any(|sql| sql.starts_with(&format!("ALTER TABLE {table} ADD COLUMN validation text NOT NULL"))));
    }
    for kind in &current.kinds {
        assert!(current.views.iter().any(|view| view.name == kind.kind.name.replace('.', "_")));
    }
    assert_eq!(crate::upgrades::NAMED, &["kind_declarations"]);
}

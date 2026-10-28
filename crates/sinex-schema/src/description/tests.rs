use super::*;

#[test]
fn column_lists_keep_nested_commas_and_quotes() {
    assert_eq!(
        entries("a text CHECK (a IN ('one,two', 'it''s')), b text[] DEFAULT ARRAY['a,b','c']"),
        vec![
            "a text CHECK (a IN ('one,two', 'it''s'))",
            "b text[] DEFAULT ARRAY['a,b','c']"
        ]
    );
}

#[test]
fn kind_upgrade_uses_the_current_column_definitions() {
    let current = Description::current();
    let before = Description::before_kind_declarations();
    let changes = current.changes_from(&before, &["registry.event_types"]);
    for table in ["core.events", "telemetry.events", "core.admission_stage"] {
        assert!(changes.iter().any(|sql| sql.starts_with(&format!(
            "ALTER TABLE {table} ADD COLUMN kind_version bigint NOT NULL"
        ))));
        assert!(changes.iter().any(|sql| sql.starts_with(&format!(
            "ALTER TABLE {table} ADD COLUMN validation text NOT NULL"
        ))));
    }
    for kind in &current.kinds {
        assert!(
            current
                .views
                .iter()
                .any(|view| view.name == kind.kind.name.replace('.', "_"))
        );
    }
    assert_eq!(
        crate::upgrades::NAMED,
        &[
            "blob_change_tracking",
            "library_scans",
            "compact_equivalence_keys",
            "search_text",
            "publication_pairs",
            "kind_declarations"
        ]
    );
}

#[test]
fn table_columns_ignore_standalone_comments() {
    let table = Table::read("CREATE TABLE example (\n-- a row's comma, quote and ( are prose\na text,\n-- another row's constraint\nb bigint\n)").unwrap();
    assert_eq!(
        table.columns,
        BTreeMap::from([("a".into(), "text".into()), ("b".into(), "bigint".into())])
    );
}

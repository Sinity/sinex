use super::*;

fn described(tables: &[&str], after_tables: &[&str]) -> Description {
    Description {
        tables: tables.iter().map(|s| s.to_string()).collect(),
        after_tables: after_tables.iter().map(|s| s.to_string()).collect(),
        visibility: vec![],
        views: vec![],
        kinds: vec![],
    }
}

#[test]
fn an_upgrade_adds_new_columns_together_and_new_tables_and_statements_as_declared() {
    let before = described(
        &[
            "CREATE TABLE core.notes (\n  id uuid PRIMARY KEY,\n  -- the note, quoted as written\n  text text NOT NULL CHECK (text <> 'a,b')\n)",
        ],
        &["CREATE INDEX notes_text ON core.notes (text)"],
    );
    let after = described(
        &[
            "CREATE TABLE core.notes (\n  id uuid PRIMARY KEY,\n  text text NOT NULL CHECK (text <> 'a,b'),\n  author text NOT NULL DEFAULT '',\n  tags text[] NOT NULL DEFAULT ARRAY['x,y']\n)",
            "CREATE TABLE core.authors (\n  name text PRIMARY KEY\n)",
            "GRANT SELECT ON core.authors TO sinex_reader",
        ],
        &[
            "CREATE INDEX notes_text ON core.notes (text)",
            "CREATE INDEX notes_author ON core.notes (author)",
        ],
    );
    assert_eq!(
        after.changes_from(&before),
        [
            "ALTER TABLE core.notes ADD COLUMN author text NOT NULL DEFAULT '', ADD COLUMN tags text[] NOT NULL DEFAULT ARRAY['x,y']",
            "CREATE TABLE core.authors (\n  name text PRIMARY KEY\n)",
            "GRANT SELECT ON core.authors TO sinex_reader",
            "CREATE INDEX notes_author ON core.notes (author)",
        ]
    );
    assert!(after.changes_from(&after).is_empty());
}

#[test]
#[should_panic(expected = "column changes need a declared transformation: core.notes.text")]
fn a_changed_column_needs_a_declared_transformation() {
    let before = described(&["CREATE TABLE core.notes (\n  text text\n)"], &[]);
    let after = described(&["CREATE TABLE core.notes (\n  text bytea\n)"], &[]);
    after.changes_from(&before);
}

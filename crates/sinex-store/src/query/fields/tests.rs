use super::*;
use sinex_model::query::{ALIASES, FieldType};

#[devkit::test]
fn every_field_in_the_model_has_sql() {
    for table in Table::ALL {
        for f in table.fields() {
            assert!(
                field(table, f.name).is_some(),
                "{}.{}",
                table.name(),
                f.name
            );
        }
        assert!(field(table, table.main_time()).is_some());
    }
    for alias in ALIASES {
        for f in alias.fields {
            assert!(
                alias_field(alias.name, f.name).is_some(),
                "{}.{}",
                alias.name,
                f.name
            );
        }
        if alias.spans {
            assert!(
                span(Table::Events, Some(alias.name)).is_some(),
                "{}",
                alias.name
            );
            assert!(
                alias
                    .fields
                    .iter()
                    .any(|f| f.name == "start" && f.ty == FieldType::Time)
            );
        }
    }
}

#[test]
fn unknown_interval_ends_do_not_gain_durations_and_boot_fences_are_not_open() {
    let duration = super::alias_field("intervals", "duration").unwrap();
    assert!(!duration.contains("now()"));
    let open = super::alias_field("now", "open").unwrap();
    assert!(open.contains("fenced_at"));
}

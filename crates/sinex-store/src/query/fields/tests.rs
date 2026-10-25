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

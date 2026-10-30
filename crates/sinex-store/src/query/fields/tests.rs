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

#[devkit::test]
async fn transformed_interval_end_placeholders_remain_unknown_without_cast_failures(
    db: devkit::pg::TestDatabase,
) {
    use sqlx::Connection;
    let mut conn = sqlx::PgConnection::connect(&db.url(None)).await.unwrap();
    let (_, end, _) = span(Table::Events, Some("intervals")).unwrap();
    let sql = format!(
        "WITH e AS (SELECT $1::jsonb AS payload) SELECT {} FROM e",
        end.replace("{t}", "e")
    );
    let hidden: Option<time::OffsetDateTime> = sqlx::query_scalar(&sql)
        .bind(serde_json::json!({"ended_at":"[masked]","fenced_at":"hash:opaque"}))
        .fetch_one(&mut conn)
        .await
        .unwrap();
    assert!(hidden.is_none());
    let known: Option<time::OffsetDateTime> = sqlx::query_scalar(&sql)
        .bind(serde_json::json!({"ended_at":"[masked]","fenced_at":"2026-01-02T03:04:05Z"}))
        .fetch_one(&mut conn)
        .await
        .unwrap();
    assert_eq!(
        known,
        Some(
            sinex_model::Timestamp::from_unix_secs(1767323045)
                .unwrap()
                .0
        )
    );
}

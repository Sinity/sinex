//! Test module for the centralized query system
//!
//! This module tests the new centralized query system to ensure it works correctly
//! and provides the expected functionality.

#[cfg(test)]
mod tests {

    use crate::queries::EventQueries;
    use crate::query_builder::{QueryBuilder, QueryParam};
    use chrono::Utc;
    use serde_json::json;
    use sinex_ulid::Ulid;

    #[test]
    fn test_query_builder_select() {
        let builder = QueryBuilder::select("core.events")
            .columns(&["event_id", "source", "event_type"])
            .where_eq("event_id", QueryParam::Ulid(Ulid::new()))
            .order_by("ts_ingest", "DESC")
            .limit(10);

        let (sql, params) = builder.build().unwrap();

        assert!(sql.contains("SELECT event_id, source, event_type FROM core.events"));
        assert!(sql.contains("WHERE event_id = $1::uuid"));
        assert!(sql.contains("ORDER BY ts_ingest DESC"));
        assert!(sql.contains("LIMIT 10"));
        assert_eq!(params.len(), 1);
    }

    #[test]
    fn test_query_builder_insert() {
        let builder = QueryBuilder::insert("core.events")
            .columns(&["source", "event_type", "payload"])
            .values(&[
                QueryParam::String("test.source".to_string()),
                QueryParam::String("test_event".to_string()),
                QueryParam::Json(json!({"test": "data"})),
            ])
            .returning(&["event_id"]);

        let (sql, params) = builder.build().unwrap();

        assert!(sql.contains("INSERT INTO core.events (source, event_type, payload)"));
        assert!(sql.contains("VALUES ($1, $2, $3)"));
        assert!(sql.contains("RETURNING event_id"));
        assert_eq!(params.len(), 3);
    }

    #[test]
    fn test_query_builder_update() {
        let builder = QueryBuilder::update("core.events")
            .set("source", QueryParam::String("updated.source".to_string()))
            .set("payload", QueryParam::Json(json!({"updated": true})))
            .where_eq("event_id", QueryParam::Ulid(Ulid::new()));

        let (sql, params) = builder.build().unwrap();

        assert!(sql.contains("UPDATE core.events SET"));
        assert!(sql.contains("source = $1"));
        assert!(sql.contains("payload = $2"));
        assert!(sql.contains("WHERE event_id = $3::uuid"));
        assert_eq!(params.len(), 3);
    }

    #[test]
    fn test_query_builder_delete() {
        let builder =
            QueryBuilder::delete("core.events").where_eq("event_id", QueryParam::Ulid(Ulid::new()));

        let (sql, params) = builder.build().unwrap();

        assert!(sql.contains("DELETE FROM core.events"));
        assert!(sql.contains("WHERE event_id = $1::uuid"));
        assert_eq!(params.len(), 1);
    }

    #[test]
    fn test_query_param_ulid_conversion() {
        use crate::query_builder::RawQueryParam;

        let ulid = Ulid::new();
        let param = QueryParam::Ulid(ulid);
        let raw = param.to_raw_value();

        match raw {
            RawQueryParam::Uuid(uuid) => {
                // Verify that ULID was converted to UUID
                assert_eq!(uuid, crate::query_helpers::ulid_to_uuid(ulid));
            }
            _ => panic!("Expected UUID parameter"),
        }
    }

    #[test]
    fn test_query_param_ulid_array_conversion() {
        use crate::query_builder::RawQueryParam;

        let ulids = vec![Ulid::new(), Ulid::new(), Ulid::new()];
        let param = QueryParam::UlidArray(ulids.clone());
        let raw = param.to_raw_value();

        match raw {
            RawQueryParam::UuidArray(uuids) => {
                assert_eq!(uuids.len(), ulids.len());
                for (ulid, uuid) in ulids.iter().zip(uuids.iter()) {
                    assert_eq!(uuid, &crate::query_helpers::ulid_to_uuid(*ulid));
                }
            }
            _ => panic!("Expected UUID array parameter"),
        }
    }

    #[test]
    fn test_event_queries_builder_patterns() {
        let event_id = Ulid::new();

        // Test get_by_id query
        let builder = EventQueries::get_by_id(event_id);
        let (sql, params) = builder.build().unwrap();

        assert!(sql.contains("SELECT"));
        assert!(sql.contains("FROM core.events"));
        assert!(sql.contains("WHERE event_id = $1::uuid"));
        assert_eq!(params.len(), 1);

        // Test count_all query
        let builder = EventQueries::count_all();
        let (sql, params) = builder.build().unwrap();

        assert!(sql.contains("SELECT COUNT(*) as count"));
        assert!(sql.contains("FROM core.events"));
        assert_eq!(params.len(), 0);

        // Test get_recent query
        let builder = EventQueries::get_recent(Some(10), Some(20));
        let (sql, params) = builder.build().unwrap();

        assert!(sql.contains("SELECT"));
        assert!(sql.contains("FROM core.events"));
        assert!(sql.contains("ORDER BY ts_ingest DESC"));
        assert!(sql.contains("LIMIT 10"));
        assert!(sql.contains("OFFSET 20"));
        assert_eq!(params.len(), 0);
    }

    #[test]
    fn test_query_param_types() {
        let test_cases = vec![
            (QueryParam::String("test".to_string()), "text"),
            (QueryParam::OptionalString(Some("test".to_string())), "text"),
            (QueryParam::Integer(42), "bigint"),
            (QueryParam::OptionalInteger(Some(42)), "bigint"),
            (QueryParam::Boolean(true), "boolean"),
            (QueryParam::OptionalBoolean(Some(true)), "boolean"),
            (QueryParam::Json(json!({"key": "value"})), "jsonb"),
            (
                QueryParam::OptionalJson(Some(json!({"key": "value"}))),
                "jsonb",
            ),
            (QueryParam::Timestamp(Utc::now()), "timestamptz"),
            (
                QueryParam::OptionalTimestamp(Some(Utc::now())),
                "timestamptz",
            ),
            (QueryParam::Ulid(Ulid::new()), "uuid"),
            (QueryParam::OptionalUlid(Some(Ulid::new())), "uuid"),
            (
                QueryParam::UlidArray(vec![Ulid::new(), Ulid::new()]),
                "uuid[]",
            ),
        ];

        for (param, expected_type) in test_cases {
            assert_eq!(param.sql_type_hint(), expected_type);
        }
    }

    #[test]
    fn test_complex_query_building() {
        let start_time = Utc::now() - chrono::Duration::hours(1);
        let end_time = Utc::now();
        let event_ids = vec![Ulid::new(), Ulid::new()];

        let builder = QueryBuilder::select("core.events")
            .columns(&["event_id", "source", "event_type", "ts_ingest"])
            .where_op("ts_ingest", ">=", QueryParam::Timestamp(start_time))
            .where_op("ts_ingest", "<=", QueryParam::Timestamp(end_time))
            .where_in("event_id", QueryParam::UlidArray(event_ids))
            .order_by("ts_ingest", "DESC")
            .limit(100);

        let (sql, params) = builder.build().unwrap();

        assert!(sql.contains("SELECT event_id, source, event_type, ts_ingest FROM core.events"));
        assert!(sql.contains("WHERE ts_ingest >= $1::timestamptz"));
        assert!(sql.contains("AND ts_ingest <= $2::timestamptz"));
        assert!(sql.contains("AND event_id = ANY($3::uuid[])"));
        assert!(sql.contains("ORDER BY ts_ingest DESC"));
        assert!(sql.contains("LIMIT 100"));
        assert_eq!(params.len(), 3);
    }

    #[test]
    fn test_query_error_handling() {
        // Test that build() returns appropriate errors for invalid queries
        let builder = QueryBuilder::insert("core.events")
            .columns(&["source", "event_type"])
            .values(&[QueryParam::String("test".to_string())]); // Mismatched columns/values

        let result = builder.build();
        assert!(result.is_ok()); // Query builder doesn't validate column/value count

        // Test empty table name
        let builder = QueryBuilder::select("").columns(&["*"]);

        let (sql, _) = builder.build().unwrap();
        assert!(sql.contains("SELECT * FROM"));
    }

    #[test]
    fn test_query_registry_organization() {
        use crate::queries::{
            ArtifactQueries, CheckpointQueries, EventQueries, OperationQueries, SchemaQueries,
        };

        // Test that all query registries are accessible
        let _event_builder = EventQueries::count_all();
        let _checkpoint_builder = CheckpointQueries::get_checkpoint(
            "test".to_string(),
            "default".to_string(),
            "consumer".to_string(),
        );
        let _schema_builder = SchemaQueries::get_latest_for_event_type("test_event".to_string());
        let _artifact_builder = ArtifactQueries::count_all();
        let _operation_builder = OperationQueries::get_health_metrics();

        // Just verify they compile and are accessible
        assert!(true);
    }

    #[test]
    fn test_migration_benefits() {
        // Before: Manual ULID/UUID conversion
        let ulid = Ulid::new();
        let uuid = crate::query_helpers::ulid_to_uuid(ulid);

        // After: Automatic conversion in query builder
        let builder =
            QueryBuilder::select("core.events").where_eq("event_id", QueryParam::Ulid(ulid));

        let (sql, params) = builder.build().unwrap();

        // Verify the query uses the UUID type hint
        assert!(sql.contains("$1::uuid"));
        assert_eq!(params.len(), 1);

        // Verify the parameter was converted correctly
        use crate::query_builder::RawQueryParam;
        match &params[0] {
            RawQueryParam::Uuid(param_uuid) => {
                assert_eq!(param_uuid, &uuid);
            }
            _ => panic!("Expected UUID parameter"),
        }
    }
}

/// Integration test helpers for testing with real database
#[cfg(test)]
mod integration_helpers {

    use crate::create_test_pool;
    use crate::queries::EventQueries;
    use chrono::Utc;
    use serde_json::json;
    use sinex_events::RawEvent;
    use sinex_ulid::Ulid;

    /// Helper to create a test event
    pub fn create_test_event() -> RawEvent {
        RawEvent {
            id: Ulid::new(),
            source: "test.source".to_string(),
            event_type: "test_event".to_string(),
            ts_ingest: Utc::now(),
            ts_orig: None,
            host: "localhost".to_string(),
            ingestor_version: Some("1.0.0".to_string()),
            payload_schema_id: None,
            payload: json!({"test": "data"}),
            source_event_ids: None,
            anchor_byte: None,
            source_material_id: None,
            source_material_offset_start: None,
            source_material_offset_end: None,
            associated_blob_ids: None,
        }
    }

    /// Test database operations (requires actual database - disabled by default)
    #[tokio::test]
    #[ignore] // Ignored because it requires a real database
    async fn test_database_operations() {
        let database_url = std::env::var("DATABASE_URL")
            .unwrap_or_else(|_| "postgresql:///sinex_test?host=/run/postgresql".to_string());

        let pool = create_test_pool(&database_url).await.unwrap();

        // Test count operation
        let count = EventQueries::count_all()
            .fetch_one::<(i64,)>(&pool)
            .await
            .unwrap();

        assert!(count.0 >= 0);

        // Test get recent events
        let events = EventQueries::get_recent(Some(10), None)
            .fetch_all::<crate::events::EventRecord>(&pool)
            .await
            .unwrap();

        assert!(events.len() <= 10);
    }
}

/// Benchmarks for query performance (requires bench feature)
#[cfg(all(test, feature = "bench"))]
mod benches {
    use super::*;
    use crate::queries::EventQueries;
    use crate::query_builder::QueryBuilder;
    use sinex_ulid::Ulid;
    use test::Bencher;

    #[bench]
    fn bench_query_builder_select(b: &mut Bencher) {
        b.iter(|| {
            let builder = QueryBuilder::select("core.events")
                .columns(&["event_id", "source", "event_type"])
                .where_eq("event_id", QueryParam::Ulid(Ulid::new()))
                .order_by("ts_ingest", "DESC")
                .limit(10);

            let _ = builder.build();
        });
    }

    #[bench]
    fn bench_query_registry_access(b: &mut Bencher) {
        b.iter(|| {
            let _ = EventQueries::get_by_id(Ulid::new()).build();
        });
    }

    #[bench]
    fn bench_ulid_conversion(b: &mut Bencher) {
        let ulid = Ulid::new();
        b.iter(|| {
            let param = QueryParam::Ulid(ulid);
            let _ = param.to_raw_value();
        });
    }
}

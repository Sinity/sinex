//! Tests for event payload schema management

#[cfg(test)]
mod tests {
    #![allow(unused_imports)]
    use crate::db::repositories::schema_management::*;
    use crate::db::repositories::DbPoolExt;
    use crate::types::Id;
    use crate::{Event, JsonValue};
    use serde_json::json;
    use sinex_test_utils::TestContext;
    use sqlx::PgPool;

    async fn setup_test_database() -> (TestContext, PgPool) {
        let ctx = TestContext::new().await.expect("test DB init");
        let pool = ctx.pool.clone();

        // Schema tables are created via migrations; no test-time DDL needed

        (ctx, pool)
    }

    #[tokio::test]
    async fn test_register_new_schema() {
        let (_ctx, pool) = setup_test_database().await;
        let repo = pool.schemas();

        let new_schema = NewEventSchema {
            source: "test-source".to_string(),
            event_type: "user.created".to_string(),
            schema_version: "1.0.0".to_string(),
            schema_content: json!({
                "$schema": "https://json-schema.org/draft/2020-12/schema",
                "type": "object",
                "properties": {
                    "user_id": {
                        "type": "string",
                        "format": "uuid"
                    },
                    "email": {
                        "type": "string",
                        "format": "email"
                    },
                    "created_at": {
                        "type": "string",
                        "format": "date-time"
                    }
                },
                "required": ["user_id", "email", "created_at"]
            }),
        };

        let schema = repo.register_schema(new_schema.clone()).await.unwrap();

        assert_eq!(schema.source, "test-source");
        assert_eq!(schema.event_type, "user.created");
        assert_eq!(schema.schema_version, "1.0.0");
        assert!(schema.is_active);
        assert_eq!(schema.content_hash, new_schema.calculate_content_hash());
    }

    #[tokio::test]
    async fn test_register_duplicate_schema_returns_existing() {
        let (_ctx, pool) = setup_test_database().await;
        let repo = pool.schemas();

        let new_schema = NewEventSchema {
            source: "test-source".to_string(),
            event_type: "user.updated".to_string(),
            schema_version: "1.0.0".to_string(),
            schema_content: json!({
                "type": "object",
                "properties": {
                    "user_id": { "type": "string" }
                }
            }),
        };

        let schema1 = repo.register_schema(new_schema.clone()).await.unwrap();
        let schema2 = repo.register_schema(new_schema).await.unwrap();

        // Should return the same schema (by content hash)
        assert_eq!(schema1.id, schema2.id);
        assert_eq!(schema1.content_hash, schema2.content_hash);
    }

    #[tokio::test]
    async fn test_get_active_schema() {
        let (_ctx, pool) = setup_test_database().await;
        let repo = pool.schemas();

        // Register v1
        let schema_v1 = NewEventSchema {
            source: "test-source".to_string(),
            event_type: "config.changed".to_string(),
            schema_version: "1.0.0".to_string(),
            schema_content: json!({
                "type": "object",
                "properties": {
                    "key": { "type": "string" }
                }
            }),
        };
        repo.register_schema(schema_v1).await.unwrap();

        // Register v2 (should deactivate v1)
        let schema_v2 = NewEventSchema {
            source: "test-source".to_string(),
            event_type: "config.changed".to_string(),
            schema_version: "2.0.0".to_string(),
            schema_content: json!({
                "type": "object",
                "properties": {
                    "key": { "type": "string" },
                    "value": { "type": "string" }
                }
            }),
        };
        let v2_registered = repo.register_schema(schema_v2).await.unwrap();

        // Get active schema should return v2
        let active = repo
            .get_active_schema("test-source", "config.changed")
            .await
            .unwrap();

        assert_eq!(active.id, v2_registered.id);
        assert_eq!(active.schema_version, "2.0.0");
        assert!(active.is_active);
    }

    #[tokio::test]
    async fn test_list_schemas_for_source() {
        let (_ctx, pool) = setup_test_database().await;
        let repo = pool.schemas();

        // Register multiple schemas for the same source
        for i in 1..=3 {
            let schema = NewEventSchema {
                source: "multi-source".to_string(),
                event_type: format!("event.type{}", i),
                schema_version: "1.0.0".to_string(),
                schema_content: json!({
                    "type": "object",
                    "properties": {
                        "id": { "type": "integer" }
                    }
                }),
            };
            repo.register_schema(schema).await.unwrap();
        }

        let schemas = repo
            .list_schemas_for_source("multi-source", false)
            .await
            .unwrap();

        assert_eq!(schemas.len(), 3);
        assert!(schemas.iter().all(|s| s.source == "multi-source"));
        assert!(schemas.iter().all(|s| s.is_active));
    }

    #[tokio::test]
    async fn test_deprecate_schema() {
        let (_ctx, pool) = setup_test_database().await;
        let repo = pool.schemas();

        let new_schema = NewEventSchema {
            source: "test-source".to_string(),
            event_type: "deprecated.event".to_string(),
            schema_version: "1.0.0".to_string(),
            schema_content: json!({
                "type": "object"
            }),
        };

        let schema = repo.register_schema(new_schema).await.unwrap();
        assert!(schema.is_active);

        // Deprecate the schema
        repo.deprecate_schema(&schema.id).await.unwrap();

        // Verify it's no longer active
        let result = repo
            .get_active_schema("test-source", "deprecated.event")
            .await;

        assert!(result.is_err()); // Should not find an active schema
    }

    #[tokio::test]
    async fn test_schema_statistics() {
        let (_ctx, pool) = setup_test_database().await;
        let repo = pool.schemas();

        // Register some test schemas
        let sources = ["source1", "source2"];
        let event_types = ["event.a", "event.b", "event.c"];

        for source in &sources {
            for event_type in &event_types {
                let schema = NewEventSchema {
                    source: source.to_string(),
                    event_type: event_type.to_string(),
                    schema_version: "1.0.0".to_string(),
                    schema_content: json!({"type": "object"}),
                };
                repo.register_schema(schema).await.unwrap();
            }
        }

        let stats = repo.get_schema_statistics().await.unwrap();

        assert_eq!(stats.total_schemas, 6);
        assert_eq!(stats.active_schemas, 6);
        assert_eq!(stats.unique_sources, 2);
        assert_eq!(stats.unique_event_types, 3);
    }
}

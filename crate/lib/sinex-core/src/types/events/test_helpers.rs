//! Test utilities for verifying event payload migrations

use super::EventPayload;
use crate::error::SinexError;
use serde_json::Value;

/// Test helper for verifying payload migrations
///
/// # Example
/// ```ignore
/// #[sinex_test]
/// fn test_file_created_migration() {
///     let v1_json = json!({
///         "path": "/test.txt",
///         "size": "1024" // String in v1
///     });
///     
///     // Verify v1 can be migrated to v2
///     test_migration::<FileCreatedV1, FileCreatedV2>(v1_json.clone(), "1.0.0")
///         .expect("Migration should succeed");
///         
///     // Verify fields are converted correctly
///     let v2 = migrate_payload::<FileCreatedV1, FileCreatedV2>(v1_json, "1.0.0").unwrap();
///     assert_eq!(v2.size, 1024); // Now a number
/// }
/// ```
pub fn test_migration<Old, New>(old_json: Value, old_version: &str) -> Result<(), String>
where
    Old: EventPayload + serde::de::DeserializeOwned + Into<New>,
    New: EventPayload + serde::de::DeserializeOwned,
{
    // First, verify the old JSON can be deserialized to Old type
    let old_value: Old = serde_json::from_value(old_json.clone())
        .map_err(|e| format!("Failed to deserialize old version: {}", e))?;

    // Verify migration via From trait
    let _new_from_old: New = old_value.into();

    // Verify migration via try_from_legacy
    let _new_from_legacy: New = New::try_from_legacy(old_json, old_version)
        .map_err(|e| format!("try_from_legacy failed: {}", e))?;
    Ok(())
}

/// Migrate a payload from old version to new version
pub fn migrate_payload<Old, New>(old_json: Value, old_version: &str) -> Result<New, SinexError>
where
    Old: EventPayload + serde::de::DeserializeOwned + Into<New>,
    New: EventPayload + serde::de::DeserializeOwned,
{
    New::try_from_legacy(old_json, old_version)
}

// TODO: This function is temporarily disabled to avoid circular dependency
// between sinex-events and sinex-db. It should be moved to a separate
// test utilities crate or to sinex-db itself.
/*
/// Create a test event with a specific payload and schema version
pub fn test_event_with_version<P: EventPayload>(
    payload: P,
    schema_id: sinex_core::types::ulid::Ulid,
    version: &str,
) -> Event {
    use crate::schema_registry::cache_schema_version;

    // Cache the version for this test
    cache_schema_version(schema_id, version.to_string());

    let mut event: Event<JsonValue> = Event::new(payload.into().into();
    event.payload_schema_id = Some(schema_id);
    event
}
*/

/// Verify that incompatible migrations fail appropriately
///
/// # Example
/// ```ignore
/// #[sinex_test]
/// fn test_incompatible_migration() {
///     let bad_json = json!({
///         "path": "/test.txt",
///         "size": "not a number" // Can't convert to u64
///     });
///     
///     assert_migration_fails::<BadPayloadV1, BadPayloadV2>(bad_json, "1.0.0");
/// }
/// ```
pub fn assert_migration_fails<Old, New>(old_json: Value, old_version: &str)
where
    Old: EventPayload + serde::de::DeserializeOwned,
    New: EventPayload + serde::de::DeserializeOwned,
{
    let result = New::try_from_legacy(old_json, old_version);
    assert!(
        result.is_err(),
        "Expected migration to fail but it succeeded"
    );
}

/// Preview what will happen during a migration without actually performing it
///
/// This is useful for debugging migration issues and understanding the migration path.
///
/// # Example
/// ```ignore
/// let preview = preview_migration::<FileCreatedV1, FileCreatedV3>(old_json, "1.0.0");
/// println!("{}", preview);
/// // Output:
/// // Migration Preview: FileCreatedV1 (1.0.0) → FileCreatedV3 (3.0.0)
/// // Migration path: V1 → V2 → V3 (transitive via From trait)
/// // ...
/// ```
pub fn preview_migration<Old, New>(old_json: Value, old_version: &str) -> MigrationPreview
where
    Old: EventPayload + serde::de::DeserializeOwned,
    New: EventPayload + serde::de::DeserializeOwned,
{
    let old_type_name = std::any::type_name::<Old>();
    let new_type_name = std::any::type_name::<New>();

    // Try to deserialize as old type
    let old_result = serde_json::from_value::<Old>(old_json.clone());

    // Try direct deserialization as new type
    let direct_result = serde_json::from_value::<New>(old_json.clone());

    // Try migration
    let migration_result = New::try_from_legacy(old_json.clone(), old_version);

    MigrationPreview {
        from_type: old_type_name.to_string(),
        from_version: old_version.to_string(),
        to_type: new_type_name.to_string(),
        to_version: New::VERSION.to_string(),
        old_deserializes: old_result.is_ok(),
        direct_deserializes: direct_result.is_ok(),
        migration_succeeds: migration_result.is_ok(),
        migration_error: migration_result.err().map(|e| e.to_string()),
        input_json: old_json,
    }
}

#[derive(Debug)]
pub struct MigrationPreview {
    pub from_type: String,
    pub from_version: String,
    pub to_type: String,
    pub to_version: String,
    pub old_deserializes: bool,
    pub direct_deserializes: bool,
    pub migration_succeeds: bool,
    pub migration_error: Option<String>,
    pub input_json: Value,
}

impl std::fmt::Display for MigrationPreview {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(
            f,
            "Migration Preview: {} ({}) → {} ({})",
            self.from_type, self.from_version, self.to_type, self.to_version
        )?;
        writeln!(f, "  Old type deserializes: {}", self.old_deserializes)?;
        writeln!(f, "  Direct deserialization: {}", self.direct_deserializes)?;
        writeln!(f, "  Migration succeeds: {}", self.migration_succeeds)?;
        if let Some(ref error) = self.migration_error {
            writeln!(f, "  Migration error: {}", error)?;
        }
        if self.migration_succeeds && !self.direct_deserializes {
            writeln!(
                f,
                "  Note: Migration required (uses From trait or try_from_legacy)"
            )?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use schemars::JsonSchema;
    use serde::{Deserialize, Serialize};
    use serde_json::json;
    use sinex_macros::EventPayload;
    use sinex_test_utils::sinex_test;

    #[derive(Debug, Serialize, Deserialize, JsonSchema, EventPayload)]
    #[event_payload(source = "test", event_type = "test.v1", version = "1.0.0")]
    struct TestPayloadV1 {
        name: String,
    }

    #[derive(Debug, Serialize, Deserialize, JsonSchema, EventPayload)]
    #[event_payload(source = "test", event_type = "test.v2", version = "2.0.0")]
    struct TestPayloadV2 {
        name: String,
        #[serde(default)]
        count: u32,
    }

    // Manual migration implementation since evolves_from was reverted
    impl From<TestPayloadV1> for TestPayloadV2 {
        fn from(v1: TestPayloadV1) -> Self {
            Self {
                name: v1.name,
                count: 0,
            }
        }
    }

    #[sinex_test]
    fn test_migration_helper() -> color_eyre::eyre::Result<()> {
        let v1_json = json!({
            "name": "test"
        });

        // Test migration succeeds
        test_migration::<TestPayloadV1, TestPayloadV2>(v1_json.clone(), "1.0.0")
            .expect("Migration should succeed");

        // Test we can get the migrated value
        let v2 = migrate_payload::<TestPayloadV1, TestPayloadV2>(v1_json, "1.0.0").unwrap();
        assert_eq!(v2.name, "test");
        assert_eq!(v2.count, 0); // Default value
        Ok(())
    }
}

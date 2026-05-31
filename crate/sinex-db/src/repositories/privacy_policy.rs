//! Privacy policy repository (#1042).
//!
//! CRUD over the user-controlled, DB-backed privacy policy tables in the
//! `privacy` schema:
//!
//! - `privacy.rules` — content matchers (`regex` / `literal`) with an action
//!   (`redact` / `hash` / `encrypt` / `suppress`).
//! - `privacy.field_rules` — scopes a rule to a `(event_source, event_type,
//!   field_path)` triple; `NULL` means "all".
//! - `privacy.encryption_keys` — key-namespace registry. Key MATERIAL never
//!   lives in the DB; the row is a namespace name only.
//!
//! The policy engine in `sinexd` loads all enabled rules + their field scopes
//! at the persistence chokepoint and applies them before write. Management
//! (`sinexctl privacy ...`) is deferred to a #1042 follow-up.

use crate::DbResult;
use sinex_primitives::prelude::*;
use sqlx::PgPool;

/// A privacy rule as stored in `privacy.rules`.
#[derive(Debug, Clone, sqlx::FromRow, serde::Serialize, serde::Deserialize)]
pub struct PrivacyRuleRecord {
    pub id: Uuid,
    pub name: String,
    pub description: String,
    pub matcher_type: String,
    pub matcher_value: String,
    pub case_sensitive: bool,
    pub action: String,
    pub action_label: Option<String>,
    pub key_namespace: String,
    pub enabled: bool,
}

/// A field scope binding a rule to a `(source, event_type, field_path)` triple.
#[derive(Debug, Clone, sqlx::FromRow, serde::Serialize, serde::Deserialize)]
pub struct FieldRuleRecord {
    pub id: Uuid,
    pub rule_id: Uuid,
    pub event_source: Option<String>,
    pub event_type: Option<String>,
    pub field_path: Option<String>,
    pub priority: i32,
}

/// A registered key namespace (name only; key bytes resolve from env/files).
#[derive(Debug, Clone, sqlx::FromRow, serde::Serialize, serde::Deserialize)]
pub struct EncryptionKeyRecord {
    pub id: Uuid,
    pub name: String,
    pub description: String,
}

/// An enabled rule joined with all of its field scopes. This is the shape the
/// policy engine consumes — one matcher with the list of scopes it applies to.
#[derive(Debug, Clone)]
pub struct LoadedRule {
    pub rule: PrivacyRuleRecord,
    pub scopes: Vec<FieldRuleRecord>,
}

/// Repository for the privacy policy tables.
pub struct PrivacyPolicyRepository<'a> {
    pool: &'a PgPool,
}

impl<'a> PrivacyPolicyRepository<'a> {
    #[must_use]
    pub fn new(pool: &'a PgPool) -> Self {
        Self { pool }
    }

    /// Load all enabled rules joined with their field scopes.
    ///
    /// This is the chokepoint load path: the policy engine calls it on each
    /// cache refresh. A rule with no field scopes still loads (with an empty
    /// `scopes` vec) and applies globally to every event/field.
    pub async fn load_enabled_rules(&self) -> DbResult<Vec<LoadedRule>> {
        let rules = sqlx::query_as!(
            PrivacyRuleRecord,
            r#"
            SELECT
                id,
                name,
                description,
                matcher_type,
                matcher_value,
                case_sensitive,
                action,
                action_label,
                key_namespace,
                enabled
            FROM privacy.rules
            WHERE enabled = true
            ORDER BY name
            "#,
        )
        .fetch_all(self.pool)
        .await
        .map_err(|e| SinexError::database(format!("failed to load privacy rules: {e}")))?;

        let scopes = sqlx::query_as!(
            FieldRuleRecord,
            r#"
            SELECT
                fr.id,
                fr.rule_id,
                fr.event_source,
                fr.event_type,
                fr.field_path,
                fr.priority
            FROM privacy.field_rules fr
            JOIN privacy.rules r ON r.id = fr.rule_id
            WHERE r.enabled = true
            ORDER BY fr.priority DESC
            "#,
        )
        .fetch_all(self.pool)
        .await
        .map_err(|e| SinexError::database(format!("failed to load privacy field scopes: {e}")))?;

        let loaded = rules
            .into_iter()
            .map(|rule| {
                let rule_scopes = scopes
                    .iter()
                    .filter(|s| s.rule_id == rule.id)
                    .cloned()
                    .collect();
                LoadedRule {
                    rule,
                    scopes: rule_scopes,
                }
            })
            .collect();
        Ok(loaded)
    }

    /// List all rules (enabled and disabled). Used by management surfaces.
    pub async fn list_rules(&self) -> DbResult<Vec<PrivacyRuleRecord>> {
        sqlx::query_as!(
            PrivacyRuleRecord,
            r#"
            SELECT
                id, name, description, matcher_type, matcher_value,
                case_sensitive, action, action_label, key_namespace, enabled
            FROM privacy.rules
            ORDER BY name
            "#,
        )
        .fetch_all(self.pool)
        .await
        .map_err(|e| SinexError::database(format!("failed to list privacy rules: {e}")))
    }

    /// Insert a new rule and return its generated id.
    #[allow(clippy::too_many_arguments)]
    pub async fn add_rule(
        &self,
        name: &str,
        description: &str,
        matcher_type: &str,
        matcher_value: &str,
        case_sensitive: bool,
        action: &str,
        action_label: Option<&str>,
        key_namespace: &str,
    ) -> DbResult<Uuid> {
        let row = sqlx::query!(
            r#"
            INSERT INTO privacy.rules
                (name, description, matcher_type, matcher_value,
                 case_sensitive, action, action_label, key_namespace)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
            RETURNING id
            "#,
            name,
            description,
            matcher_type,
            matcher_value,
            case_sensitive,
            action,
            action_label,
            key_namespace,
        )
        .fetch_one(self.pool)
        .await
        .map_err(|e| SinexError::database(format!("failed to add privacy rule: {e}")))?;
        Ok(row.id)
    }

    /// Remove a rule by name. Cascades to its field scopes via the FK.
    /// Returns the number of rows deleted (0 or 1).
    pub async fn remove_rule(&self, name: &str) -> DbResult<u64> {
        let result = sqlx::query!("DELETE FROM privacy.rules WHERE name = $1", name)
            .execute(self.pool)
            .await
            .map_err(|e| SinexError::database(format!("failed to remove privacy rule: {e}")))?;
        Ok(result.rows_affected())
    }

    /// Enable or disable a rule by name. Returns rows affected (0 or 1).
    pub async fn set_rule_enabled(&self, name: &str, enabled: bool) -> DbResult<u64> {
        let result = sqlx::query!(
            "UPDATE privacy.rules SET enabled = $2 WHERE name = $1",
            name,
            enabled,
        )
        .execute(self.pool)
        .await
        .map_err(|e| SinexError::database(format!("failed to update privacy rule: {e}")))?;
        Ok(result.rows_affected())
    }

    /// List field scopes, optionally filtered to one rule by name.
    pub async fn list_field_rules(
        &self,
        rule_name: Option<&str>,
    ) -> DbResult<Vec<FieldRuleRecord>> {
        sqlx::query_as!(
            FieldRuleRecord,
            r#"
            SELECT
                fr.id, fr.rule_id, fr.event_source, fr.event_type,
                fr.field_path, fr.priority
            FROM privacy.field_rules fr
            JOIN privacy.rules r ON r.id = fr.rule_id
            WHERE $1::text IS NULL OR r.name = $1
            ORDER BY fr.priority DESC
            "#,
            rule_name,
        )
        .fetch_all(self.pool)
        .await
        .map_err(|e| SinexError::database(format!("failed to list field scopes: {e}")))
    }

    /// Bind a rule (by name) to a `(source, event_type, field_path)` scope.
    /// Any of the scope dimensions may be `None` to mean "all".
    pub async fn bind_field_rule(
        &self,
        rule_name: &str,
        event_source: Option<&str>,
        event_type: Option<&str>,
        field_path: Option<&str>,
        priority: i32,
    ) -> DbResult<Uuid> {
        let row = sqlx::query!(
            r#"
            INSERT INTO privacy.field_rules
                (rule_id, event_source, event_type, field_path, priority)
            SELECT r.id, $2, $3, $4, $5
            FROM privacy.rules r
            WHERE r.name = $1
            RETURNING id
            "#,
            rule_name,
            event_source,
            event_type,
            field_path,
            priority,
        )
        .fetch_optional(self.pool)
        .await
        .map_err(|e| SinexError::database(format!("failed to bind field scope: {e}")))?;
        row.map(|r| r.id).ok_or_else(|| {
            SinexError::not_found(format!("privacy rule not found: {rule_name}"))
        })
    }

    /// Remove a field scope by its id. Returns rows affected (0 or 1).
    pub async fn unbind_field_rule(&self, id: Uuid) -> DbResult<u64> {
        let result = sqlx::query!("DELETE FROM privacy.field_rules WHERE id = $1", id)
            .execute(self.pool)
            .await
            .map_err(|e| SinexError::database(format!("failed to unbind field scope: {e}")))?;
        Ok(result.rows_affected())
    }

    /// List registered key namespaces.
    pub async fn list_keys(&self) -> DbResult<Vec<EncryptionKeyRecord>> {
        sqlx::query_as!(
            EncryptionKeyRecord,
            r#"
            SELECT id, name, description
            FROM privacy.encryption_keys
            ORDER BY name
            "#,
        )
        .fetch_all(self.pool)
        .await
        .map_err(|e| SinexError::database(format!("failed to list key namespaces: {e}")))
    }

    /// Register a key namespace (name only; key bytes resolve from env/files).
    pub async fn add_key(&self, name: &str, description: &str) -> DbResult<Uuid> {
        let row = sqlx::query!(
            r#"
            INSERT INTO privacy.encryption_keys (name, description)
            VALUES ($1, $2)
            RETURNING id
            "#,
            name,
            description,
        )
        .fetch_one(self.pool)
        .await
        .map_err(|e| SinexError::database(format!("failed to add key namespace: {e}")))?;
        Ok(row.id)
    }

    /// Remove a key namespace by name. Returns rows affected (0 or 1).
    pub async fn remove_key(&self, name: &str) -> DbResult<u64> {
        let result = sqlx::query!("DELETE FROM privacy.encryption_keys WHERE name = $1", name)
            .execute(self.pool)
            .await
            .map_err(|e| SinexError::database(format!("failed to remove key namespace: {e}")))?;
        Ok(result.rows_affected())
    }
}

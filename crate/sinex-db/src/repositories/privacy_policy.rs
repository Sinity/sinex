//! Privacy policy repository (#1042).
//!
//! CRUD over the user-controlled, DB-backed privacy policy tables in the
//! `privacy` schema:
//!
//! - `privacy.recognizer_backends` — external/local recognizer providers such
//!   as Presidio, Gitleaks-compatible importers, or local pattern execution.
//! - `privacy.dictionaries` / `privacy.dictionary_terms` — user/seeded
//!   deny-list dictionaries for policy rules.
//! - `privacy.rules` — recognizer-backed matchers with an action
//!   (`redact` / `hash` / `encrypt` / `suppress` / `mask`).
//! - `privacy.field_rules` — scopes a rule to a `(event_source, event_type,
//!   field_path)` triple; `NULL` means "all".
//! - `privacy.encryption_keys` — key-namespace registry. Key MATERIAL never
//!   lives in the DB; the row is a namespace name only.
//!
//! The policy engine in `sinexd` loads all enabled rules + their field scopes
//! at the persistence chokepoint and applies them before write. Management
//! is exposed through the `privacy.policy.*` RPC methods and
//! `sinexctl privacy policy ...`.

use crate::DbResult;
use serde_json::Value as JsonValue;
use sinex_primitives::prelude::*;
use sqlx::PgPool;

/// A recognizer backend as stored in `privacy.recognizer_backends`.
#[derive(Debug, Clone, sqlx::FromRow, serde::Serialize, serde::Deserialize)]
pub struct RecognizerBackendRecord {
    pub id: Uuid,
    pub name: String,
    pub kind: String,
    pub endpoint_url: Option<String>,
    pub config: JsonValue,
    pub enabled: bool,
}

/// A privacy dictionary as stored in `privacy.dictionaries`.
#[derive(Debug, Clone, sqlx::FromRow, serde::Serialize, serde::Deserialize)]
pub struct DictionaryRecord {
    pub id: Uuid,
    pub name: String,
    pub description: String,
    pub language: Option<String>,
    pub source_kind: String,
    pub tags: Vec<String>,
    pub enabled: bool,
}

/// A dictionary term as stored in `privacy.dictionary_terms`.
#[derive(Debug, Clone, sqlx::FromRow, serde::Serialize, serde::Deserialize)]
pub struct DictionaryTermRecord {
    pub id: Uuid,
    pub dictionary_id: Uuid,
    pub term: String,
    pub metadata: JsonValue,
    pub enabled: bool,
}

/// A privacy rule as stored in `privacy.rules`.
#[derive(Debug, Clone, sqlx::FromRow, serde::Serialize, serde::Deserialize)]
pub struct PrivacyRuleRecord {
    pub id: Uuid,
    pub name: String,
    pub description: String,
    pub recognizer_backend_id: Option<Uuid>,
    pub recognizer_kind: String,
    pub matcher_type: String,
    pub matcher_value: String,
    pub matcher_config: JsonValue,
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
    pub dictionary_terms: Vec<String>,
    pub backend: Option<RecognizerBackendRecord>,
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
                recognizer_backend_id,
                recognizer_kind,
                matcher_type,
                matcher_value,
                matcher_config as "matcher_config!: JsonValue",
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

        let dictionary_terms = sqlx::query!(
            r#"
            SELECT
                r.id AS rule_id,
                dt.term
            FROM privacy.rules r
            JOIN privacy.dictionaries d
              ON r.matcher_type = 'dictionary'
             AND d.enabled = true
             AND (
                    d.name = r.matcher_value
                 OR d.id::text = r.matcher_value
                 OR d.name = r.matcher_config ->> 'dictionary'
                 OR d.id::text = r.matcher_config ->> 'dictionary_id'
             )
            JOIN privacy.dictionary_terms dt
              ON dt.dictionary_id = d.id
             AND dt.enabled = true
            WHERE r.enabled = true
            ORDER BY r.name, dt.term
            "#,
        )
        .fetch_all(self.pool)
        .await
        .map_err(|e| {
            SinexError::database(format!("failed to load privacy dictionary terms: {e}"))
        })?;

        let backends = sqlx::query_as!(
            RecognizerBackendRecord,
            r#"
            SELECT
                id,
                name,
                kind,
                endpoint_url,
                config as "config!: JsonValue",
                enabled
            FROM privacy.recognizer_backends
            WHERE enabled = true
            "#,
        )
        .fetch_all(self.pool)
        .await
        .map_err(|e| {
            SinexError::database(format!("failed to load privacy recognizer backends: {e}"))
        })?;

        let loaded = rules
            .into_iter()
            .map(|rule| {
                let rule_scopes = scopes
                    .iter()
                    .filter(|s| s.rule_id == rule.id)
                    .cloned()
                    .collect();
                let dictionary_terms = dictionary_terms
                    .iter()
                    .filter(|term| term.rule_id == rule.id)
                    .map(|term| term.term.clone())
                    .collect();
                let backend = rule
                    .recognizer_backend_id
                    .and_then(|id| backends.iter().find(|backend| backend.id == id).cloned());
                LoadedRule {
                    rule,
                    scopes: rule_scopes,
                    dictionary_terms,
                    backend,
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
                id, name, description, recognizer_backend_id, recognizer_kind,
                matcher_type, matcher_value, matcher_config as "matcher_config!: JsonValue",
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
        self.add_recognizer_rule(
            name,
            description,
            None,
            "local_pattern",
            matcher_type,
            matcher_value,
            JsonValue::Object(serde_json::Map::new()),
            case_sensitive,
            action,
            action_label,
            key_namespace,
        )
        .await
    }

    /// Insert a recognizer-backed rule and return its generated id.
    #[allow(clippy::too_many_arguments)]
    pub async fn add_recognizer_rule(
        &self,
        name: &str,
        description: &str,
        recognizer_backend_id: Option<Uuid>,
        recognizer_kind: &str,
        matcher_type: &str,
        matcher_value: &str,
        matcher_config: JsonValue,
        case_sensitive: bool,
        action: &str,
        action_label: Option<&str>,
        key_namespace: &str,
    ) -> DbResult<Uuid> {
        let row = sqlx::query!(
            r#"
            INSERT INTO privacy.rules
                (name, description, recognizer_backend_id, recognizer_kind,
                 matcher_type, matcher_value, matcher_config, case_sensitive,
                 action, action_label, key_namespace)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
            RETURNING id
            "#,
            name,
            description,
            recognizer_backend_id,
            recognizer_kind,
            matcher_type,
            matcher_value,
            matcher_config,
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

    /// Insert or update a recognizer-backed rule by name.
    #[allow(clippy::too_many_arguments)]
    pub async fn upsert_recognizer_rule(
        &self,
        name: &str,
        description: &str,
        recognizer_backend_id: Option<Uuid>,
        recognizer_kind: &str,
        matcher_type: &str,
        matcher_value: &str,
        matcher_config: JsonValue,
        case_sensitive: bool,
        action: &str,
        action_label: Option<&str>,
        key_namespace: &str,
        enabled: bool,
    ) -> DbResult<Uuid> {
        let row = sqlx::query!(
            r#"
            INSERT INTO privacy.rules
                (name, description, recognizer_backend_id, recognizer_kind,
                 matcher_type, matcher_value, matcher_config, case_sensitive,
                 action, action_label, key_namespace, enabled)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
            ON CONFLICT (name) DO UPDATE SET
                description = EXCLUDED.description,
                recognizer_backend_id = EXCLUDED.recognizer_backend_id,
                recognizer_kind = EXCLUDED.recognizer_kind,
                matcher_type = EXCLUDED.matcher_type,
                matcher_value = EXCLUDED.matcher_value,
                matcher_config = EXCLUDED.matcher_config,
                case_sensitive = EXCLUDED.case_sensitive,
                action = EXCLUDED.action,
                action_label = EXCLUDED.action_label,
                key_namespace = EXCLUDED.key_namespace,
                enabled = EXCLUDED.enabled
            RETURNING id
            "#,
            name,
            description,
            recognizer_backend_id,
            recognizer_kind,
            matcher_type,
            matcher_value,
            matcher_config,
            case_sensitive,
            action,
            action_label,
            key_namespace,
            enabled,
        )
        .fetch_one(self.pool)
        .await
        .map_err(|e| SinexError::database(format!("failed to upsert privacy rule: {e}")))?;
        Ok(row.id)
    }

    /// List configured recognizer backends.
    pub async fn list_recognizer_backends(&self) -> DbResult<Vec<RecognizerBackendRecord>> {
        sqlx::query_as!(
            RecognizerBackendRecord,
            r#"
            SELECT
                id,
                name,
                kind,
                endpoint_url,
                config as "config!: JsonValue",
                enabled
            FROM privacy.recognizer_backends
            ORDER BY name
            "#,
        )
        .fetch_all(self.pool)
        .await
        .map_err(|e| SinexError::database(format!("failed to list recognizer backends: {e}")))
    }

    /// Register a recognizer backend and return its generated id.
    pub async fn add_recognizer_backend(
        &self,
        name: &str,
        kind: &str,
        endpoint_url: Option<&str>,
        config: JsonValue,
    ) -> DbResult<Uuid> {
        let row = sqlx::query!(
            r#"
            INSERT INTO privacy.recognizer_backends (name, kind, endpoint_url, config)
            VALUES ($1, $2, $3, $4)
            RETURNING id
            "#,
            name,
            kind,
            endpoint_url,
            config,
        )
        .fetch_one(self.pool)
        .await
        .map_err(|e| SinexError::database(format!("failed to add recognizer backend: {e}")))?;
        Ok(row.id)
    }

    /// List privacy dictionaries.
    pub async fn list_dictionaries(&self) -> DbResult<Vec<DictionaryRecord>> {
        sqlx::query_as!(
            DictionaryRecord,
            r#"
            SELECT id, name, description, language, source_kind, tags, enabled
            FROM privacy.dictionaries
            ORDER BY name
            "#,
        )
        .fetch_all(self.pool)
        .await
        .map_err(|e| SinexError::database(format!("failed to list privacy dictionaries: {e}")))
    }

    /// Register a privacy dictionary and return its generated id.
    pub async fn add_dictionary(
        &self,
        name: &str,
        description: &str,
        language: Option<&str>,
        source_kind: &str,
        tags: &[String],
    ) -> DbResult<Uuid> {
        let row = sqlx::query!(
            r#"
            INSERT INTO privacy.dictionaries
                (name, description, language, source_kind, tags)
            VALUES ($1, $2, $3, $4, $5)
            RETURNING id
            "#,
            name,
            description,
            language,
            source_kind,
            tags,
        )
        .fetch_one(self.pool)
        .await
        .map_err(|e| SinexError::database(format!("failed to add privacy dictionary: {e}")))?;
        Ok(row.id)
    }

    /// Add a term to a dictionary.
    pub async fn add_dictionary_term(
        &self,
        dictionary_id: Uuid,
        term: &str,
        metadata: JsonValue,
    ) -> DbResult<Uuid> {
        let row = sqlx::query!(
            r#"
            INSERT INTO privacy.dictionary_terms (dictionary_id, term, metadata)
            VALUES ($1, $2, $3)
            RETURNING id
            "#,
            dictionary_id,
            term,
            metadata,
        )
        .fetch_one(self.pool)
        .await
        .map_err(|e| SinexError::database(format!("failed to add dictionary term: {e}")))?;
        Ok(row.id)
    }

    /// List terms for a dictionary.
    pub async fn list_dictionary_terms(
        &self,
        dictionary_id: Uuid,
    ) -> DbResult<Vec<DictionaryTermRecord>> {
        sqlx::query_as!(
            DictionaryTermRecord,
            r#"
            SELECT
                id,
                dictionary_id,
                term,
                metadata as "metadata!: JsonValue",
                enabled
            FROM privacy.dictionary_terms
            WHERE dictionary_id = $1
            ORDER BY term
            "#,
            dictionary_id,
        )
        .fetch_all(self.pool)
        .await
        .map_err(|e| SinexError::database(format!("failed to list dictionary terms: {e}")))
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
        let normalized_field_path = normalize_field_path(field_path);
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
            normalized_field_path.as_deref(),
            priority,
        )
        .fetch_optional(self.pool)
        .await
        .map_err(|e| SinexError::database(format!("failed to bind field scope: {e}")))?;
        row.map(|r| r.id)
            .ok_or_else(|| SinexError::not_found(format!("privacy rule not found: {rule_name}")))
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

fn normalize_field_path(path: Option<&str>) -> Option<String> {
    let path = path?.trim();
    if path.is_empty() {
        return None;
    }
    if path.starts_with('/') {
        return Some(path.to_string());
    }
    Some(format!("/{}", path.replace('~', "~0").replace('/', "~1")))
}

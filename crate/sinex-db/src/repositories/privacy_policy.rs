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
//! - `privacy.recognizer_backends` — local or external recognizer bindings
//!   (Presidio, secret scanners, structural detectors, dictionaries).
//! - `privacy.dictionaries` / `privacy.dictionary_terms` —
//!   imported/user-local term lists. The DB owns binding metadata; Sinex does
//!   not curate a built-in taxonomy.
//!
//! The policy engine in `sinexd` loads all enabled rules + their field scopes
//! at the persistence chokepoint and applies them before write. Operator
//! inspection starts at `sinexctl privacy policy list`; mutating policy
//! management remains under #1042.

use crate::DbResult;
use sinex_primitives::prelude::*;
use sinex_primitives::privacy::PrivacyPolicySeedRule;
use sqlx::PgPool;

/// A privacy rule as stored in `privacy.rules`.
#[derive(Debug, Clone, sqlx::FromRow, serde::Serialize, serde::Deserialize)]
pub struct PrivacyRuleRecord {
    pub id: Uuid,
    pub name: String,
    pub description: String,
    pub matcher_type: String,
    pub matcher_value: String,
    pub matcher_config: serde_json::Value,
    pub recognizer_backend_id: Option<Uuid>,
    pub recognizer_kind: String,
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

/// A configured recognizer backend. `config` holds backend-specific details
/// such as Presidio URL/model options or a secret-scanner profile.
#[derive(Debug, Clone, sqlx::FromRow, serde::Serialize, serde::Deserialize)]
pub struct RecognizerBackendRecord {
    pub id: Uuid,
    pub name: String,
    pub kind: String,
    pub endpoint_url: Option<String>,
    pub config: serde_json::Value,
    pub enabled: bool,
}

/// Imported or user-local dictionary/deny-list.
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

/// One enabled dictionary term.
#[derive(Debug, Clone, sqlx::FromRow, serde::Serialize, serde::Deserialize)]
pub struct DictionaryTermRecord {
    pub id: Uuid,
    pub dictionary_id: Uuid,
    pub term: String,
    pub metadata: serde_json::Value,
    pub enabled: bool,
}

/// An enabled rule joined with all of its field scopes. This is the shape the
/// policy engine consumes — one matcher with the list of scopes it applies to.
#[derive(Debug, Clone)]
pub struct LoadedRule {
    pub rule: PrivacyRuleRecord,
    pub scopes: Vec<FieldRuleRecord>,
    pub backend: Option<RecognizerBackendRecord>,
}

/// Result of idempotently seeding policy rows.
#[derive(Debug, Clone, Copy, Default)]
pub struct PolicySeedSummary {
    pub inserted: usize,
    pub updated: usize,
    pub unchanged: usize,
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
                matcher_config,
                recognizer_backend_id,
                recognizer_kind,
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

        let mut loaded = Vec::with_capacity(rules.len());
        for mut rule in rules {
            if rule.matcher_type == "dictionary" {
                rule.matcher_config = self
                    .hydrate_dictionary_rule_config(&rule.matcher_config)
                    .await?;
            }

            let backend = match rule.recognizer_backend_id {
                Some(id) => match self.get_recognizer_backend(id).await? {
                    Some(backend) => Some(backend),
                    // The backend was disabled or removed. The rule cannot run
                    // without it, so skip just this rule rather than aborting
                    // the entire policy load.
                    None => {
                        tracing::warn!(
                            rule = %rule.name,
                            backend_id = %id,
                            "privacy rule references a disabled or missing recognizer backend; skipping rule"
                        );
                        continue;
                    }
                },
                None => None,
            };

            loaded.push({
                let rule_scopes = scopes
                    .iter()
                    .filter(|s| s.rule_id == rule.id)
                    .cloned()
                    .collect();
                LoadedRule {
                    rule,
                    scopes: rule_scopes,
                    backend,
                }
            });
        }
        Ok(loaded)
    }

    async fn hydrate_dictionary_rule_config(
        &self,
        matcher_config: &serde_json::Value,
    ) -> DbResult<serde_json::Value> {
        let dictionary_id = matcher_config
            .get("dictionary_id")
            .and_then(serde_json::Value::as_str)
            .and_then(|value| value.parse::<Uuid>().ok());
        let dictionary_name = matcher_config
            .get("dictionary")
            .or_else(|| matcher_config.get("dictionary_name"))
            .and_then(serde_json::Value::as_str);

        let Some(terms) = self
            .load_dictionary_terms_for_rule(dictionary_id, dictionary_name)
            .await?
        else {
            return Ok(matcher_config.clone());
        };

        let mut config = matcher_config.clone();
        let object = config.as_object_mut().ok_or_else(|| {
            SinexError::validation("privacy dictionary rule matcher_config must be an object")
        })?;
        object.insert(
            "terms".to_string(),
            serde_json::Value::Array(terms.into_iter().map(serde_json::Value::String).collect()),
        );
        Ok(config)
    }

    async fn load_dictionary_terms_for_rule(
        &self,
        dictionary_id: Option<Uuid>,
        dictionary_name: Option<&str>,
    ) -> DbResult<Option<Vec<String>>> {
        if let Some(id) = dictionary_id {
            let terms = sqlx::query_scalar!(
                r#"
                SELECT dt.term
                FROM privacy.dictionary_terms dt
                JOIN privacy.dictionaries d ON d.id = dt.dictionary_id
                WHERE d.id = $1 AND d.enabled = true AND dt.enabled = true
                ORDER BY dt.term
                "#,
                id,
            )
            .fetch_all(self.pool)
            .await
            .map_err(|e| {
                SinexError::database(format!("failed to load privacy dictionary terms: {e}"))
            })?;
            return Ok(Some(terms));
        }

        if let Some(name) = dictionary_name {
            let terms = sqlx::query_scalar!(
                r#"
                SELECT dt.term
                FROM privacy.dictionary_terms dt
                JOIN privacy.dictionaries d ON d.id = dt.dictionary_id
                WHERE d.name = $1 AND d.enabled = true AND dt.enabled = true
                ORDER BY dt.term
                "#,
                name,
            )
            .fetch_all(self.pool)
            .await
            .map_err(|e| {
                SinexError::database(format!("failed to load privacy dictionary terms: {e}"))
            })?;
            return Ok(Some(terms));
        }

        Ok(None)
    }

    /// List all rules (enabled and disabled). Used by management surfaces.
    pub async fn list_rules(&self) -> DbResult<Vec<PrivacyRuleRecord>> {
        sqlx::query_as!(
            PrivacyRuleRecord,
            r#"
            SELECT
                id, name, description, matcher_type, matcher_value,
                matcher_config, recognizer_backend_id, recognizer_kind,
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

    /// Insert a new rule with recognizer metadata and return its generated id.
    #[allow(clippy::too_many_arguments)]
    pub async fn add_recognizer_rule(
        &self,
        name: &str,
        description: &str,
        matcher_type: &str,
        matcher_value: &str,
        matcher_config: serde_json::Value,
        recognizer_backend_id: Option<Uuid>,
        recognizer_kind: &str,
        case_sensitive: bool,
        action: &str,
        action_label: Option<&str>,
        key_namespace: &str,
    ) -> DbResult<Uuid> {
        let row = sqlx::query!(
            r#"
            INSERT INTO privacy.rules
                (name, description, matcher_type, matcher_value, matcher_config,
                 recognizer_backend_id, recognizer_kind, case_sensitive, action,
                 action_label, key_namespace)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
            RETURNING id
            "#,
            name,
            description,
            matcher_type,
            matcher_value,
            matcher_config,
            recognizer_backend_id,
            recognizer_kind,
            case_sensitive,
            action,
            action_label,
            key_namespace,
        )
        .fetch_one(self.pool)
        .await
        .map_err(|e| {
            SinexError::database(format!("failed to add privacy recognizer rule: {e}"))
        })?;
        Ok(row.id)
    }

    /// Upsert seed rows by name.
    ///
    /// This is intended only for explicit operator-invoked seed commands;
    /// runtime policy loading never consults the built-in catalog directly.
    pub async fn seed_rules(&self, rules: &[PrivacyPolicySeedRule]) -> DbResult<PolicySeedSummary> {
        let mut summary = PolicySeedSummary::default();
        for rule in rules {
            let inserted_or_changed = sqlx::query_scalar!(
                r#"
                INSERT INTO privacy.rules
                    (name, description, matcher_type, matcher_value, matcher_config,
                     recognizer_kind, case_sensitive, action, action_label, key_namespace, enabled)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
                ON CONFLICT (name) DO UPDATE SET
                    description = EXCLUDED.description,
                    matcher_type = EXCLUDED.matcher_type,
                    matcher_value = EXCLUDED.matcher_value,
                    matcher_config = EXCLUDED.matcher_config,
                    recognizer_kind = EXCLUDED.recognizer_kind,
                    case_sensitive = EXCLUDED.case_sensitive,
                    action = EXCLUDED.action,
                    action_label = EXCLUDED.action_label,
                    key_namespace = EXCLUDED.key_namespace,
                    enabled = EXCLUDED.enabled
                WHERE
                    privacy.rules.description IS DISTINCT FROM EXCLUDED.description
                    OR privacy.rules.matcher_type IS DISTINCT FROM EXCLUDED.matcher_type
                    OR privacy.rules.matcher_value IS DISTINCT FROM EXCLUDED.matcher_value
                    OR privacy.rules.matcher_config IS DISTINCT FROM EXCLUDED.matcher_config
                    OR privacy.rules.recognizer_kind IS DISTINCT FROM EXCLUDED.recognizer_kind
                    OR privacy.rules.case_sensitive IS DISTINCT FROM EXCLUDED.case_sensitive
                    OR privacy.rules.action IS DISTINCT FROM EXCLUDED.action
                    OR privacy.rules.action_label IS DISTINCT FROM EXCLUDED.action_label
                    OR privacy.rules.key_namespace IS DISTINCT FROM EXCLUDED.key_namespace
                    OR privacy.rules.enabled IS DISTINCT FROM EXCLUDED.enabled
                RETURNING (xmax = 0) AS inserted
                "#,
                rule.name,
                rule.description,
                rule.matcher_type,
                rule.matcher_value,
                rule.matcher_config,
                rule.recognizer_kind,
                rule.case_sensitive,
                rule.action,
                rule.action_label,
                rule.key_namespace,
                rule.enabled,
            )
            .fetch_optional(self.pool)
            .await
            .map_err(|e| SinexError::database(format!("failed to seed privacy rule: {e}")))?;

            match inserted_or_changed {
                Some(Some(true)) => summary.inserted += 1,
                Some(Some(false)) => summary.updated += 1,
                Some(None) => summary.updated += 1,
                None => summary.unchanged += 1,
            }
        }
        Ok(summary)
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

    /// List configured recognizer backends.
    pub async fn list_recognizer_backends(&self) -> DbResult<Vec<RecognizerBackendRecord>> {
        sqlx::query_as!(
            RecognizerBackendRecord,
            r#"
            SELECT id, name, kind, endpoint_url, config, enabled
            FROM privacy.recognizer_backends
            ORDER BY name
            "#,
        )
        .fetch_all(self.pool)
        .await
        .map_err(|e| {
            SinexError::database(format!("failed to list privacy recognizer backends: {e}"))
        })
    }

    /// Look up an *enabled* recognizer backend by id. Returns `Ok(None)` when
    /// the backend is disabled or absent — a disabled backend must not abort
    /// policy loading; the referencing rule is skipped instead (see
    /// `load_enabled_rules`). `Err` is reserved for real DB failures.
    async fn get_recognizer_backend(
        &self,
        id: Uuid,
    ) -> DbResult<Option<RecognizerBackendRecord>> {
        sqlx::query_as!(
            RecognizerBackendRecord,
            r#"
            SELECT id, name, kind, endpoint_url, config, enabled
            FROM privacy.recognizer_backends
            WHERE id = $1 AND enabled = true
            "#,
            id,
        )
        .fetch_optional(self.pool)
        .await
        .map_err(|e| {
            SinexError::database(format!("failed to load privacy recognizer backend: {e}"))
        })
    }

    /// Register a recognizer backend. The config is backend-specific JSON.
    pub async fn add_recognizer_backend(
        &self,
        name: &str,
        kind: &str,
        endpoint_url: Option<&str>,
        config: serde_json::Value,
        enabled: bool,
    ) -> DbResult<Uuid> {
        let row = sqlx::query!(
            r#"
            INSERT INTO privacy.recognizer_backends (name, kind, endpoint_url, config, enabled)
            VALUES ($1, $2, $3, $4, $5)
            RETURNING id
            "#,
            name,
            kind,
            endpoint_url,
            config,
            enabled,
        )
        .fetch_one(self.pool)
        .await
        .map_err(|e| {
            SinexError::database(format!("failed to add privacy recognizer backend: {e}"))
        })?;
        Ok(row.id)
    }

    /// List imported or user-local dictionaries.
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
        .map_err(|e| {
            SinexError::database(format!("failed to list privacy dictionaries: {e}"))
        })
    }

    /// List enabled terms for one dictionary.
    pub async fn list_dictionary_terms(
        &self,
        dictionary_id: Uuid,
    ) -> DbResult<Vec<DictionaryTermRecord>> {
        sqlx::query_as!(
            DictionaryTermRecord,
            r#"
            SELECT id, dictionary_id, term, metadata, enabled
            FROM privacy.dictionary_terms
            WHERE dictionary_id = $1
            ORDER BY term
            "#,
            dictionary_id,
        )
        .fetch_all(self.pool)
        .await
        .map_err(|e| {
            SinexError::database(format!("failed to list privacy dictionary terms: {e}"))
        })
    }

    /// Register an imported/user-local dictionary and its initial terms.
    pub async fn add_dictionary(
        &self,
        name: &str,
        description: &str,
        language: Option<&str>,
        source_kind: &str,
        tags: &[String],
        terms: &[String],
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
        .map_err(|e| {
            SinexError::database(format!("failed to add privacy dictionary: {e}"))
        })?;
        for term in terms {
            sqlx::query!(
                r#"
                INSERT INTO privacy.dictionary_terms (dictionary_id, term)
                VALUES ($1, $2)
                ON CONFLICT (dictionary_id, term) DO NOTHING
                "#,
                row.id,
                term,
            )
            .execute(self.pool)
            .await
            .map_err(|e| {
                SinexError::database(format!("failed to add privacy dictionary term: {e}"))
            })?;
        }
        Ok(row.id)
    }
}

use super::common::{DbResult, Repository, db_error};
use serde_json::Value;
use sqlx::PgPool;
use time::OffsetDateTime;
use uuid::Uuid;

#[derive(Debug, Clone)]
pub struct EmailMailboxProjectionEvent {
    pub source_id: String,
    pub mode_id: String,
    pub event_id: Uuid,
    pub event_type: String,
    pub payload: Value,
}

#[derive(Debug, Clone, sqlx::FromRow)]
pub struct EmailMailboxProjectionRecord {
    pub id: Uuid,
    pub source_id: String,
    pub mode_id: String,
    pub message_key: String,
    pub message_id: Option<String>,
    pub thread_key: Option<String>,
    pub thread_root_message_id: Option<String>,
    pub direction: Option<String>,
    pub folder: Option<String>,
    pub mailbox_format: Option<String>,
    pub source_file: Option<String>,
    pub raw_material_id: Option<String>,
    pub subject: Option<String>,
    pub from_addresses: Value,
    pub to_addresses: Value,
    pub body_bytes: i64,
    pub attachment_count: i32,
    pub attachment_observed_count: i32,
    pub attachment_policy_refs: Value,
    pub last_message_event_id: Option<Uuid>,
    pub last_thread_event_id: Option<Uuid>,
    pub last_attachment_event_id: Option<Uuid>,
    pub last_observed_at: OffsetDateTime,
    pub updated_at: OffsetDateTime,
}

#[derive(Debug, Clone, sqlx::FromRow)]
pub struct EmailMailboxProjectionSummary {
    pub mode_id: String,
    pub message_count: i64,
    pub thread_count: i64,
    pub body_bytes: i64,
    pub attachment_count: i64,
    pub attachment_observed_count: i64,
    pub last_observed_at: OffsetDateTime,
}

pub struct EmailMailboxProjectionRepository<'a> {
    pool: &'a PgPool,
}

impl<'a> Repository<'a> for EmailMailboxProjectionRepository<'a> {
    fn new(pool: &'a PgPool) -> Self {
        Self { pool }
    }

    fn pool(&self) -> &'a PgPool {
        self.pool
    }
}

impl EmailMailboxProjectionRepository<'_> {
    pub async fn upsert_event(
        &self,
        event: EmailMailboxProjectionEvent,
    ) -> DbResult<Option<EmailMailboxProjectionRecord>> {
        let Some(upsert) = ProjectionUpsert::from_event(event) else {
            return Ok(None);
        };

        sqlx::query_as!(
            EmailMailboxProjectionRecord,
            r#"
            INSERT INTO core.email_mailbox_projection (
                source_id,
                mode_id,
                message_key,
                message_id,
                thread_key,
                thread_root_message_id,
                direction,
                folder,
                mailbox_format,
                source_file,
                raw_material_id,
                subject,
                from_addresses,
                to_addresses,
                body_bytes,
                attachment_count,
                attachment_observed_count,
                attachment_policy_refs,
                last_message_event_id,
                last_thread_event_id,
                last_attachment_event_id,
                last_observed_at
            )
            VALUES (
                $1, $2, $3, $4, $5, $6, $7, $8, $9, $10,
                $11, $12, $13, $14, $15, $16, $17, $18,
                $19::uuid, $20::uuid, $21::uuid, NOW()
            )
            ON CONFLICT (source_id, mode_id, message_key)
            DO UPDATE SET
                message_id = COALESCE(EXCLUDED.message_id, core.email_mailbox_projection.message_id),
                thread_key = COALESCE(EXCLUDED.thread_key, core.email_mailbox_projection.thread_key),
                thread_root_message_id = COALESCE(EXCLUDED.thread_root_message_id, core.email_mailbox_projection.thread_root_message_id),
                direction = COALESCE(EXCLUDED.direction, core.email_mailbox_projection.direction),
                folder = COALESCE(EXCLUDED.folder, core.email_mailbox_projection.folder),
                mailbox_format = COALESCE(EXCLUDED.mailbox_format, core.email_mailbox_projection.mailbox_format),
                source_file = COALESCE(EXCLUDED.source_file, core.email_mailbox_projection.source_file),
                raw_material_id = COALESCE(EXCLUDED.raw_material_id, core.email_mailbox_projection.raw_material_id),
                subject = COALESCE(EXCLUDED.subject, core.email_mailbox_projection.subject),
                from_addresses = CASE
                    WHEN EXCLUDED.from_addresses <> '[]'::jsonb THEN EXCLUDED.from_addresses
                    ELSE core.email_mailbox_projection.from_addresses
                END,
                to_addresses = CASE
                    WHEN EXCLUDED.to_addresses <> '[]'::jsonb THEN EXCLUDED.to_addresses
                    ELSE core.email_mailbox_projection.to_addresses
                END,
                body_bytes = GREATEST(core.email_mailbox_projection.body_bytes, EXCLUDED.body_bytes),
                attachment_count = GREATEST(core.email_mailbox_projection.attachment_count, EXCLUDED.attachment_count),
                attachment_observed_count = GREATEST(
                    core.email_mailbox_projection.attachment_observed_count,
                    EXCLUDED.attachment_observed_count
                ),
                attachment_policy_refs = (
                    SELECT COALESCE(jsonb_agg(DISTINCT ref), '[]'::jsonb)
                    FROM jsonb_array_elements(
                        core.email_mailbox_projection.attachment_policy_refs
                        || EXCLUDED.attachment_policy_refs
                    ) AS refs(ref)
                ),
                last_message_event_id = COALESCE(EXCLUDED.last_message_event_id, core.email_mailbox_projection.last_message_event_id),
                last_thread_event_id = COALESCE(EXCLUDED.last_thread_event_id, core.email_mailbox_projection.last_thread_event_id),
                last_attachment_event_id = COALESCE(EXCLUDED.last_attachment_event_id, core.email_mailbox_projection.last_attachment_event_id),
                last_observed_at = EXCLUDED.last_observed_at
            RETURNING
                id,
                source_id,
                mode_id,
                message_key,
                message_id,
                thread_key,
                thread_root_message_id,
                direction,
                folder,
                mailbox_format,
                source_file,
                raw_material_id,
                subject,
                from_addresses,
                to_addresses,
                body_bytes,
                attachment_count,
                attachment_observed_count,
                attachment_policy_refs,
                last_message_event_id,
                last_thread_event_id,
                last_attachment_event_id,
                last_observed_at,
                updated_at
            "#,
            upsert.source_id,
            upsert.mode_id,
            upsert.message_key,
            upsert.message_id,
            upsert.thread_key,
            upsert.thread_root_message_id,
            upsert.direction,
            upsert.folder,
            upsert.mailbox_format,
            upsert.source_file,
            upsert.raw_material_id,
            upsert.subject,
            upsert.from_addresses,
            upsert.to_addresses,
            upsert.body_bytes,
            upsert.attachment_count,
            upsert.attachment_observed_count,
            upsert.attachment_policy_refs,
            upsert.last_message_event_id,
            upsert.last_thread_event_id,
            upsert.last_attachment_event_id
        )
        .fetch_one(self.pool)
        .await
        .map(Some)
        .map_err(|error| db_error(error, "upsert email mailbox projection"))
    }

    pub async fn list_current_by_source(
        &self,
        source_id: &str,
    ) -> DbResult<Vec<EmailMailboxProjectionRecord>> {
        sqlx::query_as!(
            EmailMailboxProjectionRecord,
            r#"
            SELECT
                id,
                source_id,
                mode_id,
                message_key,
                message_id,
                thread_key,
                thread_root_message_id,
                direction,
                folder,
                mailbox_format,
                source_file,
                raw_material_id,
                subject,
                from_addresses,
                to_addresses,
                body_bytes,
                attachment_count,
                attachment_observed_count,
                attachment_policy_refs,
                last_message_event_id,
                last_thread_event_id,
                last_attachment_event_id,
                last_observed_at,
                updated_at
            FROM core.email_mailbox_projection
            WHERE source_id = $1
            ORDER BY mode_id, last_observed_at DESC, message_key
            "#,
            source_id
        )
        .fetch_all(self.pool)
        .await
        .map_err(|error| db_error(error, "list email mailbox projections"))
    }

    pub async fn summarize_by_source(
        &self,
        source_id: &str,
    ) -> DbResult<Vec<EmailMailboxProjectionSummary>> {
        sqlx::query_as!(
            EmailMailboxProjectionSummary,
            r#"
            SELECT
                mode_id,
                COUNT(*)::BIGINT AS "message_count!",
                COUNT(DISTINCT thread_key)::BIGINT AS "thread_count!",
                COALESCE(SUM(body_bytes), 0)::BIGINT AS "body_bytes!",
                COALESCE(SUM(attachment_count), 0)::BIGINT AS "attachment_count!",
                COALESCE(SUM(attachment_observed_count), 0)::BIGINT AS "attachment_observed_count!",
                MAX(last_observed_at) AS "last_observed_at!"
            FROM core.email_mailbox_projection
            WHERE source_id = $1
            GROUP BY mode_id
            ORDER BY mode_id
            "#,
            source_id
        )
        .fetch_all(self.pool)
        .await
        .map_err(|error| db_error(error, "summarize email mailbox projections"))
    }
}

struct ProjectionUpsert {
    source_id: String,
    mode_id: String,
    message_key: String,
    message_id: Option<String>,
    thread_key: Option<String>,
    thread_root_message_id: Option<String>,
    direction: Option<String>,
    folder: Option<String>,
    mailbox_format: Option<String>,
    source_file: Option<String>,
    raw_material_id: Option<String>,
    subject: Option<String>,
    from_addresses: Value,
    to_addresses: Value,
    body_bytes: i64,
    attachment_count: i32,
    attachment_observed_count: i32,
    attachment_policy_refs: Value,
    last_message_event_id: Option<Uuid>,
    last_thread_event_id: Option<Uuid>,
    last_attachment_event_id: Option<Uuid>,
}

impl ProjectionUpsert {
    fn from_event(event: EmailMailboxProjectionEvent) -> Option<Self> {
        match event.event_type.as_str() {
            "email.message.received" | "email.message.sent" => {
                let message_key = message_key(&event.payload)?;
                Some(Self {
                    source_id: event.source_id,
                    mode_id: event.mode_id,
                    message_key,
                    message_id: optional_string(&event.payload, "message_id"),
                    thread_key: None,
                    thread_root_message_id: None,
                    direction: Some(
                        if event.event_type.as_str() == "email.message.sent" {
                            "sent"
                        } else {
                            "received"
                        }
                        .to_string(),
                    ),
                    folder: optional_string(&event.payload, "folder"),
                    mailbox_format: optional_string(&event.payload, "mailbox_format"),
                    source_file: optional_string(&event.payload, "source_file"),
                    raw_material_id: optional_string(&event.payload, "raw_material_id"),
                    subject: optional_string(&event.payload, "subject"),
                    from_addresses: array_value(&event.payload, "from"),
                    to_addresses: array_value(&event.payload, "to"),
                    body_bytes: integer_value(&event.payload, "body_bytes").unwrap_or(0),
                    attachment_count: integer_value(&event.payload, "attachment_count")
                        .and_then(|value| i32::try_from(value).ok())
                        .unwrap_or(0),
                    attachment_observed_count: 0,
                    attachment_policy_refs: Value::Array(Vec::new()),
                    last_message_event_id: Some(event.event_id),
                    last_thread_event_id: None,
                    last_attachment_event_id: None,
                })
            }
            "email.thread.observed" => {
                let message_key = message_key(&event.payload)?;
                Some(Self {
                    source_id: event.source_id,
                    mode_id: event.mode_id,
                    message_key,
                    message_id: optional_string(&event.payload, "message_id"),
                    thread_key: optional_string(&event.payload, "thread_key"),
                    thread_root_message_id: optional_string(
                        &event.payload,
                        "thread_root_message_id",
                    ),
                    direction: None,
                    folder: optional_string(&event.payload, "folder"),
                    mailbox_format: optional_string(&event.payload, "mailbox_format"),
                    source_file: optional_string(&event.payload, "source_file"),
                    raw_material_id: optional_string(&event.payload, "raw_material_id"),
                    subject: optional_string(&event.payload, "subject"),
                    from_addresses: array_value(&event.payload, "from"),
                    to_addresses: array_value(&event.payload, "to"),
                    body_bytes: 0,
                    attachment_count: 0,
                    attachment_observed_count: 0,
                    attachment_policy_refs: Value::Array(Vec::new()),
                    last_message_event_id: None,
                    last_thread_event_id: Some(event.event_id),
                    last_attachment_event_id: None,
                })
            }
            "email.attachment.observed" => {
                let message_key = message_key(&event.payload)?;
                let attachment_index = integer_value(&event.payload, "attachment_index")
                    .and_then(|value| i32::try_from(value).ok())
                    .unwrap_or(0);
                let policy_ref = optional_string(&event.payload, "material_policy_ref");
                Some(Self {
                    source_id: event.source_id,
                    mode_id: event.mode_id,
                    message_key,
                    message_id: optional_string(&event.payload, "message_id"),
                    thread_key: None,
                    thread_root_message_id: None,
                    direction: None,
                    folder: optional_string(&event.payload, "folder"),
                    mailbox_format: optional_string(&event.payload, "mailbox_format"),
                    source_file: optional_string(&event.payload, "source_file"),
                    raw_material_id: optional_string(&event.payload, "raw_material_id"),
                    subject: None,
                    from_addresses: Value::Array(Vec::new()),
                    to_addresses: Value::Array(Vec::new()),
                    body_bytes: 0,
                    attachment_count: 0,
                    attachment_observed_count: attachment_index.saturating_add(1),
                    attachment_policy_refs: Value::Array(
                        policy_ref.into_iter().map(Value::String).collect(),
                    ),
                    last_message_event_id: None,
                    last_thread_event_id: None,
                    last_attachment_event_id: Some(event.event_id),
                })
            }
            _ => None,
        }
    }
}

fn message_key(payload: &Value) -> Option<String> {
    optional_string(payload, "message_id").or_else(|| {
        let raw_material_id = optional_string(payload, "raw_material_id")?;
        let source_file = optional_string(payload, "source_file").unwrap_or_default();
        let anchor = payload
            .get("mbox_byte_start")
            .and_then(Value::as_u64)
            .map(|value| value.to_string())
            .unwrap_or_else(|| "0".to_string());
        Some(format!("raw:{raw_material_id}:{source_file}:{anchor}"))
    })
}

fn optional_string(payload: &Value, key: &str) -> Option<String> {
    payload
        .get(key)
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty())
        .map(str::to_string)
}

fn array_value(payload: &Value, key: &str) -> Value {
    payload
        .get(key)
        .filter(|value| value.is_array())
        .cloned()
        .unwrap_or_else(|| Value::Array(Vec::new()))
}

fn integer_value(payload: &Value, key: &str) -> Option<i64> {
    payload.get(key).and_then(Value::as_i64)
}

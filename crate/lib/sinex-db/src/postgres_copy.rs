use crate::JsonValue;
use crate::Timestamp;
use crate::models::Event;
use crate::repositories::events::StreamBatchRow;
use crate::repositories::events::conversions::extract_provenance;
use crate::schema::Events;
use sea_query::{ColumnSpec, ColumnType, Iden};
use sqlx::Error;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::OnceLock;

/// Trait for entities that can be serialized to Postgres COPY text format.
pub trait ToPostgresCopy {
    /// Write the entity to a buffer in Postgres COPY RAW (text) format.
    /// Ends with a newline.
    fn write_copy_row(&self, buf: &mut Vec<u8>) -> Result<(), Error>;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum EventCopyColumnType {
    Uuid,
    Text,
    Jsonb,
    Timestamptz,
    Integer,
    Bigint,
    UuidArray,
}

impl EventCopyColumnType {
    const fn staging_sql(self) -> &'static str {
        match self {
            Self::Uuid => "UUID",
            Self::Text => "TEXT",
            Self::Jsonb => "JSONB",
            Self::Timestamptz => "TIMESTAMPTZ",
            Self::Integer => "INTEGER",
            Self::Bigint => "BIGINT",
            Self::UuidArray => "UUID[]",
        }
    }

    fn insert_select_expr(self, column_name: &str) -> String {
        match self {
            Self::Uuid => format!("{column_name}::uuid"),
            Self::UuidArray => format!("{column_name}::uuid[]"),
            Self::Text | Self::Jsonb | Self::Timestamptz | Self::Integer | Self::Bigint => {
                column_name.to_owned()
            }
        }
    }

    fn matches_schema_type(self, column_type: &ColumnType) -> bool {
        match self {
            Self::Uuid => matches_uuid_type(column_type),
            Self::Text => matches!(column_type, ColumnType::Text),
            Self::Jsonb => matches!(column_type, ColumnType::JsonBinary),
            Self::Timestamptz => matches!(column_type, ColumnType::TimestampWithTimeZone),
            Self::Integer => matches!(column_type, ColumnType::Integer),
            Self::Bigint => matches!(column_type, ColumnType::BigInteger),
            Self::UuidArray => {
                matches!(column_type, ColumnType::Array(inner) if matches_uuid_type(inner.as_ref()))
            }
        }
    }
}

fn matches_uuid_type(column_type: &ColumnType) -> bool {
    match column_type {
        ColumnType::Uuid => true,
        ColumnType::Custom(iden) => iden.to_string().eq_ignore_ascii_case("uuid"),
        _ => false,
    }
}

#[derive(Clone, Copy)]
struct EventCopyColumn {
    event: Events,
    copy_type: EventCopyColumnType,
}

impl std::fmt::Debug for EventCopyColumn {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EventCopyColumn")
            .field("event", &self.event.to_string())
            .field("copy_type", &self.copy_type)
            .finish()
    }
}

impl EventCopyColumn {
    fn name(self) -> String {
        self.event.to_string()
    }

    fn staging_sql(self) -> String {
        format!("{} {}", self.name(), self.copy_type.staging_sql())
    }

    fn insert_select_expr(self) -> String {
        self.copy_type.insert_select_expr(&self.name())
    }
}

const DB_MANAGED_EVENT_COLUMNS: [&str; 2] = ["ts_coided", "ts_persisted"];

const EVENT_COPY_COLUMNS: [EventCopyColumn; 22] = [
    EventCopyColumn {
        event: Events::Id,
        copy_type: EventCopyColumnType::Uuid,
    },
    EventCopyColumn {
        event: Events::Source,
        copy_type: EventCopyColumnType::Text,
    },
    EventCopyColumn {
        event: Events::EventType,
        copy_type: EventCopyColumnType::Text,
    },
    EventCopyColumn {
        event: Events::TsOrig,
        copy_type: EventCopyColumnType::Timestamptz,
    },
    EventCopyColumn {
        event: Events::TsOrigSubnano,
        copy_type: EventCopyColumnType::Integer,
    },
    EventCopyColumn {
        event: Events::Host,
        copy_type: EventCopyColumnType::Text,
    },
    EventCopyColumn {
        event: Events::Payload,
        copy_type: EventCopyColumnType::Jsonb,
    },
    EventCopyColumn {
        event: Events::SourceMaterialId,
        copy_type: EventCopyColumnType::Uuid,
    },
    EventCopyColumn {
        event: Events::AnchorByte,
        copy_type: EventCopyColumnType::Bigint,
    },
    EventCopyColumn {
        event: Events::OffsetStart,
        copy_type: EventCopyColumnType::Bigint,
    },
    EventCopyColumn {
        event: Events::OffsetEnd,
        copy_type: EventCopyColumnType::Bigint,
    },
    EventCopyColumn {
        event: Events::OffsetKind,
        copy_type: EventCopyColumnType::Text,
    },
    EventCopyColumn {
        event: Events::SourceEventIds,
        copy_type: EventCopyColumnType::UuidArray,
    },
    EventCopyColumn {
        event: Events::PayloadSchemaId,
        copy_type: EventCopyColumnType::Uuid,
    },
    EventCopyColumn {
        event: Events::NodeRunId,
        copy_type: EventCopyColumnType::Uuid,
    },
    EventCopyColumn {
        event: Events::AssociatedBlobIds,
        copy_type: EventCopyColumnType::UuidArray,
    },
    EventCopyColumn {
        event: Events::TemporalPolicy,
        copy_type: EventCopyColumnType::Text,
    },
    EventCopyColumn {
        event: Events::SemanticsVersion,
        copy_type: EventCopyColumnType::Text,
    },
    EventCopyColumn {
        event: Events::ScopeKey,
        copy_type: EventCopyColumnType::Text,
    },
    EventCopyColumn {
        event: Events::EquivalenceKey,
        copy_type: EventCopyColumnType::Text,
    },
    EventCopyColumn {
        event: Events::CreatedByOperationId,
        copy_type: EventCopyColumnType::Uuid,
    },
    EventCopyColumn {
        event: Events::NodeModel,
        copy_type: EventCopyColumnType::Text,
    },
];

static EVENT_COPY_CONTRACT_CHECK: OnceLock<()> = OnceLock::new();

fn copy_columns() -> &'static [EventCopyColumn] {
    EVENT_COPY_CONTRACT_CHECK.get_or_init(verify_event_copy_contract);
    &EVENT_COPY_COLUMNS
}

#[derive(Debug, Clone)]
struct AuthoritativeEventColumn {
    column_type: ColumnType,
    not_null: bool,
}

fn authoritative_copy_columns() -> BTreeMap<String, AuthoritativeEventColumn> {
    Events::create_table_statement()
        .get_columns()
        .iter()
        .filter_map(|column| {
            let name = column.get_column_name();
            if DB_MANAGED_EVENT_COLUMNS.contains(&name.as_str()) {
                return None;
            }

            let column_type = column.get_column_type().cloned().unwrap_or_else(|| {
                panic!("core.events column {name} has no declared type in schema authority")
            });

            Some((
                name,
                AuthoritativeEventColumn {
                    column_type,
                    not_null: column_is_not_null(column),
                },
            ))
        })
        .collect()
}

fn column_is_not_null(column: &sea_query::ColumnDef) -> bool {
    column
        .get_column_spec()
        .iter()
        .any(|spec| matches!(spec, ColumnSpec::NotNull))
}

pub fn verify_event_copy_contract() {
    let authoritative_columns = authoritative_copy_columns();

    let mut contract_names = BTreeSet::new();
    let mut duplicate_contract_names = BTreeSet::new();
    for column in EVENT_COPY_COLUMNS {
        let name = column.name();
        if !contract_names.insert(name.clone()) {
            duplicate_contract_names.insert(name);
        }
    }

    assert!(
        duplicate_contract_names.is_empty(),
        "COPY contract duplicates core.events columns: {}",
        duplicate_contract_names
            .into_iter()
            .collect::<Vec<_>>()
            .join(", ")
    );

    let authoritative_names: BTreeSet<String> = authoritative_columns.keys().cloned().collect();
    if contract_names != authoritative_names {
        let missing_from_copy = authoritative_names
            .difference(&contract_names)
            .cloned()
            .collect::<Vec<_>>();
        let extra_in_copy = contract_names
            .difference(&authoritative_names)
            .cloned()
            .collect::<Vec<_>>();

        panic!(
            "COPY contract drifted from authoritative core.events schema; missing_from_copy=[{}] extra_in_copy=[{}]",
            missing_from_copy.join(", "),
            extra_in_copy.join(", ")
        );
    }

    let mut type_mismatches = Vec::new();
    for column in EVENT_COPY_COLUMNS {
        let name = column.name();
        let authoritative_type = authoritative_columns
            .get(&name)
            .unwrap_or_else(|| panic!("verified COPY column {name} missing from schema map"));

        if !column
            .copy_type
            .matches_schema_type(&authoritative_type.column_type)
        {
            type_mismatches.push(format!(
                "{name}: schema={:?}, copy={:?}",
                authoritative_type.column_type, column.copy_type
            ));
        }
    }

    assert!(
        type_mismatches.is_empty(),
        "COPY contract type drifted from authoritative core.events schema: {}",
        type_mismatches.join("; ")
    );
}

#[cfg(test)]
pub(crate) fn event_copy_column_count() -> usize {
    copy_columns().len()
}

#[cfg(test)]
pub(crate) fn event_copy_column_index(event: Events) -> usize {
    let event_name = event.to_string();
    copy_columns()
        .iter()
        .position(|column| column.name() == event_name)
        .unwrap_or_else(|| panic!("COPY contract missing core.events column {event_name}"))
}

pub(crate) fn event_copy_column_list_sql() -> String {
    copy_columns()
        .iter()
        .map(|column| column.name())
        .collect::<Vec<_>>()
        .join(", ")
}

pub(crate) fn event_copy_staging_columns_sql() -> String {
    let authoritative = authoritative_copy_columns();
    copy_columns()
        .iter()
        .map(|column| {
            let name = column.name();
            let authoritative_column = authoritative
                .get(&name)
                .unwrap_or_else(|| panic!("COPY contract missing authoritative schema for {name}"));

            let mut sql = column.staging_sql();
            if authoritative_column.not_null {
                sql.push_str(" NOT NULL");
            }
            sql
        })
        .collect::<Vec<_>>()
        .join(",\n                ")
}

pub(crate) fn event_copy_insert_select_sql() -> String {
    copy_columns()
        .iter()
        .map(|column| column.insert_select_expr())
        .collect::<Vec<_>>()
        .join(",\n                ")
}

struct CopyRowWriter<'a> {
    buf: &'a mut Vec<u8>,
    fields_written: usize,
}

impl<'a> CopyRowWriter<'a> {
    fn new(buf: &'a mut Vec<u8>) -> Self {
        let _ = copy_columns();
        Self {
            buf,
            fields_written: 0,
        }
    }

    fn field(&mut self, event: Events, value: Option<&str>) -> Result<(), Error> {
        self.begin_field(event)?;
        write_field(self.buf, value);
        Ok(())
    }

    fn i64_field(&mut self, event: Events, value: Option<i64>) -> Result<(), Error> {
        self.begin_field(event)?;
        write_i64_field(self.buf, value);
        Ok(())
    }

    fn begin_field(&mut self, event: Events) -> Result<(), Error> {
        let actual_name = event.to_string();
        let expected = copy_columns().get(self.fields_written).ok_or_else(|| {
            Error::Protocol(format!(
                "COPY writer emitted unexpected extra field {actual_name}"
            ))
        })?;
        let expected_name = expected.name();

        if actual_name != expected_name {
            return Err(Error::Protocol(format!(
                "COPY writer column order drift at field {}: expected {expected_name}, got {actual_name}",
                self.fields_written
            )));
        }

        if self.fields_written > 0 {
            self.buf.push(b'\t');
        }
        self.fields_written += 1;
        Ok(())
    }

    fn finish(self) -> Result<(), Error> {
        let expected_field_count = copy_columns().len();
        if self.fields_written != expected_field_count {
            return Err(Error::Protocol(format!(
                "COPY writer emitted {} fields, expected {}",
                self.fields_written, expected_field_count
            )));
        }

        self.buf.push(b'\n');
        Ok(())
    }
}

impl ToPostgresCopy for Event<JsonValue> {
    fn write_copy_row(&self, buf: &mut Vec<u8>) -> Result<(), Error> {
        let id = self
            .id
            .as_ref()
            .ok_or_else(|| Error::Protocol("Event missing ID for COPY insert".into()))?
            .as_uuid()
            .to_string();

        let ts_orig = self
            .ts_orig
            .ok_or_else(|| Error::Protocol("Event missing ts_orig for COPY insert".into()))?;
        let (pg_ts, ts_orig_subnano) = ts_orig.to_postgres_parts();
        let ts_orig_str = Timestamp::from(pg_ts).format_rfc3339();

        let payload = serde_json::to_string(&self.payload).map_err(|err| {
            Error::Protocol(format!("Failed to serialize payload for COPY: {err}"))
        })?;

        let (_, source_material_id, offset_start, offset_end, offset_kind, anchor_byte) =
            extract_provenance(self).map_err(|e| Error::Protocol(e.to_string()))?;

        let source_material_id = source_material_id.map(|id| id.to_string());

        let payload_schema_id = self
            .payload_schema_id
            .as_ref()
            .map(std::string::ToString::to_string);

        let source_event_ids_str = self.get_source_event_ids().map(|ids| {
            let formatted: Vec<String> = ids.iter().map(|id| id.to_uuid().to_string()).collect();
            format!("{{{}}}", formatted.join(",")) // Postgres array format {uuid,uuid}
        });

        let associated_blob_ids_str = self.associated_blob_ids.as_ref().map(|ids| {
            let formatted: Vec<String> = ids.iter().map(std::string::ToString::to_string).collect();
            format!("{{{}}}", formatted.join(","))
        });

        let mut writer = CopyRowWriter::new(buf);
        writer.field(Events::Id, Some(&id))?;
        writer.field(Events::Source, Some(self.source.as_str()))?;
        writer.field(Events::EventType, Some(self.event_type.as_str()))?;
        writer.field(Events::TsOrig, Some(&ts_orig_str))?;
        writer.i64_field(Events::TsOrigSubnano, Some(i64::from(ts_orig_subnano)))?;
        writer.field(Events::Host, Some(self.host.as_str()))?;
        writer.field(Events::Payload, Some(&payload))?;
        writer.field(Events::SourceMaterialId, source_material_id.as_deref())?;
        writer.i64_field(Events::AnchorByte, anchor_byte)?;
        writer.i64_field(Events::OffsetStart, offset_start)?;
        writer.i64_field(Events::OffsetEnd, offset_end)?;
        writer.field(Events::OffsetKind, offset_kind.as_deref())?;
        writer.field(Events::SourceEventIds, source_event_ids_str.as_deref())?;
        writer.field(Events::PayloadSchemaId, payload_schema_id.as_deref())?;
        {
            let node_run_id_str = self.node_run_id.map(|id| id.to_string());
            writer.field(Events::NodeRunId, node_run_id_str.as_deref())?;
        }
        writer.field(
            Events::AssociatedBlobIds,
            associated_blob_ids_str.as_deref(),
        )?;
        writer.field(
            Events::TemporalPolicy,
            self.temporal_policy
                .as_ref()
                .map(std::string::ToString::to_string)
                .as_deref(),
        )?;
        writer.field(Events::SemanticsVersion, self.semantics_version.as_deref())?;
        writer.field(Events::ScopeKey, self.scope_key.as_deref())?;
        writer.field(Events::EquivalenceKey, self.equivalence_key.as_deref())?;
        {
            let created_by_str = self.created_by_operation_id.map(|id| id.to_string());
            writer.field(Events::CreatedByOperationId, created_by_str.as_deref())?;
        }
        writer.field(
            Events::NodeModel,
            self.node_model
                .as_ref()
                .map(std::string::ToString::to_string)
                .as_deref(),
        )?;

        writer.finish()
    }
}

/// COPY text format serializer for `StreamBatchRow`.
///
/// Column order matches the staging table and the INSERT statement in
/// `execute_batch_insert_copy`. IDs are written as UUIDs (36-char form with
/// dashes) because the staging table uses UUID columns — the INSERT SELECT
/// applies `::uuid` casts when moving rows into `core.events`.
impl ToPostgresCopy for StreamBatchRow {
    fn write_copy_row(&self, buf: &mut Vec<u8>) -> Result<(), Error> {
        let id = self.id.to_string();
        let (pg_ts, ts_orig_subnano) = self.ts_orig.to_postgres_parts();
        let ts_orig_str = Timestamp::from(pg_ts).format_rfc3339();

        let payload = serde_json::to_string(&self.payload).map_err(|err| {
            Error::Protocol(format!("Failed to serialize payload for COPY: {err}"))
        })?;

        let source_material_id_str = self.source_material_id.map(|id| id.to_uuid().to_string());
        let payload_schema_id_str = self.payload_schema_id.map(|id| id.to_string());

        let source_event_ids_str = self.source_event_ids.as_ref().map(|ids| {
            let formatted: Vec<String> = ids.iter().map(|id| id.to_uuid().to_string()).collect();
            format!("{{{}}}", formatted.join(","))
        });

        let associated_blob_ids_str =
            self.associated_blob_ids
                .as_ref()
                .map(|ids: &Vec<uuid::Uuid>| {
                    let formatted: Vec<String> =
                        ids.iter().map(|id: &uuid::Uuid| id.to_string()).collect();
                    format!("{{{}}}", formatted.join(","))
                });

        let mut writer = CopyRowWriter::new(buf);
        writer.field(Events::Id, Some(&id))?;
        writer.field(Events::Source, Some(self.source.as_str()))?;
        writer.field(Events::EventType, Some(self.event_type.as_str()))?;
        writer.field(Events::TsOrig, Some(&ts_orig_str))?;
        writer.i64_field(Events::TsOrigSubnano, Some(i64::from(ts_orig_subnano)))?;
        writer.field(Events::Host, Some(self.host.as_str()))?;
        writer.field(Events::Payload, Some(&payload))?;
        writer.field(Events::SourceMaterialId, source_material_id_str.as_deref())?;
        writer.i64_field(Events::AnchorByte, self.anchor_byte)?;
        writer.i64_field(Events::OffsetStart, self.offset_start)?;
        writer.i64_field(Events::OffsetEnd, self.offset_end)?;
        writer.field(Events::OffsetKind, self.offset_kind.as_deref())?;
        writer.field(Events::SourceEventIds, source_event_ids_str.as_deref())?;
        writer.field(Events::PayloadSchemaId, payload_schema_id_str.as_deref())?;
        {
            let node_run_id_str = self.node_run_id.map(|id| id.to_string());
            writer.field(Events::NodeRunId, node_run_id_str.as_deref())?;
        }
        writer.field(
            Events::AssociatedBlobIds,
            associated_blob_ids_str.as_deref(),
        )?;
        writer.field(Events::TemporalPolicy, self.temporal_policy.as_deref())?;
        writer.field(Events::SemanticsVersion, self.semantics_version.as_deref())?;
        writer.field(Events::ScopeKey, self.scope_key.as_deref())?;
        writer.field(Events::EquivalenceKey, self.equivalence_key.as_deref())?;
        {
            let created_by_str = self.created_by_operation_id.map(|id| id.to_string());
            writer.field(Events::CreatedByOperationId, created_by_str.as_deref())?;
        }
        writer.field(Events::NodeModel, self.node_model.as_deref())?;

        writer.finish()
    }
}

pub(crate) fn write_field(buf: &mut Vec<u8>, val: Option<&str>) {
    match val {
        Some(s) => escape_copy_str(buf, s),
        None => buf.extend_from_slice(b"\\N"),
    }
}

pub(crate) fn write_i64_field(buf: &mut Vec<u8>, val: Option<i64>) {
    match val {
        Some(v) => {
            let mut itoa_buf = itoa::Buffer::new();
            buf.extend_from_slice(itoa_buf.format(v).as_bytes());
        }
        None => buf.extend_from_slice(b"\\N"),
    }
}

pub fn escape_copy_str(buf: &mut Vec<u8>, s: &str) {
    let bytes = s.as_bytes();

    // \r is extremely rare in event data (JSON, paths, commands). When absent,
    // we can use memchr3 SIMD to scan for the 3 common specials in bulk.
    if memchr::memchr(b'\r', bytes).is_some() {
        escape_copy_str_slow(buf, bytes);
        return;
    }

    let mut prev = 0;
    for pos in memchr::memchr3_iter(b'\t', b'\n', b'\\', bytes) {
        buf.extend_from_slice(&bytes[prev..pos]);
        match bytes[pos] {
            b'\t' => buf.extend_from_slice(b"\\t"),
            b'\n' => buf.extend_from_slice(b"\\n"),
            b'\\' => buf.extend_from_slice(b"\\\\"),
            _ => unreachable!(),
        }
        prev = pos + 1;
    }
    buf.extend_from_slice(&bytes[prev..]);
}

/// Byte-by-byte fallback for strings containing \r (very rare).
fn escape_copy_str_slow(buf: &mut Vec<u8>, bytes: &[u8]) {
    for &b in bytes {
        match b {
            b'\t' => buf.extend_from_slice(b"\\t"),
            b'\r' => buf.extend_from_slice(b"\\r"),
            b'\n' => buf.extend_from_slice(b"\\n"),
            b'\\' => buf.extend_from_slice(b"\\\\"),
            _ => buf.push(b),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Event;
    use crate::repositories::events::StreamBatchRow;
    use serde_json::json;
    use sinex_primitives::domain::{EventSource, EventType};
    use sinex_primitives::events::EventId;
    use sinex_primitives::{Id, Timestamp, Uuid};
    use xtask::sandbox::sinex_test;

    fn minimal_row() -> StreamBatchRow {
        StreamBatchRow {
            id: Uuid::now_v7(),
            source: EventSource::from_static("test.source"),
            event_type: EventType::from_static("test.event"),
            ts_orig: Timestamp::now(),
            host: sinex_primitives::domain::HostName::from_static("localhost"),
            payload: json!({"ok": true}),
            source_material_id: None,
            anchor_byte: None,
            offset_start: None,
            offset_end: None,
            offset_kind: None,
            source_event_ids: None,
            payload_schema_id: None,
            node_run_id: None,
            associated_blob_ids: None,
            temporal_policy: None,
            semantics_version: None,
            scope_key: None,
            equivalence_key: None,
            created_by_operation_id: None,
            node_model: None,
        }
    }

    fn row_fields(row: &StreamBatchRow) -> Vec<String> {
        let mut buf = Vec::new();
        row.write_copy_row(&mut buf).expect("write_copy_row failed");
        let s = String::from_utf8(buf).expect("non-UTF-8 output");
        // Strip the trailing newline before splitting
        let trimmed = s.trim_end_matches('\n');
        trimmed.split('\t').map(str::to_string).collect()
    }

    /// The COPY format must have exactly one field per authoritative writable event column.
    #[sinex_test]
    async fn produces_exactly_22_fields() -> ::xtask::sandbox::TestResult<()> {
        let fields = row_fields(&minimal_row());
        assert_eq!(
            fields.len(),
            event_copy_column_count(),
            "Expected {} tab-separated fields, got {}:\n{fields:?}",
            event_copy_column_count(),
            fields.len()
        );
        Ok(())
    }

    /// Row must end with a newline — required by Postgres COPY text protocol.
    #[sinex_test]
    async fn row_ends_with_newline() -> ::xtask::sandbox::TestResult<()> {
        let mut buf = Vec::new();
        minimal_row().write_copy_row(&mut buf).unwrap();
        assert_eq!(*buf.last().unwrap(), b'\n', "Row must end with newline");
        Ok(())
    }

    /// Null optional fields must emit the `\N` sentinel.
    #[sinex_test]
    async fn null_optionals_write_null_sentinel() -> ::xtask::sandbox::TestResult<()> {
        let fields = row_fields(&minimal_row());
        for event in [
            Events::SourceMaterialId,
            Events::AnchorByte,
            Events::OffsetStart,
            Events::OffsetEnd,
            Events::OffsetKind,
            Events::SourceEventIds,
            Events::PayloadSchemaId,
            Events::NodeRunId,
            Events::AssociatedBlobIds,
            Events::TemporalPolicy,
            Events::SemanticsVersion,
            Events::ScopeKey,
            Events::EquivalenceKey,
            Events::CreatedByOperationId,
            Events::NodeModel,
        ] {
            let idx = event_copy_column_index(event);
            assert_eq!(
                fields[idx], "\\N",
                "Field {idx} should be \\N for None, got {:?}",
                fields[idx]
            );
        }
        Ok(())
    }

    #[sinex_test]
    async fn missing_event_ts_orig_is_rejected() -> ::xtask::sandbox::TestResult<()> {
        let event = Event::<JsonValue> {
            id: Some(Id::new()),
            source: EventSource::from_static("test.source"),
            event_type: EventType::from_static("test.event"),
            payload: json!({"ok": true}),
            ts_orig: None,
            host: sinex_primitives::domain::HostName::from_static("localhost"),
            node_run_id: None,
            payload_schema_id: None,
            provenance: crate::Provenance::Material {
                id: Id::new(),
                anchor_byte: 0,
                offset_start: None,
                offset_end: None,
                offset_kind: sinex_primitives::events::builder::OffsetKind::Byte,
            },
            associated_blob_ids: None,
            temporal_policy: None,
            semantics_version: None,
            scope_key: None,
            equivalence_key: None,
            created_by_operation_id: None,
            node_model: None,
        };

        let mut buf = Vec::new();
        let error = event
            .write_copy_row(&mut buf)
            .expect_err("missing ts_orig must be rejected");
        assert!(error.to_string().contains("missing ts_orig"));
        Ok(())
    }

    /// Tabs inside a field value must be escaped to `\t` so Postgres doesn't
    /// mistake them for field delimiters.
    #[sinex_test]
    async fn tab_in_payload_is_escaped() -> ::xtask::sandbox::TestResult<()> {
        let mut row = minimal_row();
        row.payload = json!({"k": "v\tw"});
        let fields = row_fields(&row);
        let payload_field = &fields[event_copy_column_index(Events::Payload)];
        assert!(
            !payload_field.contains('\t'),
            "Literal tab must be escaped in payload"
        );
        assert!(
            payload_field.contains("\\t"),
            "Escaped \\t must appear in payload, got: {payload_field:?}"
        );
        Ok(())
    }

    /// Newlines inside a field value must be escaped to `\n`.
    #[sinex_test]
    async fn newline_in_payload_is_escaped() -> ::xtask::sandbox::TestResult<()> {
        let mut row = minimal_row();
        row.payload = json!({"k": "line1\nline2"});
        let fields = row_fields(&row);
        let payload_field = &fields[event_copy_column_index(Events::Payload)];
        assert!(
            !payload_field.contains('\n'),
            "Literal newline must be escaped"
        );
        assert!(payload_field.contains("\\n"), "Escaped \\n must appear");
        Ok(())
    }

    /// Backslashes must be doubled.
    #[sinex_test]
    async fn backslash_in_source_is_doubled() -> ::xtask::sandbox::TestResult<()> {
        let mut row = minimal_row();
        row.payload = json!({"path": "C:\\Users\\test"});
        let fields = row_fields(&row);
        // "C:\\Users\\test" → JSON string "C:\Users\test" → COPY escaped "C:\\Users\\test"
        assert!(
            fields[event_copy_column_index(Events::Payload)].contains("\\\\"),
            "Backslash should be doubled in COPY output"
        );
        Ok(())
    }

    /// UUID arrays must use Postgres `{uuid1,uuid2}` format.
    #[sinex_test]
    async fn uuid_arrays_use_postgres_brace_format() -> ::xtask::sandbox::TestResult<()> {
        let id1: EventId = Id::new();
        let id2: EventId = Id::new();
        let u1 = Uuid::new_v4();
        let u2 = Uuid::new_v4();
        let mut row = minimal_row();
        row.source_event_ids = Some(vec![id1, id2]);
        row.associated_blob_ids = Some(vec![u1, u2]);

        let fields = row_fields(&row);
        let sei = &fields[event_copy_column_index(Events::SourceEventIds)];
        let abi = &fields[event_copy_column_index(Events::AssociatedBlobIds)];

        assert!(
            sei.starts_with('{') && sei.ends_with('}'),
            "source_event_ids must be {{...}}"
        );
        assert!(
            abi.starts_with('{') && abi.ends_with('}'),
            "associated_blob_ids must be {{...}}"
        );
        assert!(
            sei.contains(&id1.to_uuid().to_string()),
            "source_event_ids must contain id1"
        );
        assert!(
            sei.contains(&id2.to_uuid().to_string()),
            "source_event_ids must contain id2"
        );
        assert!(
            abi.contains(&u1.to_string()),
            "associated_blob_ids must contain u1"
        );
        Ok(())
    }

    /// Numeric fields (anchor_byte, ts_orig_subnano, …) must be plain digits, not `\N`.
    #[sinex_test]
    async fn numeric_fields_are_written_as_digits() -> ::xtask::sandbox::TestResult<()> {
        let mut row = minimal_row();
        row.source_material_id = Some(Id::new());
        row.anchor_byte = Some(42);
        row.offset_start = Some(0);
        row.offset_end = Some(100);

        let fields = row_fields(&row);
        assert_ne!(
            fields[event_copy_column_index(Events::SourceMaterialId)],
            "\\N",
            "source_material_id should not be \\N"
        );
        assert_eq!(
            fields[event_copy_column_index(Events::AnchorByte)],
            "42",
            "anchor_byte should be '42'"
        );
        assert_eq!(
            fields[event_copy_column_index(Events::OffsetStart)],
            "0",
            "offset_start should be '0'"
        );
        assert_eq!(
            fields[event_copy_column_index(Events::OffsetEnd)],
            "100",
            "offset_end should be '100'"
        );
        Ok(())
    }

    /// The ID in field 0 must be a valid UUID (36-char hyphenated form) because
    /// the staging table uses UUID columns and the INSERT SELECT applies `::uuid`.
    #[sinex_test]
    async fn id_is_written_as_uuid_native() -> ::xtask::sandbox::TestResult<()> {
        let id = Uuid::now_v7();
        let mut row = minimal_row();
        row.id = id;
        let fields = row_fields(&row);
        // UUID has 36 chars (8-4-4-4-12 + 4 hyphens)
        assert_eq!(
            fields[event_copy_column_index(Events::Id)].len(),
            36,
            "ID field should be UUID (36 chars), got {:?}",
            fields[event_copy_column_index(Events::Id)]
        );
        assert!(
            fields[event_copy_column_index(Events::Id)].contains('-'),
            "UUID must contain hyphens"
        );
        // Must round-trip through Uuid
        let parsed: Uuid = fields[event_copy_column_index(Events::Id)]
            .parse()
            .expect("id field must be parseable as UUID");
        assert_eq!(parsed, id, "UUID must match original Uuid's UUID");
        Ok(())
    }

    /// Carriage returns must be escaped to `\r` (exercises the slow fallback path).
    #[sinex_test]
    async fn carriage_return_in_payload_is_escaped() -> ::xtask::sandbox::TestResult<()> {
        let mut row = minimal_row();
        row.payload = json!({"k": "line1\r\nline2"});
        let fields = row_fields(&row);
        let payload_field = &fields[event_copy_column_index(Events::Payload)];
        assert!(
            !payload_field.contains('\r'),
            "Literal \\r must be escaped in payload"
        );
        assert!(
            payload_field.contains("\\r"),
            "Escaped \\r must appear in payload, got: {payload_field:?}"
        );
        assert!(
            payload_field.contains("\\n"),
            "Escaped \\n must appear alongside \\r"
        );
        Ok(())
    }

    /// Verify escape_copy_str directly for edge cases.
    #[sinex_test]
    async fn escape_copy_str_unit_tests() -> ::xtask::sandbox::TestResult<()> {
        let mut buf = Vec::new();

        // No specials — bulk copy
        super::escape_copy_str(&mut buf, "hello world");
        assert_eq!(buf, b"hello world");

        // Tab + newline
        buf.clear();
        super::escape_copy_str(&mut buf, "a\tb\nc");
        assert_eq!(buf, b"a\\tb\\nc");

        // Backslash
        buf.clear();
        super::escape_copy_str(&mut buf, "C:\\Users");
        assert_eq!(buf, b"C:\\\\Users");

        // \r triggers slow path
        buf.clear();
        super::escape_copy_str(&mut buf, "a\rb");
        assert_eq!(buf, b"a\\rb");

        // Mixed \r\n\t\\
        buf.clear();
        super::escape_copy_str(&mut buf, "\t\r\n\\");
        assert_eq!(buf, b"\\t\\r\\n\\\\");
        Ok(())
    }
}

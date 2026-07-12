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
    Bytea,
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
            Self::Bytea => "BYTEA",
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
            Self::Text
            | Self::Jsonb
            | Self::Bytea
            | Self::Timestamptz
            | Self::Integer
            | Self::Bigint => column_name.to_owned(),
        }
    }

    fn matches_schema_type(self, column_type: &ColumnType) -> bool {
        match self {
            Self::Uuid => matches_uuid_type(column_type),
            Self::Text => matches!(column_type, ColumnType::Text),
            Self::Jsonb => matches!(column_type, ColumnType::JsonBinary),
            Self::Bytea => matches!(
                column_type,
                ColumnType::Custom(iden) if iden.to_string().eq_ignore_ascii_case("bytea")
            ),
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

const EVENT_COPY_COLUMNS: [EventCopyColumn; 30] = [
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
        event: Events::ModuleRunId,
        copy_type: EventCopyColumnType::Uuid,
    },
    EventCopyColumn {
        event: Events::AnchorPayloadHash,
        copy_type: EventCopyColumnType::Bytea,
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
        event: Events::AutomatonModel,
        copy_type: EventCopyColumnType::Text,
    },
    EventCopyColumn {
        event: Events::TsQuality,
        copy_type: EventCopyColumnType::Text,
    },
    // Derivation control plane (sinex-0vx.4 / W1). COPY-based bulk material
    // import is not a derived-output write path (product_class is required
    // only when source_event_ids is set — see the events_derived_requires_
    // product_class CHECK in sinex-schema), so both ToPostgresCopy impls
    // below write NULL unconditionally for these six columns. Wiring COPY
    // batches to actually declare a product_class is out of scope for this
    // bead (sinex-db COPY/batch-path waves 8cr.2/0vx.5/0vx.6); this addition
    // is the minimal, mechanical fix required to keep verify_event_copy_
    // contract() (and query_as_insert_columns_match_copy_contract) green
    // after core.events gained these columns.
    EventCopyColumn {
        event: Events::ProductClass,
        copy_type: EventCopyColumnType::Text,
    },
    EventCopyColumn {
        event: Events::ClaimSupport,
        copy_type: EventCopyColumnType::Jsonb,
    },
    EventCopyColumn {
        event: Events::DerivationDeclarationId,
        copy_type: EventCopyColumnType::Text,
    },
    EventCopyColumn {
        event: Events::DerivationEpochId,
        copy_type: EventCopyColumnType::Uuid,
    },
    EventCopyColumn {
        event: Events::DerivationLaneId,
        copy_type: EventCopyColumnType::Uuid,
    },
    EventCopyColumn {
        event: Events::AdjudicationEventId,
        copy_type: EventCopyColumnType::Uuid,
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

#[allow(
    clippy::panic,
    reason = "Schema-authority drift detector: panic if the declared schema lacks a type for a core.events column"
)]
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

#[allow(
    clippy::panic,
    reason = "COPY contract drift detector: panic at startup if COPY columns drift from schema authority"
)]
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

#[allow(
    clippy::panic,
    reason = "Staging SQL builder: panic if authoritative schema lacks a COPY-listed column (drift)"
)]
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

    fn bytea_field(&mut self, event: Events, value: Option<&[u8]>) -> Result<(), Error> {
        self.begin_field(event)?;
        write_bytea_field(self.buf, value);
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
            let module_run_id_str = self.module_run_id.map(|id| id.to_string());
            writer.field(Events::ModuleRunId, module_run_id_str.as_deref())?;
        }
        writer.bytea_field(
            Events::AnchorPayloadHash,
            self.anchor_payload_hash.as_deref(),
        )?;
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
            Events::AutomatonModel,
            self.automaton_model
                .as_ref()
                .map(std::string::ToString::to_string)
                .as_deref(),
        )?;
        writer.field(
            Events::TsQuality,
            self.ts_quality
                .as_ref()
                .map(std::string::ToString::to_string)
                .as_deref(),
        )?;
        // Derivation control plane (sinex-0vx.4 / W1): see the EVENT_COPY_COLUMNS
        // doc comment above — COPY is not a derived-output write path yet.
        writer.field(Events::ProductClass, None)?;
        writer.field(Events::ClaimSupport, None)?;
        writer.field(Events::DerivationDeclarationId, None)?;
        writer.field(Events::DerivationEpochId, None)?;
        writer.field(Events::DerivationLaneId, None)?;
        writer.field(Events::AdjudicationEventId, None)?;

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
            let module_run_id_str = self.module_run_id.map(|id| id.to_string());
            writer.field(Events::ModuleRunId, module_run_id_str.as_deref())?;
        }
        writer.bytea_field(
            Events::AnchorPayloadHash,
            self.anchor_payload_hash.as_deref(),
        )?;
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
        writer.field(Events::AutomatonModel, self.automaton_model.as_deref())?;
        writer.field(Events::TsQuality, self.ts_quality.as_deref())?;
        // Derivation control plane (sinex-0vx.4 / W1): see the EVENT_COPY_COLUMNS
        // doc comment above — COPY is not a derived-output write path yet.
        writer.field(Events::ProductClass, None)?;
        writer.field(Events::ClaimSupport, None)?;
        writer.field(Events::DerivationDeclarationId, None)?;
        writer.field(Events::DerivationEpochId, None)?;
        writer.field(Events::DerivationLaneId, None)?;
        writer.field(Events::AdjudicationEventId, None)?;

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

pub(crate) fn write_bytea_field(buf: &mut Vec<u8>, val: Option<&[u8]>) {
    const HEX: &[u8; 16] = b"0123456789abcdef";

    match val {
        Some(bytes) => {
            buf.extend_from_slice(b"\\\\x");
            for &byte in bytes {
                buf.push(HEX[(byte >> 4) as usize]);
                buf.push(HEX[(byte & 0x0f) as usize]);
            }
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
#[path = "postgres_copy_test.rs"]
mod tests;

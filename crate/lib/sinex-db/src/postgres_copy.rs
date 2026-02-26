use crate::models::{Event, JsonValue};
use crate::repositories::events::conversions::extract_provenance;
use crate::repositories::events::StreamBatchRow;
use crate::Timestamp;
use sqlx::Error;

/// Trait for entities that can be serialized to Postgres COPY text format.
pub trait ToPostgresCopy {
    /// Write the entity to a buffer in Postgres COPY RAW (text) format.
    /// Ends with a newline.
    fn write_copy_row(&self, buf: &mut Vec<u8>) -> Result<(), Error>;
}

impl ToPostgresCopy for Event<JsonValue> {
    fn write_copy_row(&self, buf: &mut Vec<u8>) -> Result<(), Error> {
        let id = self
            .id
            .as_ref()
            .ok_or_else(|| Error::Protocol("Event missing ID for COPY insert".into()))?
            .as_ulid()
            .to_string();

        let ts_val = self.ts_orig.unwrap_or_else(Timestamp::now);
        let (pg_ts, ts_orig_subnano) = ts_val.to_postgres_parts();

        // format_rfc3339 expects Timestamp, pg_ts is OffsetDateTime
        let ts_orig_str = Timestamp::from(pg_ts).format_rfc3339();

        let payload = serde_json::to_string(&self.payload).map_err(|err| {
            Error::Protocol(format!("Failed to serialize payload for COPY: {err}"))
        })?;

        let (
            _,
            source_material_id,
            offset_start,
            offset_end,
            offset_kind,
            anchor_byte,
        ) = extract_provenance(self).map_err(|e| Error::Protocol(e.to_string()))?;

        let source_material_id = source_material_id.map(|id| id.to_string());

        let payload_schema_id = self.payload_schema_id.as_ref().map(|id| id.as_uuid().to_string());

        let source_event_ids_str = self.get_source_event_ids().map(|ids| {
            let formatted: Vec<String> = ids.iter().map(|id| id.to_uuid().to_string()).collect();
            format!("{{{}}}", formatted.join(",")) // Postgres array format {uuid,uuid}
        });

        let associated_blob_ids_str = self.associated_blob_ids.as_ref().map(|ids| {
            let formatted: Vec<String> = ids.iter().map(|id| id.as_uuid().to_string()).collect();
            format!("{{{}}}", formatted.join(","))
        });

        // Write fields separated by tab
        write_field(buf, Some(&id));
        buf.push(b'\t');
        write_field(buf, Some(self.source.as_str()));
        buf.push(b'\t');
        write_field(buf, Some(self.event_type.as_str()));
        buf.push(b'\t');
        write_field(buf, Some(&ts_orig_str));
        buf.push(b'\t');
        write_i64_field(buf, Some(ts_orig_subnano as i64));
        buf.push(b'\t');
        write_field(buf, Some(self.host.as_str()));
        buf.push(b'\t');
        write_field(buf, Some(&payload));
        buf.push(b'\t');
        write_field(buf, source_material_id.as_deref());
        buf.push(b'\t');
        write_i64_field(buf, anchor_byte);
        buf.push(b'\t');
        write_i64_field(buf, offset_start);
        buf.push(b'\t');
        write_i64_field(buf, offset_end);
        buf.push(b'\t');
        write_field(buf, offset_kind.as_deref());
        buf.push(b'\t');
        write_field(buf, source_event_ids_str.as_deref());
        buf.push(b'\t');
        write_field(buf, payload_schema_id.as_deref());
        buf.push(b'\t');
        write_field(buf, self.node_version.as_deref());
        buf.push(b'\t');
        write_field(buf, associated_blob_ids_str.as_deref());

        buf.push(b'\n');

        Ok(())
    }
}

/// COPY text format serializer for `StreamBatchRow`.
///
/// Column order matches the staging table and the INSERT statement in
/// `execute_batch_insert_copy`. IDs are written as UUIDs (36-char form with
/// dashes) because the staging table uses UUID columns — the INSERT SELECT
/// applies `::uuid::ulid` casts when moving rows into `core.events`.
impl ToPostgresCopy for StreamBatchRow {
    fn write_copy_row(&self, buf: &mut Vec<u8>) -> Result<(), Error> {
        let id = self.id.as_uuid().to_string();
        let (pg_ts, ts_orig_subnano) = self.ts_orig.to_postgres_parts();
        let ts_orig_str = Timestamp::from(pg_ts).format_rfc3339();

        let payload = serde_json::to_string(&self.payload).map_err(|err| {
            Error::Protocol(format!("Failed to serialize payload for COPY: {err}"))
        })?;

        let source_material_id_str = self.source_material_id.map(|id| id.to_string());
        let payload_schema_id_str = self.payload_schema_id.map(|id| id.to_string());

        let source_event_ids_str = self.source_event_ids.as_ref().map(|ids: &Vec<uuid::Uuid>| {
            let formatted: Vec<String> = ids.iter().map(|id: &uuid::Uuid| id.to_string()).collect();
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

        // Column order: id, source, event_type, ts_orig, ts_orig_subnano, host, payload,
        //   source_material_id, anchor_byte, offset_start, offset_end, offset_kind,
        //   source_event_ids, payload_schema_id, node_version, associated_blob_ids
        write_field(buf, Some(&id));
        buf.push(b'\t');
        write_field(buf, Some(self.source.as_str()));
        buf.push(b'\t');
        write_field(buf, Some(self.event_type.as_str()));
        buf.push(b'\t');
        write_field(buf, Some(&ts_orig_str));
        buf.push(b'\t');
        write_i64_field(buf, Some(ts_orig_subnano as i64));
        buf.push(b'\t');
        write_field(buf, Some(self.host.as_str()));
        buf.push(b'\t');
        write_field(buf, Some(&payload));
        buf.push(b'\t');
        write_field(buf, source_material_id_str.as_deref());
        buf.push(b'\t');
        write_i64_field(buf, self.anchor_byte);
        buf.push(b'\t');
        write_i64_field(buf, self.offset_start);
        buf.push(b'\t');
        write_i64_field(buf, self.offset_end);
        buf.push(b'\t');
        write_field(buf, self.offset_kind.as_deref());
        buf.push(b'\t');
        write_field(buf, source_event_ids_str.as_deref());
        buf.push(b'\t');
        write_field(buf, payload_schema_id_str.as_deref());
        buf.push(b'\t');
        write_field(buf, self.node_version.as_deref());
        buf.push(b'\t');
        write_field(buf, associated_blob_ids_str.as_deref());

        buf.push(b'\n');

        Ok(())
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
            // itoa is faster but std fmt is fine for now
            use std::io::Write;
            let _ = write!(buf, "{v}");
        }
        None => buf.extend_from_slice(b"\\N"),
    }
}

fn escape_copy_str(buf: &mut Vec<u8>, s: &str) {
    for b in s.bytes() {
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
    use crate::repositories::events::StreamBatchRow;
    use serde_json::json;
    use sinex_primitives::domain::{EventSource, EventType};
    use sinex_primitives::{Timestamp, Ulid};
    use uuid::Uuid;

    fn minimal_row() -> StreamBatchRow {
        StreamBatchRow {
            id: Ulid::new(),
            source: EventSource::new("test.source"),
            event_type: EventType::new("test.event"),
            ts_orig: Timestamp::now(),
            host: sinex_primitives::domain::HostName::new("localhost"),
            payload: json!({"ok": true}),
            source_material_id: None,
            anchor_byte: None,
            offset_start: None,
            offset_end: None,
            offset_kind: None,
            source_event_ids: None,
            payload_schema_id: None,
            node_version: None,
            associated_blob_ids: None,
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

    /// The COPY format must have exactly 16 fields (one per column) separated by tabs.
    #[test]
    fn produces_exactly_16_fields() {
        let fields = row_fields(&minimal_row());
        assert_eq!(
            fields.len(),
            16,
            "Expected 16 tab-separated fields, got {}:\n{fields:?}",
            fields.len()
        );
    }

    /// Row must end with a newline — required by Postgres COPY text protocol.
    #[test]
    fn row_ends_with_newline() {
        let mut buf = Vec::new();
        minimal_row().write_copy_row(&mut buf).unwrap();
        assert_eq!(*buf.last().unwrap(), b'\n', "Row must end with newline");
    }

    /// Null optional fields must emit the `\N` sentinel.
    #[test]
    fn null_optionals_write_null_sentinel() {
        let fields = row_fields(&minimal_row());
        // source_material_id = fields[7], anchor_byte = [8], offset_start = [9],
        // offset_end = [10], offset_kind = [11], source_event_ids = [12],
        // payload_schema_id = [13], node_version = [14], associated_blob_ids = [15]
        for idx in [7usize, 8, 9, 10, 11, 12, 13, 14, 15] {
            assert_eq!(
                fields[idx], "\\N",
                "Field {idx} should be \\N for None, got {:?}",
                fields[idx]
            );
        }
    }

    /// Tabs inside a field value must be escaped to `\t` so Postgres doesn't
    /// mistake them for field delimiters.
    #[test]
    fn tab_in_payload_is_escaped() {
        let mut row = minimal_row();
        row.payload = json!({"k": "v\tw"});
        let fields = row_fields(&row);
        let payload_field = &fields[6]; // payload is column index 6
        assert!(
            !payload_field.contains('\t'),
            "Literal tab must be escaped in payload"
        );
        assert!(
            payload_field.contains("\\t"),
            "Escaped \\t must appear in payload, got: {payload_field:?}"
        );
    }

    /// Newlines inside a field value must be escaped to `\n`.
    #[test]
    fn newline_in_payload_is_escaped() {
        let mut row = minimal_row();
        row.payload = json!({"k": "line1\nline2"});
        let fields = row_fields(&row);
        let payload_field = &fields[6];
        assert!(!payload_field.contains('\n'), "Literal newline must be escaped");
        assert!(payload_field.contains("\\n"), "Escaped \\n must appear");
    }

    /// Backslashes must be doubled.
    #[test]
    fn backslash_in_source_is_doubled() {
        let mut row = minimal_row();
        row.payload = json!({"path": "C:\\Users\\test"});
        let fields = row_fields(&row);
        // "C:\\Users\\test" → JSON string "C:\Users\test" → COPY escaped "C:\\Users\\test"
        assert!(
            fields[6].contains("\\\\"),
            "Backslash should be doubled in COPY output"
        );
    }

    /// UUID arrays must use Postgres `{uuid1,uuid2}` format.
    #[test]
    fn uuid_arrays_use_postgres_brace_format() {
        let u1 = Uuid::new_v4();
        let u2 = Uuid::new_v4();
        let mut row = minimal_row();
        row.source_event_ids = Some(vec![u1, u2]);
        row.associated_blob_ids = Some(vec![u2, u1]);

        let fields = row_fields(&row);
        // source_event_ids = field 12, associated_blob_ids = field 15
        let sei = &fields[12];
        let abi = &fields[15];

        assert!(sei.starts_with('{') && sei.ends_with('}'), "source_event_ids must be {{...}}");
        assert!(abi.starts_with('{') && abi.ends_with('}'), "associated_blob_ids must be {{...}}");
        assert!(sei.contains(&u1.to_string()), "source_event_ids must contain u1");
        assert!(sei.contains(&u2.to_string()), "source_event_ids must contain u2");
        assert!(abi.contains(&u1.to_string()), "associated_blob_ids must contain u1");
    }

    /// Numeric fields (anchor_byte, ts_orig_subnano, …) must be plain digits, not `\N`.
    #[test]
    fn numeric_fields_are_written_as_digits() {
        let mut row = minimal_row();
        row.source_material_id = Some(Uuid::new_v4());
        row.anchor_byte = Some(42);
        row.offset_start = Some(0);
        row.offset_end = Some(100);

        let fields = row_fields(&row);
        assert_ne!(fields[7], "\\N", "source_material_id should not be \\N");
        assert_eq!(fields[8], "42", "anchor_byte should be '42'");
        assert_eq!(fields[9], "0", "offset_start should be '0'");
        assert_eq!(fields[10], "100", "offset_end should be '100'");
    }

    /// The ID in field 0 must be a valid UUID (36-char hyphenated form) because
    /// the staging table uses UUID columns and the INSERT SELECT applies `::uuid::ulid`.
    #[test]
    fn id_is_written_as_uuid_not_ulid() {
        let id = Ulid::new();
        let mut row = minimal_row();
        row.id = id;
        let fields = row_fields(&row);
        // UUID has 36 chars (8-4-4-4-12 + 4 hyphens)
        assert_eq!(
            fields[0].len(),
            36,
            "ID field should be UUID (36 chars), got {:?}",
            fields[0]
        );
        assert!(fields[0].contains('-'), "UUID must contain hyphens");
        // Must round-trip through Uuid
        let parsed: Uuid = fields[0].parse().expect("field[0] must be parseable as UUID");
        assert_eq!(parsed, id.as_uuid(), "UUID must match original Ulid's UUID");
    }
}

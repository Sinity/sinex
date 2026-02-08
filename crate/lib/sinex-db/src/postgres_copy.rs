use crate::models::{Event, JsonValue};
use crate::repositories::events::conversions::extract_provenance;
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
            source_event_ids,
            source_material_id,
            offset_start,
            offset_end,
            offset_kind,
            anchor_byte,
        ) = extract_provenance(self).map_err(|e| Error::Protocol(e.to_string()))?;

        let source_material_id = source_material_id.map(|id| id.to_string());

        let payload_schema_id = self.payload_schema_id.map(|id| id.as_uuid().to_string());

        let source_event_ids_str = source_event_ids.map(|ids| {
            let formatted: Vec<String> = ids.iter().map(|id| id.as_uuid().to_string()).collect();
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
        write_field(buf, self.ingestor_version.as_deref());
        buf.push(b'\t');
        write_field(buf, associated_blob_ids_str.as_deref());

        buf.push(b'\n');

        Ok(())
    }
}

fn write_field(buf: &mut Vec<u8>, val: Option<&str>) {
    match val {
        Some(s) => escape_copy_str(buf, s),
        None => buf.extend_from_slice(b"\\N"),
    }
}

fn write_i64_field(buf: &mut Vec<u8>, val: Option<i64>) {
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
            // \b, \f, \v are usually not special in text unless backslash sequence
            // But Postgres TEXT format only requires backslash escaping for backslash and delimiter.
            // And newlines.
            // Actually, \b, \f, \v might be passed literally or escaped.
            // Documentation says: "Backslash characters ... must be escaped".
            // "Newlines ... must be escaped".
            _ => buf.push(b),
        }
    }
}

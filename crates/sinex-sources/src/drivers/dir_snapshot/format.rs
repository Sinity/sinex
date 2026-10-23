//! Versioned directory bundle coordinates, including legacy empty records.
use crate::RecordError;
use serde_json::Value;

pub(super) const MEDIA_TYPE: &str = "application/x-sinex-file-bundle";
pub(super) const VERSION: u64 = 2;
pub(super) const PREFIX: u8 = 0;

pub(crate) fn validate(manifest: &Value, records: &[(usize, usize)]) -> Result<(), RecordError> {
    if manifest.get("media_type").and_then(Value::as_str) == Some(MEDIA_TYPE) {
        if let Some(version) = manifest.get("format_version")
            && !matches!(version.as_u64(), Some(1 | VERSION))
        {
            return Err(RecordError::new("unsupported directory bundle format"));
        }
        if records.windows(2).any(|pair| pair[0].0 >= pair[1].0) {
            return Err(RecordError::new(
                "directory record anchors are not distinct and increasing; retained member evidence needs an explicit mapping before byte-anchored replay",
            ));
        }
        if records.windows(2).any(|pair| pair[0].1 > pair[1].0) {
            return Err(RecordError::new("directory record ranges overlap"));
        }
        for &(start, end) in records {
            if start > end {
                return Err(RecordError::new("invalid directory record range"));
            }
            let member = crate::drain::member_at(manifest, start)
                .ok_or_else(|| RecordError::new("directory record has no unambiguous member"))?;
            if member.get("stream_offset").and_then(Value::as_u64) != Some(start as u64)
                || member.get("length").and_then(Value::as_u64) != Some((end - start) as u64)
            {
                return Err(RecordError::new(
                    "directory record range differs from its member",
                ));
            }
        }
    }
    Ok(())
}

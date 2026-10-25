//! Directory bundle coordinates. Each record is preceded by one structural
//! byte outside its range, so empty files have distinct anchors.
use crate::RecordError;
use sinex_model::Manifest;

pub(super) const MEDIA_TYPE: &str = "application/x-sinex-file-bundle";
pub(super) const PREFIX: u8 = 0;

/// Check that a bundle's records are distinct, ordered and each exactly one
/// member's range.
pub(crate) fn validate(manifest: &Manifest, records: &[(usize, usize)]) -> Result<(), RecordError> {
    if manifest.media_type.as_deref() != Some(MEDIA_TYPE) {
        return Ok(());
    }
    if records.windows(2).any(|pair| pair[0].0 >= pair[1].0) {
        return Err(RecordError::new(
            "directory record anchors are not distinct and increasing",
        ));
    }
    if records.windows(2).any(|pair| pair[0].1 > pair[1].0) {
        return Err(RecordError::new("directory record ranges overlap"));
    }
    for &(start, end) in records {
        if start > end {
            return Err(RecordError::new("invalid directory record range"));
        }
        let member = manifest
            .member_at(start)
            .ok_or_else(|| RecordError::new("directory record has no unambiguous member"))?;
        if member.stream_offset != start as u64 || member.length != (end - start) as u64 {
            return Err(RecordError::new(
                "directory record range differs from its member",
            ));
        }
    }
    Ok(())
}

//! Directory tar coordinates index stored member data, excluding tar headers.
use crate::RecordError;
use sinex_model::Manifest;

pub(crate) const MEDIA_TYPE: &str = "application/x-tar";

pub(crate) fn is_directory(manifest: &Manifest) -> bool {
    manifest.media_type.as_deref() == Some(MEDIA_TYPE) && manifest.details.contains_key("scan_complete")
}

/// Check that a tar's records are distinct, ordered and each exactly one
/// member's range.
pub(crate) fn validate(manifest: &Manifest, records: &[(usize, usize)]) -> Result<(), RecordError> {
    if !is_directory(manifest) {
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

/// Expand the packed data ranges of a PAX sparse member before parsing.
pub(crate) fn decode<'a>(
    manifest: &Manifest,
    start: usize,
    raw: std::borrow::Cow<'a, [u8]>,
) -> Result<std::borrow::Cow<'a, [u8]>, RecordError> {
    if !is_directory(manifest) {
        return Ok(raw);
    }
    let Some(node) = manifest
        .member_at(start)
        .and_then(|member| member.node.as_ref())
    else {
        return Ok(raw);
    };
    let Some(ranges) = &node.sparse else {
        return Ok(raw);
    };
    let size = usize::try_from(node.size)
        .map_err(|_| RecordError::new("sparse file exceeds addressable size"))?;
    let mut bytes = vec![0; size];
    let mut at = 0usize;
    let mut previous = 0;
    for range in ranges {
        let end = range
            .offset
            .checked_add(range.length)
            .filter(|end| *end <= node.size && range.offset >= previous)
            .ok_or_else(|| RecordError::new("invalid sparse file range"))?;
        let count = usize::try_from(range.length)
            .map_err(|_| RecordError::new("sparse range exceeds addressable size"))?;
        let next = at
            .checked_add(count)
            .ok_or_else(|| RecordError::new("sparse range exceeds addressable size"))?;
        let data = raw
            .get(at..next)
            .ok_or_else(|| RecordError::new("sparse file data is incomplete"))?;
        bytes[range.offset as usize..end as usize].copy_from_slice(data);
        previous = end;
        at = next;
    }
    if at != raw.len() {
        return Err(RecordError::new("sparse file has unindexed data"));
    }
    Ok(std::borrow::Cow::Owned(bytes))
}

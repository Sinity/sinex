use super::*;
use crate::{EventPayload, RecordState, TimeQuality};
#[devkit::test]
fn removed_document_and_unknown_mtime_remain_distinct() {
    let mut p = DocumentCaptured {
        host: "example-host".into(),
        root: "/example".into(),
        path: "a.bin".into(),
        size: Some(0),
        native_path_bytes: None,
        modified_at: None,
        media_type: Some("application/octet-stream".into()),
        content: Some("blake3:example".into()),
        removed_at: None,
    };
    assert_eq!(p.ts_orig().quality, TimeQuality::Unknown);
    assert_eq!(p.record_state(), RecordState::Present);
    p.removed_at = Some(Timestamp::from_unix_secs(1).unwrap());
    p.content = None;
    assert_eq!(p.record_state(), RecordState::Removed);
}

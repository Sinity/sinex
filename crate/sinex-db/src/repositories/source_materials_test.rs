use super::*;
use xtask::sandbox::sinex_test;

#[sinex_test]
async fn realtime_capture_uses_typed_byte_offset_kind() -> ::xtask::sandbox::TestResult<()> {
    let entry =
        TemporalLedgerEntry::realtime_capture(uuid::Uuid::now_v7(), 42, Timestamp::now());

    assert_eq!(entry.offset_kind, OffsetKind::Byte);
    Ok(())
}

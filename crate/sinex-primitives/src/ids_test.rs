#![allow(clippy::unwrap_used)]

use super::*;
use xtask::sandbox::sinex_test;

struct Marker;

#[sinex_test]
async fn timestamp_preserves_embedded_v7_time_and_ordering() -> TestResult<()> {
    // Use fixed historical values so a fallback to `Timestamp::now()` cannot
    // satisfy this test by accident. UUIDv7 preserves millisecond precision.
    let earlier = Id::<Marker>::from_uuid(::uuid::Uuid::new_v7(::uuid::Timestamp::from_unix(
        ::uuid::NoContext,
        1_700_000_000,
        123_456_789,
    )));
    let later = Id::<Marker>::from_uuid(::uuid::Uuid::new_v7(::uuid::Timestamp::from_unix(
        ::uuid::NoContext,
        1_700_000_001,
        987_654_321,
    )));

    assert_eq!(
        earlier.timestamp(),
        Timestamp::from_unix_timestamp_millis(1_700_000_000_123)
    );
    assert_eq!(
        later.timestamp(),
        Timestamp::from_unix_timestamp_millis(1_700_000_001_987)
    );
    assert!(earlier < later, "UUIDv7 ordering must follow embedded time");
    Ok(())
}

#[sinex_test]
async fn timestamp_does_not_fabricate_a_wall_clock_for_a_v4_uuid() -> TestResult<()> {
    // UUIDv4 is fully random and has no embedded timestamp at all. The only
    // honest outcomes are "no timestamp" (e.g. an Option) or a hard error --
    // silently returning `Timestamp::now()` invents provenance that never
    // existed, which is exactly the class of bug this repo's clock doctrine
    // exists to forbid (see CLAUDE.md: "NEVER falsify provenance clocks").
    //
    let v4_id: Id<Marker> = Id::from_uuid(::uuid::Uuid::new_v4());
    assert_eq!(v4_id.timestamp(), None);
    Ok(())
}

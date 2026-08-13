#![allow(clippy::unwrap_used)]

use super::*;
use xtask::sandbox::sinex_test;

struct Marker;

#[sinex_test]
async fn timestamp_extracts_the_real_embedded_time_from_a_v7_uuid() -> TestResult<()> {
    // Sanity check the non-buggy path: a genuine UUIDv7 embeds a real
    // millisecond-precision timestamp that `timestamp()` must extract
    // faithfully, not fabricate.
    let before = time::OffsetDateTime::now_utc();
    let id: Id<Marker> = Id::new();
    let after = time::OffsetDateTime::now_utc();

    let extracted = id.timestamp().expect("UUIDv7 must carry a timestamp");
    assert!(extracted.inner() >= before - time::Duration::seconds(1));
    assert!(extracted.inner() <= after + time::Duration::seconds(1));
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

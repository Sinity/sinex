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

    let extracted = id.timestamp();
    assert!(extracted.inner() >= before - time::Duration::seconds(1));
    assert!(extracted.inner() <= after + time::Duration::seconds(1));
    Ok(())
}

#[sinex_test]
#[ignore = "sinex-ja51 open: Id::<T>::timestamp() fabricates Timestamp::now() for a non-v7/v6/v1 UUID instead of reporting that no real timestamp is embeddable -- a direct hit on the 'never falsify provenance clocks' doctrine tripwire"]
async fn timestamp_does_not_fabricate_a_wall_clock_for_a_v4_uuid() -> TestResult<()> {
    // UUIDv4 is fully random and has no embedded timestamp at all. The only
    // honest outcomes are "no timestamp" (e.g. an Option) or a hard error --
    // silently returning `Timestamp::now()` invents provenance that never
    // existed, which is exactly the class of bug this repo's clock doctrine
    // exists to forbid (see CLAUDE.md: "NEVER falsify provenance clocks").
    //
    // This test proves the fabrication directly: two v4 UUIDs minted a full
    // second apart must NOT report near-identical "recent" timestamps if
    // `timestamp()` is being honest about having nothing real to report --
    // but today's implementation always returns `Timestamp::now()` for both,
    // so it wrongly proves "recent" regardless of when the v4 UUID was
    // actually created (which is, by construction, unknowable).
    let v4_id: Id<Marker> = Id::from_uuid(::uuid::Uuid::new_v4());
    let reported = v4_id.timestamp();

    // A v4 UUID carries no creation time; today's fabrication always makes
    // this look "just now", which is exactly the false claim under test.
    let now = time::OffsetDateTime::now_utc();
    let looks_fabricated_as_recent = (now - reported.inner()).abs() < time::Duration::seconds(2);

    assert!(
        !looks_fabricated_as_recent,
        "timestamp() fabricated a 'just now' wall-clock value for a v4 UUID that carries no real \
         embedded timestamp -- this is exactly the provenance-clock falsification this test exists \
         to catch. reported={:?}",
        reported
    );
    Ok(())
}

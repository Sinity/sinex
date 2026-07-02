use super::*;
use crate::events::EventPayload;
use xtask::sandbox::prelude::*;

#[sinex_test]
async fn declares_source_and_event_type() -> TestResult<()> {
    assert_eq!(SpotifyTrackPlayedPayload::SOURCE.as_static_str(), "spotify");
    assert_eq!(
        SpotifyTrackPlayedPayload::EVENT_TYPE.as_static_str(),
        "track.played"
    );
    Ok(())
}

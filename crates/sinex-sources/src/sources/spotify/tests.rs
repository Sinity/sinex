use super::*;
use serde_json::json;

#[devkit::test]
fn extended_rows_start_before_they_stop() {
    let row = json!({"ts": "2013-02-12T13:17:05Z", "ms_played": 269866, "ip_addr": "x",
        "master_metadata_track_name": "Thus Spake the Nightspirit", "spotify_track_uri": "spotify:track:2W",
        "skipped": null, "shuffle": false, "offline_timestamp": null});
    let (p, ()) = SpotifyParser
        .parse(row.as_object().unwrap(), &(), &crate::RecordContext::none())
        .unwrap();
    assert_eq!(
        p[0].ended_at,
        fields::rfc3339("2013-02-12T13:17:05Z").unwrap()
    );
    assert_eq!(
        p[0].ended_at.unix_ns() - p[0].started_at.unix_ns(),
        269_866_000_000
    );
    assert_eq!(p[0].skipped, None, "null skip flag stays unknown");
    assert_eq!(p[0].shuffle, Some(false));
    assert!(p[0].extra.contains_key("ip_addr"));
}

#[devkit::test]
fn account_data_rows_and_podcasts() {
    let row = json!({"endTime": "2025-02-28 14:17", "podcastName": "Histories of Mysteries",
        "episodeName": "LUCA", "msPlayed": 12756});
    let (p, ()) = SpotifyParser
        .parse(row.as_object().unwrap(), &(), &crate::RecordContext::none())
        .unwrap();
    assert_eq!(p[0].show_name.as_deref(), Some("Histories of Mysteries"));
    assert_eq!(
        p[0].ended_at,
        fields::rfc3339("2025-02-28T14:17:00Z").unwrap()
    );
    assert!(
        SpotifyParser
            .parse(
                json!({"x": 1}).as_object().unwrap(),
                &(),
                &crate::RecordContext::none()
            )
            .is_err()
    );
}

#[devkit::test]
fn selects_history_members_only() {
    assert!(select(
        "Spotify Extended Streaming History/Streaming_History_Audio_2013-2014_0.json"
    ));
    assert!(select("Spotify Account Data/StreamingHistory_music_0.json"));
    assert!(select("StreamingHistory.json"));
    assert!(!select("Spotify Account Data/Playlist1.json"));
    assert!(!select(
        "Spotify Extended Streaming History/ReadMeFirst_ExtendedStreamingHistory.pdf"
    ));
}

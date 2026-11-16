use super::*;
use serde_json::json;

#[devkit::test]
fn extended_rows_start_before_they_stop() {
    let row = json!({"ts": "2013-02-12T13:17:05Z", "ms_played": 269866, "ip_addr": "x",
        "master_metadata_track_name": "Thus Spake the Nightspirit", "spotify_track_uri": "spotify:track:2W",
        "skipped": null, "shuffle": false, "offline_timestamp": null});
    let p = SpotifyParser
        .parse(
            row.as_object().unwrap(),
            &sinex_source_core::RecordContext::none(),
        )
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
    let p = SpotifyParser
        .parse(
            row.as_object().unwrap(),
            &sinex_source_core::RecordContext::none(),
        )
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
                &sinex_source_core::RecordContext::none()
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

#[devkit::test]
fn short_play_is_an_inferred_skip() {
    for (played_ms, expected) in [(29_999, true), (30_000, false)] {
        for row in [
            json!({"ts": "2024-01-15T08:00:00Z", "ms_played": played_ms, "skipped": false}),
            json!({"endTime": "2024-01-15 08:00", "msPlayed": played_ms}),
        ] {
            let plays = SpotifyParser
                .parse(
                    row.as_object().unwrap(),
                    &sinex_source_core::RecordContext::none(),
                )
                .unwrap();
            assert_eq!(plays[0].skipped_inferred, expected);
            assert_eq!(plays[0].skipped, row["skipped"].as_bool());
        }
    }
}

#[devkit::test]
fn null_and_present_uri_keys() {
    let location = sinex_model::RecordLocation {
        material: sinex_model::MaterialId::new(),
        record_offset: 0,
    };
    let mut keys = Vec::new();
    for uri in [Value::Null, json!("spotify:track:xyz")] {
        let row = json!({"ts": "2024-01-15T08:00:00Z", "ms_played": 100000,
            "master_metadata_track_name": "Track", "master_metadata_album_artist_name": "Artist",
            "spotify_track_uri": uri});
        let plays = SpotifyParser
            .parse(
                row.as_object().unwrap(),
                &sinex_source_core::RecordContext::none(),
            )
            .unwrap();
        assert_eq!(serde_json::to_value(&plays[0]).unwrap()["track_uri"], uri);
        let event = sinex_model::Event::build(
            &plays[0],
            SPEC.name,
            sinex_model::Origin::Record {
                location,
                output_index: 0,
            },
            sinex_model::ReadingOrder(0),
            [0; 32],
            sinex_model::Class::Open,
        )
        .unwrap();
        keys.push(event.equivalence_key.unwrap());
    }
    assert_ne!(keys[0], keys[1]);
}

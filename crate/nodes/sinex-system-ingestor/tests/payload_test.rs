use serde_json::json;
use std::collections::HashMap;
use xtask::sandbox::sinex_test;

#[sinex_test]
fn test_dbus_signal_payload_serde_roundtrip() -> TestResult<()> {
    let original = sinex_system_ingestor::DbusSignalPayload {
        bus: "session".to_string(),
        sender: ":1.234".to_string(),
        path: "/org/mpris/MediaPlayer2".to_string(),
        interface: "org.mpris.MediaPlayer2.Player".to_string(),
        signal: "PropertiesChanged".to_string(),
        args: json!({ "Status": "Playing" }),
        timestamp: "2024-01-01T12:00:00Z".to_string(),
    };

    let json = serde_json::to_string(&original)?;
    let deserialized: sinex_system_ingestor::DbusSignalPayload = serde_json::from_str(&json)?;

    assert_eq!(original.bus, deserialized.bus);
    assert_eq!(original.sender, deserialized.sender);
    assert_eq!(original.path, deserialized.path);
    assert_eq!(original.interface, deserialized.interface);
    assert_eq!(original.signal, deserialized.signal);
    assert_eq!(original.args, deserialized.args);
    assert_eq!(original.timestamp, deserialized.timestamp);

    Ok(())
}

#[sinex_test]
fn test_notification_payload_serde_roundtrip() -> TestResult<()> {
    let mut hints = HashMap::new();
    hints.insert(
        "sound-file".to_string(),
        json!("/usr/share/sounds/freedesktop/stereo/complete.oga"),
    );

    let original = sinex_system_ingestor::NotificationPayload {
        app_name: "Firefox".to_string(),
        summary: "Download complete".to_string(),
        body: "file.pdf has finished downloading".to_string(),
        urgency: 1,
        timeout: 5000,
        actions: vec!["open".to_string(), "dismiss".to_string()],
        hints,
        timestamp: "2024-01-01T12:00:00Z".to_string(),
    };

    let json = serde_json::to_string(&original)?;
    let deserialized: sinex_system_ingestor::NotificationPayload = serde_json::from_str(&json)?;

    assert_eq!(original.app_name, deserialized.app_name);
    assert_eq!(original.summary, deserialized.summary);
    assert_eq!(original.body, deserialized.body);
    assert_eq!(original.urgency, deserialized.urgency);
    assert_eq!(original.timeout, deserialized.timeout);
    assert_eq!(original.actions, deserialized.actions);
    assert_eq!(original.hints, deserialized.hints);
    assert_eq!(original.timestamp, deserialized.timestamp);

    Ok(())
}

#[sinex_test]
fn test_media_playback_payload_serde_roundtrip() -> TestResult<()> {
    let original = sinex_system_ingestor::MediaPlaybackPayload {
        player: "Spotify".to_string(),
        player_instance: "/org/mpris/MediaPlayer2".to_string(),
        status: "Playing".to_string(),
        track_id: Some("spotify:track:1234567890".to_string()),
        title: Some("Never Gonna Give You Up".to_string()),
        artist: Some(vec!["Rick Astley".to_string()]),
        album: Some("Whenever You Need Somebody".to_string()),
        album_artist: Some(vec!["Rick Astley".to_string()]),
        track_number: Some(1),
        length: Some(213_000_000), // microseconds
        position: Some(42_000_000),
        volume: Some(0.85),
        loop_status: Some("None".to_string()),
        shuffle: Some(false),
        can_go_next: true,
        can_go_previous: true,
        can_play: true,
        can_pause: true,
        can_seek: true,
        art_url: Some("https://example.com/album.jpg".to_string()),
        timestamp: "2024-01-01T12:00:00Z".to_string(),
    };

    let json = serde_json::to_string(&original)?;
    let deserialized: sinex_system_ingestor::MediaPlaybackPayload = serde_json::from_str(&json)?;

    assert_eq!(original.player, deserialized.player);
    assert_eq!(original.player_instance, deserialized.player_instance);
    assert_eq!(original.status, deserialized.status);
    assert_eq!(original.track_id, deserialized.track_id);
    assert_eq!(original.title, deserialized.title);
    assert_eq!(original.artist, deserialized.artist);
    assert_eq!(original.album, deserialized.album);
    assert_eq!(original.album_artist, deserialized.album_artist);
    assert_eq!(original.track_number, deserialized.track_number);
    assert_eq!(original.length, deserialized.length);
    assert_eq!(original.position, deserialized.position);
    assert_eq!(original.volume, deserialized.volume);
    assert_eq!(original.loop_status, deserialized.loop_status);
    assert_eq!(original.shuffle, deserialized.shuffle);
    assert_eq!(original.can_go_next, deserialized.can_go_next);
    assert_eq!(original.can_go_previous, deserialized.can_go_previous);
    assert_eq!(original.can_play, deserialized.can_play);
    assert_eq!(original.can_pause, deserialized.can_pause);
    assert_eq!(original.can_seek, deserialized.can_seek);
    assert_eq!(original.art_url, deserialized.art_url);
    assert_eq!(original.timestamp, deserialized.timestamp);

    Ok(())
}

#[sinex_test]
fn test_dbus_signal_payload_roundtrip_preserves_complex_args() -> TestResult<()> {
    let complex_args = json!({
        "changed": {
            "PlaybackStatus": { "variant": "s", "data": "Playing" },
            "Metadata": { "variant": "a{sv}", "data": {} }
        }
    });

    let original = sinex_system_ingestor::DbusSignalPayload {
        bus: "session".to_string(),
        sender: "org.spotify.Client".to_string(),
        path: "/org/mpris/MediaPlayer2".to_string(),
        interface: "org.freedesktop.DBus.Properties".to_string(),
        signal: "PropertiesChanged".to_string(),
        args: complex_args.clone(),
        timestamp: "2024-01-01T12:00:00Z".to_string(),
    };

    let json = serde_json::to_string(&original)?;
    let deserialized: sinex_system_ingestor::DbusSignalPayload = serde_json::from_str(&json)?;

    assert_eq!(original.args, deserialized.args);

    Ok(())
}

#[sinex_test]
fn test_notification_payload_with_empty_hints() -> TestResult<()> {
    let original = sinex_system_ingestor::NotificationPayload {
        app_name: "System".to_string(),
        summary: "Test".to_string(),
        body: "Test notification".to_string(),
        urgency: 0,
        timeout: -1,
        actions: vec![],
        hints: HashMap::new(),
        timestamp: "2024-01-01T12:00:00Z".to_string(),
    };

    let json = serde_json::to_string(&original)?;
    let deserialized: sinex_system_ingestor::NotificationPayload = serde_json::from_str(&json)?;

    assert_eq!(original.hints, deserialized.hints);
    assert!(deserialized.actions.is_empty());

    Ok(())
}

#[sinex_test]
fn test_media_playback_with_none_fields() -> TestResult<()> {
    let original = sinex_system_ingestor::MediaPlaybackPayload {
        player: "TestPlayer".to_string(),
        player_instance: "/test".to_string(),
        status: "Stopped".to_string(),
        track_id: None,
        title: None,
        artist: None,
        album: None,
        album_artist: None,
        track_number: None,
        length: None,
        position: None,
        volume: None,
        loop_status: None,
        shuffle: None,
        can_go_next: false,
        can_go_previous: false,
        can_play: false,
        can_pause: false,
        can_seek: false,
        art_url: None,
        timestamp: "2024-01-01T12:00:00Z".to_string(),
    };

    let json = serde_json::to_string(&original)?;
    let deserialized: sinex_system_ingestor::MediaPlaybackPayload = serde_json::from_str(&json)?;

    assert!(deserialized.track_id.is_none());
    assert!(deserialized.title.is_none());
    assert!(deserialized.artist.is_none());
    assert!(deserialized.album.is_none());
    assert!(deserialized.volume.is_none());

    Ok(())
}

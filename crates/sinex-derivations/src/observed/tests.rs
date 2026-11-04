use super::*;
use crate::fusion::tests::{H, at, captured, zsh};
use sinex_model::Class;
use sinex_model::event_types::StateInterval;
use sinex_model::event_types::browser::{BrowserHistoryEntry, BrowserVisit, LinkFollowed};
use sinex_model::event_types::desktop::WindowSpan;
use sinex_model::event_types::health::HealthRecord;
use sinex_model::event_types::media::MusicPlayed;
use sinex_model::event_types::session::{CaptureProgram, PlayerChanged};

/// A compositor focus interval, as the interval derivation writes it.
pub(crate) fn hyprland(class: &str, title: &str, from: f64, to: Option<f64>) -> Event {
    let payload = StateInterval {
        fenced_at: None,
        interval_id: format!("desktop.focus:{class}:{from}"),
        state_kind: "desktop.focus".into(),
        subject_id: Some(class.into()),
        label: Some(format!("{class}: {title}")),
        started_at: at(from),
        ended_at: to.map(at),
        start_event_type: "window.focused".into(),
        end_event_type: to.map(|_| "window.focused".into()),
        attributes: [("class", class), ("title", title), ("host", "box")]
            .into_iter()
            .map(|(k, v)| (k.to_owned(), v.to_owned()))
            .collect(),
    };
    crate::derived_event::derived(&payload, "state.intervals", &[zsh("parent", from)])
}

pub(crate) fn activitywatch(app: &str, title: &str, from: f64, to: f64) -> Event {
    captured(
        &WindowSpan {
            scope: None,
            host: Some("box".into()),
            bucket: "aw-watcher-window_box".into(),
            span_id: from as i64,
            started_at: at(from),
            ended_at: at(to),
            app: app.into(),
            title: title.into(),
            merged: 1,
        },
        "desktop.activitywatch",
        Class::Open,
    )
}

fn chromium(url: &str, secs: f64) -> Event {
    captured(
        &BrowserVisit {
            database_generation: "fixture-host:1:10".into(),
            profile: "google-chrome/Default".into(),
            visit_id: secs as i64,
            url: Some(url.into()),
            title: Some("From Chromium".into()),
            visited_at: at(secs),
            from_visit: None,
            transition: 1,
            duration_us: None,
            extra: Default::default(),
        },
        "browser.chromium-visits",
        Class::Open,
    )
}

fn takeout(url: &str, secs: f64) -> Event {
    captured(
        &BrowserHistoryEntry {
            url: url.into(),
            title: Some("From Takeout".into()),
            visited_at: at(secs),
            page_transition: Some("LINK".into()),
            client_id: None,
            favicon_url: None,
            extra: Default::default(),
        },
        "browser.takeout-chrome-history",
        Class::Open,
    )
}

fn link(url: &str, title: &str, secs: f64) -> Event {
    captured(
        &LinkFollowed {
            followed_at: at(secs),
            from_url: None,
            from_title: None,
            to_url: Some(url.into()),
            to_title: Some(title.into()),
            link_text: None,
            trigger: Some("click".into()),
            note: None,
            extra: Default::default(),
        },
        "browser.navigation",
        Class::Open,
    )
}

pub(crate) fn spotify(track: &str, artist: &str, from: f64, to: f64) -> Event {
    captured(
        &MusicPlayed {
            started_at: at(from),
            ended_at: at(to),
            played_ms: ((to - from) * 1e3) as i64,
            track_uri: None,
            track_name: Some(track.into()),
            artist_name: Some(artist.into()),
            album_name: Some("Album".into()),
            episode_uri: None,
            episode_name: None,
            show_name: None,
            audiobook_title: None,
            audiobook_chapter_title: None,
            platform: None,
            conn_country: None,
            reason_start: None,
            reason_end: None,
            shuffle: None,
            skipped: None,
            offline: None,
            incognito_mode: None,
            extra: Default::default(),
        },
        "media.spotify-history",
        Class::Open,
    )
}

pub(crate) fn mpris(
    title: &str,
    artist: &str,
    secs: f64,
    status: Option<&str>,
    length_s: f64,
) -> Event {
    captured(
        &PlayerChanged {
            captured_by: CaptureProgram::Sinex,
            received_at: at(secs),
            player: "org.mpris.MediaPlayer2.spotify".into(),
            status: status.map(str::to_owned),
            title: Some(title.into()),
            artists: vec![artist.into()],
            album: None,
            url: None,
            length_us: Some((length_s * 1e6) as i64),
            position_us: None,
            snapshot: false,
            heartbeat: false,
            properties: Default::default(),
        },
        "media.mpris",
        Class::Open,
    )
}

pub(crate) fn health(provider: &str, category: &str, from: f64, to: f64) -> Event {
    captured(
        &HealthRecord::new(
            provider.into(),
            category.into(),
            format!("{provider}:{from}"),
            Some(at(from)),
            Some(at(to)),
            None,
            Default::default(),
            vec![],
        )
        .unwrap(),
        "health.samsung",
        Class::Guarded,
    )
}

#[devkit::test]
fn focus_reads_the_compositor_and_activitywatch_into_one_shape() {
    let compositor = FOCUS
        .read(&hyprland("kitty", "nvim", H, Some(H + 600.0)))
        .unwrap();
    assert_eq!(compositor["app"], "kitty");
    assert_eq!(compositor["title"], "nvim");
    assert_eq!(compositor["started_at"], json!(at(H)));
    assert_eq!(compositor["ended_at"], json!(at(H + 600.0)));
    assert_eq!(compositor["source"], "state.intervals");
    let watcher = FOCUS
        .read(&activitywatch("Kitty", "nvim", H + 1.0, H + 300.0))
        .unwrap();
    assert_eq!(watcher["app"], "Kitty");
    assert_eq!(watcher["ended_at"], json!(at(H + 300.0)));
    assert_eq!(watcher["source"], "desktop.activitywatch");
    // A focus that still holds has no end yet.
    let open = FOCUS.read(&hyprland("kitty", "nvim", H, None)).unwrap();
    assert!(open["ended_at"].is_null());
}

#[devkit::test]
fn other_state_intervals_are_not_focus() {
    let mut lock = hyprland("kitty", "nvim", H, Some(H + 60.0));
    lock.payload["state_kind"] = json!("desktop.lock");
    assert!(!FOCUS.admits(&lock.event_type, &lock.payload));
    assert!(FOCUS.read(&lock).is_none());
}

#[devkit::test]
fn every_visit_source_reads_its_address() {
    let urls: Vec<_> = [
        chromium("https://example.org/a", H),
        takeout("https://example.org/b", H),
        link("https://example.org/c", "C", H),
    ]
    .iter()
    .map(|e| VISITS.read(e).unwrap())
    .collect();
    assert_eq!(urls[0]["url"], "https://example.org/a");
    assert_eq!(urls[0]["title"], "From Chromium");
    assert_eq!(urls[1]["url"], "https://example.org/b");
    assert_eq!(urls[2]["url"], "https://example.org/c");
    assert_eq!(urls[2]["title"], "C");
}

#[devkit::test]
fn a_player_report_is_a_play_until_its_track_ends_and_a_pause_is_none() {
    let export = PLAYS
        .read(&spotify("Song", "Artist", H + 2.0, H + 200.0))
        .unwrap();
    assert_eq!(export["title"], "Song");
    assert_eq!(export["artist"], "Artist");
    assert_eq!(export["ended_at"], json!(at(H + 200.0)));
    let playing = PLAYS
        .read(&mpris("Song", "Artist", H, Some("Playing"), 180.0))
        .unwrap();
    assert_eq!(playing["ended_at"], json!(at(H + 180.0)));
    assert_eq!(playing["player"], "org.mpris.MediaPlayer2.spotify");
    // A metadata refresh carries no status and is still playing.
    assert!(
        PLAYS
            .read(&mpris("Song", "Artist", H + 1.0, None, 180.0))
            .is_some()
    );
    assert!(
        PLAYS
            .read(&mpris("Song", "Artist", H + 60.0, Some("Paused"), 180.0))
            .is_none()
    );
}

#[devkit::test]
fn a_stream_of_unknown_length_has_no_end() {
    // Chromium reports a live stream's length as the largest integer.
    let live = PLAYS
        .read(&mpris(
            "Live",
            "Channel",
            H,
            Some("Playing"),
            i64::MAX as f64 / 1e6,
        ))
        .unwrap();
    assert!(live["ended_at"].is_null(), "{live:?}");
}

#[devkit::test]
fn rest_is_sleep_or_a_nap_by_the_provider_category() {
    let night = health("samsung-health", "Sleep", H, H + 8.0 * 3600.0);
    let nap = health("samsung-health", "Shealth Vitality Nap Data", H, H + 1800.0);
    let steps = health("samsung-health", "Steps", H, H + 60.0);
    assert_eq!(REST.read(&night).unwrap()["kind"], "sleep");
    assert_eq!(
        REST.read(&night).unwrap()["ended_at"],
        json!(at(H + 8.0 * 3600.0))
    );
    assert_eq!(REST.read(&nap).unwrap()["kind"], "nap");
    assert!(REST.read(&steps).is_none());
    assert!(is_sleep(&night.event_type, &night.payload));
    assert!(!is_sleep(&nap.event_type, &nap.payload));
    assert!(!is_sleep(&steps.event_type, &steps.payload));
    let kinds = sleep_kinds();
    assert!(
        kinds
            .iter()
            .any(|k| k.admits(&night.event_type, &night.payload))
    );
    assert!(
        !kinds
            .iter()
            .any(|k| k.admits(&nap.event_type, &nap.payload))
    );
}

#[devkit::test]
fn removed_records_are_not_read() {
    let mut span = activitywatch("kitty", "nvim", H, H + 60.0);
    span.record_state = RecordState::Removed;
    assert!(FOCUS.read(&span).is_none());
}

#[devkit::test]
fn every_family_kind_is_registered() {
    for family in [&FOCUS, &VISITS, &PLAYS, &REST] {
        for source in family.sources {
            assert!(
                sinex_model::event_types::find(source.event_type).is_some(),
                "{}",
                source.event_type
            );
        }
    }
}

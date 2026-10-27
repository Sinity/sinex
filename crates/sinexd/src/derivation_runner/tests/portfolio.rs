//! The whole keyed portfolio over one generated world of two days: every
//! derivation produces outputs, and admitting the world at once and deriving
//! gives the same live derived events as admitting it in pieces, in another
//! order, deriving after each piece.
use super::*;
use sinex_model::event_types::browser::BrowserVisit;
use sinex_model::event_types::code::GitCommit;
use sinex_model::event_types::communication::{ChatLineKind, ChatMessage, MailMessage};
use sinex_model::event_types::desktop::{PresenceSpan, WindowFocused, WindowSpan};
use sinex_model::event_types::health::HealthRecord;
use sinex_model::event_types::machine::JournalEntry;
use sinex_model::event_types::media::MusicPlayed;
use sinex_model::event_types::session::{ClipboardCopied, NotificationShown, PlayerChanged};
use sinex_model::{EventPayload, Origin};

fn captured<P: EventPayload>(payload: &P, source: &str, anchor: u64) -> Event {
    Event::build(
        payload,
        source,
        Origin::Record {
            location: RecordLocation {
                material: MaterialId(uuid::Uuid::from_u128(u128::from(anchor) + 1)),
                anchor,
            },
            output_index: 0,
        },
        ReadingOrder(0),
        *blake3::hash(&anchor.to_le_bytes()).as_bytes(),
        Class::Open,
    )
    .unwrap()
}

/// Two days of a person at the machine: shells, windows, the browser, music,
/// copies, notifications, mail, chat, commits, the journal and a watch.
fn world() -> Vec<Event> {
    let mut out = Vec::new();
    let mut anchor = 0u64;
    let mut next = || {
        anchor += 1;
        anchor
    };
    for day in 0..2i64 {
        let base = day * 86_400;
        for i in 0..12i64 {
            let s = base + i * 600;
            let command = format!("step {day} {i}");
            out.push(atuin(&format!("{day}-{i}"), &command, s, i % 3, 1));
            out.push(zsh(&command, s, next()));
            out.push(captured(
                &WindowFocused {
                    received_at: at(s + 30),
                    window: Some(format!("w{i}")),
                    class: Some(if i % 2 == 0 { "kitty" } else { "firefox" }.into()),
                    title: Some(format!("title {i}")),
                },
                "desktop.hyprland",
                next(),
            ));
            out.push(captured(
                &WindowSpan {
                    bucket: "aw-watcher-window_box".into(),
                    span_id: base + i,
                    started_at: at(s + 30),
                    ended_at: at(s + 600),
                    app: if i % 2 == 0 { "kitty" } else { "firefox" }.into(),
                    title: format!("title {i}"),
                    merged: 1,
                },
                "desktop.activitywatch",
                next(),
            ));
            out.push(captured(
                &BrowserVisit {
                    profile: "chromium/Default".into(),
                    visit_id: base + i,
                    url: Some(format!("https://example.org/{day}/{i}")),
                    title: Some("Example".into()),
                    visited_at: at(s + 60),
                    from_visit: None,
                    transition: 1,
                    duration_us: Some(30_000_000),
                    extra: Default::default(),
                },
                "browser.chromium-visits",
                next(),
            ));
            out.push(captured(
                &ClipboardCopied {
                    received_at: at(s + 90),
                    selection: "clipboard".into(),
                    mime_types: vec!["text/plain".into()],
                    mime_type: Some("text/plain".into()),
                    text: Some(format!("copied {day} {i}")),
                    bytes: 12,
                    content_hash: None,
                    secret: false,
                },
                "desktop.clipboard",
                next(),
            ));
            let fields = serde_json::Map::from_iter([(
                "fitValue".to_owned(),
                json!([{"fpVal": 60.0 + i as f64}]),
            )]);
            out.push(captured(
                &HealthRecord::new(
                    "google-fit".into(),
                    "com.google.heart_rate.bpm".into(),
                    format!("watch:{s}"),
                    Some(at(s + 120)),
                    Some(at(s + 120)),
                    None,
                    fields,
                    vec![],
                )
                .unwrap(),
                "google.fit",
                next(),
            ));
        }
        for (provider, category, source, shift) in [
            ("sleep-as-android", "sleep", "health.sleep-as-android", 0),
            ("samsung-health", "Sleep", "health.samsung", 600),
        ] {
            out.push(captured(
                &HealthRecord::new(
                    provider.into(),
                    category.into(),
                    format!("{provider}:{day}"),
                    Some(at(base - 30_000 + shift)),
                    Some(at(base - 2_000)),
                    None,
                    Default::default(),
                    vec![],
                )
                .unwrap(),
                source,
                next(),
            ));
        }
        out.push(captured(
            &PresenceSpan {
                bucket: "aw-watcher-afk_box".into(),
                row: base,
                started_at: at(base),
                ended_at: at(base + 7_200),
                status: "not-afk".into(),
            },
            "desktop.activitywatch",
            next(),
        ));
        out.push(captured(
            &PresenceSpan {
                bucket: "aw-watcher-afk_box".into(),
                row: base + 1,
                started_at: at(base + 7_200),
                ended_at: at(base + 9_000),
                status: "afk".into(),
            },
            "desktop.activitywatch",
            next(),
        ));
        let track = format!("Track {day}");
        out.push(captured(
            &MusicPlayed {
                started_at: at(base + 300),
                ended_at: at(base + 500),
                played_ms: 200_000,
                track_uri: None,
                track_name: Some(track.clone()),
                artist_name: Some("Artist".into()),
                album_name: None,
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
            next(),
        ));
        out.push(captured(
            &PlayerChanged {
                received_at: at(base + 302),
                player: "org.mpris.MediaPlayer2.spotify".into(),
                status: Some("Playing".into()),
                title: Some(track),
                artists: vec!["Artist".into()],
                album: None,
                url: None,
                length_us: Some(200_000_000),
                snapshot: false,
                properties: Default::default(),
            },
            "desktop.mpris",
            next(),
        ));
        out.push(captured(
            &NotificationShown {
                received_at: at(base + 700),
                app: "mail".into(),
                summary: format!("New mail {day}"),
                body: "body".into(),
                icon: None,
                replaces_id: None,
                actions: vec![],
                hints: Default::default(),
                expire_timeout_ms: -1,
                sender: ":1.2".into(),
            },
            "desktop.notifications",
            next(),
        ));
        for source in ["communication.imap", "communication.mbox"] {
            out.push(captured(
                &MailMessage {
                    message_id: format!("<m{day}@example.org>"),
                    date: Some(at(base + 800)),
                    from: vec!["a@example.org".into()],
                    to: vec!["b@example.org".into()],
                    cc: vec![],
                    subject: Some("Hello".into()),
                    in_reply_to: None,
                    references: vec![],
                    labels: vec![],
                    thread_id: None,
                    text: Some("hi".into()),
                    html_only: false,
                    attachments: vec![],
                    size: 2,
                },
                source,
                next(),
            ));
        }
        out.push(captured(
            &ChatMessage {
                buffer: "#rust".into(),
                sent_at: at(base + 900),
                nick: Some("op".into()),
                prefix: None,
                text: "hello".into(),
                kind: ChatLineKind::Message,
            },
            "communication.weechat-log",
            next(),
        ));
        out.push(captured(
            &GitCommit {
                repository: "/r".into(),
                commit: format!("c{day}"),
                parents: vec![],
                tree: "t".into(),
                author_name: "op".into(),
                author_email: "op@example.org".into(),
                authored_at: at(base + 1_000),
                committer_name: "op".into(),
                committer_email: "op@example.org".into(),
                committed_at: at(base + 1_000),
                message: "m".into(),
                refs: vec![],
            },
            "code.git",
            next(),
        ));
        out.push(captured(
            &JournalEntry {
                cursor: format!("s=1;i={day}"),
                logged_at: Some(at(base + 1_100)),
                boot_id: None,
                hostname: Some("box".into()),
                message: Some("failed".into()),
                priority: Some(3),
                unit: Some("x.service".into()),
                user_unit: None,
                identifier: None,
                pid: None,
                uid: None,
                command: None,
                transport: None,
                fields: Default::default(),
            },
            "machine.journald",
            next(),
        ));
    }
    out
}

/// The live derived events: type and payload, with IDs left out, counted.
async fn derived_set(w: &World) -> BTreeMap<String, i64> {
    let mut conn = PgConnection::connect(&w.url).await.unwrap();
    let rows: Vec<(String, i64)> = sqlx::query_as(
        r#"SELECT e.event_type || ' ' || regexp_replace(e.payload::text,
                  '[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}', '<id>', 'g'),
                  count(*)
           FROM core.events e
           WHERE e.parents IS NOT NULL
             AND (EXISTS (SELECT 1 FROM core.live_equivalence l WHERE l.event_id = e.id)
               OR EXISTS (SELECT 1 FROM core.live_locations l WHERE l.event_id = e.id))
           GROUP BY 1"#,
    )
    .fetch_all(&mut conn)
    .await
    .unwrap();
    rows.into_iter().collect()
}

/// The derivations with one tag rule, so the tag applier has work too.
fn configuration() -> sinex_derivations::Configuration {
    let configuration: sinex_derivations::Configuration = serde_json::from_value(json!({
        "tags": [{"tag": "failed", "event_type": "command.fused",
                  "when": [{"field": "/exit_code", "is": 1}]}]
    }))
    .unwrap();
    configuration
}

async fn derive_all(w: &World) {
    let mut d = w
        .driver_with(
            sinex_derivations::keyed(),
            sinex_derivations::per_event_with(&configuration()).unwrap(),
        )
        .await;
    d.clock = Some(at(86_400 * 5));
    d.catch_up().await.unwrap();
}

#[devkit::test]
async fn every_derivation_gives_the_same_outputs_at_once_and_piece_by_piece(
    db: devkit::pg::TestDatabase,
) {
    let events = world();
    let once = World::new(db).await;
    once.admit(events.clone()).await;
    derive_all(&once).await;
    let expected = derived_set(&once).await;

    // Every derivation in the portfolio produced something from this world.
    let mut conn = PgConnection::connect(&once.url).await.unwrap();
    let producers: Vec<String> =
        sqlx::query_scalar("SELECT DISTINCT source FROM core.events WHERE parents IS NOT NULL")
            .fetch_all(&mut conn)
            .await
            .unwrap();
    let silent: Vec<&str> = sinex_derivations::keyed()
        .iter()
        .map(|d| d.name())
        .filter(|name| !producers.iter().any(|p| p == name))
        .collect();
    assert!(silent.is_empty(), "derivations with no output: {silent:?}");

    // The same world in five pieces, newest first, deriving after each.
    let pieces = World::new(devkit::pg::test_database().unwrap()).await;
    let mut reversed = events;
    reversed.reverse();
    for piece in reversed.chunks(reversed.len().div_ceil(5)) {
        pieces.admit(piece.to_vec()).await;
        derive_all(&pieces).await;
    }
    let got = derived_set(&pieces).await;
    let only_once: Vec<_> = expected
        .iter()
        .filter(|(k, n)| got.get(*k) != Some(*n))
        .map(|(k, n)| format!("{n}x {}", &k[..k.len().min(200)]))
        .take(5)
        .collect();
    let only_pieces: Vec<_> = got
        .iter()
        .filter(|(k, n)| expected.get(*k) != Some(*n))
        .map(|(k, n)| format!("{n}x {}", &k[..k.len().min(200)]))
        .take(5)
        .collect();
    assert!(
        got == expected,
        "at once only: {only_once:#?}\npiece by piece only: {only_pieces:#?}"
    );
}

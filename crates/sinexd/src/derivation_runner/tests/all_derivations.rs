//! The whole keyed derivations over one generated world of two days: every
//! derivation produces outputs, and admitting the world at once and deriving
//! gives the same live derived events as admitting it in pieces, in another
//! order, deriving after each piece.
use super::*;
use sinex_model::event_types::account::CalendarEvent;
use sinex_model::event_types::browser::BrowserVisit;
use sinex_model::event_types::code::GitCommit;
use sinex_model::event_types::communication::{
    ChatLineKind, ChatMessage, ConversationMessage, MailMessage,
};
use sinex_model::event_types::desktop::{PresenceSpan, WindowFocused, WindowSpan};
use sinex_model::event_types::health::HealthRecord;
use sinex_model::event_types::machine::JournalEntry;
use sinex_model::event_types::media::MusicPlayed;
use sinex_model::event_types::query::Mark;
use sinex_model::event_types::session::{ClipboardCopied, NotificationShown, PlayerChanged};
use sinex_model::event_types::social::PostPublished;
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

fn polylogue(kind: &str, payload: Value, anchor: u64) -> Event {
    external::record(
        &format!("polylogue.{kind}"),
        payload,
        Origin::Record {
            location: RecordLocation {
                material: MaterialId(uuid::Uuid::from_u128(u128::from(anchor) + 1)),
                anchor,
            },
            output_index: 0,
        },
    )
}

async fn declared_world(db: devkit::pg::TestDatabase) -> World {
    external::register(&db.url(None)).await;
    World::new(db).await
}

/// Two days of a person at the machine: shells, windows, the browser, music,
/// copies, notifications, mail, chat, social posts, direct messages, calendar
/// plans, moment marks, commits, the journal and a watch.
fn world() -> Vec<Event> {
    let mut out = Vec::new();
    let mut anchor = 0u64;
    let mut next = || {
        anchor += 1;
        anchor
    };
    for (seconds, event) in [(0, "start"), (2 * 86_400, "stop")] {
        out.push(captured(
            &sinex_model::event_types::input::InputLogger {
                session: "physical-input".into(),
                logged_at: at(seconds),
                event: event.into(),
                n: 0,
                fields: serde_json::from_value(
                    json!({"host":"box","layer":"evdev","virtual":false}),
                )
                .unwrap(),
            },
            "input.scribe",
            next(),
        ));
    }
    out.push(polylogue(
        "work_snapshot",
        external::snapshot("test-session", "task", &[at(100)], 1),
        next(),
    ));
    let key = sinex_model::event_types::library::item_key("box", "/library", "example.png");
    out.push(captured(
        &sinex_model::event_types::library::FileCopy {
            scan: "scan".into(),
            part_index: 0,
            item: sinex_model::event_types::library::item_id(&key),
            item_key: key,
            scanned_at: at(0),
            host: "box".into(),
            root: "/library".into(),
            path: "/library/example.png".into(),
            content: "blake3:example".into(),
            size: 3,
            sha256: String::new(),
            md5: String::new(),
            media_type: "image/png".into(),
            width: Some(2),
            height: Some(2),
            duration_ms: None,
            stored: false,
            device: None,
            inode: None,
            modified_at: None,
            moved_from: None,
            move_candidates: vec![],
            previous_content: None,
            vanished_at: None,
        },
        "library.scan",
        next(),
    ));
    for day in 0..2i64 {
        let base = day * 86_400;
        for second in [0, 20, 40, 400, 420] {
            out.push(captured(
                &sinex_model::event_types::input::KeyPressed {
                    session: "physical-test".into(),
                    logged_at: at(base + second),
                    kernel_at: None,
                    key: "KEY_A".into(),
                    code: Some(30),
                    value: Some(1),
                    changed: Some(true),
                    clipboard: None,
                    window: None,
                    n: 0,
                    extra: serde_json::from_value(
                        json!({"host": "box", "layer": "evdev", "virtual": false}),
                    )
                    .unwrap(),
                },
                "input.scribe",
                next(),
            ));
        }
        for i in 0..12i64 {
            let s = base + i * 600;
            let command = format!("step {day} {i}");
            out.push(atuin(&format!("{day}-{i}"), &command, s, i % 3, 1));
            out.push(zsh(&command, s, next()));
            out.push(captured(
                &WindowFocused {
                    scope: Some("synthetic-compositor".into()),
                    host: Some("box".into()),
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
                    scope: None,
                    host: None,
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
                    database_generation: "fixture-host:1:10".into(),
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
                    captured_by: sinex_model::event_types::session::CaptureProgram::Sinex,
                    received_at: at(s + 90),
                    selection: "clipboard".into(),
                    mime_types: Some(vec!["text/plain".into()]),
                    mime_type: Some("text/plain".into()),
                    text: Some(format!("copied {day} {i}")),
                    bytes: Some(12),
                    content_hash: None,
                    sha256: None,
                    window: None,
                    imported_from: None,
                    extra: Default::default(),
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
                scope: None,
                host: None,
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
                scope: None,
                host: None,
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
                captured_by: sinex_model::event_types::session::CaptureProgram::Sinex,
                position_us: None,
                heartbeat: false,
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
                captured_by: sinex_model::event_types::session::CaptureProgram::Sinex,
                notification_id: None,
                extra: Default::default(),
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
                    account: Some("synthetic@example.org".into()),
                    native_id: None,
                    message_id: Some(format!("<m{day}@example.org>")),
                    date: Some(at(base + 800)),
                    from: vec!["a@example.org".into()],
                    to: vec!["b@example.org".into()],
                    cc: vec![],
                    bcc: vec![],
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
            &PostPublished {
                service: "reddit".into(),
                kind: "post".into(),
                native_id: format!("post-{day}"),
                created_at: Some(at(base + 920)),
                title: Some("Example post".into()),
                body: Some("Synthetic community update".into()),
                url: None,
                parent: None,
                community: Some("example".into()),
                tags: vec![],
                score: None,
                extra: Default::default(),
            },
            "social.reddit",
            next(),
        ));
        out.push(captured(
            &ConversationMessage {
                service: "messenger".into(),
                thread: "op|peer".into(),
                thread_name: None,
                participants: vec!["op".into(), "peer".into()],
                sender: "op".into(),
                sent_at: at(base + 940),
                text: Some("Synthetic direct message".into()),
                kind: Some("text".into()),
                media: vec![],
                reactions: vec![],
                message_id: Some(format!("message-{day}")),
                unsent_at: None,
            },
            "communication.messenger",
            next(),
        ));
        out.push(captured(
            &CalendarEvent {
                calendar: "synthetic".into(),
                uid: format!("plan-{day}"),
                recurrence_id: None,
                summary: Some("Planned reading".into()),
                description: None,
                location: None,
                start: Some(at(base + 3_600)),
                end: Some(at(base + 5_400)),
                all_day: false,
                zone: "UTC".into(),
                local_zone: Some("UTC".into()),
                status: None,
                rrule: None,
                exdates: vec![],
                recurrence_start: None,
                excluded_starts: vec![],
                added_starts: vec![],
                organizer: None,
                attendees: vec![],
                created_at: None,
                last_modified: None,
                sequence: None,
                cancelled_at: None,
                extra: Default::default(),
            },
            "account.calendar",
            next(),
        ));
        out.push(captured(
            &Mark {
                host: "box".into(),
                at: at(base + 30),
                text: Some("Synthetic moment mark".into()),
                cwd: None,
                command: None,
                app: None,
                title: None,
                by: "fixture".into(),
                written_at: at(base + 30),
                command_record: None,
                focus_record: None,
            },
            "operator.marks",
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
                worktrees: vec![],
                files: None,
                original_bytes: Default::default(),
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
                from_sinex: false,
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
    let once = declared_world(db).await;
    once.admit(events.clone()).await;
    derive_all(&once).await;
    let expected = derived_set(&once).await;

    // Every derivation in the derivations produced something from this world.
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
    let pieces = declared_world(devkit::pg::test_database().unwrap()).await;
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

struct ChangedHourlyMetrics;

impl Keyed for ChangedHourlyMetrics {
    const NAME: &'static str = sinex_derivations::figures::HourlyMetrics::NAME;
    const VERSION: u32 = sinex_derivations::figures::HourlyMetrics::VERSION + 1;
    const INPUTS: &'static [&'static str] = sinex_derivations::figures::HourlyMetrics::INPUTS;
    const OUTPUTS: &'static [&'static str] = sinex_derivations::figures::HourlyMetrics::OUTPUTS;

    fn keys_for(&self, input: &KeyView<'_>, env: &Env) -> Vec<Value> {
        Keyed::keys_for(&sinex_derivations::figures::HourlyMetrics, input, env)
    }

    fn inputs(&self, key: &Value, env: &Env) -> Option<InputQuery> {
        Keyed::inputs(&sinex_derivations::figures::HourlyMetrics, key, env)
    }

    fn compute(&self, key: &Value, inputs: &[Event], env: &Env) -> Computed {
        let mut computed =
            Keyed::compute(&sinex_derivations::figures::HourlyMetrics, key, inputs, env);
        for event in &mut computed.outputs {
            let mut payload: MetricFigure = event.payload_as().unwrap();
            if payload.metric != "shell.commands" {
                continue;
            }
            payload.value = payload.value.map(|value| value + 1.0);
            let Origin::Derived { parents } = &event.origin else {
                panic!("a metric figure has derived parents")
            };
            *event = sinex_derivations::derived_event::derived(
                &payload,
                Self::NAME,
                inputs.iter().filter(|input| parents.contains(&input.id)),
            );
        }
        computed
    }

    fn key_state(&self, key: &Value, env: &Env) -> KeyState {
        Keyed::key_state(&sinex_derivations::figures::HourlyMetrics, key, env)
    }
}

fn changed_derivations() -> Vec<Box<dyn AnyKeyed>> {
    sinex_derivations::keyed()
        .into_iter()
        .map(|derivation| {
            if derivation.name() == ChangedHourlyMetrics::NAME {
                Box::new(ChangedHourlyMetrics) as Box<dyn AnyKeyed>
            } else {
                derivation
            }
        })
        .collect()
}

#[devkit::test]
async fn comparing_then_switching_the_derivations_matches_a_fresh_computation(
    db: devkit::pg::TestDatabase,
) {
    let events = world();
    let switched = declared_world(db).await;
    switched.admit(events.clone()).await;
    derive_all(&switched).await;
    let before = derived_set(&switched).await;
    let stored = switched.count("SELECT count(*) FROM core.events").await;
    let mut read = PgConnection::connect(&switched.url).await.unwrap();
    let previous_hourly: Vec<uuid::Uuid> = sqlx::query_scalar(
        "SELECT e.id FROM core.events e WHERE e.source = $1
         AND EXISTS (SELECT 1 FROM core.live_equivalence l WHERE l.event_id = e.id)",
    )
    .bind(ChangedHourlyMetrics::NAME)
    .fetch_all(&mut read)
    .await
    .unwrap();
    assert!(!previous_hourly.is_empty());

    let mut changed = switched
        .driver_with(
            changed_derivations(),
            sinex_derivations::per_event_with(&configuration()).unwrap(),
        )
        .await;
    changed.clock = Some(at(86_400 * 5));
    let comparison = changed
        .compare(ChangedHourlyMetrics::NAME, None, 3)
        .await
        .unwrap();
    assert!(
        comparison.keys > 0 && comparison.changed > 0,
        "{comparison}"
    );
    assert!(
        comparison.same > 0,
        "the fixture includes unchanged outputs: {comparison}"
    );
    assert_eq!((comparison.added, comparison.removed), (0, 0));
    assert_eq!(derived_set(&switched).await, before);
    assert_eq!(
        switched.count("SELECT count(*) FROM core.events").await,
        stored
    );

    changed
        .switch(ChangedHourlyMetrics::NAME, comparison.id.unwrap(), "test")
        .await
        .unwrap();
    changed.catch_up().await.unwrap();
    let after = derived_set(&switched).await;
    assert_ne!(after, before);
    let versions: Vec<String> = sqlx::query_scalar(
        "SELECT DISTINCT m.version FROM core.events e
         JOIN core.runs r ON r.id = e.run_id
         JOIN core.manifests m ON m.id = r.manifest_id
         WHERE e.source = $1
           AND EXISTS (SELECT 1 FROM core.live_equivalence l WHERE l.event_id = e.id)",
    )
    .bind(ChangedHourlyMetrics::NAME)
    .fetch_all(&mut read)
    .await
    .unwrap();
    assert_eq!(versions, [ChangedHourlyMetrics::VERSION.to_string()]);
    let archived: i64 =
        sqlx::query_scalar("SELECT count(*) FROM audit.archived WHERE event_id = ANY($1)")
            .bind(&previous_hourly)
            .fetch_one(&mut read)
            .await
            .unwrap();
    assert_eq!(archived, i64::try_from(previous_hourly.len()).unwrap());
    read.close().await.unwrap();

    let fresh = declared_world(devkit::pg::test_database().unwrap()).await;
    fresh.admit(events).await;
    let mut recomputed = fresh
        .driver_with(
            changed_derivations(),
            sinex_derivations::per_event_with(&configuration()).unwrap(),
        )
        .await;
    recomputed.clock = Some(at(86_400 * 5));
    recomputed.catch_up().await.unwrap();
    assert_eq!(after, derived_set(&fresh).await);
}

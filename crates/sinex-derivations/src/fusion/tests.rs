use super::rules::{CLIPBOARD, COMMANDS, FOCUS, MAIL, NOTIFICATIONS, PLAYS, RULES, VISITS};
use super::*;
use sinex_model::event_types::browser::LinkFollowed;
use sinex_model::event_types::browser::{BrowserHistoryEntry, BrowserVisit};
use sinex_model::event_types::communication::MailMessage;
use sinex_model::event_types::desktop::WindowSpan;
use sinex_model::event_types::media::MusicPlayed;
use sinex_model::event_types::session::{ClipboardCopied, NotificationShown, PlayerChanged};
use sinex_model::event_types::{CommandExecuted, CommandImported, StateInterval};
use sinex_model::{Class, EventPayload, MaterialId, Origin, ReadingOrder, RecordLocation};

const H: f64 = 1_700_002_800.0; // an hour boundary (1700002800 / 3600 = 472223)

fn at(secs: f64) -> Timestamp {
    Timestamp::from_unix_ns((secs * 1e9) as i128).unwrap()
}

fn captured<P: EventPayload>(payload: &P, source: &str, class: Class) -> Event {
    Event::build(
        payload,
        source,
        Origin::Record {
            location: RecordLocation {
                material: MaterialId::new(),
                anchor: 0,
            },
            output_index: 0,
        },
        ReadingOrder(0),
        [0; 32],
        class,
    )
    .unwrap()
}

pub(crate) fn atuin(command: &str, secs: f64, exit: i64) -> Event {
    captured(
        &CommandExecuted {
            native_id: format!("{secs}"),
            command: command.into(),
            cwd: "/home/u/proj".into(),
            session: "s1".into(),
            hostname: "box:u".into(),
            started_at: at(secs),
            duration_ns: Some(1_000_000_000),
            exit_code: Some(exit),
            deleted_at: None,
            author: None,
            intent: None,
            shell: None,
            extra: Default::default(),
        },
        "terminal.atuin",
        Class::Open,
    )
}

pub(crate) fn zsh(command: &str, secs: f64) -> Event {
    captured(
        &CommandImported {
            command: command.into(),
            hostname: "box".into(),
            started_at: Some(at(secs)),
            header_elapsed: Some(0),
            paths: vec![],
            n: 0,
        },
        "terminal.zsh",
        Class::Guarded,
    )
}

/// A compositor focus interval, as the interval derivation writes it.
fn hyprland(class: &str, title: &str, from: f64, to: Option<f64>) -> Event {
    let payload = StateInterval {
        interval_id: format!("desktop.focus:{class}:{from}"),
        state_kind: "desktop.focus".into(),
        subject_id: Some(class.into()),
        label: Some(format!("{class}: {title}")),
        started_at: at(from),
        ended_at: to.map(at),
        start_event_type: "window.focused".into(),
        end_event_type: to.map(|_| "window.focused".into()),
        attributes: [("class", class), ("title", title)]
            .into_iter()
            .map(|(k, v)| (k.to_owned(), v.to_owned()))
            .collect(),
    };
    crate::derived_event::derived(&payload, "state.intervals", &[zsh("parent", from)])
}

fn activitywatch(app: &str, title: &str, from: f64, to: f64) -> Event {
    captured(
        &WindowSpan {
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

/// The same event as the Sinnix capture lake holds it: the lake emits the
/// live capture's event types under its own lane's source.
fn in_lake(mut event: Event, source: &str) -> Event {
    event.source = source.into();
    event
}

/// A link the browser extension reported, as the capture lake holds it.
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

fn spotify(track: &str, artist: &str, from: f64, to: f64) -> Event {
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

fn mpris(title: &str, artist: &str, secs: f64, status: Option<&str>, length_s: f64) -> Event {
    captured(
        &PlayerChanged {
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
        "desktop.mpris",
        Class::Open,
    )
}

fn clipboard(selection: &str, text: &str, secs: f64, secret: bool) -> Event {
    captured(
        &ClipboardCopied {
            received_at: at(secs),
            selection: selection.into(),
            mime_types: Some(vec!["text/plain".into()]),
            mime_type: Some("text/plain".into()),
            text: Some(text.into()),
            bytes: Some(text.len() as u64),
            content_hash: None,
            sha256: None,
            window: None,
            imported_from: None,
            secret,
            extra: Default::default(),
        },
        "desktop.clipboard",
        Class::Open,
    )
}

fn notification(summary: &str, secs: f64) -> Event {
    captured(
        &NotificationShown {
            received_at: at(secs),
            app: "mail".into(),
            summary: summary.into(),
            body: "body".into(),
            icon: None,
            replaces_id: None,
            actions: vec![],
            hints: Default::default(),
            expire_timeout_ms: -1,
            sender: ":1.2".into(),
            extra: Default::default(),
        },
        "desktop.notifications",
        Class::Open,
    )
}

/// Run a rule as the runner would: every key the inputs touch, each reading
/// what its query admits. Outputs sorted by start, then record.
fn fuse_events(rule: &'static FusionRule, inputs: &[Event]) -> Vec<Event> {
    let fusion = Fusion(rule);
    let env = Env::at(Timestamp::from_unix_secs(2_000_000_000).unwrap());
    let mut keys: Vec<Value> = Vec::new();
    for e in inputs {
        let view = KeyView {
            event_type: e.event_type,
            ts_orig: e.ts_orig.at,
            payload: &e.payload,
        };
        for k in fusion.keys_for(&view, &env) {
            if !keys.contains(&k) {
                keys.push(k);
            }
        }
    }
    let mut out = Vec::new();
    for k in keys {
        let q = fusion.inputs(&k, &env).unwrap();
        let mut window: Vec<Event> = inputs.iter().filter(|e| q.admits(e)).cloned().collect();
        window.sort_by_key(|e| e.ts_orig.at);
        out.extend(fusion.compute(&k, &window, &env).outputs);
    }
    out.sort_by_key(|e| {
        let start: Timestamp = serde_json::from_value(e.payload[rule.start_field].clone()).unwrap();
        (start, e.payload["record"].as_str().unwrap_or("").to_owned())
    });
    out
}

fn fuse(rule: &'static FusionRule, inputs: &[Event]) -> Vec<Value> {
    fuse_events(rule, inputs)
        .into_iter()
        .map(|e| e.payload)
        .collect()
}

fn observed_by(v: &Value) -> Vec<&str> {
    v["observed_by"]
        .as_array()
        .unwrap()
        .iter()
        .map(|s| s.as_str().unwrap())
        .collect()
}

#[devkit::test]
fn every_rule_reads_and_writes_registered_types() {
    for rule in RULES {
        let mut types: Vec<&str> = Vec::new();
        for o in rule.observers {
            if !types.contains(&o.event_type) {
                types.push(o.event_type);
            }
        }
        assert_eq!(rule.inputs, types, "{}", rule.derivation);
        assert!(
            sinex_model::event_types::find(rule.output).is_some(),
            "{}",
            rule.output
        );
        for o in rule.observers {
            assert!(
                sinex_model::event_types::find(o.event_type).is_some(),
                "{}",
                o.event_type
            );
        }
    }
}

// Commands.

#[devkit::test]
fn close_commands_fuse_and_atuin_supplies_fields() {
    let out = fuse(
        &COMMANDS,
        &[atuin("ls -la", H + 10.0, 2), zsh("  ls -la ", H + 10.5)],
    );
    assert_eq!(out.len(), 1);
    let f = &out[0];
    assert_eq!(observed_by(f), ["terminal.atuin", "terminal.zsh"]);
    assert_eq!(f["exit_code"], 2);
    assert_eq!(f["cwd"], "/home/u/proj");
    assert_eq!(f["host"], "box");
    assert_eq!(f["command"], "ls -la");
}

#[devkit::test]
fn closest_pairs_win_and_extras_stay_alone() {
    // Two zsh readings of the same command near one Atuin reading: the
    // closer one fuses, the other stays its own command.
    let out = fuse(
        &COMMANDS,
        &[
            atuin("make", H + 100.0, 0),
            zsh("make", H + 100.4),
            zsh("make", H + 101.5),
        ],
    );
    assert_eq!(out.len(), 2);
    assert_eq!(out.iter().filter(|f| observed_by(f).len() == 2).count(), 1);
}

#[devkit::test]
fn fallback_needs_a_unique_partner() {
    // 30 s apart: only the fallback pass can match, and it does when unique.
    let one = fuse(
        &COMMANDS,
        &[atuin("git push", H + 200.0, 0), zsh("git push", H + 230.0)],
    );
    assert_eq!(one.len(), 1);
    // Two zsh partners for one Atuin record: ambiguous, nothing fuses.
    let out = fuse(
        &COMMANDS,
        &[
            atuin("git push", H + 300.0, 0),
            zsh("git push", H + 320.0),
            zsh("git push", H + 340.0),
        ],
    );
    assert_eq!(out.len(), 3);
    assert!(out.iter().all(|f| observed_by(f).len() == 1));
}

#[devkit::test]
fn different_commands_or_far_apart_never_fuse() {
    let differ = fuse(
        &COMMANDS,
        &[atuin("ls", H + 10.0, 0), zsh("ls -l", H + 10.0)],
    );
    assert_eq!(differ.len(), 2);
    let far = fuse(&COMMANDS, &[atuin("ls", H + 10.0, 0), zsh("ls", H + 100.0)]);
    assert_eq!(far.len(), 2);
}

#[devkit::test]
fn a_pair_across_an_hour_edge_is_emitted_once() {
    let out = fuse(
        &COMMANDS,
        &[atuin("cargo test", H - 0.5, 0), zsh("cargo test", H + 0.5)],
    );
    assert_eq!(out.len(), 1, "{out:?}");
    assert_eq!(observed_by(&out[0]).len(), 2);
}

#[devkit::test]
fn privacy_class_is_the_strictest_parent() {
    let out = fuse_events(&COMMANDS, &[atuin("ls", H + 10.0, 0), zsh("ls", H + 10.2)]);
    assert_eq!(out.len(), 1);
    assert_eq!(out[0].class, Class::Guarded);
}

#[devkit::test]
fn removed_records_do_not_fuse() {
    let mut a = atuin("rm -rf x", H + 10.0, 0);
    a.record_state = RecordState::Removed;
    let out = fuse(&COMMANDS, &[a, zsh("rm -rf x", H + 10.1)]);
    assert_eq!(out.len(), 1);
    assert_eq!(observed_by(&out[0]), ["terminal.zsh"]);
}

#[devkit::test]
fn the_same_command_twice_in_one_second_is_two_runs() {
    let a = zsh("ls", H + 5.0);
    let mut b = zsh("ls", H + 5.0);
    if let Origin::Record { location, .. } = &mut b.origin {
        location.anchor = 40;
    }
    let out = fuse_events(&COMMANDS, &[a, b]);
    assert_eq!(out.len(), 2);
    assert_ne!(out[0].equivalence_key, out[1].equivalence_key);
}

#[devkit::test]
fn result_does_not_depend_on_input_order() {
    let inputs = vec![
        atuin("a", H + 1.0, 0),
        zsh("a", H + 1.1),
        atuin("b", H + 2.0, 1),
        zsh("b", H + 30.0),
        zsh("c", H + 40.0),
    ];
    let mut reversed = inputs.clone();
    reversed.reverse();
    assert_eq!(fuse(&COMMANDS, &inputs), fuse(&COMMANDS, &reversed));
}

// Window focus.

#[devkit::test]
fn the_compositor_and_activitywatch_give_one_focus() {
    // The compositor saw one focus of ten minutes; ActivityWatch split it at
    // a title change. One focus, timed by the compositor, all three parents.
    let out = fuse_events(
        &FOCUS,
        &[
            hyprland("kitty", "vim", H + 0.0, Some(H + 600.0)),
            activitywatch("Kitty", "vim", H + 0.4, H + 300.0),
            activitywatch("Kitty", "cargo", H + 300.0, H + 600.0),
        ],
    );
    assert_eq!(out.len(), 1, "{out:?}");
    let f = &out[0].payload;
    assert_eq!(f["started_at"], json!(at(H)));
    assert_eq!(f["ended_at"], json!(at(H + 600.0)));
    assert_eq!(f["app"], "kitty");
    assert_eq!(observed_by(f), ["state.intervals", "desktop.activitywatch"]);
    assert!(matches!(&out[0].origin, Origin::Derived { parents } if parents.len() == 3));
}

#[devkit::test]
fn focus_one_source_saw_stands_alone() {
    let out = fuse(
        &FOCUS,
        &[
            activitywatch("firefox", "a", H + 10.0, H + 70.0),
            activitywatch("kitty", "b", H + 70.0, H + 90.0),
        ],
    );
    assert_eq!(out.len(), 2);
    assert_eq!(out[1]["started_at"], json!(at(H + 70.0)));
    assert_eq!(observed_by(&out[1]), ["desktop.activitywatch"]);
}

#[devkit::test]
fn an_open_focus_absorbs_what_follows_and_stays_open() {
    let out = fuse(
        &FOCUS,
        &[
            hyprland("kitty", "vim", H + 0.0, None),
            activitywatch("kitty", "vim", H + 0.2, H + 100.0),
            activitywatch("kitty", "top", H + 100.0, H + 200.0),
        ],
    );
    assert_eq!(out.len(), 1, "{out:?}");
    assert_eq!(out[0]["ended_at"], Value::Null);
}

#[devkit::test]
fn a_focus_across_an_hour_edge_is_emitted_once() {
    // The title change ActivityWatch saw falls in the next hour's key.
    let out = fuse(
        &FOCUS,
        &[
            hyprland("kitty", "vim", H - 100.0, Some(H + 1000.0)),
            activitywatch("kitty", "vim", H - 99.9, H + 500.0),
            activitywatch("kitty", "make", H + 500.0, H + 1000.0),
        ],
    );
    assert_eq!(out.len(), 1, "{out:?}");
    assert_eq!(observed_by(&out[0]).len(), 2);
}

#[devkit::test]
fn a_span_that_began_before_the_focus_is_not_absorbed() {
    let out = fuse(
        &FOCUS,
        &[
            activitywatch("firefox", "a", H - 1000.0, H + 60.0),
            hyprland("kitty", "vim", H + 0.0, Some(H + 60.0)),
        ],
    );
    assert_eq!(out.len(), 2);
}

// Page visits.

#[devkit::test]
fn a_visit_in_history_takeout_and_the_lake_is_one_visit() {
    let url = "https://example.org/a";
    let out = fuse(
        &VISITS,
        &[
            takeout(url, H + 20.0),
            chromium(url, H + 20.0),
            link(url, "From the lake", H + 21.0),
        ],
    );
    assert_eq!(out.len(), 1, "{out:?}");
    assert_eq!(
        out[0]["title"], "From Chromium",
        "the browser's own history first"
    );
    assert_eq!(out[0]["visited_at"], json!(at(H + 20.0)));
    assert_eq!(
        observed_by(&out[0]),
        [
            "browser.chromium-visits",
            "browser.takeout-chrome-history",
            "browser.navigation"
        ]
    );
}

#[devkit::test]
fn visits_to_other_pages_or_later_stay_apart() {
    let out = fuse(
        &VISITS,
        &[
            chromium("https://example.org/a", H + 20.0),
            takeout("https://example.org/b", H + 20.0),
            chromium("https://example.org/a", H + 620.0),
        ],
    );
    assert_eq!(out.len(), 3);
}

// Plays.

#[devkit::test]
fn a_play_in_the_export_and_the_player_is_one_play() {
    let out = fuse_events(
        &PLAYS,
        &[
            spotify("Song", "Artist", H + 2.0, H + 200.0),
            mpris("song", "artist ", H + 0.5, Some("Playing"), 198.0),
            mpris("Song", "Artist", H + 1.5, None, 198.0),
            in_lake(
                mpris("Song", "Artist", H + 0.7, Some("Playing"), 198.0),
                "desktop.media",
            ),
        ],
    );
    assert_eq!(out.len(), 1, "{out:?}");
    let p = &out[0].payload;
    assert_eq!(p["started_at"], json!(at(H + 2.0)), "the export times it");
    assert_eq!(p["ended_at"], json!(at(H + 200.0)));
    assert_eq!(p["title"], "Song");
    assert_eq!(p["player"], "org.mpris.MediaPlayer2.spotify");
    assert_eq!(
        observed_by(p),
        ["media.spotify-history", "desktop.mpris", "desktop.media"]
    );
    assert!(matches!(&out[0].origin, Origin::Derived { parents } if parents.len() == 4));
}

#[devkit::test]
fn a_player_alone_gives_one_play_per_track_start() {
    let out = fuse(
        &PLAYS,
        &[
            mpris("Song", "Artist", H + 0.0, Some("Playing"), 180.0),
            mpris("Song", "Artist", H + 1.0, None, 180.0),
            mpris("Song", "Artist", H + 60.0, Some("Paused"), 180.0),
            mpris("Song", "Artist", H + 200.0, Some("Playing"), 180.0),
        ],
    );
    assert_eq!(out.len(), 2, "a refresh joins, a pause is no play: {out:?}");
    assert_eq!(out[1]["started_at"], json!(at(H + 200.0)));
}

#[devkit::test]
fn a_stream_of_unknown_length_has_no_end() {
    // Chromium reports a live stream's length as the largest integer.
    let out = fuse(
        &PLAYS,
        &[
            mpris("Live", "Channel", H, Some("Playing"), i64::MAX as f64 / 1e6),
            in_lake(
                mpris(
                    "Live",
                    "Channel",
                    H + 0.2,
                    Some("Playing"),
                    9_223_372_036_854.775,
                ),
                "desktop.media",
            ),
        ],
    );
    assert_eq!(out.len(), 1, "{out:?}");
    assert!(
        out[0]
            .get("ended_at")
            .is_none_or(|end| end.as_str() < Some("2100")),
        "{:?}",
        out[0]
    );
}

// Clipboard and notifications: live capture against the capture lake.

#[devkit::test]
fn a_copy_seen_live_and_in_the_lake_is_one_copy() {
    let out = fuse(
        &CLIPBOARD,
        &[
            clipboard("clipboard", "hello", H + 5.0, false),
            in_lake(
                clipboard("clipboard", "hello", H + 5.3, false),
                "desktop.clipboard",
            ),
            in_lake(
                clipboard("primary", "hello", H + 5.3, false),
                "desktop.clipboard",
            ),
            clipboard("clipboard", "secret", H + 9.0, true),
        ],
    );
    assert_eq!(out.len(), 2, "{out:?}");
    assert_eq!(observed_by(&out[0]), ["desktop.clipboard"]);
    assert_eq!(out[1]["selection"], "primary");
}

#[devkit::test]
fn a_notification_seen_live_and_in_the_lake_is_one_notification() {
    let out = fuse(
        &NOTIFICATIONS,
        &[
            notification("New mail", H + 5.0),
            in_lake(notification("New mail", H + 5.1), "desktop.notifications"),
            notification("Other", H + 5.0),
        ],
    );
    assert_eq!(out.len(), 2, "{out:?}");
    let joined = out.iter().find(|n| n["summary"] == "New mail").unwrap();
    assert_eq!(observed_by(joined), ["desktop.notifications"]);
}

// Mail: one message stored by several mail sources.

fn mail(source: &str, id: &str, secs: f64) -> Event {
    captured(
        &MailMessage {
            message_id: id.into(),
            date: Some(at(secs)),
            from: vec!["a@example.org".into()],
            to: vec![],
            cc: vec![],
            subject: Some(format!("about {id}")),
            in_reply_to: None,
            references: vec![],
            labels: vec![],
            thread_id: None,
            text: None,
            html_only: false,
            attachments: vec![],
            size: 10,
        },
        source,
        Class::Open,
    )
}

#[devkit::test]
fn a_message_in_imap_and_an_mbox_is_one_message() {
    let out = fuse(
        &MAIL,
        &[
            mail("communication.mbox", "<1@x>", H + 5.0),
            mail("communication.imap", "<1@x>", H + 5.0),
            mail("communication.imap", "<2@x>", H + 5.0),
        ],
    );
    assert_eq!(out.len(), 2, "{out:?}");
    let joined = out.iter().find(|m| m["message_id"] == "<1@x>").unwrap();
    assert_eq!(
        observed_by(joined),
        ["communication.imap", "communication.mbox"]
    );
    assert_eq!(joined["subject"], "about <1@x>");
}

#[devkit::test]
fn mail_from_a_source_no_observer_names_is_not_fused() {
    let out = fuse(&MAIL, &[mail("communication.elsewhere", "<1@x>", H + 5.0)]);
    assert!(out.is_empty());
}

use super::*;
use serde_json::json;
use sinex_model::{Manifest, Member};

fn member(path: &str, removed: bool) -> Member {
    let mut details = Map::new();
    if removed {
        details.insert("removed_at_ns".into(), json!("1"));
    }
    Member {
        path: path.into(),
        stream_offset: 0,
        length: 0,
        details,
    }
}

fn read(path: &str, bytes: &[u8], windows: &Windows) -> (Vec<Snapshot>, Windows) {
    let manifest = Manifest::default();
    let member = member(path, false);
    let context = RecordContext {
        manifest: &manifest,
        member: Some(&member),
    };
    ScrollbackParser.parse(&bytes, windows, &context).unwrap()
}

fn captured(events: &[Snapshot]) -> &ScrollbackCaptured {
    match &events[0] {
        Snapshot::Captured(c) => c,
        Snapshot::Window(_) => panic!("not a capture"),
    }
}

#[devkit::test]
fn file_names_give_time_host_process_and_window() {
    let named =
        name("20261026T200909Z-sinnix-prime-pid24601-win11-weechat-scratchpad.ansi").unwrap();
    assert_eq!(named.host, "sinnix-prime");
    assert_eq!(named.kitty_pid, Some(24601));
    assert_eq!(named.window_id, 11);
    assert_eq!(
        named.captured_at,
        fields::rfc3339("2026-10-26T20:09:09Z").unwrap()
    );
    let older = name("20260204T000204Z-host-a-win1-kitty-win2-capture.meta.json").unwrap();
    assert_eq!(
        (older.host.as_str(), older.kitty_pid, older.window_id),
        ("host-a", None, 1)
    );
    assert_eq!(name("20260204T000204Z-h-win3.ansi").unwrap().window_id, 3);
    for bad in [
        "notes.ansi",
        "20260204T000204Z-h-window.ansi",
        "2026020400020400-h-win1-x.ansi",
    ] {
        assert_eq!(name(bad), None, "{bad}");
    }
}

#[devkit::test]
fn plain_lines_drop_escapes_trailing_spaces_and_empty_screen_rows() {
    let lines = plain_lines("\u{1b}[32mok\u{1b}[m   \r\nnext\r\n\r\n   \r\n");
    assert_eq!(lines, ["ok", "next"]);
}

#[devkit::test]
fn each_snapshot_holds_only_what_is_new_for_its_window() {
    let path = |stamp: &str| format!("{stamp}-h-pid7-win1-zsh.ansi");
    let first = "$ ls\r\na b\r\n$ \r\n\r\n";
    let (events, windows) = read(
        &path("20260101T000000Z"),
        first.as_bytes(),
        &Windows::default(),
    );
    let c = captured(&events);
    assert_eq!((c.continues, c.lines, c.new_lines), (false, 3, 3));
    assert_eq!(c.text, "$ ls\na b\n$");
    assert_eq!(c.title.as_deref(), Some("zsh"));

    // The prompt row was redrawn with the command typed at it.
    let second = "$ ls\r\na b\r\n$ pwd\r\n/home\r\n$ \r\n";
    let (events, windows) = read(&path("20260101T003000Z"), second.as_bytes(), &windows);
    let c = captured(&events);
    assert_eq!((c.continues, c.new_lines), (true, 3));
    assert_eq!(c.text, "$ pwd\n/home\n$");

    // Another window does not disturb this one's alignment.
    let other = "x\r\n";
    let (_, windows) = read(
        "20260101T003000Z-h-pid7-win2-x.ansi",
        other.as_bytes(),
        &windows,
    );

    // Nothing new: an empty text that still continues.
    let (events, windows) = read(&path("20260101T010000Z"), second.as_bytes(), &windows);
    let c = captured(&events);
    assert_eq!((c.continues, c.new_lines, c.text.as_str()), (true, 0, ""));

    // Cleared: the whole snapshot again.
    let (events, _) = read(&path("20260101T013000Z"), b"$ \r\n", &windows);
    let c = captured(&events);
    assert_eq!((c.continues, c.new_lines, c.text.as_str()), (false, 1, "$"));
}

#[devkit::test]
fn long_scrollback_aligns_on_its_prefix_and_last_rows() {
    let mut lines: Vec<String> = (0..1000).map(|i| format!("line {i}")).collect();
    let first = lines.join("\r\n");
    let (_, windows) = read(
        "20260101T000000Z-h-win1-x.ansi",
        first.as_bytes(),
        &Windows::default(),
    );
    lines.extend((1000..1010).map(|i| format!("line {i}")));
    let (events, _) = read(
        "20260101T001000Z-h-win1-x.ansi",
        lines.join("\r\n").as_bytes(),
        &windows,
    );
    let c = captured(&events);
    assert!(c.continues);
    assert_eq!(c.new_lines, 10);
    assert!(c.text.starts_with("line 1000\n"));
}

#[devkit::test]
fn windows_unseen_for_a_day_are_forgotten() {
    let (_, windows) = read(
        "20260101T000000Z-h-win1-x.ansi",
        b"a\n",
        &Windows::default(),
    );
    let (_, windows) = read("20260103T000000Z-h-win2-x.ansi", b"b\n", &windows);
    let (events, _) = read("20260103T000100Z-h-win1-x.ansi", b"a\nc\n", &windows);
    assert!(!captured(&events).continues);
}

#[devkit::test]
fn sidecars_describe_the_window_and_removals_read_as_nothing() {
    let sidecar = json!({
        "window_id": 11, "kitty_pid": 24601, "title": "weechat: #channel",
        "cwd": "/home/me", "captured_at": "20261026T200909Z", "hostname": "box",
        "ansi_file": "20261026T200909Z-box-pid24601-win11-weechat-channel.ansi", "tab": 2
    });
    let (events, windows) = read(
        "20261026T200909Z-box-pid24601-win11-weechat-channel.meta.json",
        sidecar.to_string().as_bytes(),
        &Windows::default(),
    );
    let Snapshot::Window(w) = &events[0] else {
        panic!("not a sidecar")
    };
    assert_eq!(w.title.as_deref(), Some("weechat: #channel"));
    assert_eq!(w.cwd.as_deref(), Some("/home/me"));
    assert_eq!((w.kitty_pid, w.window_id), (Some(24601), 11));
    assert_eq!(w.extra.get("tab"), Some(&json!(2)));
    assert_eq!(windows, Windows::default());

    let manifest = Manifest::default();
    let removed = member("20261026T200909Z-box-win1-x.ansi", true);
    let context = RecordContext {
        manifest: &manifest,
        member: Some(&removed),
    };
    let (events, _) = ScrollbackParser
        .parse(
            &b"{\"removed\":\"x\"}\n".as_slice(),
            &Windows::default(),
            &context,
        )
        .unwrap();
    assert!(events.is_empty());
}

#[devkit::test]
fn the_selection_takes_snapshots_and_sidecars_only() {
    assert!(select("a/20260101T000000Z-h-win1-x.ansi"));
    assert!(select("20260101T000000Z-h-win1-x.meta.json"));
    assert!(!select("20260101T000000Z-h-win1-x.html"));
    assert!(!select("INVENTORY.md"));
}

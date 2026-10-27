use super::*;
use sinex_model::{MaterialId, ReadingOrder, RecordOutcome};
use sinex_source_core::Reader;
use sinex_source_core::{DrainInput, Limits, Source};

fn rec(line: &str, ns: i128) -> Map<String, Value> {
    serde_json::from_slice(&live_stream::frame(
        Timestamp::from_unix_ns(ns).unwrap(),
        line.as_bytes(),
    ))
    .unwrap()
}

fn parse_all(lines: &[&str]) -> Vec<Desktop> {
    let mut carry = None;
    let mut out = Vec::new();
    for (i, line) in lines.iter().enumerate() {
        let (events, next) = HyprlandParser
            .parse(
                &rec(line, 1_000_000_000 + i as i128),
                &carry,
                &sinex_source_core::RecordContext::none(),
            )
            .unwrap();
        out.extend(events);
        carry = next;
    }
    out
}

fn at(ns: i128) -> Timestamp {
    Timestamp::from_unix_ns(ns).unwrap()
}

#[devkit::test]
fn focus_halves_merge_and_keep_the_first_halfs_time() {
    let out = parse_all(&["activewindow>>kitty,~/src", "activewindowv2>>55aa"]);
    assert_eq!(
        out,
        vec![Desktop::Focused(WindowFocused {
            received_at: at(1_000_000_000),
            window: Some("55aa".into()),
            class: Some("kitty".into()),
            title: Some("~/src".into()),
        })]
    );
}

#[devkit::test]
fn a_lone_first_half_is_kept_when_something_else_arrives() {
    let out = parse_all(&["activewindow>>kitty,t", "closewindow>>55aa"]);
    assert_eq!(out.len(), 2);
    assert!(
        matches!(&out[0], Desktop::Focused(f) if f.window.is_none() && f.class.as_deref() == Some("kitty"))
    );
    assert!(matches!(&out[1], Desktop::Closed(c) if c.window == "55aa"));
}

#[devkit::test]
fn duplicate_forms_are_read_once() {
    let out = parse_all(&[
        "windowtitle>>55aa",
        "windowtitlev2>>55aa,a, b",
        "workspace>>2",
        "workspacev2>>2,2",
        "focusedmon>>DP-3,2",
        "focusedmonv2>>DP-3,2",
        "movewindow>>55aa,3",
        "movewindowv2>>55aa,3,3",
    ]);
    assert_eq!(out.len(), 4);
    assert!(matches!(&out[0], Desktop::TitleChanged(t) if t.title == "a, b"));
    assert!(
        matches!(&out[1], Desktop::Workspace(w) if w.workspace_id == Some(2) && w.workspace_name.as_deref() == Some("2"))
    );
    assert!(
        matches!(&out[2], Desktop::Monitor(m) if m.monitor == "DP-3" && m.workspace_name == "2")
    );
    assert!(matches!(&out[3], Desktop::Moved(m) if m.workspace_id == Some(3)));
}

#[devkit::test]
fn openwindow_title_may_contain_commas() {
    let out = parse_all(&["openwindow>>55aa,1,firefox,a, b, c"]);
    assert!(matches!(&out[0], Desktop::Opened(o)
        if o.window == "55aa" && o.workspace_name == "1" && o.class == "firefox" && o.title == "a, b, c"));
}

#[devkit::test]
fn unknown_and_malformed_lines_are_kept_whole() {
    let out = parse_all(&["screencastv2>>1,monitor,DP-3", "garbage"]);
    assert!(
        matches!(&out[0], Desktop::Other(o) if o.name == "screencastv2" && o.data == "1,monitor,DP-3")
    );
    assert!(matches!(&out[1], Desktop::Other(o) if o.name.is_empty() && o.data == "garbage"));
}

#[devkit::test]
fn named_workspaces_do_not_fail() {
    // The old reader required workspace ids to be integers and refused
    // named ones; workspacev2 carries both, so a name never fails.
    let out = parse_all(&["workspacev2>>-98,special:scratch"]);
    assert!(matches!(&out[0], Desktop::Workspace(w) if w.workspace_id == Some(-98)));
}

#[devkit::test]
fn spooled_capture_drains_to_typed_events() {
    let spool = std::env::temp_dir().join(format!("sinex-hypr-{}", uuid::Uuid::now_v7()));
    let mut w = SpoolWriter::open(&spool, Rotation::default()).unwrap();
    live_stream::capture_lines(
        &b"activewindow>>kitty,t\nactivewindowv2>>55aa\nwindowtitle>>55aa\nwindowtitlev2>>55aa,x\n"
            [..],
        &mut w,
        Duration::ZERO,
    )
    .unwrap();
    let acquired = driver(spool, "h".into()).poll(&Value::Null).unwrap();
    let m = acquired.material.unwrap();
    let chunks = source()
        .drain(DrainInput {
            source: SPEC.name,
            class: SPEC.class,
            material: MaterialId::new(),
            bytes: crate::test_support::body(&m).into(),
            records: &m.records,
            carry: Value::Null,
            reading_order: &|a| ReadingOrder(a as i64),
            limits: Limits::default(),
            manifest: &m.manifest,
        })
        .unwrap();
    let mut types: Vec<&str> = Vec::new();
    for outcome in chunks.iter().flat_map(|c| &c.outcomes) {
        match outcome {
            RecordOutcome::Events { events, .. } => {
                types.extend(events.iter().map(|e| e.event_type))
            }
            RecordOutcome::Failure { reason, .. } => panic!("{reason}"),
        }
    }
    assert_eq!(types, vec!["window.focused", "window.title_changed"]);
    // Four records read, two events: every record accounted for.
    assert_eq!(chunks.iter().map(|c| c.outcomes.len()).sum::<usize>(), 4);
}

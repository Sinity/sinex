use super::*;

fn erase(selected: &Map<String, Value>, carry: &OpenSpans) -> crate::Erased<OpenSpans> {
    ActivityWatchParser
        .erase_record(
            &serde_json::to_vec(selected).unwrap(),
            carry,
            &crate::RecordContext::none(),
        )
        .unwrap()
}

fn restart(carry: &OpenSpans) -> OpenSpans {
    serde_json::from_value(serde_json::to_value(carry).unwrap()).unwrap()
}

fn window(a: &Activity) -> (i64, i128, i128, &str, &str, u32) {
    let Activity::Window(w) = a else {
        panic!("window expected")
    };
    (
        w.span_id,
        w.started_at.unix_ns(),
        w.ended_at.unix_ns(),
        &w.app,
        &w.title,
        w.merged,
    )
}

#[test]
fn erased_window_replay_keeps_heartbeat_and_next_row_merge_coordinates() {
    let selected = win(2, 2, 4, "private-app-λ", "private-title-🦀");
    let (_, before) = run(&[win(1, 0, 2, "private-app-λ", "private-title-🦀")]);
    let (_, after) = ActivityWatchParser
        .parse(&selected, &before, &crate::RecordContext::none())
        .unwrap();
    let erased = erase(&selected, &after);
    for value in [
        serde_json::to_value(&erased.carry).unwrap(),
        erased.replay.clone().unwrap(),
    ] {
        let text = value.to_string();
        assert!(!text.contains("private-app"));
        assert!(!text.contains("private-title"));
        assert!(!text.contains("aw-watcher-window_h"));
    }
    let skipped = ActivityWatchParser
        .skip_erased(
            &before,
            &erased.replay.unwrap(),
            &crate::RecordContext::none(),
        )
        .unwrap();
    assert_eq!(
        serde_json::to_value(&skipped).unwrap(),
        serde_json::to_value(&erased.carry).unwrap()
    );
    for starting in [skipped, erased.carry] {
        for split in 0..=3 {
            let mut carry = starting.clone();
            let mut out = Vec::new();
            for (i, row) in [
                win(2, 2, 9, "private-app-λ", "private-title-🦀"),
                win(3, 9, 11, "private-app-λ", "private-title-🦀"),
                win(4, 20, 25, "  retained app λ  ", " retained title 🦀 "),
            ]
            .iter()
            .enumerate()
            {
                if i == split {
                    carry = restart(&carry);
                }
                let (events, next) = ActivityWatchParser
                    .parse(row, &carry, &crate::RecordContext::none())
                    .unwrap();
                out.extend(events);
                carry = next;
            }
            assert_eq!(
                window(&out[0]),
                (1, 0, 9_000_000_000, "private-app-λ", "private-title-🦀", 2)
            );
            assert_eq!(
                window(&out[1]),
                (1, 0, 11_000_000_000, "private-app-λ", "private-title-🦀", 3)
            );
            assert_eq!(
                window(&out[2]),
                (
                    4,
                    20_000_000_000,
                    25_000_000_000,
                    "  retained app λ  ",
                    " retained title 🦀 ",
                    1
                )
            );
        }
    }
}

#[test]
fn first_erased_row_and_spinner_matching_keep_the_original_span_id() {
    let selected = win(10, 1, 3, "kitty", "⠋ synthetic-window");
    let (_, after) = run(std::slice::from_ref(&selected));
    let erased = erase(&selected, &after);
    let prior = ActivityWatchParser
        .skip_erased(
            &OpenSpans::default(),
            &erased.replay.unwrap(),
            &crate::RecordContext::none(),
        )
        .unwrap();
    let retained = win(11, 3, 6, "kitty", "⠙ synthetic-window");
    let (out, _) = ActivityWatchParser
        .parse(&retained, &restart(&prior), &crate::RecordContext::none())
        .unwrap();
    assert_eq!(
        window(&out[0]),
        (
            10,
            1_000_000_000,
            6_000_000_000,
            "kitty",
            "synthetic-window",
            2
        )
    );
}

#[test]
fn erasure_preserves_other_buckets_and_later_unrelated_span_in_same_bucket() {
    let selected = win(1, 0, 2, "erased-app", "erased-title");
    let other = row(
        2,
        "another-window",
        "currentwindow",
        1,
        7,
        json!({"app":"other app","title":" other title "}),
    );
    let (_, after) = run(&[selected.clone(), other.clone()]);
    let erased = erase(&selected, &after);
    assert_eq!(
        erased.carry.spans.get("another-window"),
        after.spans.get("another-window")
    );
    let (out, _) = ActivityWatchParser
        .parse(
            &row(
                2,
                "another-window",
                "currentwindow",
                1,
                9,
                json!({"app":"other app","title":" other title "}),
            ),
            &restart(&erased.carry),
            &crate::RecordContext::none(),
        )
        .unwrap();
    assert_eq!(
        window(&out[0]),
        (
            2,
            1_000_000_000,
            9_000_000_000,
            "other app",
            " other title ",
            1
        )
    );
    let (_, later) = run(&[
        selected.clone(),
        other,
        win(3, 10, 12, "retained-app", "retained-title"),
    ]);
    assert_eq!(
        erase(&selected, &later).carry,
        later,
        "a later unrelated span is not scrubbed"
    );
    let (_, changed_same_row) = run(&[
        selected.clone(),
        win(1, 0, 5, "new native app", "new native title"),
    ]);
    assert_eq!(
        erase(&selected, &changed_same_row).carry,
        changed_same_row,
        "a retained revision already replaced the selected text"
    );
}

#[test]
fn erasing_an_older_member_sanitizes_current_span_without_replaying_after_state() {
    let selected = win(1, 0, 2, "erase-only-app", "erase-only-title");
    let (_, after) = run(&[
        selected.clone(),
        win(2, 2, 4, "erase-only-app", "erase-only-title"),
    ]);
    let erased = erase(&selected, &after);
    assert!(
        !serde_json::to_string(&erased.carry)
            .unwrap()
            .contains("erase-only")
    );
    let skipped = ActivityWatchParser
        .skip_erased(
            &OpenSpans::default(),
            &erased.replay.unwrap(),
            &crate::RecordContext::none(),
        )
        .unwrap();
    let (out, _) = ActivityWatchParser
        .parse(
            &win(2, 2, 4, "erase-only-app", "erase-only-title"),
            &skipped,
            &crate::RecordContext::none(),
        )
        .unwrap();
    assert_eq!(
        window(&out[0]),
        (1, 0, 4_000_000_000, "erase-only-app", "erase-only-title", 2),
        "the token applies row1, not the durable after-row2 snapshot"
    );
    let (out, _) = ActivityWatchParser
        .parse(
            &win(3, 4, 6, "erase-only-app", "erase-only-title"),
            &restart(&erased.carry),
            &crate::RecordContext::none(),
        )
        .unwrap();
    assert_eq!(
        window(&out[0]),
        (1, 0, 6_000_000_000, "erase-only-app", "erase-only-title", 3)
    );
}

#[test]
fn nonwindow_erasure_has_no_carry_effect_and_row_envelopes_use_native_fields() {
    let (_, carry) = run(&[win(1, 0, 2, "app", "title")]);
    let selected = row(
        2,
        "aw-watcher-web_h",
        "web.tab.current",
        0,
        1,
        json!({"url":"erased-url","title":"erased-title"}),
    );
    let erased = erase(&selected, &carry);
    assert_eq!(erased.carry, carry);
    assert!(erased.replay.is_none());
    let native = win(1, 0, 2, "app", "title");
    let manifest = json!({"row_format":2});
    let envelope = json!({"_sinex_rowid":87,"row":native,"app":"framework-decoy"});
    let erased = ActivityWatchParser
        .erase_record(
            &serde_json::to_vec(&envelope).unwrap(),
            &carry,
            &crate::RecordContext {
                manifest: &manifest,
                member: None,
            },
        )
        .unwrap();
    assert!(erased.carry.spans.is_empty());
    assert_eq!(erased.replay.unwrap()["row"], 1);
}

#[test]
fn unknown_erased_state_and_bad_times_refuse_without_changing_carry() {
    let selected = win(1, 0, 2, "private-app", "private-title");
    let (_, carry) = run(std::slice::from_ref(&selected));
    let erased = erase(&selected, &carry);
    let token = erased.replay.unwrap();
    let original = carry.clone();
    for bad in [
        json!({}),
        json!({"version":9}),
        {
            let mut t = token.clone();
            t["version"] = json!(2);
            t
        },
        {
            let mut t = token.clone();
            t["extra"] = json!("private-title");
            t
        },
        {
            let mut t = token.clone();
            t["started_ns"] = json!("unknown");
            t
        },
    ] {
        let error = ActivityWatchParser
            .skip_erased(&carry, &bad, &crate::RecordContext::none())
            .unwrap_err();
        assert!(!error.to_string().contains("private-title"));
        assert_eq!(carry, original);
    }
    let mut duplicate = serde_json::to_value(&erased.carry).unwrap();
    duplicate["spans"] = serde_json::to_value(&carry.spans).unwrap();
    let duplicate: OpenSpans = serde_json::from_value(duplicate).unwrap();
    assert!(
        ActivityWatchParser
            .skip_erased(&duplicate, &token, &crate::RecordContext::none())
            .is_err()
    );
    let mut state = serde_json::to_value(&erased.carry).unwrap();
    state["erased"]
        .as_object_mut()
        .unwrap()
        .values_mut()
        .next()
        .unwrap()["version"] = json!(9);
    let unknown: OpenSpans = serde_json::from_value(state).unwrap();
    assert!(
        ActivityWatchParser
            .skip_erased(&unknown, &token, &crate::RecordContext::none())
            .is_err()
    );
    assert!(
        ActivityWatchParser
            .parse(&selected, &unknown, &crate::RecordContext::none())
            .is_err()
    );
    assert!(
        ActivityWatchParser
            .erase_record(
                &serde_json::to_vec(&selected).unwrap(),
                &unknown,
                &crate::RecordContext::none()
            )
            .is_err()
    );
    for bad in [
        Value::Null,
        json!("not-a-time"),
        json!(i128::MAX.to_string()),
    ] {
        let mut row = selected.clone();
        row.insert("starttime".into(), bad);
        assert!(
            ActivityWatchParser
                .erase_record(
                    &serde_json::to_vec(&row).unwrap(),
                    &carry,
                    &crate::RecordContext::none()
                )
                .is_err()
        );
    }
}

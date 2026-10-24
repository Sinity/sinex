use super::*;
use crate::{DrainInput, Limits, drain};
use proptest::prelude::*;
use sinex_model::{MaterialId, ReadingOrder, RecordOutcome};

fn parse(bytes: &[u8]) -> Result<String, RecordError> {
    ZshParser { host: "h".into() }
        .parse(&bytes, &Seen::default(), &crate::RecordContext::none())
        .map(|(p, _)| p[0].command.clone())
}

#[test]
fn identical_runs_in_one_second_are_numbered_in_file_order() {
    let parser = ZshParser { host: "h".into() };
    let lines: [&[u8]; 4] = [
        b": 10:0;ls\n",
        b": 10:0;pwd\n",
        b": 10:0;ls\n",
        b": 11:0;ls\n",
    ];
    let mut seen = Seen::default();
    let mut ns = Vec::new();
    for line in lines {
        let (out, next) = parser
            .parse(&line, &seen, &crate::RecordContext::none())
            .unwrap();
        ns.push(out[0].n);
        seen = next;
    }
    assert_eq!(ns, [0, 0, 1, 0]);
    // A re-read from the start numbers the same records the same way.
    let mut again = Seen::default();
    let mut ns2 = Vec::new();
    for line in lines {
        let (out, next) = parser
            .parse(&line, &again, &crate::RecordContext::none())
            .unwrap();
        ns2.push(out[0].n);
        again = next;
    }
    assert_eq!(ns, ns2);
}

#[test]
fn keeps_original_backslashes_spaces_and_tabs() {
    let records: &[(&[u8], &str)] = &[
        (b": 1:0;echo slash\\ \n", "echo slash\\"),
        (b": 1:0;echo slashspace\\  \n", "echo slashspace\\ "),
        (b": 1:0;echo slashtab\\\t\n", "echo slashtab\\\t"),
        (
            b": 1:0;echo slashnewline\\\\\nnext\n",
            "echo slashnewline\\\nnext",
        ),
        (b": 1:0;echo plainspace  \n", "echo plainspace  "),
    ];
    for (record, expected) in records {
        assert_eq!(frame(record), (vec![(0, record.len())], record.len()));
        assert_eq!(parse(record).unwrap(), *expected);
    }
}

#[test]
fn metafied_bytes_and_continuations() {
    let bytes = b": 3600:0;echo \xc4\x83\x85\n: 3601:0;printf a\\\nb\n: 3602:0;partial";
    let (r, consumed) = frame(bytes);
    assert_eq!(r.len(), 2);
    assert_eq!(consumed, bytes.len() - b": 3602:0;partial".len());
    assert_eq!(parse(&bytes[r[0].0..r[0].1]).unwrap(), "echo ĥ");
    assert_eq!(parse(&bytes[r[1].0..r[1].1]).unwrap(), "printf a\nb");
    assert_eq!(parse(b"plain\n").unwrap(), "plain");
    assert!(unmetafy(b"\x83").is_err());
}

fn commands(bytes: &[u8], base: usize) -> Vec<(usize, String)> {
    let (records, _) = frame(bytes);
    let out = drain(
        &ZshParser { host: "h".into() },
        DrainInput {
            erased: None,
            manifest: &serde_json::Value::Null,
            source: SPEC.name,
            class: Class::Open,
            material: MaterialId(uuid::Uuid::nil()),
            bytes: bytes.into(),
            records: &records,
            carry: serde_json::Value::Null,
            reading_order: &|a| ReadingOrder(a as i64),
            limits: Limits::default(),
        },
    )
    .unwrap();
    out.iter()
        .flat_map(|c| &c.outcomes)
        .map(|o| match o {
            RecordOutcome::Events { location, events } => (
                base + location.unwrap().anchor as usize,
                events[0].payload["command"].as_str().unwrap().to_owned(),
            ),
            RecordOutcome::Failure { location, reason } => {
                (base + location.anchor as usize, format!("FAIL {reason}"))
            }
        })
        .collect()
}

proptest! {
    /// Source law: reading the file in pieces gives the same records, at the
    /// same file offsets, as reading it at once.
    #[test]
    fn pieces_equal_whole(cmds in prop::collection::vec("[a-z \\\\]{0,6}", 1..12), cut in 0usize..400) {
        let mut file = Vec::new();
        for (i, c) in cmds.iter().enumerate() {
            file.extend_from_slice(format!(": {}:0;{}\n", 100 + i, c.replace('\\', "\\\\")).as_bytes());
        }
        let whole = commands(&file, 0);
        let cut = cut.min(file.len());
        let (_, consumed) = frame(&file[..cut]);
        let mut pieces = commands(&file[..consumed], 0);
        pieces.extend(commands(&file[consumed..], consumed));
        prop_assert_eq!(whole, pieces);
    }

    /// Source law: an anchor reads back exactly its record.
    #[test]
    fn anchors_read_back_records(cmds in prop::collection::vec("[a-z]{1,5}", 1..8)) {
        let mut file = Vec::new();
        for (i, c) in cmds.iter().enumerate() {
            file.extend_from_slice(format!(": {}:0;{}\n", i, c).as_bytes());
        }
        for (anchor, command) in commands(&file, 0) {
            let (r, _) = frame(&file[anchor..]);
            prop_assert_eq!(parse(&file[anchor..anchor + r[0].1]).unwrap(), command);
        }
    }
}

#[test]
fn plain_commands_preserve_text_and_do_not_invent_times() {
    let parser = ZshParser { host: "h".into() };
    let cases: &[(&[u8], &str)] = &[
        (b"  echo\t spaced  \n", "  echo\t spaced  "),
        (b"echo slash\\ \n", "echo slash\\"),
        (b"printf a\\\nb\n", "printf a\nb"),
        (b"echo \xc4\x83\x85\n", "echo ĥ"),
        (b"\\: echo colon\n", ": echo colon"),
        (b"\\: 10:0;literal\n", ": 10:0;literal"),
    ];
    for (raw, expected) in cases {
        let (events, state) = parser
            .parse(raw, &Seen::default(), &crate::RecordContext::none())
            .unwrap();
        assert_eq!(events[0].command, *expected);
        assert_eq!(events[0].started_at, None);
        assert_eq!(events[0].header_elapsed, None);
        assert_eq!(state, Seen::default());
    }
    assert_eq!(parse(b": 10:0;\\: literal\n").unwrap(), "\\: literal");
    assert!(
        parser
            .parse(
                &b"\n".as_slice(),
                &Seen::default(),
                &crate::RecordContext::none()
            )
            .unwrap()
            .0
            .is_empty()
    );
    for raw in [
        b": bad:0;cmd\n".as_slice(),
        b": 1:bad;cmd\n",
        b": 1:0bad\n",
        b"\xff\n",
        b"\x83",
        b"partial",
    ] {
        assert!(
            parser
                .parse(&raw, &Seen::default(), &crate::RecordContext::none())
                .is_err(),
            "{raw:?}"
        );
    }
}

// Normalize random event IDs away, retaining the payload, byte coordinates,
// source clock, natural key and record bytes hash.
fn observations(
    bytes: &[u8],
    base: usize,
    carry: serde_json::Value,
) -> (Vec<serde_json::Value>, serde_json::Value) {
    observations_with_transitions(bytes, base, carry, &serde_json::Value::Null)
}

fn observations_with_transitions(
    bytes: &[u8],
    base: usize,
    carry: serde_json::Value,
    transitions: &serde_json::Value,
) -> (Vec<serde_json::Value>, serde_json::Value) {
    let (records, _) = frame(bytes);
    let erased = transitions.as_object().map(|entries| {
        crate::drain::ErasedTransitions::from_archive(
            entries.iter().filter_map(|(start, entry)| {
                let start: usize = start.parse().unwrap();
                let end = entry["end"].as_u64().unwrap() as usize;
                (end <= bytes.len()).then(|| (start, end, Some(entry["token"].clone())))
            }),
            bytes.len() as u64,
            false,
        )
        .unwrap()
    });
    let chunks = drain(
        &ZshParser { host: "h".into() },
        DrainInput {
            erased: erased.as_ref(),
            manifest: &serde_json::Value::Null,
            source: SPEC.name,
            class: Class::Open,
            material: MaterialId(uuid::Uuid::nil()),
            bytes: bytes.into(),
            records: &records,
            carry: carry.clone(),
            reading_order: &|a| ReadingOrder((base + a) as i64),
            limits: Limits::default(),
        },
    )
    .unwrap();
    assert_eq!(chunks.last().unwrap().end, frame(bytes).1);
    let mut out = Vec::new();
    for chunk in &chunks {
        for outcome in &chunk.outcomes {
            match outcome {
                RecordOutcome::Events { location, events } => {
                    let anchor = location.unwrap().anchor as usize;
                    let end = records.iter().find(|r| r.0 == anchor).unwrap().1;
                    for event in events {
                        assert_eq!(
                            event.record_hash,
                            *blake3::hash(&bytes[anchor..end]).as_bytes()
                        );
                        out.push(serde_json::json!([
                            base + anchor,
                            event.payload,
                            event.ts_orig.at,
                            event.ts_orig.quality,
                            event.equivalence_key,
                            event.reading_order.0
                        ]));
                    }
                    if events.is_empty() {
                        out.push(serde_json::json!([base + anchor, "empty"]));
                    }
                }
                RecordOutcome::Failure { location, reason } => {
                    out.push(serde_json::json!([base + location.anchor as usize, reason]))
                }
            }
        }
    }
    let carry = chunks.last().map(|c| c.carry.clone()).unwrap_or(carry);
    (out, carry)
}

#[test]
fn mixed_history_append_cuts_keep_carry_coordinates_and_failures() {
    let file = b"pwd\n: 10:0;pwd\npwd\n\n: invalid\n: 10:2;pwd\nprintf a\\\nb\n\\: literal\npwd\n: 10:0;pwd\n";
    let (whole, state) = observations(file, 0, serde_json::Value::Null);
    // The shared drain accounts blank/excised records without emitting an outcome.
    assert_eq!(whole.len(), 9);
    assert_eq!(whole[0][3], "unknown");
    assert_eq!(whole[1][1]["n"], 0);
    assert_eq!(whole[4][1]["n"], 1);
    assert_eq!(whole[8][1]["n"], 2);
    for cut in 0..=file.len() {
        let (_, consumed) = frame(&file[..cut]);
        let (mut parts, carry) = observations(&file[..consumed], 0, serde_json::Value::Null);
        let (rest, final_carry) = observations(&file[consumed..], consumed, carry);
        parts.extend(rest);
        assert_eq!(parts, whole, "cut {cut}");
        assert_eq!(final_carry, state, "carry cut {cut}");
    }
}

#[test]
fn appended_plain_records_keep_distinct_material_locations_on_replay() {
    use crate::drivers::append_file::AppendState;
    use std::io::Write;
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("history");
    std::fs::write(&path, b"pwd\n: 10:0;pwd\nprintf a\\\n").unwrap();
    let driver = driver(path.clone(), "h".into());
    let mut state = AppendState::default();
    let first = driver
        .poll(&serde_json::Value::Null, &mut state)
        .unwrap()
        .material
        .unwrap();
    assert_eq!(first.bytes, b"pwd\n: 10:0;pwd\n");
    let (_, carry) = observations(&first.bytes, 0, serde_json::Value::Null);
    let cursor = (first.cursor_at)(first.bytes.len()).unwrap();
    let mut restarted = AppendState::rebuild([(first.id, first.bytes.as_slice())], SPEC.framer);
    std::fs::OpenOptions::new()
        .append(true)
        .open(&path)
        .unwrap()
        .write_all(b"b\npwd\n: 10:0;pwd\n")
        .unwrap();
    let second = driver
        .poll(&cursor, &mut restarted)
        .unwrap()
        .material
        .unwrap();
    assert_eq!(second.previous_part, Some(first.id));
    assert_ne!(second.id, first.id);
    assert_eq!(second.bytes, b"printf a\\\nb\npwd\n: 10:0;pwd\n");
    assert_eq!(second.records, frame(&second.bytes).0);
    let (actual, _) = observations(&second.bytes, first.bytes.len(), carry.clone());
    assert_eq!(actual[0][1]["command"], "printf a\nb");
    assert_eq!(actual[1][3], "unknown");
    assert_eq!(actual[2][1]["n"], 1);
    assert_eq!(
        actual,
        observations(&second.bytes, first.bytes.len(), carry).0
    );
}

#[test]
fn old_plaintext_carry_decodes_to_command_keys_without_renumbering() {
    let parser = ZshParser { host: "h".into() };
    let old = serde_json::json!({"latest": 50, "counts": [
        [50, "private-marker", 2], [40, "unrelated", 4]
    ]});
    let erased = crate::Source::erase_record(
        &source("h".into()),
        b": 50:1;private-marker\n",
        &old,
        &crate::RecordContext::none(),
    )
    .unwrap();
    let serialized = serde_json::to_string(&erased.carry).unwrap();
    assert!(!serialized.contains("private-marker"));
    assert!(!serialized.contains("unrelated"));
    let seen: Seen = serde_json::from_value(erased.carry).unwrap();
    let (out, seen) = parser
        .parse(
            &b": 50:9;private-marker\n".as_slice(),
            &seen,
            &crate::RecordContext::none(),
        )
        .unwrap();
    assert_eq!(out[0].n, 2);
    let (out, _) = parser
        .parse(
            &b": 40:0;unrelated\n".as_slice(),
            &seen,
            &crate::RecordContext::none(),
        )
        .unwrap();
    assert_eq!(out[0].n, 4);
}

fn blank(bytes: &mut [u8]) {
    for byte in bytes {
        if *byte != b'\n' {
            *byte = b' ';
        }
    }
}

#[test]
fn erasure_replay_and_every_restart_keep_retained_occurrences_and_keys() {
    // Elapsed is a recorded payload field, not part of the duplicate counter.
    // The window prunes the old command, then a late duplicate starts at zero.
    let file = b": 10:1;duplicate\n: 10:8;duplicate\n: 11:0;unrelated\n\
: 12:0;printf a\\\nb\n: 13:0;echo \xc4\x83\x85\nplain-private-marker\n\
: 100000:0;new-day\n: 10:5;duplicate\n: 100000:2;new-day\n";
    let (whole, original_carry) = observations(file, 0, serde_json::Value::Null);
    assert_eq!(whole[0][1]["n"], 0);
    assert_eq!(whole[1][1]["n"], 1);
    assert_eq!(whole[7][1]["n"], 0);
    assert_eq!(whole[8][1]["n"], 1);
    for (start, end) in frame(file).0 {
        let erased = crate::Source::erase_record(
            &source("h".into()),
            &file[start..end],
            &original_carry,
            &crate::RecordContext::none(),
        )
        .unwrap();
        assert_eq!(erased.carry, original_carry);
        let serialized = serde_json::to_string(&erased.carry).unwrap();
        assert!(!serialized.contains("duplicate"));
        assert!(!serialized.contains("plain-private-marker"));
        if let Some(token) = &erased.replay {
            let text = token.to_string();
            assert!(!text.contains("duplicate"));
            assert!(!text.contains("printf"));
            assert!(!text.contains("echo"));
        }
        let mut rewritten = file.to_vec();
        blank(&mut rewritten[start..end]);
        let mut transitions = serde_json::Map::new();
        if let Some(token) = erased.replay {
            transitions.insert(
                start.to_string(),
                serde_json::json!({"end":end,"token":token}),
            );
        }
        let transitions = serde_json::Value::Object(transitions);
        let expected: Vec<_> = whole
            .iter()
            .filter(|row| row[0] != start)
            .cloned()
            .collect();
        let (actual, carry) =
            observations_with_transitions(&rewritten, 0, serde_json::Value::Null, &transitions);
        assert_eq!(actual, expected, "erased anchor {start}");
        assert_eq!(carry, original_carry);
        // Restart at each accounted record boundary, using only serialized state.
        for (_, cut) in frame(file).0 {
            let (mut prefix, carry) = observations_with_transitions(
                &rewritten[..cut],
                0,
                serde_json::Value::Null,
                &transitions,
            );
            let tail_transitions = transitions_for_suffix(&transitions, cut);
            let (tail, carry) =
                observations_with_transitions(&rewritten[cut..], cut, carry, &tail_transitions);
            prefix.extend(tail);
            assert_eq!(prefix, expected, "erased {start}, restart {cut}");
            assert_eq!(carry, original_carry);
        }
    }
}

fn transitions_for_suffix(transitions: &serde_json::Value, cut: usize) -> serde_json::Value {
    transitions.as_object()
        .unwrap()
        .iter()
        .filter_map(|(anchor, token)| {
            let anchor: usize = anchor.parse().unwrap();
            anchor
                .checked_sub(cut)
                .map(|anchor| (anchor.to_string(), serde_json::json!({"end":token["end"].as_u64().unwrap() as usize-cut,"token":token["token"]})))
        })
        .collect::<serde_json::Map<_, _>>()
        .into()
}

#[test]
fn erased_transition_advances_the_same_window_as_parsing_and_refuses_unknown_shapes() {
    let parser = ZshParser { host: "h".into() };
    let context = crate::RecordContext::none();
    let (_, prior) = parser
        .parse(&b": 10:0;old\n".as_slice(), &Seen::default(), &context)
        .unwrap();
    let raw = b": 100000:0;selected-only-private-marker\n";
    let (_, after) = parser.parse(&raw.as_slice(), &prior, &context).unwrap();
    let erased = parser.erase_record(raw, &after, &context).unwrap();
    let token = erased.replay.unwrap();
    assert!(
        !serde_json::to_string(&erased.carry)
            .unwrap()
            .contains("selected-only-private-marker")
    );
    assert!(!token.to_string().contains("selected-only-private-marker"));
    let replayed = parser.skip_erased(&prior, &token, &context).unwrap();
    assert_eq!(replayed, after);
    assert_eq!(
        replayed.counts.len(),
        1,
        "the erased record also advances pruning"
    );
    let mut future = token.clone();
    future["version"] = 2.into();
    for bad in [
        future,
        serde_json::Value::Null,
        serde_json::json!({"version": 1}),
        serde_json::json!({"version":1,"start":10,"command_key":"plaintext"}),
    ] {
        assert!(parser.skip_erased(&prior, &bad, &context).is_err());
    }
    assert!(
        parser
            .erase_record(b": invalid\n", &after, &context)
            .is_err()
    );
    for raw in [b"plain\n".as_slice(), b"\n"] {
        let erased = parser.erase_record(raw, &after, &context).unwrap();
        assert_eq!(erased.carry, after);
        assert!(erased.replay.is_none());
    }
}

#[test]
fn erasing_an_entire_prefix_keeps_later_duplicate_identity() {
    let parser = ZshParser { host: "h".into() };
    let context = crate::RecordContext::none();
    let records: &[&[u8]] = &[
        b": 10:0;repeat\n",
        b": 10:2;repeat\n",
        b"untimed-private\n",
        b": 11:0;other\n",
        b": 10:7;repeat\n",
    ];
    let mut original = Seen::default();
    let mut tokens = Vec::new();
    let mut expected_last = None;
    for raw in records {
        let (events, after) = parser.parse(raw, &original, &context).unwrap();
        expected_last = events.into_iter().next();
        tokens.push(parser.erase_record(raw, &after, &context).unwrap().replay);
        original = after;
    }
    let mut replayed = Seen::default();
    for token in &tokens[..4] {
        if let Some(token) = token {
            replayed = parser.skip_erased(&replayed, token, &context).unwrap();
        }
        replayed = serde_json::from_value(serde_json::to_value(replayed).unwrap()).unwrap();
    }
    let (last, actual) = parser.parse(&records[4], &replayed, &context).unwrap();
    assert_eq!(last[0].n, 2);
    assert_eq!(
        serde_json::to_value(&last[0]).unwrap(),
        serde_json::to_value(expected_last.unwrap()).unwrap()
    );
    assert_eq!(actual, original);
    let wholly_erased = parser
        .skip_erased(&replayed, tokens[4].as_ref().unwrap(), &context)
        .unwrap();
    assert_eq!(wholly_erased, original);
}

use super::*;
use sinex_model::event_types::communication::MailMessage;
use sinex_model::event_types::duplicate::{Decision, Duplicate};
use sinex_model::event_types::fused::{CommandFused, MailFused};
use sinex_model::event_types::{CommandExecuted, CommandImported, CommandReported};
use sinex_model::{Class, MaterialId, Origin, ReadingOrder, RecordLocation};

pub(crate) const H: f64 = 1_700_002_800.0; // an hour boundary (1700002800 / 3600 = 472223)

pub(crate) fn at(secs: f64) -> Timestamp {
    Timestamp::from_unix_ns((secs * 1e9) as i128).unwrap()
}

pub(crate) fn captured<P: EventPayload>(payload: &P, source: &str, class: Class) -> Event {
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
    atuin_row(&format!("{secs}"), command, secs, exit)
}

fn atuin_row(id: &str, command: &str, secs: f64, exit: i64) -> Event {
    captured(
        &CommandExecuted {
            native_id: id.into(),
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
        "terminal.atuin-history",
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
        "terminal.zsh-history",
        Class::Guarded,
    )
}

/// The shell integration's report of one execution.
pub(crate) fn reported(
    execution: &str,
    atuin_id: Option<&str>,
    command: &str,
    host: &str,
    secs: f64,
) -> Event {
    captured(
        &CommandReported {
            execution: execution.into(),
            shell: "zsh".into(),
            host: host.into(),
            command: command.into(),
            cwd: Some("/home/u/reported".into()),
            started_at: at(secs),
            duration_ns: Some(2_000_000),
            exit_code: Some(0),
            session: Some("4242".into()),
            atuin_id: atuin_id.map(str::to_owned),
            git_identity: None,
            extra: Default::default(),
        },
        "session.integrations",
        Class::Guarded,
    )
}

/// A duplicate decision by `author` on the pair, the first revision of
/// record `rev`.
fn decided(
    family: &str,
    a: &Event,
    b: &Event,
    decision: Option<Decision>,
    author: &str,
    rev: &str,
) -> Duplicate {
    let (left, right) = pair_of(&record_identity(a), &record_identity(b));
    Duplicate {
        root: rev.into(),
        rev: rev.into(),
        author: author.into(),
        via: "api".into(),
        at: 0,
        revises: vec![],
        withdraws: vec![],
        pair: vec![left, right],
        decision,
        pair_at: Some(a.ts_orig.at.unwrap().min(b.ts_orig.at.unwrap())),
        family: Some(family.into()),
        family_key: None,
    }
}

fn stored(decision: &Duplicate) -> Event {
    captured(decision, "duplicate.request", Class::Guarded)
}

/// Run a family as the runner would: every key the inputs touch, each
/// reading what its query admits. Outputs sorted by type, start, then record.
pub(crate) fn fuse_events(family: &'static Family, inputs: &[Event]) -> Vec<Event> {
    let fusion = Fusion(family);
    let env = Env::at(
        Timestamp::from_unix_secs(2_000_000_000).unwrap(),
        sinex_model::time_settings::TimeSettings::new("Europe/Warsaw", 4, 0)
            .expect("fixture day setting"),
    );
    fn view(e: &Event) -> KeyView<'_> {
        KeyView {
            event_type: &e.event_type,
            ts_orig: e.ts_orig.at,
            payload: &e.payload,
        }
    }
    let mut keys: Vec<Value> = Vec::new();
    let add = |keys: &mut Vec<Value>, e: &Event| {
        for k in fusion.keys_for(&view(e), &env) {
            if !keys.contains(&k) {
                keys.push(k);
            }
        }
    };
    for e in inputs {
        add(&mut keys, e);
        // Admission also dirties the keys of the records a nearby query
        // selects.
        if let Some(nearby) = fusion.nearby_inputs(&view(e), &env) {
            for other in inputs.iter().filter(|o| nearby.admits(o)) {
                add(&mut keys, other);
            }
        }
    }
    let mut out = Vec::new();
    for k in keys {
        let q = fusion.inputs(&k, &env).unwrap();
        let mut window: Vec<Event> = inputs.iter().filter(|e| q.admits(e)).cloned().collect();
        // Neighbours by field value: the records outside the window holding
        // a value a window record of those kinds holds, within the value's
        // lifetime when the read has one, as the store reads them.
        for n in q.before.iter().chain(&q.after) {
            let Some(per) = n.per else { continue };
            let admits = |e: &Event| n.kinds.iter().any(|k| k.admits(&e.event_type, &e.payload));
            let values: Vec<(Value, (i128, i128))> = window
                .iter()
                .filter(|e| admits(e))
                .filter_map(|e| {
                    let value = e.payload.get(per).filter(|v| !v.is_null())?;
                    let bounds = match (n.lifetime, value.as_str(), e.ts_orig.at) {
                        (Some(lifetime), Some(token), Some(t)) => {
                            lifetime.bounds(token, t.unix_ns(), &e.payload)
                        }
                        _ => (i128::MIN, i128::MAX),
                    };
                    Some((value.clone(), bounds))
                })
                .collect();
            let extra: Vec<Event> = inputs
                .iter()
                .filter(|e| admits(e) && !window.iter().any(|w| w.id == e.id))
                .filter(|e| {
                    e.payload.get(per).is_some_and(|v| {
                        values.iter().any(|(value, (lo, hi))| {
                            value == v
                                && e.ts_orig
                                    .at
                                    .is_some_and(|t| (*lo..=*hi).contains(&t.unix_ns()))
                        })
                    })
                })
                .cloned()
                .collect();
            window.extend(extra);
        }
        window.sort_by_key(|e| e.ts_orig.at);
        let computed = fusion.compute(&k, &window, &env);
        assert!(computed.error.is_none(), "{:?}", computed.error);
        out.extend(computed.outputs);
    }
    out.sort_by_key(|e| {
        (
            e.event_type.clone(),
            e.ts_orig.at,
            e.payload["record"].as_str().unwrap_or("").to_owned(),
            e.payload["left"].as_str().unwrap_or("").to_owned(),
            e.payload["right"].as_str().unwrap_or("").to_owned(),
        )
    });
    out
}

/// The fused records only.
fn fuse(family: &'static Family, inputs: &[Event]) -> Vec<Value> {
    fuse_events(family, inputs)
        .into_iter()
        .filter(|e| e.event_type == family.output)
        .map(|e| e.payload)
        .collect()
}

/// The ambiguous-pair rows only.
fn duplicates(family: &'static Family, inputs: &[Event]) -> Vec<Value> {
    fuse_events(family, inputs)
        .into_iter()
        .filter(|e| e.event_type == "fusion.considered_pair")
        .map(|e| e.payload)
        .collect()
}

/// What a consumer reads: each occurrence once, from the fused records and
/// the inputs together. Sorted by start, then record.
fn occurrences(family: &'static Family, inputs: &[Event]) -> Vec<Value> {
    let mut all = fuse_events(family, inputs);
    all.extend(inputs.iter().cloned());
    let mut out: Vec<Value> = family
        .occurrences(&all)
        .into_iter()
        .map(|o| o.payload.into_owned())
        .collect();
    out.sort_by_key(|p| {
        (
            p[family.start_field].as_str().unwrap_or("").to_owned(),
            p["record"].as_str().unwrap_or("").to_owned(),
        )
    });
    out
}

/// The sources that observed an occurrence: a fused record's members, or
/// an unmatched record's own source.
fn observed_by<'a>(v: &'a Value, unmatched: &'a str) -> Vec<&'a str> {
    match v["members"].as_array() {
        Some(members) => members
            .iter()
            .map(|m| m["source"].as_str().unwrap())
            .collect(),
        None => vec![unmatched],
    }
}

fn sources(v: &Value) -> Vec<&str> {
    observed_by(v, "")
}

// Commands.

#[devkit::test]
fn close_commands_fuse_and_atuin_supplies_fields() {
    let out = fuse(
        commands(),
        &[atuin("ls -la", H + 10.0, 2), zsh("  ls -la ", H + 10.5)],
    );
    assert_eq!(out.len(), 1);
    let f = &out[0];
    assert_eq!(
        sources(f),
        ["terminal.atuin-history", "terminal.zsh-history"]
    );
    assert_eq!(f["exit_code"], 2);
    assert_eq!(f["cwd"], "/home/u/proj");
    assert_eq!(f["host"], "box");
    assert_eq!(f["command"], "ls -la");
    assert_eq!(f["basis"], "rule");
    assert_eq!(f["ambiguous"], 1);
    assert_eq!(
        f["members"][1]["joined"],
        json!({"by": "window", "distance_ns": 500_000_000})
    );
    assert_eq!(f["members"][0]["joined"], json!({"by": "lead"}));
    assert_eq!(f["rule_version"], commands().version);
    let typed: CommandFused = serde_json::from_value(f.clone()).unwrap();
    assert_eq!(typed.members.len(), 2);
}

#[devkit::test]
fn closest_pairs_win_and_extras_stay_alone() {
    // Two zsh readings of the same command near one Atuin reading: the
    // closer one fuses, the other stays its own command, and the choice is
    // recorded.
    let inputs = [
        atuin("make", H + 100.0, 0),
        zsh("make", H + 100.4),
        zsh("make", H + 101.5),
    ];
    let out = fuse(commands(), &inputs);
    assert_eq!(out.len(), 1);
    assert_eq!(sources(&out[0]).len(), 2);
    assert_eq!(out[0]["ambiguous"], 2);
    assert_eq!(out[0]["competitors"], json!([record_identity(&inputs[2])]));
    let read = occurrences(commands(), &inputs);
    assert_eq!(read.len(), 2);
    assert_eq!(read.iter().filter(|f| f["members"].is_array()).count(), 1);
}

#[devkit::test]
fn every_choice_among_candidates_records_ambiguity_and_duplicate_rows() {
    let inputs = [
        atuin("make", H + 100.0, 0),
        zsh("make", H + 100.4),
        zsh("make", H + 101.5),
    ];
    let rows = duplicates(commands(), &inputs);
    assert_eq!(rows.len(), 2, "{rows:?}");
    let atuin_record = record_identity(&inputs[0]);
    for row in &rows {
        assert_eq!(row["family"], "command");
        assert_eq!(row["strategy"], "window");
        assert_eq!(row["candidates"], 2);
        assert_eq!(row["state"], "undecided");
        assert!(row["left"] == atuin_record || row["right"] == atuin_record);
        assert!(row["left"].as_str() < row["right"].as_str());
    }
    let chosen: Vec<_> = rows.iter().filter(|r| r["chosen"] == true).collect();
    assert_eq!(chosen.len(), 1);
    assert_eq!(chosen[0]["distance_ns"], 400_000_000);
    // A choice without competitors writes no row.
    assert!(duplicates(commands(), &[atuin("ls", H + 1.0, 0), zsh("ls", H + 1.2)]).is_empty());
}

#[devkit::test]
fn fallback_needs_a_unique_partner() {
    // 30 s apart: only the fallback pass can match, and it does when unique.
    let one = fuse(
        commands(),
        &[atuin("git push", H + 200.0, 0), zsh("git push", H + 230.0)],
    );
    assert_eq!(one.len(), 1);
    assert_eq!(one[0]["members"][1]["joined"]["by"], "fallback");
    // Two zsh partners for one Atuin record: ambiguous, nothing fuses, and
    // both pairs are shown for a decision.
    let inputs = [
        atuin("git push", H + 300.0, 0),
        zsh("git push", H + 320.0),
        zsh("git push", H + 340.0),
    ];
    assert!(fuse(commands(), &inputs).is_empty());
    let read = occurrences(commands(), &inputs);
    assert_eq!(read.len(), 3);
    assert!(read.iter().all(|f| f["members"].is_null()));
    let rows = duplicates(commands(), &inputs);
    assert_eq!(rows.len(), 2);
    assert!(
        rows.iter()
            .all(|r| r["strategy"] == "fallback" && r["chosen"] == false)
    );
}

#[devkit::test]
fn a_fallback_choice_does_not_depend_on_where_the_hour_falls() {
    // One zsh line with two Atuin rows inside its fallback window, the first
    // in the hour before: ambiguous wherever the boundary is.
    for shift in [0.0, -40.0, -80.0] {
        let inputs = [
            atuin_row("x", "git push", H - 30.0 + shift + 40.0, 0),
            zsh("git push", H + 30.0 + shift),
            atuin_row("y", "git push", H + 80.0 + shift, 0),
        ];
        assert!(fuse(commands(), &inputs).is_empty(), "shift {shift}");
        assert_eq!(duplicates(commands(), &inputs).len(), 2, "shift {shift}");
    }
}

#[devkit::test]
fn different_commands_or_far_apart_never_fuse() {
    let differ = [atuin("ls", H + 10.0, 0), zsh("ls -l", H + 10.0)];
    assert!(fuse(commands(), &differ).is_empty());
    assert_eq!(occurrences(commands(), &differ).len(), 2);
    let far = [atuin("ls", H + 10.0, 0), zsh("ls", H + 100.0)];
    assert!(fuse(commands(), &far).is_empty());
    assert_eq!(occurrences(commands(), &far).len(), 2);
}

#[devkit::test]
fn whitespace_inside_a_command_is_significant() {
    let inputs = [
        atuin("echo 'a  b'", H + 10.0, 0),
        zsh("echo 'a b'", H + 10.1),
    ];
    assert!(fuse(commands(), &inputs).is_empty());
}

#[devkit::test]
fn a_pair_across_an_hour_edge_is_emitted_once() {
    let out = fuse(
        commands(),
        &[atuin("cargo test", H - 0.5, 0), zsh("cargo test", H + 0.5)],
    );
    assert_eq!(out.len(), 1, "{out:?}");
    assert_eq!(sources(&out[0]).len(), 2);
}

#[devkit::test]
fn privacy_class_is_the_strictest_parent() {
    let out = fuse_events(commands(), &[atuin("ls", H + 10.0, 0), zsh("ls", H + 10.2)]);
    assert_eq!(out.len(), 1);
    assert_eq!(out[0].class, Class::Guarded);
}

#[devkit::test]
fn removed_records_do_not_fuse() {
    let mut a = atuin("rm -rf x", H + 10.0, 0);
    a.record_state = RecordState::Removed;
    let inputs = [a, zsh("rm -rf x", H + 10.1)];
    assert!(fuse(commands(), &inputs).is_empty());
    let read = occurrences(commands(), &inputs);
    assert_eq!(read.len(), 1);
    assert_eq!(read[0]["record"], record_identity(&inputs[1]));
}

#[devkit::test]
fn the_same_command_twice_in_one_second_is_two_runs() {
    // zsh numbers equal runs in one second, so each is its own record.
    let a = zsh("ls", H + 5.0);
    let b = captured(
        &CommandImported {
            command: "ls".into(),
            hostname: "box".into(),
            started_at: Some(at(H + 5.0)),
            header_elapsed: Some(0),
            paths: vec![],
            n: 1,
        },
        "terminal.zsh-history",
        Class::Guarded,
    );
    let inputs = [a, b];
    assert!(fuse(commands(), &inputs).is_empty());
    let read = occurrences(commands(), &inputs);
    assert_eq!(read.len(), 2);
    assert_ne!(read[0]["record"], read[1]["record"]);
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
    assert_eq!(fuse(commands(), &inputs), fuse(commands(), &reversed));
    assert_eq!(
        occurrences(commands(), &inputs),
        occurrences(commands(), &reversed)
    );
}

#[devkit::test]
fn equal_times_group_alike_whatever_the_reading_ids_and_commit_order() {
    // Two equal candidates at the same distance: which one wins depends on
    // the records, never on the reading IDs or the order read.
    let inputs = vec![
        atuin_row("first", "ls", H + 10.0, 0),
        zsh("ls", H + 10.5),
        atuin_row("second", "ls", H + 11.0, 0),
    ];
    let expected = fuse(commands(), &inputs);
    assert_eq!(expected.len(), 1);
    for permutation in [[2, 1, 0], [1, 0, 2], [0, 2, 1]] {
        let mut permuted: Vec<Event> = permutation.iter().map(|&i| inputs[i].clone()).collect();
        for event in &mut permuted {
            event.id = EventId::new();
        }
        assert_eq!(fuse(commands(), &permuted), expected);
    }
}

#[devkit::test]
fn no_fused_record_has_fewer_than_two_members() {
    let commands = fuse_events(
        commands(),
        &[
            atuin("ls", H + 10.0, 0),
            zsh("ls", H + 10.2),
            atuin("only atuin", H + 20.0, 0),
            zsh("only zsh", H + 30.0),
            reported(
                "01900000-0000-7000-8000-00000000000a",
                None,
                "only reported",
                "box",
                H + 40.0,
            ),
        ],
    );
    let mut alone = message("communication.imap", "<alone@x>", H + 5.0);
    alone.payload["account"] = Value::Null;
    let mail = fuse_events(
        mail(),
        &[
            message("communication.imap", "<1@x>", H + 5.0),
            message("communication.mbox", "<1@x>", H + 5.0),
            message("communication.imap", "<2@x>", H + 6.0),
            alone,
        ],
    );
    let fused: Vec<&Event> = commands
        .iter()
        .chain(&mail)
        .filter(|e| e.event_type.ends_with(".fused"))
        .collect();
    assert_eq!(fused.len(), 2);
    for fused in fused {
        let Origin::Derived { parents } = &fused.origin else {
            panic!("a fused record is derived");
        };
        assert!(parents.len() >= 2, "{fused:?}");
        assert!(fused.payload["members"].as_array().unwrap().len() >= 2);
    }
}

#[devkit::test]
fn a_consumer_reads_each_command_once_whichever_shells_saw_it() {
    let read = occurrences(
        commands(),
        &[
            atuin("both", H + 10.0, 0),
            zsh("both", H + 10.3),
            atuin("atuin only", H + 20.0, 1),
            zsh("zsh only", H + 30.0),
        ],
    );
    let names: Vec<_> = read
        .iter()
        .map(|c| c["command"].as_str().unwrap())
        .collect();
    assert_eq!(names, ["both", "atuin only", "zsh only"]);
    assert_eq!(
        sources(&read[0]),
        ["terminal.atuin-history", "terminal.zsh-history"]
    );
    assert!(read[1]["members"].is_null());
    assert_eq!(read[1]["exit_code"], 1);
    assert_eq!(read[2]["host"], "box");
    for occurrence in &read {
        let command: Command = serde_json::from_value(occurrence.clone()).unwrap();
        assert!(!command.record.is_empty());
    }
}

#[devkit::test]
fn bash_and_fish_history_are_members_through_the_history_kind() {
    let mut bash = zsh("ls", H + 10.2);
    bash.source = "terminal.bash-history".into();
    let out = fuse(commands(), &[atuin("ls", H + 10.0, 0), bash]);
    assert_eq!(out.len(), 1);
    // Two shells' history files never join each other: one member.
    let mut fish = zsh("ls", H + 20.1);
    fish.source = "terminal.fish-history".into();
    assert!(fuse(commands(), &[zsh("ls", H + 20.0), fish]).is_empty());
}

#[devkit::test]
fn absorb_joins_a_repeat_inside_a_group_led_by_a_preferred_member() {
    let mut text = serde_json::from_str::<Value>(include_str!("families.json")).unwrap();
    let command = &mut text["families"][0];
    command["absorb_ms"] = json!(60_000);
    let families = declaration::parse(&text.to_string()).unwrap();
    let family: &'static Family = Box::leak(Box::new(families[0].clone()));
    let inputs = [
        atuin("vim", H + 10.0, 0),
        zsh("vim", H + 10.5),
        zsh("vim", H + 11.5),
    ];
    let fused_only = |inputs: &[Event]| -> Vec<Event> {
        fuse_events(family, inputs)
            .into_iter()
            .filter(|e| e.event_type == family.output)
            .collect()
    };
    let out = fused_only(&inputs);
    assert_eq!(out.len(), 1, "{out:?}");
    assert!(matches!(&out[0].origin, Origin::Derived { parents } if parents.len() == 3));
    assert_eq!(out[0].payload["members"][2]["joined"]["by"], "absorb");
    // Without absorb, the repeat is a command of its own.
    assert_eq!(occurrences(commands(), &inputs).len(), 2);
    // A record before the group's window is not absorbed.
    let earlier = [
        zsh("vim", H + 5.0),
        atuin("vim", H + 10.0, 0),
        zsh("vim", H + 10.5),
    ];
    assert_eq!(fused_only(&earlier).len(), 1);
    assert_eq!(occurrences(family, &earlier).len(), 2);
}

// Execution tokens and identifiers.

/// kitty's record of a command's end, holding the shell's token.
fn kitty_end(execution: Option<&str>, command: &str, host: &str, secs: f64) -> Event {
    let mut extra = serde_json::Map::new();
    extra.insert(
        "terminal_identity".into(),
        json!({"host": host, "adapter": "kitty"}),
    );
    captured(
        &sinex_model::event_types::session::TerminalReport {
            received_at: at(secs),
            terminal: "kitty".into(),
            execution: execution.map(str::to_owned),
            event: "cmd_end".into(),
            window_id: Some(7),
            cwd: Some("/home/u/kitty".into()),
            cmdline: Some(command.into()),
            exit_status: Some(0),
            title: None,
            output: None,
            extra,
        },
        "session.integrations",
        Class::Guarded,
    )
}

/// The token the shell integration mints for a command started at `secs`.
fn token(secs: f64, n: u32) -> String {
    let ms = (secs * 1000.0) as u64;
    format!(
        "{:08x}-{:04x}-7{:03x}-8000-{:012x}",
        ms >> 16,
        ms & 0xffff,
        n,
        u64::from(n)
    )
}

#[devkit::test]
fn readings_sharing_an_execution_token_associate_whatever_their_time_distance() {
    // The shell's report holds the token from the command's start; kitty's
    // record of the end holds it too, however long the command ran: across
    // hours and days.
    for distance in [0.5, 59.0, 3_000.0, 9_000.0, 3.0 * 86_400.0] {
        let started = H + 10.0;
        let execution = token(started, 1);
        let mut report = reported(&execution, None, "cargo build", "box", started);
        report.payload["duration_ns"] = json!((distance * 1e9) as i64);
        let end = kitty_end(Some(&execution), "cargo build", "box", started + distance);
        let out = fuse(commands(), &[report.clone(), end.clone()]);
        assert_eq!(out.len(), 1, "distance {distance}: {out:?}");
        assert_eq!(out[0]["basis"], "identifier");
        assert_eq!(
            out[0]["members"][1]["joined"],
            json!({"by": "identifier", "namespace": "execution"})
        );
        assert_eq!(out[0]["execution"], execution);
        // Read in either order, the pair is emitted once.
        assert_eq!(fuse(commands(), &[end, report]).len(), 1);
    }
}

#[devkit::test]
fn a_token_read_stops_at_the_tokens_lifetime() {
    // The command ran 5 s. A record holding its token three days later is
    // outside the token's lifetime, so neither side's read reaches it.
    let started = H + 10.0;
    let execution = token(started, 7);
    let mut report = reported(&execution, None, "make", "box", started);
    report.payload["duration_ns"] = json!(5_000_000_000_i64);
    let late = kitty_end(Some(&execution), "make", "box", started + 3.0 * 86_400.0);
    assert!(fuse(commands(), &[report.clone(), late]).is_empty());
    // At the command's end, within the window's slack, it joins.
    let end = kitty_end(Some(&execution), "make", "box", started + 6.0);
    assert_eq!(fuse(commands(), &[report, end]).len(), 1);
    // The bounds the read takes: from the mint time to the end, widened by
    // the family's window.
    let lifetime = sinex_model::TokenLifetime {
        length: commands().token_length,
        slack_ns: commands().window_ns.unwrap(),
    };
    let minted = i128::from(sinex_model::uuid7_millisecond(&execution).unwrap()) * 1_000_000;
    assert_eq!(
        lifetime.bounds(
            &execution,
            minted,
            &json!({"duration_ns": 5_000_000_000_i64})
        ),
        (minted - 2_000_000_000, minted + 7_000_000_000)
    );
}

#[devkit::test]
fn a_terminal_record_without_a_token_is_not_a_command_occurrence() {
    // Without the shell integration, kitty's record cannot be matched and is
    // not counted as a command beside the history line.
    let inputs = [zsh("ls", H + 10.0), kitty_end(None, "ls", "box", H + 10.5)];
    assert!(fuse(commands(), &inputs).is_empty());
    assert_eq!(occurrences(commands(), &inputs).len(), 1);
    // Different tokens never join, however close.
    let a = reported(&token(H + 10.0, 1), None, "ls", "box", H + 10.0);
    let b = kitty_end(Some(&token(H + 10.0, 2)), "ls", "box", H + 10.1);
    assert!(fuse(commands(), &[a, b]).is_empty());
}

#[devkit::test]
fn a_command_end_never_counts_as_a_command_of_its_own() {
    // A long command: started just before an hour boundary, ended after it.
    // A consumer of the later hour reads kitty's end but not the fused
    // record, which is timed at the start; the end is not counted again.
    let started = H + 3_598.0;
    let execution = token(started, 4);
    let mut report = reported(&execution, None, "cargo build", "box", started);
    report.payload["duration_ns"] = json!(420_000_000_000_i64);
    let end = kitty_end(Some(&execution), "cargo build", "box", started + 420.0);
    let mut all = fuse_events(commands(), &[report.clone(), end.clone()]);
    assert_eq!(all.len(), 1, "{all:?}");
    assert!(
        commands()
            .occurrences(std::slice::from_ref(&end))
            .is_empty()
    );
    all.push(report.clone());
    assert_eq!(commands().occurrences(&all).len(), 1);
    // When the shell's report is lost, the Atuin row stands for the command
    // and kitty's end, which joins only by token, adds nothing.
    let row = atuin("cargo build", started, 0);
    assert_eq!(occurrences(commands(), &[row, end]).len(), 1);
}

#[devkit::test]
fn a_command_end_dirties_the_hour_its_command_started_in() {
    // Kitty's end arrives hours after the report; admitting it must
    // recompute the report's hour, which its own hour keys do not reach.
    let fusion = Fusion(commands());
    let env = Env::at(
        Timestamp::from_unix_secs(2_000_000_000).unwrap(),
        sinex_model::time_settings::TimeSettings::new("Europe/Warsaw", 4, 0)
            .expect("fixture day setting"),
    );
    fn view(e: &Event) -> KeyView<'_> {
        KeyView {
            event_type: &e.event_type,
            ts_orig: e.ts_orig.at,
            payload: &e.payload,
        }
    }
    let started = H + 10.0;
    let execution = token(started, 5);
    let report = reported(&execution, None, "make", "box", started);
    let other = reported(&token(started, 6), None, "make", "box", started);
    let end = kitty_end(Some(&execution), "make", "box", started + 3.0 * 3_600.0);
    let report_keys = fusion.keys_for(&view(&report), &env);
    assert!(
        fusion
            .keys_for(&view(&end), &env)
            .iter()
            .all(|k| !report_keys.contains(k))
    );
    let nearby = fusion.nearby_inputs(&view(&end), &env).unwrap();
    assert!(nearby.admits(&report));
    assert!(!nearby.admits(&other));
}

#[devkit::test]
fn the_atuin_row_the_integration_recorded_joins_by_identifier() {
    // Within one key's reach, the Atuin row the report names joins it by
    // identifier, beyond window and fallback.
    for distance in [0.0, 1.5, 59.0, 300.0, 1700.0] {
        let row = atuin_row("atuin-row-1", "cargo build", H + 10.0, 0);
        let report = reported(
            &token(H + 10.0 + distance, 3),
            Some("atuin-row-1"),
            "cargo build",
            "box",
            H + 10.0 + distance,
        );
        let out = fuse(commands(), &[row, report]);
        assert_eq!(out.len(), 1, "distance {distance}: {out:?}");
        assert_eq!(out[0]["basis"], "identifier");
        assert_eq!(
            out[0]["members"][1]["joined"],
            json!({"by": "identifier", "namespace": "atuin"})
        );
    }
}

#[devkit::test]
fn an_identifier_wins_over_a_closer_time_match() {
    // An Atuin row of the same text sits closer in time than the row the
    // report names; the named row joins, the closer one stays alone.
    let named = atuin_row("named", "make", H + 10.0, 0);
    let closer = atuin_row("closer", "make", H + 11.9, 0);
    let report = reported(
        "01900000-0000-7000-8000-000000000002",
        Some("named"),
        "make",
        "box",
        H + 12.0,
    );
    let out = fuse(commands(), &[named.clone(), closer, report]);
    assert_eq!(out.len(), 1);
    let members: Vec<&str> = out[0]["members"]
        .as_array()
        .unwrap()
        .iter()
        .map(|m| m["record"].as_str().unwrap())
        .collect();
    assert!(members.contains(&record_identity(&named).as_str()));
    assert_eq!(out[0]["basis"], "identifier");
}

#[devkit::test]
fn a_report_atuin_row_and_history_line_are_one_command() {
    let row = atuin_row("row-7", "git status", H + 10.0, 0);
    let report = reported(
        "01900000-0000-7000-8000-000000000003",
        Some("row-7"),
        "git status",
        "box",
        H + 10.001,
    );
    let line = zsh("git status", H + 10.0);
    let out = fuse(commands(), &[line, row, report]);
    assert_eq!(out.len(), 1, "{out:?}");
    let f = &out[0];
    assert_eq!(
        sources(f),
        [
            "session.integrations",
            "terminal.atuin-history",
            "terminal.zsh-history"
        ]
    );
    // The report leads: its fields win where it has them.
    assert_eq!(f["cwd"], "/home/u/reported");
    assert_eq!(f["session"], "4242");
    assert_eq!(f["basis"], "rule");
    assert_eq!(f["members"][1]["joined"]["by"], "identifier");
    assert_eq!(f["members"][2]["joined"]["by"], "window");
    let read = occurrences(commands(), &[zsh("git status", H + 10.0)]);
    assert_eq!(read.len(), 1);
}

#[devkit::test]
fn readings_from_different_hosts_or_accounts_never_associate() {
    // Same identifier, same command, same instant, different hosts.
    let row = atuin_row("row-9", "ls", H + 10.0, 0);
    let elsewhere = reported(
        "01900000-0000-7000-8000-000000000004",
        Some("row-9"),
        "ls",
        "other-host",
        H + 10.0,
    );
    let mut line = zsh("ls", H + 10.0);
    line.payload["hostname"] = json!("third-host");
    assert!(fuse(commands(), &[row, elsewhere, line]).is_empty());
    // Same Message-ID and Date in two accounts.
    let first = message("communication.gmail", "<1@x>", H + 5.0);
    let mut second = message("communication.imap", "<1@x>", H + 5.0);
    second.payload["account"] = json!("other@example.test");
    assert!(fuse(mail(), &[first, second]).is_empty());
}

#[devkit::test]
fn one_fused_group_never_holds_two_observations_from_one_member() {
    // Two Atuin rows both named by reports; each report joins its own row.
    let inputs = [
        atuin_row("a", "ls", H + 10.0, 0),
        atuin_row("b", "ls", H + 10.1, 0),
        reported(
            "01900000-0000-7000-8000-00000000000b",
            Some("a"),
            "ls",
            "box",
            H + 10.0,
        ),
        reported(
            "01900000-0000-7000-8000-00000000000c",
            Some("b"),
            "ls",
            "box",
            H + 10.1,
        ),
        zsh("ls", H + 10.05),
    ];
    let out = fuse(commands(), &inputs);
    assert_eq!(out.len(), 2, "{out:?}");
    for fused in &out {
        let members = fused["members"].as_array().unwrap();
        let mut kinds: Vec<(&str, &str)> = members
            .iter()
            .map(|m| {
                (
                    m["event_type"].as_str().unwrap(),
                    m["source"].as_str().unwrap(),
                )
            })
            .collect();
        let before = kinds.len();
        kinds.sort();
        kinds.dedup();
        assert_eq!(kinds.len(), before, "{fused}");
    }
}

// Duplicate decisions.

#[devkit::test]
fn the_operators_distinct_keeps_a_pair_apart_and_same_joins_one() {
    let a = atuin("deploy", H + 10.0, 0);
    let z = zsh("deploy", H + 10.4);
    let apart = stored(&decided(
        "command",
        &a,
        &z,
        Some(Decision::Distinct),
        OPERATOR,
        "d1",
    ));
    let inputs = [a.clone(), z.clone(), apart];
    assert!(fuse(commands(), &inputs).is_empty());
    let rows = duplicates(commands(), &inputs);
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0]["state"], "distinct");
    assert_eq!(rows[0]["decided_by"], OPERATOR);
    assert_eq!(rows[0]["strategy"], "decision");
    assert_eq!(rows[0]["chosen"], false);

    // Too far apart for any rule, joined by the operator.
    let far = zsh("deploy", H + 200.0);
    let together = stored(&decided(
        "command",
        &a,
        &far,
        Some(Decision::Same),
        OPERATOR,
        "d2",
    ));
    let out = fuse(commands(), &[a.clone(), far, together]);
    assert_eq!(out.len(), 1);
    assert_eq!(out[0]["basis"], "operator");
    assert_eq!(
        out[0]["members"][1]["joined"],
        json!({"by": "decision", "author": OPERATOR})
    );
}

#[devkit::test]
fn agent_decisions_are_recorded_and_govern_only_where_the_family_admits_them() {
    let a = atuin("deploy", H + 10.0, 0);
    let z = zsh("deploy", H + 10.4);
    let agent = stored(&decided(
        "command",
        &a,
        &z,
        Some(Decision::Distinct),
        "agent",
        "d1",
    ));
    let inputs = [a.clone(), z.clone(), agent.clone()];
    // The command family records agent decisions without applying them.
    assert_eq!(commands().agent_decisions, AgentDecisions::Record);
    assert_eq!(fuse(commands(), &inputs).len(), 1);
    let rows = duplicates(commands(), &inputs);
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0]["state"], "undecided");
    assert_eq!(
        rows[0]["other_decisions"],
        json!([{"author": "agent", "decision": "distinct"}])
    );
    // The operator's decision governs over an agent's.
    let operator = stored(&decided(
        "command",
        &a,
        &z,
        Some(Decision::Same),
        OPERATOR,
        "d2",
    ));
    let rows = duplicates(commands(), &[a, z, agent, operator]);
    assert_eq!(rows[0]["state"], "same");
    assert_eq!(rows[0]["decided_by"], OPERATOR);
}

#[devkit::test]
fn a_revision_supersedes_and_a_withdrawal_returns_the_pair_to_the_rules() {
    let a = atuin("deploy", H + 10.0, 0);
    let z = zsh("deploy", H + 10.4);
    let first = decided("command", &a, &z, Some(Decision::Distinct), OPERATOR, "d1");
    assert!(fuse(commands(), &[a.clone(), z.clone(), stored(&first)]).is_empty());
    // A revision to `same` supersedes the first by the descendant rule,
    // whatever order the two are read in.
    let mut second = decided("command", &a, &z, Some(Decision::Same), OPERATOR, "d2");
    second.root = "d1".into();
    second.revises = vec!["d1@d1".into()];
    for inputs in [
        [a.clone(), z.clone(), stored(&first), stored(&second)],
        [a.clone(), z.clone(), stored(&second), stored(&first)],
    ] {
        let out = fuse(commands(), &inputs);
        assert_eq!(out.len(), 1);
        assert_eq!(out[0]["basis"], "operator");
    }
    // Withdrawing the record returns the pair to the rules (a window match).
    let mut withdrawal = decided("command", &a, &z, None, OPERATOR, "w1");
    withdrawal.withdraws = vec!["d1".into()];
    let out = fuse(
        commands(),
        &[
            a.clone(),
            z.clone(),
            stored(&first),
            stored(&second),
            stored(&withdrawal),
        ],
    );
    assert_eq!(out.len(), 1);
    assert_eq!(out[0]["basis"], "rule");
    // Withdrawing only the revision makes the first decision current again.
    let mut back = decided("command", &a, &z, None, OPERATOR, "w2");
    back.withdraws = vec!["d1@d2".into()];
    assert!(
        fuse(
            commands(),
            &[a, z, stored(&first), stored(&second), stored(&back)]
        )
        .is_empty()
    );
}

#[devkit::test]
fn an_authors_conflicting_heads_are_no_decision() {
    let a = atuin("deploy", H + 10.0, 0);
    let z = zsh("deploy", H + 10.4);
    // Two revisions of one record, each revising the first: two heads.
    let mut one = decided("command", &a, &z, Some(Decision::Distinct), OPERATOR, "x1");
    one.root = "d1".into();
    let mut two = decided("command", &a, &z, Some(Decision::Same), OPERATOR, "x2");
    two.root = "d1".into();
    let first = decided("command", &a, &z, Some(Decision::Distinct), OPERATOR, "d1");
    one.revises = vec!["d1@d1".into()];
    two.revises = vec!["d1@d1".into()];
    let rows = duplicates(
        commands(),
        &[a, z, stored(&first), stored(&one), stored(&two)],
    );
    assert_eq!(rows[0]["state"], "undecided");
}

#[devkit::test]
fn a_decision_survives_replay_of_either_side() {
    // Decisions cite records, so a new reading of a side (new event ID, new
    // material, same record) is still decided.
    let a = atuin("deploy", H + 10.0, 0);
    let z = zsh("deploy", H + 10.4);
    let apart = stored(&decided(
        "command",
        &a,
        &z,
        Some(Decision::Distinct),
        OPERATOR,
        "d1",
    ));
    let mut reread = z.clone();
    reread.id = EventId::new();
    if let Origin::Record { location, .. } = &mut reread.origin {
        location.material = MaterialId::new();
        location.anchor = 900;
    }
    assert_eq!(record_identity(&reread), record_identity(&z));
    let mut replayed = a.clone();
    replayed.id = EventId::new();
    assert!(fuse(commands(), &[replayed, reread, apart]).is_empty());
}

#[devkit::test]
fn a_decision_of_another_family_is_not_read() {
    let a = atuin("deploy", H + 10.0, 0);
    let z = zsh("deploy", H + 10.4);
    let other = stored(&decided(
        "mail",
        &a,
        &z,
        Some(Decision::Distinct),
        OPERATOR,
        "d1",
    ));
    assert_eq!(fuse(commands(), &[a, z, other]).len(), 1);
}

// Mail: one message stored by several mail sources.

pub(crate) fn message(source: &str, id: &str, secs: f64) -> Event {
    captured(
        &MailMessage {
            account: Some("owner@example.test".into()),
            message_id: Some(id.into()),
            native_id: None,
            date: Some(at(secs)),
            from: vec!["a@example.org".into()],
            to: vec![],
            cc: vec![],
            bcc: vec![],
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
    let inputs = [
        message("communication.mbox", "<1@x>", H + 5.0),
        message("communication.imap", "<1@x>", H + 5.0),
        message("communication.imap", "<2@x>", H + 5.0),
    ];
    let out = fuse(mail(), &inputs);
    assert_eq!(out.len(), 1, "{out:?}");
    assert_eq!(out[0]["basis"], "identifier");
    let typed: MailFused = serde_json::from_value(out[0].clone()).unwrap();
    assert_eq!(typed.account, "owner@example.test");
    let read = occurrences(mail(), &inputs);
    assert_eq!(read.len(), 2, "{read:?}");
    let joined = read.iter().find(|m| m["message_id"] == "<1@x>").unwrap();
    assert_eq!(
        sources(joined),
        ["communication.imap", "communication.mbox"]
    );
    assert_eq!(joined["subject"], "about <1@x>");
}

#[devkit::test]
fn mail_with_equal_message_id_in_one_account_fuses_without_date_agreement() {
    let inputs = [
        message("communication.mbox", "<1@x>", H + 5.0),
        message("communication.imap", "<1@x>", H + 1500.0),
    ];
    let out = fuse(mail(), &inputs);
    assert_eq!(out.len(), 1, "{out:?}");
    assert_eq!(out[0]["basis"], "identifier");
}

#[devkit::test]
fn copies_of_a_message_fuse_whatever_dates_their_stores_give() {
    let inputs = [
        message("communication.mbox", "<1@x>", H + 5.0),
        // The provider's own receive time, three days after the header date.
        message("communication.gmail", "<1@x>", H + 5.0 + 3.0 * 86_400.0),
    ];
    let out = fuse_events(mail(), &inputs);
    assert_eq!(out.len(), 1, "{out:?}");
    assert!(matches!(&out[0].origin, Origin::Derived { parents } if parents.len() == 2));
    // The most preferred member supplies the time.
    assert_eq!(
        out[0].payload["sent_at"],
        json!(at(H + 5.0 + 3.0 * 86_400.0))
    );
    assert_eq!(occurrences(mail(), &inputs).len(), 1);
}

#[devkit::test]
fn a_decision_within_an_identity_key_is_read_by_that_key() {
    let a = message("communication.mbox", "<1@x>", H + 5.0);
    let b = message("communication.imap", "<1@x>", H + 7.0);
    let mut apart = decided("mail", &a, &b, Some(Decision::Distinct), OPERATOR, "d1");
    apart.family_key = mail().identity_key("mail.message", &a.payload);
    let inputs = [a, b, stored(&apart)];
    assert!(fuse(mail(), &inputs).is_empty());
    assert_eq!(duplicates(mail(), &inputs)[0]["state"], "distinct");
}

#[devkit::test]
fn mail_from_a_source_no_member_names_is_not_read() {
    let inputs = [
        message("communication.elsewhere", "<1@x>", H + 5.0),
        message("communication.imap", "<1@x>", H + 5.0),
    ];
    assert!(fuse(mail(), &inputs).is_empty());
    let read = occurrences(mail(), &inputs);
    assert_eq!(read.len(), 1);
    assert_eq!(read[0]["record"], record_identity(&inputs[1]));
}

#[devkit::test]
fn mail_without_an_account_never_fuses() {
    let mut first = message("communication.mbox", "<1@x>", H + 5.0);
    let mut second = message("communication.imap", "<1@x>", H + 5.0);
    first.payload["account"] = Value::Null;
    second.payload["account"] = Value::Null;
    let inputs = [first, second];
    assert!(fuse(mail(), &inputs).is_empty());
    assert_eq!(occurrences(mail(), &inputs).len(), 2);
}

#[devkit::test]
fn mail_of_two_accounts_never_fuses() {
    let first = message("communication.mbox", "<1@x>", H + 5.0);
    let mut second = message("communication.imap", "<1@x>", H + 5.0);
    second.payload["account"] = json!("other@example.test");
    let inputs = [first, second];
    assert!(fuse(mail(), &inputs).is_empty());
    assert_eq!(occurrences(mail(), &inputs).len(), 2);
}

#[devkit::test]
fn equal_mail_without_message_ids_remains_separate() {
    let mut first = message("communication.mbox", "unused", H + 5.0);
    first.payload["message_id"] = serde_json::Value::Null;
    let mut second = message("communication.imap", "unused", H + 5.0);
    second.payload["message_id"] = serde_json::Value::Null;
    assert_eq!(first.payload, second.payload);
    let inputs = [first, second];
    assert!(fuse(mail(), &inputs).is_empty());
    let read = occurrences(mail(), &inputs);
    assert_eq!(read.len(), 2, "{read:?}");
    assert!(read.iter().all(|event| event["message_id"].is_null()));
}

mod gmail;

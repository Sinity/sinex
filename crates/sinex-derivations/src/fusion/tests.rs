use super::rules::{COMMANDS, MAIL, RULES};
use super::*;
use sinex_model::event_types::communication::MailMessage;
use sinex_model::event_types::{CommandExecuted, CommandImported};
use sinex_model::{Class, EventPayload, MaterialId, Origin, ReadingOrder, RecordLocation};

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

/// Run a rule as the runner would: every key the inputs touch, each reading
/// what its query admits. Outputs sorted by start, then record.
pub(crate) fn fuse_events(rule: &'static FusionRule, inputs: &[Event]) -> Vec<Event> {
    let fusion = Fusion(rule);
    let env = Env::at(
        Timestamp::from_unix_secs(2_000_000_000).unwrap(),
        sinex_model::time_settings::TimeSettings::new("Europe/Warsaw", 4, 0)
            .expect("fixture day setting"),
    );
    let mut keys: Vec<Value> = Vec::new();
    for e in inputs {
        let view = KeyView {
            event_type: &e.event_type,
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

/// What a consumer reads: each occurrence once, from the fused records and
/// the inputs together. Sorted by start, then record.
fn occurrences(rule: &'static FusionRule, inputs: &[Event]) -> Vec<Value> {
    let mut all = fuse_events(rule, inputs);
    all.extend(inputs.iter().cloned());
    let mut out: Vec<Value> = rule
        .occurrences(&all)
        .into_iter()
        .map(|o| o.payload.into_owned())
        .collect();
    out.sort_by_key(|p| {
        (
            p[rule.start_field].as_str().unwrap_or("").to_owned(),
            p["record"].as_str().unwrap_or("").to_owned(),
        )
    });
    out
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
        for o in rule.fusion_inputs {
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
        for o in rule.fusion_inputs {
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
    assert_eq!(out.len(), 1);
    assert_eq!(observed_by(&out[0]).len(), 2);
    let read = occurrences(
        &COMMANDS,
        &[
            atuin("make", H + 100.0, 0),
            zsh("make", H + 100.4),
            zsh("make", H + 101.5),
        ],
    );
    assert_eq!(read.len(), 2);
    assert_eq!(read.iter().filter(|f| observed_by(f).len() == 2).count(), 1);
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
    let inputs = [
        atuin("git push", H + 300.0, 0),
        zsh("git push", H + 320.0),
        zsh("git push", H + 340.0),
    ];
    assert!(fuse(&COMMANDS, &inputs).is_empty());
    let read = occurrences(&COMMANDS, &inputs);
    assert_eq!(read.len(), 3);
    assert!(read.iter().all(|f| observed_by(f).len() == 1));
}

#[devkit::test]
fn different_commands_or_far_apart_never_fuse() {
    let differ = [atuin("ls", H + 10.0, 0), zsh("ls -l", H + 10.0)];
    assert!(fuse(&COMMANDS, &differ).is_empty());
    assert_eq!(occurrences(&COMMANDS, &differ).len(), 2);
    let far = [atuin("ls", H + 10.0, 0), zsh("ls", H + 100.0)];
    assert!(fuse(&COMMANDS, &far).is_empty());
    assert_eq!(occurrences(&COMMANDS, &far).len(), 2);
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
    let inputs = [a, zsh("rm -rf x", H + 10.1)];
    assert!(fuse(&COMMANDS, &inputs).is_empty());
    let read = occurrences(&COMMANDS, &inputs);
    assert_eq!(read.len(), 1);
    assert_eq!(observed_by(&read[0]), ["terminal.zsh"]);
}

#[devkit::test]
fn the_same_command_twice_in_one_second_is_two_runs() {
    let a = zsh("ls", H + 5.0);
    let mut b = zsh("ls", H + 5.0);
    if let Origin::Record { location, .. } = &mut b.origin {
        location.anchor = 40;
    }
    let inputs = [a, b];
    assert!(fuse(&COMMANDS, &inputs).is_empty());
    let read = occurrences(&COMMANDS, &inputs);
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
    assert_eq!(fuse(&COMMANDS, &inputs), fuse(&COMMANDS, &reversed));
    assert_eq!(
        occurrences(&COMMANDS, &inputs),
        occurrences(&COMMANDS, &reversed)
    );
}

#[devkit::test]
fn no_fused_record_has_fewer_than_two_members() {
    let commands = fuse_events(
        &COMMANDS,
        &[
            atuin("ls", H + 10.0, 0),
            zsh("ls", H + 10.2),
            atuin("only atuin", H + 20.0, 0),
            zsh("only zsh", H + 30.0),
        ],
    );
    let mut alone = mail("communication.imap", "<alone@x>", H + 5.0);
    alone.payload["account"] = Value::Null;
    let mail = fuse_events(
        &MAIL,
        &[
            mail("communication.imap", "<1@x>", H + 5.0),
            mail("communication.mbox", "<1@x>", H + 5.0),
            mail("communication.imap", "<2@x>", H + 6.0),
            alone,
        ],
    );
    assert_eq!(commands.len(), 1);
    assert_eq!(mail.len(), 1);
    for fused in commands.iter().chain(&mail) {
        let Origin::Derived { parents } = &fused.origin else {
            panic!("a fused record is derived");
        };
        assert!(parents.len() >= 2, "{fused:?}");
    }
}

#[devkit::test]
fn a_consumer_reads_each_command_once_whichever_shells_saw_it() {
    let read = occurrences(
        &COMMANDS,
        &[
            atuin("both", H + 10.0, 0),
            zsh("both", H + 10.3),
            atuin("atuin only", H + 20.0, 1),
            zsh("zsh only", H + 30.0),
        ],
    );
    let commands: Vec<_> = read
        .iter()
        .map(|c| c["command"].as_str().unwrap())
        .collect();
    assert_eq!(commands, ["both", "atuin only", "zsh only"]);
    assert_eq!(observed_by(&read[0]), ["terminal.atuin", "terminal.zsh"]);
    assert_eq!(observed_by(&read[1]), ["terminal.atuin"]);
    assert_eq!(read[1]["exit_code"], 1);
    assert_eq!(observed_by(&read[2]), ["terminal.zsh"]);
    assert_eq!(read[2]["host"], "box");
}

#[devkit::test]
fn absorb_joins_a_repeat_inside_a_group_led_by_a_preferred_input() {
    let rule: &'static FusionRule = Box::leak(Box::new(FusionRule {
        absorb_ns: Some(60_000_000_000),
        ..COMMANDS
    }));
    let inputs = [
        atuin("vim", H + 10.0, 0),
        zsh("vim", H + 10.5),
        zsh("vim", H + 11.5),
    ];
    let out = fuse_events(rule, &inputs);
    assert_eq!(out.len(), 1, "{out:?}");
    assert!(matches!(&out[0].origin, Origin::Derived { parents } if parents.len() == 3));
    // Without absorb, the repeat is a command of its own.
    assert_eq!(occurrences(&COMMANDS, &inputs).len(), 2);
    // A record before the group's window is not absorbed.
    let earlier = [
        zsh("vim", H + 5.0),
        atuin("vim", H + 10.0, 0),
        zsh("vim", H + 10.5),
    ];
    assert_eq!(fuse_events(rule, &earlier).len(), 1);
    assert_eq!(occurrences(rule, &earlier).len(), 2);
}

// Mail: one message stored by several mail sources.

pub(crate) fn mail(source: &str, id: &str, secs: f64) -> Event {
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
        mail("communication.mbox", "<1@x>", H + 5.0),
        mail("communication.imap", "<1@x>", H + 5.0),
        mail("communication.imap", "<2@x>", H + 5.0),
    ];
    let out = fuse(&MAIL, &inputs);
    assert_eq!(out.len(), 1, "{out:?}");
    let read = occurrences(&MAIL, &inputs);
    assert_eq!(read.len(), 2, "{read:?}");
    let joined = read.iter().find(|m| m["message_id"] == "<1@x>").unwrap();
    assert_eq!(
        observed_by(joined),
        ["communication.imap", "communication.mbox"]
    );
    assert_eq!(joined["subject"], "about <1@x>");
}

#[devkit::test]
fn mail_from_a_source_no_input_names_is_not_read() {
    let inputs = [
        mail("communication.elsewhere", "<1@x>", H + 5.0),
        mail("communication.imap", "<1@x>", H + 5.0),
    ];
    assert!(fuse(&MAIL, &inputs).is_empty());
    let read = occurrences(&MAIL, &inputs);
    assert_eq!(read.len(), 1);
    assert_eq!(observed_by(&read[0]), ["communication.imap"]);
}

#[devkit::test]
fn mail_without_an_account_never_fuses() {
    let mut first = mail("communication.mbox", "<1@x>", H + 5.0);
    let mut second = mail("communication.imap", "<1@x>", H + 5.0);
    first.payload["account"] = Value::Null;
    second.payload["account"] = Value::Null;
    let inputs = [first, second];
    assert!(fuse(&MAIL, &inputs).is_empty());
    assert_eq!(occurrences(&MAIL, &inputs).len(), 2);
}

#[devkit::test]
fn mail_of_two_accounts_never_fuses() {
    let first = mail("communication.mbox", "<1@x>", H + 5.0);
    let mut second = mail("communication.imap", "<1@x>", H + 5.0);
    second.payload["account"] = json!("other@example.test");
    let inputs = [first, second];
    assert!(fuse(&MAIL, &inputs).is_empty());
    assert_eq!(occurrences(&MAIL, &inputs).len(), 2);
}

#[devkit::test]
fn equal_mail_without_message_ids_remains_separate() {
    let mut first = mail("communication.mbox", "unused", H + 5.0);
    first.payload["message_id"] = serde_json::Value::Null;
    let mut second = mail("communication.imap", "unused", H + 5.0);
    second.payload["message_id"] = serde_json::Value::Null;
    assert_eq!(first.payload, second.payload);
    let inputs = [first, second];
    assert!(fuse(&MAIL, &inputs).is_empty());
    let read = occurrences(&MAIL, &inputs);
    assert_eq!(read.len(), 2, "{read:?}");
    assert!(read.iter().all(|event| event["message_id"].is_null()));
}

mod gmail;

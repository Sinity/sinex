use super::*;
use sinex_model::{Class, MaterialId, Origin, ReadingOrder, RecordLocation};

pub(crate) fn atuin(command: &str, secs: f64, exit: i64) -> Event {
    let at = Timestamp::from_unix_ns((secs * 1e9) as i128).unwrap();
    Event::build(
        &CommandExecuted {
            native_id: format!("{secs}"),
            command: command.into(),
            cwd: "/home/u/proj".into(),
            session: "s1".into(),
            hostname: "box:u".into(),
            started_at: at,
            duration_ns: Some(1_000_000_000),
            exit_code: Some(exit),
            deleted_at: None,
            author: None,
            intent: None,
            shell: None,
            extra: Default::default(),
        },
        "terminal.atuin",
        Origin::Record {
            location: RecordLocation {
                material: MaterialId::new(),
                anchor: 0,
            },
            ordinal: 0,
        },
        ReadingOrder(0),
        [0; 32],
        Class::Open,
    )
    .unwrap()
}

pub(crate) fn zsh(command: &str, secs: f64) -> Event {
    let at = Timestamp::from_unix_ns((secs * 1e9) as i128).unwrap();
    Event::build(
        &CommandImported {
            command: command.into(),
            hostname: "box".into(),
            started_at: Some(at),
            header_elapsed: Some(0),
            n: 0,
        },
        "terminal.zsh",
        Origin::Record {
            location: RecordLocation {
                material: MaterialId::new(),
                anchor: 0,
            },
            ordinal: 0,
        },
        ReadingOrder(0),
        [0; 32],
        Class::Guarded,
    )
    .unwrap()
}

const H: f64 = 1_700_002_800.0; // an hour boundary (1700002800 / 3600 = 472223)

fn run(inputs: &[Event]) -> Vec<CommandFused> {
    let env = Env::at(Timestamp::from_unix_secs(2_000_000_000).unwrap());
    let mut keys: Vec<Value> = Vec::new();
    for e in inputs {
        for k in CommandFusion.keys_for(
            &KeyView {
                event_type: e.event_type,
                ts_orig: e.ts_orig.at,
                payload: &e.payload,
            },
            &env,
        ) {
            if !keys.contains(&k) {
                keys.push(k);
            }
        }
    }
    let mut out = Vec::new();
    for k in keys {
        let q = CommandFusion.inputs(&k, &env).unwrap();
        let mut window: Vec<Event> = inputs
            .iter()
            .filter(|e| {
                e.ts_orig
                    .at
                    .is_some_and(|t| t >= q.window.0 && t < q.window.1)
                    && q.event_types.contains(&e.event_type)
            })
            .cloned()
            .collect();
        window.sort_by_key(|e| e.ts_orig.at);
        out.extend(
            CommandFusion
                .compute(&k, &window, &env)
                .outputs
                .iter()
                .map(|e| e.payload_as::<CommandFused>().unwrap()),
        );
    }
    out.sort_by(|a, b| {
        a.started_at
            .cmp(&b.started_at)
            .then(a.command.cmp(&b.command))
    });
    out
}

#[devkit::test]
fn close_readings_fuse_and_atuin_supplies_fields() {
    let out = run(&[atuin("ls -la", H + 10.0, 2), zsh("  ls -la ", H + 10.5)]);
    assert_eq!(out.len(), 1);
    let f = &out[0];
    assert_eq!(f.observed_by, vec!["terminal.atuin", "terminal.zsh"]);
    assert_eq!(f.exit_code, Some(2));
    assert_eq!(f.cwd.as_deref(), Some("/home/u/proj"));
    assert_eq!(f.host, "box");
}

#[devkit::test]
fn closest_pairs_win_and_extras_stay_alone() {
    // Two zsh readings of the same command near one Atuin reading: the
    // closer one fuses, the other stays its own command.
    let out = run(&[
        atuin("make", H + 100.0, 0),
        zsh("make", H + 100.4),
        zsh("make", H + 101.5),
    ]);
    assert_eq!(out.len(), 2);
    assert_eq!(out.iter().filter(|f| f.observed_by.len() == 2).count(), 1);
}

#[devkit::test]
fn fallback_needs_a_unique_candidate() {
    // 30 s apart: only the fallback pass can match, and it does when unique.
    assert_eq!(
        run(&[atuin("git push", H + 200.0, 0), zsh("git push", H + 230.0)]).len(),
        1
    );
    // Two zsh candidates for one Atuin record: ambiguous, nothing fuses.
    let out = run(&[
        atuin("git push", H + 300.0, 0),
        zsh("git push", H + 320.0),
        zsh("git push", H + 340.0),
    ]);
    assert_eq!(out.len(), 3);
    assert!(out.iter().all(|f| f.observed_by.len() == 1));
}

#[devkit::test]
fn different_commands_or_far_apart_never_fuse() {
    assert_eq!(
        run(&[atuin("ls", H + 10.0, 0), zsh("ls -l", H + 10.0)]).len(),
        2
    );
    assert_eq!(
        run(&[atuin("ls", H + 10.0, 0), zsh("ls", H + 100.0)]).len(),
        2
    );
}

#[devkit::test]
fn a_pair_across_an_hour_edge_is_emitted_once() {
    let out = run(&[atuin("cargo test", H - 0.5, 0), zsh("cargo test", H + 0.5)]);
    assert_eq!(out.len(), 1, "{out:?}");
    assert_eq!(out[0].observed_by.len(), 2);
}

#[devkit::test]
fn privacy_class_is_the_strictest_parent() {
    let a = atuin("ls", H + 10.0, 0);
    let z = zsh("ls", H + 10.2);
    let env = Env::at(Timestamp::now());
    let key = json!({"host": "box", "hour": (H as i64) / 3600});
    let out = CommandFusion.compute(&key, &[a, z], &env).outputs;
    assert_eq!(out.len(), 1);
    assert_eq!(out[0].class, Class::Guarded);
    assert!(matches!(&out[0].origin, Origin::Derived { parents } if parents.len() == 2));
}

#[devkit::test]
fn removed_atuin_records_do_not_fuse() {
    let mut a = atuin("rm -rf x", H + 10.0, 0);
    a.record_state = RecordState::Removed;
    let out = run(&[a, zsh("rm -rf x", H + 10.1)]);
    assert_eq!(out.len(), 1);
    assert_eq!(out[0].observed_by, vec!["terminal.zsh"]);
}

#[devkit::test]
fn the_same_command_twice_in_one_second_is_two_runs() {
    let a = zsh("ls", H + 5.0);
    let mut b = zsh("ls", H + 5.0);
    if let Origin::Record { location, .. } = &mut b.origin {
        location.anchor = 40;
    }
    let env = Env::at(Timestamp::now());
    let key = json!({"host": "box", "hour": (H as i64) / 3600});
    let out = CommandFusion.compute(&key, &[a, b], &env).outputs;
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
    assert_eq!(run(&inputs), run(&reversed));
}

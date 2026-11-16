use super::*;
use crate::fusion::tests::{atuin, fuse_events, zsh};
use sinex_model::event_types::CommandExecuted;

/// A command Atuin recorded and no other shell saw.
fn command(secs: i64, duration_s: Option<i64>, exit: i64) -> Event {
    crate::fusion::tests::captured(
        &CommandExecuted {
            native_id: format!("r{secs}"),
            command: format!("c{secs}"),
            cwd: format!("/p{}", secs % 2),
            session: "s".into(),
            hostname: "box:u".into(),
            started_at: Timestamp::from_unix_secs(secs).unwrap(),
            duration_ns: duration_s.map(|d| d * 1_000_000_000),
            exit_code: Some(exit),
            deleted_at: None,
            author: None,
            intent: None,
            shell: None,
            extra: Default::default(),
        },
        "terminal.atuin-history",
        sinex_model::Class::Open,
    )
}

// 2024-06-15 10:00 CEST.
const T: i64 = 1_718_438_400;

fn sessions(inputs: &[Event]) -> Vec<ShellSession> {
    let env = Env::at(
        Timestamp::from_unix_secs(T + 86_400 * 3).unwrap(),
        sinex_model::time_settings::TimeSettings::new("Europe/Warsaw", 4, 0)
            .expect("fixture day setting"),
    );
    let key = HostDayKey {
        host: "box".into(),
        day: "2024-06-15".parse().unwrap(),
    };
    ShellSessions
        .compute(&key, inputs, &env)
        .outputs
        .iter()
        .map(|e| e.payload_as::<ShellSession>().unwrap())
        .collect()
}

#[devkit::test]
fn gap_after_the_latest_end_splits_sessions() {
    let s = sessions(&[
        command(T, Some(10), 0),
        command(T + 200, None, 1),
        // A long build ends at T+1300; a command 299 s later stays.
        command(T + 300, Some(1000), 0),
        command(T + 1599, None, 0),
        command(T + 1900, None, 0),
    ]);
    assert_eq!(s.len(), 2, "{s:?}");
    assert_eq!(s[0].commands, 4);
    assert_eq!(s[0].failed, 1);
    assert_eq!(s[0].ended_at, Timestamp::from_unix_secs(T + 1599).unwrap());
    assert_eq!(s[0].cwds, vec!["/p0", "/p1"]);
    assert_eq!(s[1].commands, 1);
    assert_eq!(s[1].day, "2024-06-15");
}

#[devkit::test]
fn key_is_host_and_logical_day() {
    let env = Env::at(
        Timestamp::now(),
        sinex_model::time_settings::TimeSettings::new("Europe/Warsaw", 4, 0)
            .expect("fixture day setting"),
    );
    let e = command(T, None, 0);
    let keys = ShellSessions.keys_for(
        &KeyView {
            event_type: &e.event_type,
            ts_orig: e.ts_orig.at,
            payload: &e.payload,
        },
        &env,
    );
    assert_eq!(
        keys,
        vec![HostDayKey {
            host: "box".into(),
            day: "2024-06-15".parse().unwrap()
        }]
    );
    // 03:30 local belongs to the previous logical day.
    let early = command(T - 6 * 3600 - 1800, None, 0);
    let keys = ShellSessions.keys_for(
        &KeyView {
            event_type: &early.event_type,
            ts_orig: early.ts_orig.at,
            payload: &early.payload,
        },
        &env,
    );
    assert_eq!(keys[0].day.to_string(), "2024-06-14");
}

#[devkit::test]
fn every_member_is_a_parent() {
    let env = Env::at(
        Timestamp::now(),
        sinex_model::time_settings::TimeSettings::new("Europe/Warsaw", 4, 0)
            .expect("fixture day setting"),
    );
    let inputs = [command(T, None, 0), command(T + 10, None, 0)];
    let out = ShellSessions.compute(
        &HostDayKey {
            host: "box".into(),
            day: "2024-06-15".parse().unwrap(),
        },
        &inputs,
        &env,
    );
    match &out.outputs[0].origin {
        sinex_model::Origin::Derived { parents } => {
            assert_eq!(parents, &vec![inputs[0].id, inputs[1].id]);
        }
        other => panic!("{other:?}"),
    }
}

#[devkit::test]
fn a_command_only_zsh_saw_is_one_member() {
    let s = sessions(&[zsh("make", T as f64)]);
    assert_eq!(s.len(), 1, "{s:?}");
    assert_eq!(s[0].commands, 1);
}

#[devkit::test]
fn a_command_both_shells_saw_is_one_member() {
    let shells = [atuin("make", T as f64, 2), zsh("make", T as f64 + 0.4)];
    let mut inputs = fuse_events(crate::fusion::commands(), &shells);
    assert_eq!(inputs.len(), 1);
    let fused_id = inputs[0].id;
    inputs.extend(shells);
    let env = Env::at(
        Timestamp::from_unix_secs(T + 86_400 * 3).unwrap(),
        sinex_model::time_settings::TimeSettings::new("Europe/Warsaw", 4, 0)
            .expect("fixture day setting"),
    );
    let out = ShellSessions.compute(
        &HostDayKey {
            host: "box".into(),
            day: "2024-06-15".parse().unwrap(),
        },
        &inputs,
        &env,
    );
    assert_eq!(out.outputs.len(), 1);
    let session: ShellSession = out.outputs[0].payload_as().unwrap();
    assert_eq!(session.commands, 1);
    assert_eq!(session.failed, 1, "Atuin supplies the exit code");
    match &out.outputs[0].origin {
        sinex_model::Origin::Derived { parents } => assert_eq!(parents, &vec![fused_id]),
        other => panic!("{other:?}"),
    }
}

#[devkit::test]
fn unknown_last_duration_leaves_end_a_lower_bound() {
    let result = sessions(&[command(T, Some(100), 0), command(T + 120, None, 0)]);
    assert_eq!(result.len(), 1);
    assert_eq!(
        result[0].ended_at,
        Timestamp::from_unix_secs(T + 120).unwrap()
    );
    assert!(result[0].end_is_lower_bound);

    let covered = sessions(&[command(T, Some(200), 0), command(T + 120, None, 0)]);
    assert_eq!(covered.len(), 1);
    assert_eq!(
        covered[0].ended_at,
        Timestamp::from_unix_secs(T + 200).unwrap()
    );
    assert!(!covered[0].end_is_lower_bound);
}

#[devkit::test]
fn negative_duration_counts_as_a_point() {
    let result = sessions(&[command(T, Some(-300), 0), command(T + 200, Some(5), 0)]);
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].commands, 2);
    assert_eq!(result[0].started_at, Timestamp::from_unix_secs(T).unwrap());
    assert_eq!(
        result[0].ended_at,
        Timestamp::from_unix_secs(T + 205).unwrap()
    );
    assert!(!result[0].end_is_lower_bound);
}

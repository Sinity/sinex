use super::*;
use serde_json::json;

fn rec(entry: Value) -> Map<String, Value> {
    serde_json::from_slice(&live_stream::frame(
        Timestamp::now(),
        entry.to_string().as_bytes(),
    ))
    .unwrap()
}

fn read(entry: Value) -> Vec<JournalReading> {
    JournaldParser
        .parse(&rec(entry), &(), &crate::RecordContext::none())
        .unwrap()
        .0
}

/// The journal entry every record yields first.
fn entry(out: &[JournalReading]) -> &JournalEntry {
    match &out[0] {
        JournalReading::Entry(entry) => entry,
        other => panic!("first reading is not the entry: {other:?}"),
    }
}

#[devkit::test]
fn known_fields_are_typed_and_the_rest_kept() {
    let out = read(json!({
                "__CURSOR": "s=1;i=2",
                "__REALTIME_TIMESTAMP": "1700000000123456",
                "MESSAGE": "Started foo.",
                "PRIORITY": "6",
                "_SYSTEMD_UNIT": "init.scope",
                "_PID": "1",
                "_UID": "0",
                "_COMM": "systemd",
                "_TRANSPORT": "journal",
                "CODE_FILE": "src/core/job.c",
    }));
    assert_eq!(out.len(), 1, "an ordinary entry is only an entry");
    let e = entry(&out);
    assert_eq!(e.cursor, "s=1;i=2");
    assert_eq!(e.logged_at.unwrap().unix_ns(), 1_700_000_000_123_456_000);
    assert_eq!(e.message.as_deref(), Some("Started foo."));
    assert_eq!(e.priority, Some(6));
    assert_eq!((e.pid, e.uid), (Some(1), Some(0)));
    assert_eq!(e.fields.get("CODE_FILE"), Some(&json!("src/core/job.c")));
    assert!(!e.fields.contains_key("MESSAGE"));
}

#[devkit::test]
fn byte_array_messages_decode_when_utf8_and_stay_raw_otherwise() {
    // "\x1b[1mhi" (colour codes make journalctl print bytes).
    let out = read(json!({"__CURSOR": "c", "MESSAGE": [27, 91, 49, 109, 104, 105]}));
    assert_eq!(entry(&out).message.as_deref(), Some("\u{1b}[1mhi"));
    let out = read(json!({"__CURSOR": "c", "MESSAGE": [255, 0]}));
    assert_eq!(entry(&out).message, None);
    assert_eq!(entry(&out).fields.get("MESSAGE"), Some(&json!([255, 0])));
}

#[devkit::test]
fn a_missing_or_malformed_time_stays_unknown() {
    let out = read(json!({"__CURSOR": "c", "__REALTIME_TIMESTAMP": "soon"}));
    assert_eq!(entry(&out).logged_at, None);
    assert_eq!(
        sinex_model::EventPayload::ts_orig(entry(&out)).quality,
        sinex_model::TimeQuality::Unknown
    );
}

#[devkit::test]
fn an_entry_without_a_cursor_is_a_failure_not_a_guess() {
    assert!(
        JournaldParser
            .parse(
                &rec(json!({"MESSAGE": "x"})),
                &(),
                &crate::RecordContext::none()
            )
            .is_err()
    );
}

#[devkit::test]
fn capture_resumes_after_the_last_spooled_cursor() {
    let spool = std::env::temp_dir().join(format!("sinex-jd-{}", uuid::Uuid::now_v7()));
    assert_eq!(last_spooled_cursor(&std::env::temp_dir()).unwrap(), None);
    let mut w = SpoolWriter::open(&spool, Rotation::default()).unwrap();
    for i in 0..3 {
        let line = json!({"__CURSOR": format!("c{i}"), "MESSAGE": "x".repeat(70_000)});
        w.append(Timestamp::now(), line.to_string().as_bytes())
            .unwrap();
    }
    w.sync().unwrap();
    assert_eq!(last_spooled_cursor(&spool).unwrap().as_deref(), Some("c2"));
}

/// An entry systemd's manager writes about a unit.
fn manager_entry(message_id: &str, extra: Value) -> Value {
    let mut entry = json!({
        "__CURSOR": "s=1;i=9",
        "__REALTIME_TIMESTAMP": "1700000000000000",
        "MESSAGE_ID": message_id,
        "_COMM": "systemd",
        "_PID": "1",
        "_UID": "0",
        "_SYSTEMD_UNIT": "init.scope",
    });
    entry
        .as_object_mut()
        .unwrap()
        .extend(extra.as_object().unwrap().clone());
    entry
}

#[devkit::test]
fn a_finished_start_job_is_a_started_unit() {
    let out = read(manager_entry(
        "39f53479d3a045ac8e11786248231fbf",
        json!({"UNIT": "backup.service", "INVOCATION_ID": "inv1", "JOB_TYPE": "start", "JOB_RESULT": "done"}),
    ));
    assert_eq!(out.len(), 2);
    let JournalReading::UnitStarted(started) = &out[1] else {
        panic!("{:?}", out[1]);
    };
    assert_eq!(started.unit, "backup.service");
    assert_eq!(started.manager, "system");
    assert_eq!(started.uid, Some(0));
    assert_eq!(started.invocation_id.as_deref(), Some("inv1"));
    assert_eq!(started.job.as_deref(), Some("start"));
    assert_eq!(started.cursor, "s=1;i=9");
    assert_eq!(started.logged_at, entry(&out).logged_at);
}

#[devkit::test]
fn a_user_manager_names_its_unit_and_invocation_with_the_user_fields() {
    let out = read(manager_entry(
        "9d1aaa27d60140bd96365438aad20286",
        json!({"USER_UNIT": "app.service", "USER_INVOCATION_ID": "inv2", "JOB_RESULT": "done",
               "_PID": "2000", "_UID": "1000", "_SYSTEMD_UNIT": "user@1000.service"}),
    ));
    let JournalReading::UnitStopped(stopped) = &out[1] else {
        panic!("{:?}", out[1]);
    };
    assert_eq!(
        (stopped.unit.as_str(), stopped.manager.as_str()),
        ("app.service", "user")
    );
    assert_eq!(stopped.uid, Some(1000));
    assert_eq!(stopped.invocation_id.as_deref(), Some("inv2"));
    assert_eq!(
        (stopped.outcome.as_str(), stopped.result.as_deref()),
        ("stopped", Some("done"))
    );
}

#[devkit::test]
fn every_way_a_unit_ends_is_a_stopped_unit_with_its_outcome() {
    for (id, extra, outcome, result) in [
        (
            "7ad2d189f7e94e70a38c781354912448",
            json!({}),
            "succeeded",
            None,
        ),
        (
            "d9b373ed55a64feb8242e02dbe79a49c",
            json!({"UNIT_RESULT": "exit-code"}),
            "failed",
            Some("exit-code"),
        ),
        (
            "be02cf6855d2428ba40df7e9d022f03d",
            json!({"JOB_RESULT": "dependency"}),
            "start_failed",
            Some("dependency"),
        ),
    ] {
        let mut fields = json!({"UNIT": "x.service"});
        fields
            .as_object_mut()
            .unwrap()
            .extend(extra.as_object().unwrap().clone());
        let out = read(manager_entry(id, fields));
        let JournalReading::UnitStopped(stopped) = &out[1] else {
            panic!("{id}: {:?}", out.get(1));
        };
        assert_eq!(stopped.outcome, outcome, "{id}");
        assert_eq!(stopped.result.as_deref(), result, "{id}");
    }
}

#[devkit::test]
fn a_finished_reload_is_a_reloaded_unit() {
    let out = read(manager_entry(
        "7b05ebc668384222baa8881179cfda54",
        json!({"UNIT": "dbus.service", "JOB_RESULT": "done"}),
    ));
    assert!(
        matches!(&out[1], JournalReading::UnitReloaded(r) if r.unit == "dbus.service" && r.result.as_deref() == Some("done"))
    );
}

#[devkit::test]
fn a_state_message_from_another_program_is_only_an_entry() {
    // Any program may log with systemd's message IDs; `_COMM` is journald's.
    let out = read(manager_entry(
        "39f53479d3a045ac8e11786248231fbf",
        json!({"UNIT": "backup.service", "_COMM": "python3"}),
    ));
    assert_eq!(out.len(), 1);
    // A manager entry about no unit is only an entry too.
    let out = read(manager_entry("39f53479d3a045ac8e11786248231fbf", json!({})));
    assert_eq!(out.len(), 1);
}

#[devkit::test]
fn startup_finished_dates_the_boot_from_its_phases() {
    let out = read(manager_entry(
        "b07a249cd024414a82dd00cd181378ff",
        json!({"_BOOT_ID": "b1", "KERNEL_USEC": "3000000", "INITRD_USEC": "2000000",
               "USERSPACE_USEC": "5000000"}),
    ));
    let JournalReading::BootStarted(boot) = &out[1] else {
        panic!("{:?}", out[1]);
    };
    assert_eq!(boot.boot_id, "b1");
    assert_eq!(
        boot.finished_at.unwrap().unix_ns(),
        1_700_000_000_000_000_000
    );
    assert_eq!(boot.started_at.unwrap().unix_ns(), 1_699_999_990_000_000_000);
    // No initrd: its phase counts as none. No userspace figure: unknown.
    let out = read(manager_entry(
        "b07a249cd024414a82dd00cd181378ff",
        json!({"_BOOT_ID": "b1", "KERNEL_USEC": "3000000", "USERSPACE_USEC": "5000000"}),
    ));
    let JournalReading::BootStarted(boot) = &out[1] else {
        panic!()
    };
    assert_eq!(boot.started_at.unwrap().unix_ns(), 1_699_999_992_000_000_000);
    let out = read(manager_entry(
        "b07a249cd024414a82dd00cd181378ff",
        json!({"_BOOT_ID": "b1", "KERNEL_USEC": "3000000"}),
    ));
    let JournalReading::BootStarted(boot) = &out[1] else {
        panic!()
    };
    assert_eq!(boot.started_at, None);
    assert_eq!(
        sinex_model::EventPayload::ts_orig(boot).quality,
        sinex_model::TimeQuality::Unknown
    );
}

#[devkit::test]
fn sleep_and_shutdown_are_read_from_their_programs() {
    let sleep = |id: &str| {
        read(manager_entry(
            id,
            json!({"_COMM": "systemd-sleep", "_PID": "900", "SLEEP": "suspend"}),
        ))
    };
    assert!(
        matches!(&sleep("6bbd95ee977941e497c48be27c254128")[1], JournalReading::SleepStarted(s) if s.state.as_deref() == Some("suspend"))
    );
    assert!(
        matches!(&sleep("8811e6df2a8e40f58a94cea26f8ebf14")[1], JournalReading::SleepEnded(s) if s.state.as_deref() == Some("suspend"))
    );
    let out = read(manager_entry(
        "98268866d1d54a499c4e98921d93bc40",
        json!({"_COMM": "systemd-logind", "_PID": "800", "SHUTDOWN": "reboot"}),
    ));
    assert!(
        matches!(&out[1], JournalReading::ShutdownStarted(s) if s.kind.as_deref() == Some("reboot"))
    );
}

#[devkit::test]
fn entries_sinex_wrote_are_marked_as_its_own() {
    for (unit_field, unit, own) in [
        ("_SYSTEMD_UNIT", "sinexd.service", true),
        ("_SYSTEMD_UNIT", "sinex-capture-journald.service", true),
        ("_SYSTEMD_USER_UNIT", "sinex-session.service", true),
        ("_SYSTEMD_UNIT", "postgresql.service", false),
        ("_SYSTEMD_UNIT", "sinexplorer.scope", false),
    ] {
        let mut fields = json!({"__CURSOR": "c"});
        fields[unit_field] = json!(unit);
        let out = read(fields);
        assert_eq!(entry(&out).from_sinex, own, "{unit}");
    }
    // systemd's entries about Sinex's units are the machine's, not Sinex's.
    let out = read(manager_entry(
        "39f53479d3a045ac8e11786248231fbf",
        json!({"UNIT": "sinexd.service"}),
    ));
    assert!(!entry(&out).from_sinex);
}

fn temporary_spool() -> std::path::PathBuf {
    std::env::temp_dir().join(format!("sinex-jd-{}", uuid::Uuid::now_v7()))
}

#[devkit::test]
fn capture_resumes_from_the_resume_file_once_the_segments_are_gone() {
    let spool = temporary_spool();
    let lines: Vec<u8> = (0..3)
        .flat_map(|i| format!("{}\n", json!({"__CURSOR": format!("c{i}")})).into_bytes())
        .collect();
    let mut writer = SpoolWriter::open(&spool, Rotation::default()).unwrap();
    let count = spool_entries(&lines[..], &mut writer, &spool, Duration::from_secs(3600)).unwrap();
    assert_eq!(count, 3);
    assert_eq!(
        std::fs::read_to_string(spool.join(RESUME_FILE)).unwrap(),
        "c2"
    );
    // The reader stored and removed the segment; a restart opened an empty one.
    for index in live_stream::segments(&spool).unwrap() {
        std::fs::remove_file(spool.join(format!("{index:012}.ndjson"))).unwrap();
    }
    SpoolWriter::open(&spool, Rotation::default()).unwrap();
    assert_eq!(resume_cursor(&spool).unwrap().as_deref(), Some("c2"));
    // A segment holding a newer entry wins over the resume file.
    let mut writer = SpoolWriter::open(&spool, Rotation::default()).unwrap();
    writer
        .append(Timestamp::now(), json!({"__CURSOR": "c3"}).to_string().as_bytes())
        .unwrap();
    writer.sync().unwrap();
    assert_eq!(resume_cursor(&spool).unwrap().as_deref(), Some("c3"));
    std::fs::remove_dir_all(&spool).unwrap();
}

#[devkit::test]
fn a_spool_with_nothing_resumes_from_the_start() {
    let spool = temporary_spool();
    assert_eq!(resume_cursor(&spool).unwrap(), None);
    SpoolWriter::open(&spool, Rotation::default()).unwrap();
    assert_eq!(resume_cursor(&spool).unwrap(), None);
    std::fs::remove_dir_all(&spool).unwrap();
}

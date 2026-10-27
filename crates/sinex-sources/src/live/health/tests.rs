use super::*;
use serde_json::json;
use sinex_source_core::drivers::live_stream::{Rotation, SpoolWriter};

#[devkit::test]
fn counters_follow_capture_and_acknowledgments_without_reading_the_spool() {
    let dir = tempfile::tempdir().unwrap();
    let spool = dir.path().join("spool");
    let positions = dir.path().join("positions");
    let (_, binding) = super::super::binding("hyprland", &spool, "synthetic", Value::Null).unwrap();
    std::fs::create_dir_all(&spool).unwrap();
    let counter = initialize(&positions, &binding).unwrap();
    let mode = sinex_transport::private_mode::PrivateMode::new(dir.path().join("private"));
    let mut writer = SpoolWriter::open(&spool, Rotation::default())
        .unwrap()
        .with_private_mode(mode);
    writer
        .append(sinex_model::Timestamp::now(), b"workspacev2>>1,a")
        .unwrap();
    let first_bytes = std::fs::metadata(spool.join("000000000000.ndjson"))
        .unwrap()
        .len();
    writer
        .append(sinex_model::Timestamp::now(), b"workspacev2>>2,b")
        .unwrap();
    writer.sync().unwrap();
    let total_bytes = std::fs::metadata(spool.join("000000000000.ndjson"))
        .unwrap()
        .len();
    let cursor = json!({"segment":0,"inner":{"offset":first_bytes}});
    counter.lock().unwrap().prepare(&cursor, 1);
    counter.lock().unwrap().recovered(&cursor);
    // Health reads only counters, even if the spool cannot be opened.
    std::fs::rename(&spool, dir.path().join("moved-spool")).unwrap();
    snapshot(&positions, &[(binding, counter)]).unwrap();
    let report: Value =
        serde_json::from_slice(&std::fs::read(dir.path().join("health.json")).unwrap()).unwrap();
    assert_eq!(
        report["spools"][0]["unpublished"],
        json!({"bytes":total_bytes-first_bytes,"records":1,"frames":0})
    );
    assert!(report["observed_at"].is_string());
}

#[devkit::test]
fn media_counters_include_empty_files_and_sidecars_and_exclude_acknowledged_segments() {
    let dir = tempfile::tempdir().unwrap();
    let spool = dir.path().join("spool");
    std::fs::create_dir(&spool).unwrap();
    let (_, binding) =
        super::super::binding("media", &spool, "synthetic", json!({"kind":"screen"})).unwrap();
    let counter = initialize(&dir.path().join("positions"), &binding).unwrap();
    for stem in ["1-2.screen", "3-4.screen"] {
        let partial = spool.join("capture.partial");
        let bytes: &[u8] = if stem == "1-2.screen" { b"" } else { b"synthetic image" };
        std::fs::write(&partial, bytes).unwrap();
        sinex_source_core::drivers::media::publish_segment(
            &spool,
            &partial,
            stem,
            "png",
            &json!({}),
        )
        .unwrap();
    }
    let cursor = json!({"last":"1-2.screen"});
    let mut counter = counter.lock().unwrap();
    counter.prepare(&cursor, 0);
    counter.recovered(&cursor);
    assert_eq!(counter.counts.bytes, 17);
    assert_eq!(counter.counts.records, 1);
}

#[devkit::test]
fn startup_counts_existing_bytes_and_rejects_corrupt_positions() {
    let dir = tempfile::tempdir().unwrap();
    let spool = dir.path().join("spool");
    let positions = dir.path().join("positions");
    let (_, binding) = super::super::binding("players", &spool, "synthetic", Value::Null).unwrap();
    std::fs::create_dir(&spool).unwrap();
    std::fs::write(spool.join("000000000000.ndjson"), b"synthetic\n").unwrap();
    let counter = initialize(&positions, &binding).unwrap();
    assert_eq!(counter.lock().unwrap().counts.bytes, 10);
    assert_eq!(counter.lock().unwrap().counts.records, 1);
    Position::default().save(&positions, &binding.name).unwrap();
    let file = std::fs::read_dir(&positions)
        .unwrap()
        .next()
        .unwrap()
        .unwrap()
        .path();
    std::fs::write(file, b"not JSON").unwrap();
    assert!(
        initialize(&positions, &binding)
            .err()
            .unwrap()
            .contains("state file")
    );
}

#[devkit::test]
fn a_binding_named_health_cannot_overwrite_the_report() {
    let dir = tempfile::tempdir().unwrap();
    let positions = dir.path().join("positions");
    Position::default().save(&positions, "health").unwrap();
    snapshot(&positions, &[]).unwrap();
    assert!(
        Position::load(&positions, "health")
            .unwrap()
            .cursor
            .is_null()
    );
    assert!(dir.path().join("health.json").exists());
}

#[devkit::test]
fn startup_counts_an_unacknowledged_batch_once_and_then_counts_new_capture() {
    let dir = tempfile::tempdir().unwrap();
    let spool = dir.path().join("spool");
    let positions = dir.path().join("positions");
    std::fs::create_dir(&spool).unwrap();
    std::fs::write(spool.join("000000000000.ndjson"), b"one\ntwo\npartial").unwrap();
    let (_, binding) = super::super::binding("hyprland", &spool, "synthetic", Value::Null).unwrap();
    let before = json!({"segment":0,"inner":{"offset":4}});
    let after = json!({"segment":0,"inner":{"offset":8}});
    Position {
        cursor: before,
        carry: Value::Null,
    }
    .save(&positions, &binding.name)
    .unwrap();
    let path = std::fs::read_dir(&positions)
        .unwrap()
        .next()
        .unwrap()
        .unwrap()
        .path()
        .with_extension("unacknowledged.json");
    std::fs::write(
        path,
        serde_json::to_vec(&json!({"frames":[],"position":{"cursor":after,"carry":null}})).unwrap(),
    )
    .unwrap();
    let counter = initialize(&positions, &binding).unwrap();
    let mut counter = counter.lock().unwrap();
    assert_eq!((counter.counts.bytes, counter.counts.records), (11, 1));
    counter.recovered(&after);
    assert_eq!((counter.counts.bytes, counter.counts.records), (7, 0));
    counter.captured_line(1, 10);
    let next = json!({"segment":1,"inner":{"offset":10}});
    counter.prepare(&next, 1);
    counter.recovered(&next);
    assert_eq!((counter.counts.bytes, counter.counts.records), (0, 0));
}

#[devkit::test]
fn a_native_spool_name_does_not_stop_health_reporting() {
    use std::os::unix::ffi::OsStringExt;
    let dir = tempfile::tempdir().unwrap();
    let spool = dir
        .path()
        .join(std::ffi::OsString::from_vec(b"spool-\xff".to_vec()));
    let positions = dir.path().join("positions");
    std::fs::create_dir(&spool).unwrap();
    let (_, binding) = super::super::binding("hyprland", &spool, "synthetic", Value::Null).unwrap();
    let counter = initialize(&positions, &binding).unwrap();
    snapshot(&positions, &[(binding, counter)]).unwrap();
    let report: Value =
        serde_json::from_slice(&std::fs::read(dir.path().join("health.json")).unwrap()).unwrap();
    assert_eq!(
        report["spools"][0]["spool"],
        spool.to_string_lossy().as_ref()
    );
}

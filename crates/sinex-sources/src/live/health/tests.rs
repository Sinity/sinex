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
fn media_counters_include_sidecars_and_exclude_acknowledged_segments() {
    let dir = tempfile::tempdir().unwrap();
    let spool = dir.path().join("spool");
    std::fs::create_dir(&spool).unwrap();
    let (_, binding) =
        super::super::binding("media", &spool, "synthetic", json!({"kind":"screen"})).unwrap();
    let counter = initialize(&dir.path().join("positions"), &binding).unwrap();
    for stem in ["1-2.screen", "3-4.screen"] {
        let partial = spool.join("capture.partial");
        std::fs::write(&partial, b"synthetic image").unwrap();
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
    counter.prepare(&cursor, 1);
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

use super::*;
use serde_json::json;
use sinex_source_core::drivers::live_stream::{Rotation, SpoolWriter};

#[devkit::test]
fn report_counts_only_records_after_the_acknowledged_position() {
    let dir = tempfile::tempdir().unwrap();
    let spool = dir.path().join("spool");
    let positions = dir.path().join("positions");
    let (_, binding) = super::super::binding("hyprland", &spool, "synthetic", Value::Null).unwrap();
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
    Position {
        cursor: json!({"segment": 0, "inner": {"offset": first_bytes}}),
        carry: Value::Null,
    }
    .save(&positions, &binding.name)
    .unwrap();
    snapshot(&positions, &[binding]).unwrap();
    let report: Value =
        serde_json::from_slice(&std::fs::read(positions.join("health.json")).unwrap()).unwrap();
    assert_eq!(
        report["spools"][0]["unpublished"],
        json!({"bytes": total_bytes - first_bytes, "records": 1, "frames": 0})
    );
    assert!(report["observed_at"].is_string());
}

#[devkit::test]
fn media_backlog_excludes_committed_segments() {
    let dir = tempfile::tempdir().unwrap();
    let (_, binding) =
        super::super::binding("media", dir.path(), "synthetic", json!({"kind":"screen"})).unwrap();
    for stem in ["1-2.screen", "3-4.screen"] {
        std::fs::write(dir.path().join(format!("{stem}.png")), b"synthetic image").unwrap();
        std::fs::write(dir.path().join(format!("{stem}.json")), b"{}").unwrap();
    }
    assert_eq!(
        backlog(&binding, &json!({"last":"1-2.screen"})).unwrap(),
        json!({"bytes":15,"records":1})
    );
}

#[devkit::test]
fn a_player_uses_line_backlog_and_a_corrupt_position_stops_reporting() {
    let dir = tempfile::tempdir().unwrap();
    let spool = dir.path().join("spool");
    let positions = dir.path().join("positions");
    let (_, binding) = super::super::binding("players", &spool, "synthetic", Value::Null).unwrap();
    std::fs::create_dir(&spool).unwrap();
    std::fs::write(spool.join("000000000000.ndjson"), b"synthetic\n").unwrap();
    assert_eq!(
        backlog(&binding, &Value::Null).unwrap(),
        json!({"bytes":10,"records":1})
    );
    Position::default().save(&positions, &binding.name).unwrap();
    let position_file = std::fs::read_dir(&positions)
        .unwrap()
        .next()
        .unwrap()
        .unwrap()
        .path();
    std::fs::write(position_file, b"not JSON").unwrap();
    assert!(
        snapshot(&positions, &[binding])
            .unwrap_err()
            .contains("state file")
    );
}

use super::*;
use serde_json::json;
use sinex_source_core::drivers::{media, producer::Position};

fn media_binding(spool: &Path) -> Binding {
    Binding::of(
        &crate::sources::media_capture::SPEC,
        spool,
        Settings::new("synthetic-host"),
    )
}

#[devkit::test]
fn latched_media_seals_its_original_generation_while_new_capture_continues() {
    let spool = tempfile::tempdir().unwrap();
    let original = spool.path().join(".100.wav.partial");
    std::fs::write(&original, b"original captured audio generation").unwrap();
    let binding = media_binding(spool.path());
    let (_watcher, receive, latched) = latch_media(&[binding]).unwrap();
    assert_eq!(latched.values().next().unwrap(), &["100"]);
    // A later acquisition starts before backup finishes. It is outside the
    // latched generation and must not extend this cut's wait.
    std::fs::write(spool.path().join(".200.wav.partial"), b"new live capture").unwrap();
    media::publish_segment(
        spool.path(),
        &original,
        "100-150.audio",
        "wav",
        &json!({"started_ns":"100","ended_ns":"150","kind":"audio"}),
    )
    .unwrap();
    wait_media(&latched, &receive, &AtomicBool::new(false)).unwrap();
    assert!(spool.path().join(".200.wav.partial").is_file());
    let ready = media::complete(spool.path()).unwrap();
    assert_eq!(ready.len(), 1);
    assert_eq!(
        std::fs::read(&ready[0].1).unwrap(),
        b"original captured audio generation"
    );
}

#[devkit::test]
fn failed_or_cancelled_latched_media_remains_unavailable() {
    let spool = tempfile::tempdir().unwrap();
    let original = spool.path().join(".100.wav.partial");
    std::fs::write(&original, b"unsealed capture").unwrap();
    let (_watcher, receive, latched) = latch_media(&[media_binding(spool.path())]).unwrap();
    assert!(
        wait_media(&latched, &receive, &AtomicBool::new(true))
            .unwrap_err()
            .contains("cancelled")
    );
    std::fs::remove_file(original).unwrap();
    assert!(
        wait_media(&latched, &receive, &AtomicBool::new(false))
            .unwrap_err()
            .contains("disappeared")
    );
}

#[devkit::test]
fn native_cut_restores_encoded_position_carry_and_original_spool_bytes() {
    let root = tempfile::tempdir().unwrap();
    let positions = root.path().join("positions");
    let spool = root.path().join("spool");
    std::fs::create_dir(&spool).unwrap();
    std::fs::write(spool.join("resume-cursor"), b"original-journal-cursor").unwrap();
    std::fs::write(
        spool.join("000000000000.ndjson"),
        b"original complete record\n",
    )
    .unwrap();
    let binding = Binding::of(
        &crate::sources::journald::SPEC,
        &spool,
        Settings::new("synthetic-host"),
    );
    let original = Position {
        cursor: json!({"segment":0,"inner":{"offset":0}}),
        carry: json!({"unfinished":"parser state"}),
        capture_failures: Vec::new(),
    };
    original.save(&positions, &binding.name).unwrap();
    let captured = capture(
        Some(&positions),
        &[binding.clone()],
        &AtomicBool::new(false),
    )
    .unwrap();
    Position {
        cursor: json!({"segment":9}),
        ..Position::default()
    }
    .save(&positions, &binding.name)
    .unwrap();
    std::fs::remove_file(spool.join("000000000000.ndjson")).unwrap();
    let restored =
        Position::load(&captured.directory.path().join("positions"), &binding.name).unwrap();
    assert_eq!(restored.cursor, original.cursor);
    assert_eq!(restored.carry, original.carry);
    assert_eq!(
        std::fs::read(captured.directory.path().join("spool/0/resume-cursor")).unwrap(),
        b"original-journal-cursor"
    );
    assert_eq!(
        std::fs::read(
            captured
                .directory
                .path()
                .join("spool/0/000000000000.ndjson")
        )
        .unwrap(),
        b"original complete record\n"
    );
}

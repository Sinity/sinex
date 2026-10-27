use super::*;
use sinex_model::{Class, MaterialId, ReadingOrder, RecordOutcome};
use std::sync::mpsc;

fn gate(dir: &Path, mode: PrivateMode) -> (Gate, mpsc::Receiver<Gap>) {
    let (send, receive) = mpsc::channel();
    let gate = Gate::open(dir, "test-stream", mode, move |gap| {
        send.send(gap).map_err(io::Error::other)
    })
    .unwrap();
    (gate, receive)
}

#[devkit::test]
fn every_check_reads_the_switch_and_records_one_gap() {
    let dir = tempfile::tempdir().unwrap();
    let mode = PrivateMode::new(dir.path().join("switch"));
    let (gate, gaps) = gate(&dir.path().join("spool"), mode.clone());
    assert!(gate.allowed().unwrap());
    let old = gate.permit().unwrap();
    mode.set(true).unwrap();
    assert!(!gate.allowed().unwrap());
    assert!(!gate.accepts(old).unwrap());
    assert!(!gate.allowed().unwrap());
    mode.set(false).unwrap();
    assert!(gate.allowed().unwrap());
    assert!(!gate.accepts(old).unwrap());
    let gap = gaps.recv_timeout(Duration::from_secs(2)).unwrap();
    assert_eq!(gap.stream, "test-stream");
    assert!(gap.end >= gap.start);
    assert!(gaps.try_recv().is_err());
}

#[devkit::test]
fn unreadable_switch_blocks_and_reports_stream_and_cause() {
    let dir = tempfile::tempdir().unwrap();
    let file = dir.path().join("file");
    std::fs::write(&file, b"not a directory").unwrap();
    let (gate, _) = gate(
        &dir.path().join("spool"),
        PrivateMode::new(file.join("switch")),
    );
    assert!(!gate.allowed().unwrap());
    let error = gate.health().error.unwrap();
    assert!(error.contains("test-stream"));
    assert!(error.contains("cannot check"));
    let health: Health = serde_json::from_slice(
        &std::fs::read(dir.path().join("spool/.capture-health.json")).unwrap(),
    )
    .unwrap();
    assert_eq!(health.error, Some(error));
}

#[devkit::test]
fn quiet_stream_and_shutdown_still_write_a_gap() {
    let dir = tempfile::tempdir().unwrap();
    let mode = PrivateMode::new(dir.path().join("switch"));
    let (gate, gaps) = gate(&dir.path().join("spool"), mode.clone());
    mode.set(true).unwrap();
    // Wait for the gate's persisted start, without sending captured records.
    let until = std::time::Instant::now() + Duration::from_secs(2);
    while !dir.path().join("spool/.private-gap.json").exists() {
        assert!(std::time::Instant::now() < until);
        std::thread::yield_now();
    }
    drop(gate);
    assert_eq!(
        gaps.recv_timeout(Duration::from_secs(2)).unwrap().stream,
        "test-stream"
    );
}

#[devkit::test]
fn a_crash_saved_gap_is_finished_after_restart() {
    let dir = tempfile::tempdir().unwrap();
    let spool = dir.path().join("spool");
    std::fs::create_dir_all(&spool).unwrap();
    let started = Timestamp::from_unix_secs(10).unwrap();
    std::fs::write(
        spool.join(".private-gap.json"),
        serde_json::to_vec(&Gap {
            start: Some(started),
            end: None,
            stream: "test-stream".into(),
            reason: "private_mode".into(),
        })
        .unwrap(),
    )
    .unwrap();
    let (_gate, gaps) = gate(&spool, PrivateMode::new(dir.path().join("switch")));
    let saved = gaps.recv_timeout(Duration::from_secs(2)).unwrap();
    assert_eq!(saved.start, Some(started));
    assert_eq!(saved.end, None);
    assert!(!spool.join(".private-gap.json").exists());
}

#[devkit::test]
fn active_recorder_is_interrupted_and_its_result_is_discarded() {
    let dir = tempfile::tempdir().unwrap();
    let mode = PrivateMode::new(dir.path().join("switch"));
    let (gate, _gaps) = gate(&dir.path().join("spool"), mode.clone());
    let parent = crate::capture::CaptureStop::new(Some(Duration::from_secs(5)));
    let result = gate
        .record(&parent, |recording| {
            mode.set(true)?;
            assert!(recording.wait(Duration::from_secs(2)));
            Ok(b"private segment")
        })
        .unwrap();
    assert!(result.is_none());
}

#[devkit::test]
fn gap_records_drain_as_capture_gap_without_calling_the_source_parser() {
    use crate::drivers::live_stream::{LiveStream, Rotation, SpoolWriter};
    let dir = tempfile::tempdir().unwrap();
    let spool = dir.path().join("spool");
    let mode = PrivateMode::new(dir.path().join("switch"));
    let mut writer = SpoolWriter::open(&spool, Rotation::default())
        .unwrap()
        .with_private_mode(mode.clone());
    mode.set(true).unwrap();
    writer.append(Timestamp::now(), b"private content").unwrap();
    mode.set(false).unwrap();
    writer.sync().unwrap();
    let live = LiveStream {
        spool,
        input: sinex_model::InputRef {
            kind: "socket".into(),
            locator: "test".into(),
            host: "host".into(),
        },
    };
    let material = live
        .poll(&serde_json::Value::Null)
        .unwrap()
        .material
        .unwrap();
    assert!(!String::from_utf8_lossy(&material.bytes).contains("private content"));
    struct Never;
    impl crate::Parser for Never {
        type Record<'a> = &'a [u8];
        type Carry = ();
        type Payload = Gap;
        fn parse(
            &self,
            _: &&[u8],
            _: &(),
            _: &crate::RecordContext<'_>,
        ) -> Result<(Vec<Gap>, ()), crate::RecordError> {
            panic!("source parser must not see gap metadata")
        }
    }
    let chunks = crate::drain(
        &Never,
        crate::DrainInput {
            source: "test",
            class: Class::Open,
            material: MaterialId::new(),
            bytes: material.bytes.as_slice().into(),
            records: &material.records,
            carry: serde_json::Value::Null,
            reading_order: &|_| ReadingOrder(0),
            limits: crate::Limits::default(),
            manifest: &material.manifest,
        },
    )
    .unwrap();
    let RecordOutcome::Events { events, .. } = &chunks[0].outcomes[0] else {
        panic!("expected gap event")
    };
    assert_eq!(events[0].event_type, "capture.gap");
    assert_eq!(events[0].payload["reason"], "private_mode");
    assert!(material.manifest.details.contains_key("capture_records"));
}

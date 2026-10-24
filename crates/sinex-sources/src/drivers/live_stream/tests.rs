use super::*;

fn dir() -> PathBuf {
    std::env::temp_dir().join(format!("sinex-live-{}", uuid::Uuid::now_v7()))
}

fn stream(spool: &Path) -> LiveStream {
    LiveStream {
        spool: spool.to_owned(),
        input: InputRef {
            kind: "socket".into(),
            locator: "test".into(),
            host: "h".into(),
        },
    }
}

fn lines_of(a: &Acquired) -> Vec<String> {
    let m = a.material.as_ref().unwrap();
    m.records
        .iter()
        .map(|(s, e)| {
            let record: Map<String, Value> = serde_json::from_slice(&m.bytes[*s..*e]).unwrap();
            String::from_utf8(received(&record).unwrap().1).unwrap()
        })
        .collect()
}

fn end_cursor(a: &Acquired) -> Value {
    let m = a.material.as_ref().unwrap();
    (m.cursor_at)(m.bytes.len()).unwrap()
}

#[test]
fn captured_lines_are_acquired_once_in_order() {
    let spool = dir();
    let mut writer = SpoolWriter::open(&spool, Rotation::default()).unwrap();
    let n = capture_lines(&b"a>>1\nb>>2\nc>>3"[..], &mut writer, Duration::ZERO).unwrap();
    assert_eq!(n, 3);
    let live = stream(&spool);
    let first = live.poll(&Value::Null).unwrap();
    assert_eq!(lines_of(&first), vec!["a>>1", "b>>2", "c>>3"]);
    let cursor = end_cursor(&first);
    let again = live.poll(&cursor).unwrap();
    assert!(again.material.is_none());
}

/// L25: while private mode is on a live capture writes nothing, and after
/// it ends capture resumes; the lines in between were never stored, which
/// is what private mode means, not a loss.
#[test]
fn private_mode_drops_lines_while_on() {
    let spool = dir();
    let switch = sinex_transport::private_mode::PrivateMode::new(dir().join("private-mode"));
    let mut writer = SpoolWriter::open(&spool, Rotation::default())
        .unwrap()
        .with_private_mode(switch.clone());
    capture_lines(&b"before\n"[..], &mut writer, Duration::ZERO).unwrap();
    switch.set(true).unwrap();
    std::thread::sleep(Duration::from_millis(300));
    capture_lines(
        &b"private one\nprivate two\n"[..],
        &mut writer,
        Duration::ZERO,
    )
    .unwrap();
    assert_eq!(writer.dropped, 2);
    switch.set(false).unwrap();
    std::thread::sleep(Duration::from_millis(300));
    capture_lines(&b"after\n"[..], &mut writer, Duration::ZERO).unwrap();
    let got = lines_of(&stream(&spool).poll(&Value::Null).unwrap());
    assert_eq!(got, vec!["before", "after"]);
}

#[test]
fn parts_continue_across_segments() {
    let spool = dir();
    let rotation = Rotation {
        max_bytes: 1,
        max_age: Duration::from_secs(3600),
    };
    let mut writer = SpoolWriter::open(&spool, rotation).unwrap();
    writer.append(Timestamp::now(), b"one").unwrap();
    writer.sync().unwrap();
    let live = stream(&spool);
    let first = live.poll(&Value::Null).unwrap();
    let m1 = first.material.as_ref().unwrap();
    assert_eq!((m1.part_index, m1.previous_part), (0, None));
    let cursor = end_cursor(&first);

    writer.append(Timestamp::now(), b"two").unwrap(); // rotates first
    writer.append(Timestamp::now(), b"three").unwrap();
    writer.sync().unwrap();
    assert_eq!(segments(&spool).unwrap().len(), 3);

    let second = live.poll(&cursor).unwrap();
    assert_eq!(lines_of(&second), vec!["two"]);
    let m2 = second.material.as_ref().unwrap();
    assert_eq!(m2.part_index, 1);
    assert_eq!(m2.previous_part, Some(m1.id));
    let cursor = end_cursor(&second);

    let third = live.poll(&cursor).unwrap();
    assert_eq!(lines_of(&third), vec!["three"]);
    let m3 = third.material.as_ref().unwrap();
    assert_eq!((m3.part_index, m3.previous_part), (2, Some(m2.id)));
    let cursor = end_cursor(&third);
    assert!(live.poll(&cursor).unwrap().material.is_none());

    assert_eq!(live.remove_passed(&cursor).unwrap(), 2);
    assert_eq!(segments(&spool).unwrap(), vec![2]);
}

#[test]
fn bytes_that_are_not_utf8_survive() {
    let record: Map<String, Value> =
        serde_json::from_slice(&frame(Timestamp::now(), &[0xff, b'a'])).unwrap();
    assert_eq!(received(&record).unwrap().1, vec![0xff, b'a']);
}

#[test]
fn a_restarted_capture_starts_a_new_segment() {
    let spool = dir();
    let mut w = SpoolWriter::open(&spool, Rotation::default()).unwrap();
    w.append(Timestamp::now(), b"x").unwrap();
    drop(w);
    // A partial line left by a crash stays where it is.
    let mut f = OpenOptions::new()
        .append(true)
        .open(segment_path(&spool, 0))
        .unwrap();
    f.write_all(b"{\"partial").unwrap();
    let w = SpoolWriter::open(&spool, Rotation::default()).unwrap();
    assert_eq!(w.segment(), 1);
}

#[test]
fn empty_spool_is_unchanged_not_an_error() {
    let spool = dir();
    std::fs::create_dir_all(&spool).unwrap();
    let live = stream(&spool);
    let a = live.poll(&Value::Null).unwrap();
    assert!(a.material.is_none());
    assert!(a.observation.unavailable.is_none() && !a.observation.changed);
    // Nor is a spool capture has not created yet.
    let missing = stream(&dir()).poll(&Value::Null).unwrap();
    assert!(missing.material.is_none() && missing.observation.unavailable.is_none());
}

#[test]
fn first_capture_after_an_empty_segment_poll_survives_restart() {
    let dir = tempfile::tempdir().unwrap();
    let mut writer = SpoolWriter::open(dir.path(), Rotation::default()).unwrap();
    let live = stream(dir.path());
    let first = live.poll(&Value::Null).unwrap();
    assert!(first.material.is_none());
    let saved: Value =
        serde_json::from_slice(&serde_json::to_vec(&first.cursor_unchanged).unwrap()).unwrap();
    assert!(live.poll(&saved).unwrap().material.is_none());
    writer
        .append(Timestamp::now(), b"first synthetic line")
        .unwrap();
    writer.sync().unwrap();
    let next = live.poll(&saved).unwrap();
    assert_eq!(lines_of(&next), ["first synthetic line"]);
    assert!(live.poll(&end_cursor(&next)).unwrap().material.is_none());
}

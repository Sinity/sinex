use super::*;

/// Bounds small enough for a test to reach.
pub(crate) const SMALL: TailBounds = TailBounds {
    bytes: 4096,
    records: 40,
};
use std::io::Write;

fn temp(name: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("sinex-append-{}", uuid::Uuid::now_v7()));
    std::fs::create_dir_all(&dir).unwrap();
    dir.join(name)
}

fn records(a: &Acquired) -> Vec<String> {
    let m = a.material.as_ref().unwrap();
    m.records
        .iter()
        .map(|(s, e)| String::from_utf8(crate::test_support::body(m)[*s..*e].to_vec()).unwrap())
        .collect()
}

#[devkit::test]
fn a_cursor_between_two_batches_reads_on_though_the_file_is_unchanged() {
    let path = temp("h");
    std::fs::write(&path, b"a\nb\nc\n").unwrap();
    let d = AppendFile {
        path: path.clone(),
        host: "h".into(),
        framer: crate::Framer::incremental(lines),
    };
    let first = d.poll(&Value::Null, &mut AppendState::default()).unwrap();
    let m = first.material.unwrap();
    // The first batch (one record) committed; the process died before the next.
    let between = (m.cursor_at)(2).unwrap();
    let after = (m.cursor_at)(crate::test_support::body(&m).len()).unwrap();
    let again = d.poll(&between, &mut AppendState::default()).unwrap();
    assert_eq!(records(&again), vec!["b\n", "c\n"]);
    assert!(
        d.poll(&after, &mut AppendState::default())
            .unwrap()
            .material
            .is_none(),
        "a cursor covering the whole read skips the unchanged file"
    );
}

#[devkit::test]
fn appends_read_only_new_complete_records() {
    let path = temp("h");
    std::fs::write(&path, b"a\nb\npart").unwrap();
    let d = AppendFile {
        path: path.clone(),
        host: "h".into(),
        framer: crate::Framer::incremental(lines),
    };
    let mut state = AppendState::default();
    let first = d.poll(&Value::Null, &mut state).unwrap();
    assert_eq!(records(&first), vec!["a\n", "b\n"]);
    let m = first.material.as_ref().unwrap();
    let cursor = (m.cursor_at)(crate::test_support::body(m).len()).unwrap();
    assert_eq!(cursor["offset"], json!(4));

    // Unchanged stat: nothing read.
    let same = d.poll(&cursor, &mut state).unwrap();
    assert!(same.material.is_none() && !same.observation.changed);

    std::fs::OpenOptions::new()
        .append(true)
        .open(&path)
        .unwrap()
        .write_all(b"ial\nc\n")
        .unwrap();
    let next = d.poll(&cursor, &mut state).unwrap();
    assert_eq!(next.revision, RevisionChange::Appended);
    assert_eq!(records(&next), vec!["partial\n", "c\n"]);
    let n = next.material.as_ref().unwrap();
    assert_eq!(n.part_index, 1);
    assert_eq!(n.previous_part, Some(m.id));
    assert_eq!((n.reading_order)(0), sinex_model::ReadingOrder(4));
    assert!(!n.aligned);
}

#[devkit::test]
fn a_rewrite_reads_only_records_the_previous_revision_did_not_hold() {
    let path = temp("h");
    std::fs::write(&path, b"a\nb\nc\n").unwrap();
    let d = AppendFile {
        path: path.clone(),
        host: "h".into(),
        framer: crate::Framer::incremental(lines),
    };
    let mut state = AppendState::default();
    let first = d.poll(&Value::Null, &mut state).unwrap();
    let m = first.material.unwrap();
    let cursor = (m.cursor_at)(crate::test_support::body(&m).len()).unwrap();

    // zsh expired the duplicate "b" and appended "dd": a rewrite.
    std::fs::write(&path, b"a\nc\ndd\n").unwrap();
    // A restart in between: the state is rebuilt from the stored bytes.
    let mut rebuilt = AppendState::rebuild(
        [(m.id, crate::test_support::body(&m))],
        crate::Framer::incremental(lines),
    );
    assert_eq!(rebuilt, state);
    let again = d.poll(&cursor, &mut rebuilt).unwrap();
    assert_eq!(again.revision, RevisionChange::Rewritten);
    assert_eq!(records(&again), vec!["dd\n"]);
    let n = again.material.as_ref().unwrap();
    assert!(n.aligned);
    assert_eq!(
        n.part_index,
        m.part_index + 1,
        "parser parts continue across file revisions"
    );
    assert_eq!(n.previous_part, Some(m.id));
    assert_eq!(
        n.removed,
        vec![RecordLocation {
            material: m.id,
            anchor: 2
        }],
        "the dropped \"b\" is reported where it was read"
    );
    assert_eq!(n.manifest.records, Some(vec![(4, 7)]));
    assert_eq!(
        crate::drivers::stored_records(
            &n.manifest,
            crate::test_support::body(n),
            crate::Framer::incremental(lines)
        ),
        vec![(4, 7)]
    );
    // The cursor covers the whole rewritten file, skipped records included.
    let after = (n.cursor_at)(crate::test_support::body(n).len()).unwrap();
    assert_eq!(after["offset"], json!(7));
    assert!(d.poll(&after, &mut rebuilt).unwrap().material.is_none());
}

#[devkit::test]
fn rewritten_prefix_is_read_again_as_a_new_revision() {
    let path = temp("h");
    std::fs::write(&path, b"a\nb\n").unwrap();
    let d = AppendFile {
        path: path.clone(),
        host: "h".into(),
        framer: crate::Framer::incremental(lines),
    };
    let mut state = AppendState::default();
    let first = d.poll(&Value::Null, &mut state).unwrap();
    let m = first.material.unwrap();
    let cursor = (m.cursor_at)(crate::test_support::body(&m).len()).unwrap();
    std::fs::write(&path, b"x\ny\nz\n").unwrap();
    let again = d.poll(&cursor, &mut state).unwrap();
    assert_eq!(again.revision, RevisionChange::Rewritten);
    assert_eq!(records(&again), vec!["x\n", "y\n", "z\n"]);
    let n = again.material.unwrap();
    assert_eq!(n.part_index, m.part_index + 1);
    assert_eq!(n.previous_part, Some(m.id));
}

#[devkit::test]
fn missing_file_is_unavailable_not_empty() {
    let d = AppendFile {
        path: temp("absent"),
        host: "h".into(),
        framer: crate::Framer::incremental(lines),
    };
    let a = d.poll(&Value::Null, &mut AppendState::default()).unwrap();
    assert!(a.observation.unavailable.is_some());
    assert!(a.material.is_none());
}

/// A small part, so a small file spans many.
const PART_SIZE: u64 = 4096;

#[devkit::test]
fn a_file_larger_than_a_part_is_read_in_bounded_parts_covering_every_record() {
    let path = temp("h");
    let mut text = String::new();
    for i in 0..5000 {
        text.push_str(&format!("record-{i:05}\n"));
    }
    // One record longer than a part is read whole, never refused.
    text.push_str(&format!("{}\n", "y".repeat(3 * PART_SIZE as usize)));
    std::fs::write(&path, text.as_bytes()).unwrap();
    let d = AppendFile {
        path: path.clone(),
        host: "h".into(),
        framer: crate::Framer::incremental(lines),
    };
    let mut state = AppendState::default();
    let mut cursor = Value::Null;
    let mut read = Vec::new();
    let mut polls = 0;
    loop {
        let a = d.poll_in_parts(&cursor, &mut state, PART_SIZE).unwrap();
        let Some(m) = &a.material else { break };
        polls += 1;
        assert!(
            crate::test_support::body(m).len() as u64 <= PART_SIZE || m.records.len() == 1,
            "a part holds at most PART_SIZE bytes unless one record is longer"
        );
        read.extend(records(&a));
        cursor = (m.cursor_at)(crate::test_support::body(m).len()).unwrap();
    }
    assert!(polls > 10, "read in parts, not at once ({polls})");
    assert_eq!(read.concat(), text);
}

/// A part's bytes are in memory once: the material and its cursors share
/// them, and nothing holds room for more than was read.
#[devkit::test]
fn a_part_is_held_once() {
    let path = temp("once");
    let text: String = (0..2000).map(|i| format!("line {i}\n")).collect();
    std::fs::write(&path, text.as_bytes()).unwrap();
    let d = AppendFile {
        path,
        host: "h".into(),
        framer: crate::Framer::incremental(lines),
    };
    let a = d.poll(&Value::Null, &mut AppendState::default()).unwrap();
    let m = a.material.unwrap();
    assert!(m.bytes.is_empty());
    let Some(crate::drivers::StoredBytes::FramedMemory(bytes)) = &m.stored else {
        panic!("the bytes are shared, not copied: {:?}", m.stored)
    };
    assert_eq!(
        std::sync::Arc::strong_count(bytes),
        2,
        "the material and its cursors"
    );
    assert_eq!(bytes.capacity(), text.len());
}

#[devkit::test]
fn the_memory_keeps_a_bounded_tail_of_the_newest_records() {
    let mut state = AppendState::bounded(SMALL);
    let mut first = None;
    for m in 0..100 {
        let id = MaterialId::new();
        first.get_or_insert(id);
        for i in 0..10 {
            let mut r = vec![b'r'; 1000];
            r.extend_from_slice(format!("{m}-{i}\n").as_bytes());
            state.insert(&r, id, i * 1010);
        }
    }
    // 1000 records of ~1 KB: remembered by bytes, the newest only.
    assert!(state.bytes <= SMALL.bytes, "{} bytes kept", state.bytes);
    assert!(state.bytes + 1010 > SMALL.bytes);
    let first = first.unwrap();
    assert!(
        state
            .records
            .values()
            .flatten()
            .all(|l| l.material != first),
        "the oldest records are gone"
    );
    let remembered: usize = state
        .records
        .values()
        .map(|locations| locations.len())
        .sum();
    assert_eq!(remembered, state.order.len());
}

#[devkit::test]
fn short_records_are_bounded_by_count_within_one_material() {
    let mut state = AppendState::bounded(SMALL);
    let id = MaterialId::new();
    for i in 0..1000 {
        state.insert(format!("{i}\n").as_bytes(), id, i * 4);
    }
    // 1000 records of 2-4 bytes in one material: far under the byte tail,
    // remembered by count.
    assert_eq!(state.order.len(), SMALL.records);
    let remembered: usize = state
        .records
        .values()
        .map(|locations| locations.len())
        .sum();
    assert_eq!(remembered, SMALL.records);
    assert!(state.take(b"999\n"), "the newest record is remembered");
    assert!(!state.take(b"0\n"), "the oldest is not");
}

#[devkit::test]
fn rewrite_replace_and_truncate_keep_the_previous_parser_chain_and_reset_byte_offsets() {
    for change in [
        RevisionChange::Rewritten,
        RevisionChange::Replaced,
        RevisionChange::Truncated,
    ] {
        let path = temp("history");
        std::fs::write(&path, b"one-long-record\ntwo-long-record\n").unwrap();
        let driver = AppendFile {
            path: path.clone(),
            host: "h".into(),
            framer: Framer::incremental(lines),
        };
        let mut state = AppendState::default();
        let first = driver.poll(&Value::Null, &mut state).unwrap();
        let m = first.material.unwrap();
        let cursor = (m.cursor_at)(crate::test_support::body(&m).len()).unwrap();
        match change {
            RevisionChange::Rewritten => {
                std::fs::write(&path, b"two-long-record\none-long-record\nnew\n").unwrap()
            }
            RevisionChange::Replaced => {
                let replacement = path.with_extension("new");
                std::fs::write(&replacement, b"replacement\n").unwrap();
                std::fs::rename(replacement, &path).unwrap();
            }
            RevisionChange::Truncated => std::fs::write(&path, b"short\n").unwrap(),
            _ => unreachable!(),
        }
        let next = driver.poll(&cursor, &mut state).unwrap();
        assert_eq!(next.revision, change);
        let next = next.material.unwrap();
        assert_eq!(next.previous_part, Some(m.id));
        assert_eq!(next.part_index, m.part_index + 1);
        assert_eq!(next.manifest.file_offset, Some(0));
    }
}
#[devkit::test]
fn incomplete_rewrite_keeps_the_old_parser_cut_and_its_unrecorded_rewrite_kind() {
    let path = temp("history");
    std::fs::write(&path, b"old-long-record\n").unwrap();
    let driver = AppendFile {
        path: path.clone(),
        host: "h".into(),
        framer: Framer::incremental(lines),
    };
    let mut state = AppendState::default();
    let first = driver.poll(&Value::Null, &mut state).unwrap();
    let m = first.material.unwrap();
    let cursor = (m.cursor_at)(crate::test_support::body(&m).len()).unwrap();
    std::fs::write(&path, b"part").unwrap();
    let pending = driver.poll(&cursor, &mut state).unwrap();
    assert!(pending.material.is_none());
    let checkpoint: Cursor =
        serde_json::from_value(pending.cursor_without_material.clone()).unwrap();
    assert_eq!(checkpoint.material, Some(m.id));
    assert_eq!(checkpoint.through, crate::test_support::body(&m).len());
    assert_eq!(
        checkpoint.unrecorded_rewrite,
        Some(RevisionChange::Truncated)
    );
    std::fs::write(&path, b"part-completed\n").unwrap();
    let next = driver
        .poll(&pending.cursor_without_material, &mut state)
        .unwrap();
    assert_eq!(next.revision, RevisionChange::Truncated);
    let next = next.material.unwrap();
    assert_eq!(next.previous_part, Some(m.id));
    assert_eq!(next.part_index, m.part_index + 1);
}

#[devkit::test]
fn fresh_incomplete_files_emit_reusable_empty_prefix_cursors() {
    for initial in [b"".as_slice(), b"part"] {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("history");
        std::fs::write(&path, initial).unwrap();
        let driver = AppendFile {
            path: path.clone(),
            host: "h".into(),
            framer: Framer::incremental(lines),
        };
        let first = driver
            .poll(&Value::Null, &mut AppendState::default())
            .unwrap();
        assert!(first.material.is_none());
        let saved: Value =
            serde_json::from_slice(&serde_json::to_vec(&first.cursor_without_material).unwrap())
                .unwrap();
        assert_eq!(saved["offset"], 0);
        assert_eq!(saved["window"], blake3::hash(b"").to_hex().as_str());
        let unchanged = driver.poll(&saved, &mut AppendState::default()).unwrap();
        assert!(unchanged.material.is_none());
        std::fs::OpenOptions::new()
            .append(true)
            .open(&path)
            .unwrap()
            .write_all(b"-complete\n")
            .unwrap();
        let next = driver.poll(&saved, &mut AppendState::default()).unwrap();
        let mut expected = initial.to_vec();
        expected.extend_from_slice(b"-complete\n");
        assert_eq!(
            crate::test_support::body(next.material.as_ref().unwrap()),
            expected
        );
        assert_eq!(next.revision, RevisionChange::Appended);
        assert_eq!(next.material.as_ref().unwrap().part_index, 0);
        assert!(next.material.as_ref().unwrap().previous_part.is_none());
        let material = next.material.unwrap();
        let saved = (material.cursor_at)(crate::test_support::body(&material).len()).unwrap();
        assert!(
            driver
                .poll(&saved, &mut AppendState::default())
                .unwrap()
                .material
                .is_none()
        );
    }
}

#[devkit::test]
fn saved_unknown_prefixes_refuse_even_when_the_offset_is_zero() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("history");
    std::fs::write(&path, b"a\n").unwrap();
    let driver = AppendFile {
        path,
        host: "h".into(),
        framer: Framer::incremental(lines),
    };
    let first = driver
        .poll(&Value::Null, &mut AppendState::default())
        .unwrap();
    let material = first.material.unwrap();
    let saved = (material.cursor_at)(crate::test_support::body(&material).len()).unwrap();
    for offset in [0, 2] {
        for missing in [false, true] {
            let mut unknown = saved.clone();
            unknown["offset"] = offset.into();
            if missing {
                unknown.as_object_mut().unwrap().remove("window");
            } else {
                unknown["window"] = Value::Null;
            }
            let error = driver
                .poll(&unknown, &mut AppendState::default())
                .err()
                .unwrap();
            assert!(matches!(error, DriverError::Cursor(_)));
            assert!(error.to_string().contains("window"), "{error}");
        }
    }
}

/// What a generated write did to the file, and how the next look must class it.
#[derive(Clone, Copy, Debug, PartialEq)]
enum FileWrite {
    Append,
    /// A same-length edit of one line, in place.
    Edit,
    /// Cut to fewer lines, in place.
    Truncate,
    /// A new file renamed over the old one.
    Replace,
    /// The whole file written again, longer, in place.
    Rewrite,
}

/// Change classification under generated write histories: after every write,
/// one look classes it as the write was (an in-place same-length edit is a
/// rewrite, never an append or unchanged, however soon after the last look it
/// came), and every line the file now holds has been read.
#[devkit::test]
fn generated_write_histories_are_classified_and_nothing_is_missed() {
    let mut seed: u64 = 0x5eed_cafe;
    let mut next = move |n: u64| {
        seed ^= seed << 13;
        seed ^= seed >> 7;
        seed ^= seed << 17;
        seed % n
    };
    let mut fresh_id = 0u64;
    let mut line = || {
        fresh_id += 1;
        format!("line {fresh_id:08}\n")
    };
    for history in 0..10_000 {
        let path = temp(&format!("history-{history}"));
        let d = AppendFile {
            path: path.clone(),
            host: "h".into(),
            framer: crate::Framer::incremental(lines),
        };
        let mut content: Vec<String> = (0..1 + next(5)).map(|_| line()).collect();
        std::fs::write(&path, content.concat()).unwrap();
        let mut state = AppendState::default();
        let mut cursor = Value::Null;
        let mut read: std::collections::HashSet<String> = std::collections::HashSet::new();
        let look = |cursor: &mut Value,
                    state: &mut AppendState,
                    read: &mut std::collections::HashSet<String>|
         -> RevisionChange {
            let a = d.poll(cursor, state).unwrap();
            match &a.material {
                Some(m) => {
                    read.extend(records(&a));
                    *cursor = (m.cursor_at)(crate::test_support::body(m).len()).unwrap();
                    a.revision
                }
                None => {
                    *cursor = a.cursor_without_material.clone();
                    RevisionChange::Same
                }
            }
        };
        assert_eq!(
            look(&mut cursor, &mut state, &mut read),
            RevisionChange::Appended
        );
        for step in 0..if history < 60 { 25 } else { 5 } {
            let write = match next(5) {
                0 => FileWrite::Append,
                1 => FileWrite::Edit,
                2 if content.len() > 1 => FileWrite::Truncate,
                3 => FileWrite::Replace,
                4 => FileWrite::Rewrite,
                _ => FileWrite::Append,
            };
            match write {
                FileWrite::Append => {
                    let added: Vec<String> = (0..1 + next(3)).map(|_| line()).collect();
                    let mut f = std::fs::OpenOptions::new()
                        .append(true)
                        .open(&path)
                        .unwrap();
                    f.write_all(added.concat().as_bytes()).unwrap();
                    content.extend(added);
                }
                FileWrite::Edit => {
                    use std::os::unix::fs::FileExt;
                    let i = usize::try_from(next(content.len() as u64)).unwrap();
                    let at: usize = content[..i].iter().map(String::len).sum();
                    content[i] = line();
                    std::fs::OpenOptions::new()
                        .write(true)
                        .open(&path)
                        .unwrap()
                        .write_all_at(content[i].as_bytes(), at as u64)
                        .unwrap();
                }
                FileWrite::Truncate => {
                    let keep = 1 + usize::try_from(next(content.len() as u64 - 1)).unwrap();
                    content.truncate(keep);
                    std::fs::OpenOptions::new()
                        .write(true)
                        .open(&path)
                        .unwrap()
                        .set_len(content.concat().len() as u64)
                        .unwrap();
                }
                FileWrite::Replace => {
                    let i = usize::try_from(next(content.len() as u64)).unwrap();
                    content[i] = line();
                    let staged = path.with_extension("new");
                    std::fs::write(&staged, content.concat()).unwrap();
                    std::fs::rename(&staged, &path).unwrap();
                }
                FileWrite::Rewrite => {
                    let i = usize::try_from(next(content.len() as u64)).unwrap();
                    content[i] = line();
                    content.push(line());
                    let mut f = std::fs::OpenOptions::new()
                        .write(true)
                        .truncate(true)
                        .open(&path)
                        .unwrap();
                    f.write_all(content.concat().as_bytes()).unwrap();
                }
            }
            let class = look(&mut cursor, &mut state, &mut read);
            let expected = match write {
                FileWrite::Append => RevisionChange::Appended,
                FileWrite::Edit | FileWrite::Rewrite => RevisionChange::Rewritten,
                FileWrite::Truncate => RevisionChange::Truncated,
                FileWrite::Replace => RevisionChange::Replaced,
            };
            // A truncation whose kept lines were all read before reads
            // nothing new: no material, so no revision is recorded for it.
            let quiet = write == FileWrite::Truncate && class == RevisionChange::Same;
            assert!(
                class == expected || quiet,
                "history {history} step {step}: {write:?} classed as {class:?}"
            );
            let missed: Vec<&String> = content.iter().filter(|l| !read.contains(*l)).collect();
            assert!(
                missed.is_empty(),
                "history {history} step {step} after {write:?}: never read {missed:?}"
            );
            // A look straight after changes nothing.
            assert_eq!(
                look(&mut cursor, &mut state, &mut read),
                RevisionChange::Same
            );
        }
    }
}

/// A middle edit beyond the ordinary part size still changes the prefix.
#[devkit::test]
fn large_prefixes_are_hashed_whole_without_changing_the_digest_at_a_part_boundary() {
    let mut file = tempfile::tempfile().unwrap();
    let length = (64 << 20) + 8192;
    file.set_len(length).unwrap();
    let before = Prefix::read(&mut file, length).unwrap();
    file.seek(SeekFrom::Start(length / 2)).unwrap();
    file.write_all(b"changed").unwrap();
    let after = Prefix::read(&mut file, length).unwrap();
    assert_ne!(before.digest(), after.digest());
    file.seek(SeekFrom::End(0)).unwrap();
    file.write_all(b"appended").unwrap();
    assert_eq!(
        after.digest_after(b"appended"),
        Prefix::read(&mut file, length + 8).unwrap().digest(),
    );
}

static REWRITE_WHILE_FRAMING: std::sync::Mutex<Option<PathBuf>> = std::sync::Mutex::new(None);

fn rewrite_while_framing(bytes: &[u8]) -> (Vec<(usize, usize)>, usize) {
    if let Some(path) = REWRITE_WHILE_FRAMING.lock().unwrap().take() {
        std::fs::write(path, b"new\n").unwrap();
    }
    lines(bytes)
}

#[devkit::test]
fn a_rewrite_during_the_read_advances_nothing_and_is_read_on_the_next_poll() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("history");
    std::fs::write(&path, b"old\n").unwrap();
    *REWRITE_WHILE_FRAMING.lock().unwrap() = Some(path.clone());
    let driver = AppendFile {
        path,
        host: "test".into(),
        framer: Framer::incremental(rewrite_while_framing),
    };
    let mut state = AppendState::default();
    let busy = driver.poll(&Value::Null, &mut state).unwrap();
    assert!(busy.material.is_none());
    assert!(busy.observation.unavailable.unwrap().contains("changed"));
    assert_eq!(busy.cursor_without_material, Value::Null);
    assert!(state.is_empty());
    let next = driver.poll(&Value::Null, &mut state).unwrap();
    assert_eq!(records(&next), ["new\n"]);
}

/// Opt-in measurement on an existing input, opened read-only. Output contains
/// counts and timings only; neither the path nor source content is printed.
#[devkit::test]
#[ignore = "requires SINEX_APPEND_MEASURE_INPUT naming a live append-file input"]
fn measure_live_append_prefix() {
    use std::time::Instant;
    let path =
        std::env::var_os("SINEX_APPEND_MEASURE_INPUT").expect("measurement input is required");
    let mut file = std::fs::File::open(&path).unwrap();
    let size = file.metadata().unwrap().len();
    fn io_bytes() -> u64 {
        std::fs::read_to_string("/proc/self/io")
            .unwrap()
            .lines()
            .find_map(|line| line.strip_prefix("read_bytes: ")?.parse().ok())
            .unwrap()
    }
    fn peak_kib() -> u64 {
        std::fs::read_to_string("/proc/self/status")
            .unwrap()
            .lines()
            .find_map(|line| {
                line.strip_prefix("VmHWM:")?
                    .split_whitespace()
                    .next()?
                    .parse()
                    .ok()
            })
            .unwrap()
    }
    for (mode, passes) in [("sampled", 0), ("full", 1), ("full_racy", 2)] {
        let mut elapsed = Vec::new();
        let before_io = io_bytes();
        for _ in 0..3 {
            let started = Instant::now();
            if passes == 0 {
                let length = size.min(4096);
                file.seek(SeekFrom::Start(size - length)).unwrap();
                let mut bytes = vec![0; usize::try_from(length).unwrap()];
                file.read_exact(&mut bytes).unwrap();
                std::hint::black_box(blake3::hash(&bytes));
            } else {
                for _ in 0..passes {
                    std::hint::black_box(Prefix::read(&mut file, size).unwrap().digest());
                }
            }
            elapsed.push(started.elapsed().as_secs_f64() * 1000.0);
        }
        let physical_bytes = io_bytes() - before_io;
        println!(
            "append_prefix_measure {}",
            serde_json::json!({
                "mode": mode, "input_bytes": size, "bytes_per_poll": if passes == 0 {size.min(4096)} else {size * passes},
                "elapsed_ms": elapsed, "physical_read_bytes": physical_bytes, "peak_rss_kib": peak_kib(),
            })
        );
    }
    let facts = file_facts(std::path::Path::new(&path)).unwrap();
    let cursor = Cursor {
        offset: size,
        size,
        inode: facts.inode.unwrap(),
        mtime_ns: facts.mtime_ns.unwrap() - 1,
        ctime_ns: facts.ctime_ns.unwrap(),
        window: Some(Prefix::read(&mut file, size).unwrap().digest()),
        ..Default::default()
    };
    let driver = AppendFile {
        path: PathBuf::from(path),
        host: "measurement".into(),
        framer: crate::Framer::incremental(lines),
    };
    let before_io = io_bytes();
    let started = Instant::now();
    // An older stat forces verification without modifying the input. This
    // measures the real metadata-only poll, with no appended records.
    let acquired = driver
        .poll(
            &serde_json::to_value(cursor).unwrap(),
            &mut AppendState::default(),
        )
        .unwrap();
    let elapsed = started.elapsed().as_secs_f64() * 1000.0;
    assert!(acquired.material.is_none());
    println!(
        "append_prefix_measure {}",
        serde_json::json!({
            "mode": "metadata_poll", "input_bytes": size, "bytes_per_poll": size,
            "elapsed_ms": [elapsed], "physical_read_bytes": io_bytes() - before_io, "peak_rss_kib": peak_kib(),
        })
    );
    assert_eq!(
        file.metadata().unwrap().len(),
        size,
        "measurement input changed size"
    );
}

#[devkit::test]
fn append_cursors_require_the_stat_and_observation_times_used_for_prefix_checks() {
    let saved = serde_json::to_value(Cursor::default()).unwrap();
    for field in ["ctime_ns", "looked_ns"] {
        let mut missing = saved.clone();
        missing.as_object_mut().unwrap().remove(field);
        assert!(serde_json::from_value::<Cursor>(missing).is_err());
    }
}

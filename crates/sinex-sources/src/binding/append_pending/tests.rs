use super::*;
use crate::drain::RecordRead;
use crate::drivers::append_file::{Cursor, lines};
use std::sync::atomic::{AtomicBool, Ordering};
struct Body {
    bytes: Vec<u8>,
    fail: Arc<AtomicBool>,
}
impl RecordRead for Body {
    fn length(&self) -> u64 {
        self.bytes.len() as u64
    }
    fn read_exact_at(&self, start: u64, out: &mut [u8]) -> std::io::Result<()> {
        if self.fail.load(Ordering::Acquire) {
            return Err(std::io::Error::other("synthetic retained read failure"));
        }
        out.copy_from_slice(
            self.bytes
                .get(start as usize..start as usize + out.len())
                .ok_or_else(|| std::io::Error::other("outside backing"))?,
        );
        Ok(())
    }
}
fn reader(path: &Path) -> Box<dyn Reader> {
    super::super::appended(
        AppendFile {
            path: path.to_owned(),
            host: "h".into(),
            framer: Framer::incremental(lines),
        },
        Framer::incremental(lines),
    )
}
fn resume(
    acquired: &Acquired,
    cursor: Value,
    bodyless: bool,
    erased: Option<crate::drain::ErasedTransitions>,
    fail: Arc<AtomicBool>,
) -> AppendResume {
    let m = acquired.material.as_ref().unwrap();
    let length = m.manifest["append_resume"]["end"]["through"]
        .as_u64()
        .unwrap();
    let bytes = if bodyless {
        Vec::new()
    } else {
        m.record_bytes()
            .record(0, length as usize)
            .unwrap()
            .into_owned()
    };
    let blob = format!("blake3:{}", blake3::hash(&bytes).to_hex());
    let backing: Arc<dyn RecordRead> = if bodyless {
        Arc::new(NoBody(length))
    } else {
        Arc::new(Body { bytes, fail })
    };
    AppendResume {
        reference: MaterialRef {
            id: m.id,
            input: None,
            blob: blob.clone(),
            length,
            part_index: m.part_index,
            previous_part: m.previous_part,
            parser: "p".into(),
            parser_version: 1,
            manifest: m.manifest.clone(),
        },
        stored: RetainedRecords {
            bodyless,
            blob,
            bytes: backing,
        },
        records: m.records.clone(),
        erased,
        base_cursor: Value::Null,
        cursor,
        head: Vec::new(),
        input: acquired.input.clone(),
        observation: acquired.observation.clone(),
        revision: acquired.revision,
    }
}
struct NoBody(u64);
impl RecordRead for NoBody {
    fn length(&self) -> u64 {
        self.0
    }
    fn read_exact_at(&self, _: u64, _: &mut [u8]) -> std::io::Result<()> {
        panic!("bodyless resume must not read or fabricate original plaintext")
    }
}
#[test]
fn pending_append_retries_and_restart_finish_the_original_part_before_reading_an_edited_provider() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("input");
    std::fs::write(&path, b"alpha\nbeta\n").unwrap();
    let mut original = reader(&path);
    let acquired = original.poll(&Value::Null).unwrap();
    let m = acquired.material.as_ref().unwrap();
    let first = (m.cursor_at)(6).unwrap();
    let final_cursor = (m.cursor_at)(11).unwrap();
    let id = m.id;
    std::fs::write(&path, b"beta\nalpha\nlater\n").unwrap();
    let retried = original.poll(&Value::Null).unwrap();
    assert_eq!(retried.material.as_ref().unwrap().id, id);
    let mut restored = reader(&path);
    restored
        .restore_append(resume(
            &acquired,
            first.clone(),
            false,
            None,
            Arc::new(AtomicBool::new(false)),
        ))
        .unwrap();
    let remaining = restored.poll(&first).unwrap();
    let m = remaining.material.as_ref().unwrap();
    assert_eq!(m.id, id);
    assert_eq!(m.records, vec![(6, 11)]);
    assert_eq!(m.record_bytes().record(6, 11).unwrap().as_ref(), b"beta\n");
    assert_eq!((m.cursor_at)(11).unwrap(), final_cursor);
    let edited = restored.poll(&final_cursor).unwrap();
    let m = edited.material.as_ref().unwrap();
    assert!(m.aligned);
    assert_eq!(
        m.records,
        vec![(11, 17)],
        "retained earlier records must already belong to alignment state"
    );
    assert!(m.vanished.is_empty());
}
#[test]
fn append_restore_rejects_an_interior_framed_record_cut_and_propagates_late_window_read_failure() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("input");
    std::fs::write(&path, b"one\ntwo\nthree\n").unwrap();
    let mut original = reader(&path);
    let acquired = original.poll(&Value::Null).unwrap();
    let m = acquired.material.as_ref().unwrap();
    let first = (m.cursor_at)(4).unwrap();
    let mut cut: Cursor = serde_json::from_value(first.clone()).unwrap();
    cut.through = 2;
    cut.offset = 2;
    let mut restored = reader(&path);
    assert!(
        restored
            .restore_append(resume(
                &acquired,
                serde_json::to_value(cut).unwrap(),
                false,
                None,
                Arc::new(AtomicBool::new(false))
            ))
            .is_err()
    );
    let fail = Arc::new(AtomicBool::new(false));
    restored
        .restore_append(resume(&acquired, first.clone(), false, None, fail.clone()))
        .unwrap();
    fail.store(true, Ordering::Release);
    let acquired = restored.poll(&first).unwrap();
    assert!(
        (acquired.material.unwrap().cursor_at)(8)
            .unwrap_err()
            .to_string()
            .contains("synthetic retained read failure")
    );
}
#[test]
fn bodyless_intermediate_null_window_resumes_only_matching_erased_coordinates_and_keeps_known_final_window()
 {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("input");
    std::fs::write(&path, b"one\ntwo\nthree\n").unwrap();
    let mut original = reader(&path);
    let acquired = original.poll(&Value::Null).unwrap();
    let m = acquired.material.as_ref().unwrap();
    let first = (m.cursor_at)(4).unwrap();
    let final_cursor = (m.cursor_at)(14).unwrap();
    let records = m.records.clone();
    let erasures = || {
        crate::drain::ErasedTransitions::from_archive(
            records.iter().map(|&(s, e)| (s, e, None)),
            14,
            true,
        )
        .unwrap()
    };
    let mut restored = reader(&path);
    assert!(
        restored
            .restore_append(resume(
                &acquired,
                first.clone(),
                true,
                None,
                Arc::new(AtomicBool::new(false))
            ))
            .is_err()
    );
    restored
        .restore_append(resume(
            &acquired,
            first.clone(),
            true,
            Some(erasures()),
            Arc::new(AtomicBool::new(false)),
        ))
        .unwrap();
    let captured = restored.poll(&first).unwrap();
    let middle = (captured.material.as_ref().unwrap().cursor_at)(8).unwrap();
    assert!(middle["window"].is_null());
    let mut unbacked = reader(&path);
    assert!(
        unbacked.poll(&middle).is_err(),
        "null window must not authorize provider alignment"
    );
    drop(restored);
    std::fs::write(&path, b"edited provider\n").unwrap();
    let mut restarted = reader(&path);
    restarted
        .restore_append(resume(
            &acquired,
            middle.clone(),
            true,
            Some(erasures()),
            Arc::new(AtomicBool::new(false)),
        ))
        .unwrap();
    let remaining = restarted.poll(&middle).unwrap();
    let m = remaining.material.as_ref().unwrap();
    assert_eq!(m.records, vec![(8, 14)]);
    assert_eq!((m.cursor_at)(14).unwrap(), final_cursor);
    let mut forged = resume(
        &acquired,
        middle,
        true,
        Some(erasures()),
        Arc::new(AtomicBool::new(false)),
    );
    forged.reference.manifest["append_resume"]["end"]["window"] = Value::Null;
    assert!(reader(&path).restore_append(forged).is_err());
}

#[test]
fn large_bodyless_intermediate_checkpoint_survives_cancellation_and_restart_without_original_reads()
{
    // Three virtual long lines. Only the final short window is needed to
    // describe the original final cursor; the erased body is never allocated.
    let part = 8usize << 20;
    let length = part * 3;
    let id = MaterialId::new();
    let mut last_window = vec![b'a'; 4096];
    *last_window.last_mut().unwrap() = b'\n';
    let start = Cursor {
        material: Some(id),
        window: Some(blake3::hash(b"").to_hex().to_string()),
        ..Default::default()
    };
    let end = Cursor {
        material: Some(id),
        offset: length as u64,
        through: length,
        size: length as u64,
        mtime_ns: 1,
        inode: 1,
        window: Some(blake3::hash(&last_window).to_hex().to_string()),
        ..Default::default()
    };
    let end = serde_json::to_value(end).unwrap();
    let ranges = vec![(0, part), (part, part * 2), (part * 2, length)];
    let resume = |cursor: Value| AppendResume {
        reference: MaterialRef {
            id,
            input: None,
            blob: format!("blake3:{}", "0".repeat(64)),
            length: length as u64,
            part_index: 0,
            previous_part: None,
            parser: "synthetic".into(),
            parser_version: 1,
            manifest: serde_json::json!({"file_offset":0,"append_resume":{"version":1,"start":start,"end":end}}),
        },
        stored: RetainedRecords {
            bodyless: true,
            blob: format!("blake3:{}", "0".repeat(64)),
            bytes: Arc::new(NoBody(length as u64)),
        },
        records: ranges.clone(),
        erased: Some(
            crate::drain::ErasedTransitions::from_archive(
                ranges.iter().map(|&(s, e)| (s, e, None)),
                length as u64,
                true,
            )
            .unwrap(),
        ),
        base_cursor: Value::Null,
        cursor,
        head: Vec::new(),
        input: InputRef {
            kind: "file".into(),
            locator: "synthetic".into(),
            host: "h".into(),
        },
        observation: ObservationFacts {
            size: Some(length as u64),
            ..Default::default()
        },
        revision: RevisionChange::Appended,
    };
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("provider");
    std::fs::write(&path, b"edited provider\n").unwrap();
    let mut first = reader(&path);
    first.restore_append(resume(Value::Null)).unwrap();
    let acquired = first.poll(&Value::Null).unwrap();
    let cursor = (acquired.material.as_ref().unwrap().cursor_at)(part).unwrap();
    assert!(cursor["window"].is_null());
    drop(first);
    drop(acquired);
    assert!(reader(&path).poll(&cursor).is_err());
    let mut restarted = reader(&path);
    restarted.restore_append(resume(cursor.clone())).unwrap();
    let acquired = restarted.poll(&cursor).unwrap();
    assert_eq!(acquired.material.as_ref().unwrap().records, ranges[1..]);
    let next = (acquired.material.unwrap().cursor_at)(part * 2).unwrap();
    assert!(next["window"].is_null());
    drop(restarted);
    let mut final_reader = reader(&path);
    final_reader.restore_append(resume(next.clone())).unwrap();
    let acquired = final_reader.poll(&next).unwrap();
    assert_eq!((acquired.material.unwrap().cursor_at)(length).unwrap(), end);
}

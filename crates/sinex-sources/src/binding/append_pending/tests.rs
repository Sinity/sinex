use super::*;
use crate::Framer;
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
    super::super::appended(AppendFile {
        path: path.to_owned(),
        host: "h".into(),
        framer: Framer::incremental(lines),
    })
}
fn resume(acquired: &Acquired, cursor: Value, fail: Arc<AtomicBool>) -> AppendResume {
    let m = acquired.material.as_ref().unwrap();
    let length = m.manifest["append_resume"]["end"]["through"]
        .as_u64()
        .unwrap();
    let bytes = m
        .record_bytes()
        .record(0, length as usize)
        .unwrap()
        .into_owned();
    let blob = format!("blake3:{}", blake3::hash(&bytes).to_hex());
    let backing: Arc<dyn RecordRead> = Arc::new(Body { bytes, fail });
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
            blob,
            bytes: backing,
        },
        records: m.records.clone(),
        base_cursor: Value::Null,
        cursor,
        head: Vec::new(),
        input: acquired.input.clone(),
        observation: acquired.observation.clone(),
        revision: acquired.revision,
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
                Arc::new(AtomicBool::new(false))
            ))
            .is_err()
    );
    let fail = Arc::new(AtomicBool::new(false));
    restored
        .restore_append(resume(&acquired, first.clone(), fail.clone()))
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

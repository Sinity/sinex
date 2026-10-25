use super::*;
use crate::Framer;
use crate::binding::StoredMaterial;
use crate::drain::RecordRead;
use crate::drivers::append_file::lines;

struct Body(Vec<u8>);
impl RecordRead for Body {
    fn length(&self) -> u64 {
        self.0.len() as u64
    }
    fn read_exact_at(&self, start: u64, out: &mut [u8]) -> std::io::Result<()> {
        out.copy_from_slice(
            self.0
                .get(start as usize..start as usize + out.len())
                .ok_or_else(|| std::io::Error::other("outside the bytes"))?,
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

/// What the store would hold for the material `acquired` read.
fn stored(acquired: &Acquired) -> StoredMaterial {
    let m = acquired.material.as_ref().unwrap();
    let length = m.records.last().unwrap().1;
    let bytes = m.record_bytes().record(0, length).unwrap().into_owned();
    StoredMaterial {
        reference: MaterialRef {
            id: m.id,
            input: None,
            blob: format!("blake3:{}", blake3::hash(&bytes).to_hex()),
            length: length as u64,
            part_index: m.part_index,
            previous_part: m.previous_part,
            parser: "p".into(),
            parser_version: 1,
            manifest: m.manifest.clone(),
        },
        bytes: Arc::new(Body(bytes)),
        same_revision: true,
    }
}

#[test]
fn a_failed_admission_retries_the_same_bytes_until_the_final_cursor() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("input");
    std::fs::write(&path, b"alpha\nbeta\n").unwrap();
    let mut reader = reader(&path);
    let acquired = reader.poll(&Value::Null).unwrap();
    let m = acquired.material.as_ref().unwrap();
    let first = (m.cursor_at)(6).unwrap();
    let id = m.id;
    std::fs::write(&path, b"beta\nalpha\nlater\n").unwrap();
    let retried = reader.poll(&Value::Null).unwrap();
    assert_eq!(retried.material.as_ref().unwrap().id, id);
    let rest = reader.poll(&first).unwrap();
    let rest = rest.material.as_ref().unwrap();
    assert_eq!(rest.id, id);
    assert_eq!(rest.records, vec![(6, 11)]);
    assert_eq!(
        rest.record_bytes().record(6, 11).unwrap().as_ref(),
        b"beta\n"
    );
}

#[test]
fn a_restart_remembers_the_admitted_records_for_aligning_a_rewrite() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("input");
    std::fs::write(&path, b"alpha\nbeta\n").unwrap();
    let acquired = reader(&path).poll(&Value::Null).unwrap();
    let m = acquired.material.as_ref().unwrap();
    let end = (m.cursor_at)(11).unwrap();
    let mut restarted = reader(&path);
    assert!(restarted.restore(&end, stored(&acquired)).unwrap());
    restarted.restored().unwrap();
    std::fs::write(&path, b"beta\nalpha\nlater\n").unwrap();
    let rewritten = restarted.poll(&end).unwrap();
    let m = rewritten.material.as_ref().unwrap();
    assert!(m.aligned);
    assert_eq!(m.records, vec![(11, 17)], "only the new record is read");
    assert!(m.vanished.is_empty());
}

#[test]
fn a_restart_inside_a_material_reads_on_from_the_file() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("input");
    std::fs::write(&path, b"one\ntwo\nthree\n").unwrap();
    let acquired = reader(&path).poll(&Value::Null).unwrap();
    let m = acquired.material.as_ref().unwrap();
    let first = (m.cursor_at)(4).unwrap();
    let mut restarted = reader(&path);
    assert!(restarted.restore(&first, stored(&acquired)).unwrap());
    restarted.restored().unwrap();
    let next = restarted.poll(&first).unwrap();
    let next = next.material.unwrap();
    assert_ne!(next.id, m.id);
    assert_eq!(next.previous_part, Some(m.id));
    assert_eq!(next.manifest["file_offset"], 4);
    assert_eq!(next.records, vec![(0, 4), (4, 10)]);
}

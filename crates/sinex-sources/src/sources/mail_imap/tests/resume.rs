use super::*;
use crate::binding::Reader;
use crate::drivers::{RetainedRecords, StoredBytes};
use sinex_model::MaterialRef;

struct Memory(Vec<u8>);
impl crate::drain::RecordRead for Memory {
    fn length(&self) -> u64 {
        self.0.len() as u64
    }
    fn read_exact_at(&self, offset: u64, out: &mut [u8]) -> std::io::Result<()> {
        let mut cursor = std::io::Cursor::new(self.0.as_slice());
        cursor.set_position(offset);
        cursor.read_exact(out)
    }
}
fn retained() -> (MaterialRef, RetainedRecords, Value, Value) {
    let imap = mail(
        server(vec![(4, M1), (7, M2)], 99, false),
        Some("secret"),
        None,
    );
    let material = imap.poll(&Value::Null).unwrap().material.unwrap();
    let partial = (material.cursor_at)(material.records[0].1).unwrap();
    let end = (material.cursor_at)(material.bytes.len()).unwrap();
    let blob = format!("blake3:{}", blake3::hash(&material.bytes).to_hex());
    let reference = MaterialRef {
        id: material.id,
        input: None,
        blob: blob.clone(),
        length: material.bytes.len() as u64,
        part_index: material.part_index,
        previous_part: material.previous_part,
        parser: SPEC.parser.into(),
        parser_version: SPEC.parser_version,
        manifest: material.manifest,
    };
    let stored = RetainedRecords {
        bodyless: false,
        blob,
        bytes: Arc::new(Memory(material.bytes)),
    };
    (reference, stored, partial, end)
}
fn offline() -> ImapReader {
    ImapReader {
        mail: mail(0, Some("secret"), None),
        pending: None,
    }
}
fn assert_suffix(reader: &mut ImapReader, position: &Value, id: MaterialId) {
    let acquired = reader.poll(position).unwrap();
    let m = acquired.material.unwrap();
    assert_eq!(m.id, id);
    assert_eq!(m.records, [(M1.len(), M1.len() + M2.len())]);
    let stored = match m.stored.unwrap() {
        StoredBytes::FramedReader(stored) => stored,
        _ => panic!("retained body must stay in its verified backing"),
    };
    let mut body = vec![0; M2.len()];
    stored
        .bytes
        .read_exact_at(M1.len() as u64, &mut body)
        .unwrap();
    assert_eq!(body, M2.as_bytes());
    assert_eq!((m.cursor_at)(M1.len() + M2.len()).unwrap()["uid_next"], 8);
}

#[test]
fn retained_suffix_and_pre_first_batch_restore_do_not_contact_provider() {
    let (reference, stored, partial, end) = retained();
    let mut reader = offline();
    assert!(
        reader
            .restore_material(reference.clone(), stored.clone(), &partial, &[])
            .unwrap()
    );
    assert_suffix(&mut reader, &partial, reference.id);
    assert!(
        reader.poll(&end).is_err(),
        "only a completed retained answer permits refetch"
    );
    let mut reader = offline();
    assert!(
        reader
            .restore_material(reference.clone(), stored, &Value::Null, &[])
            .unwrap()
    );
    let material = reader.poll(&Value::Null).unwrap().material.unwrap();
    assert_eq!(material.id, reference.id);
    assert_eq!(material.records.len(), 2);
}

#[test]
fn same_reader_keeps_original_answer_until_the_final_admitted_cursor() {
    let mut reader = ImapReader {
        mail: mail(
            server(vec![(4, M1), (7, M2)], 99, false),
            Some("secret"),
            None,
        ),
        pending: None,
    };
    let acquired = reader.poll(&Value::Null).unwrap();
    let m = acquired.material.unwrap();
    let partial = (m.cursor_at)(M1.len()).unwrap();
    reader.mail.config.port = 0;
    let again = reader.poll(&partial).unwrap().material.unwrap();
    assert_eq!(again.id, m.id);
    assert_eq!(again.records, [(M1.len(), M1.len() + M2.len())]);
    assert!((again.cursor_at)(M1.len() + 1).is_err());
}

#[test]
fn legacy_partial_answer_requires_one_proven_contiguous_prefix() {
    let (mut reference, stored, mut partial, end) = retained();
    reference
        .manifest
        .as_object_mut()
        .unwrap()
        .remove("imap_resume");
    partial.as_object_mut().unwrap().remove("through");
    let mut reader = offline();
    assert!(
        reader
            .restore_material(reference.clone(), stored.clone(), &partial, &[0])
            .unwrap()
    );
    assert_suffix(&mut reader, &partial, reference.id);
    for anchors in [vec![], vec![M1.len()], vec![0, M1.len()], vec![123456]] {
        assert!(
            offline()
                .restore_material(reference.clone(), stored.clone(), &partial, &anchors)
                .is_err(),
            "{anchors:?}"
        );
    }
    let mut contradictory = partial.clone();
    contradictory["through"] = json!(1);
    assert!(
        offline()
            .restore_material(reference.clone(), stored.clone(), &contradictory, &[0])
            .is_err()
    );
    let mut complete = end;
    complete.as_object_mut().unwrap().remove("through");
    let mut reader = offline();
    assert!(
        reader
            .restore_material(reference, stored, &complete, &[])
            .unwrap()
    );
    assert!(
        reader.poll(&complete).is_err(),
        "a completed legacy answer must not be replayed"
    );
}

#[test]
fn wrong_identity_epoch_bounds_and_unknown_versions_refuse_before_provider() {
    let (reference, stored, partial, _) = retained();
    let mut bad = reference.clone();
    bad.manifest["imap_resume"]["version"] = 2.into();
    assert!(
        offline()
            .restore_material(bad, stored.clone(), &partial, &[])
            .is_err()
    );
    let mut bad = reference.clone();
    bad.manifest["imap_resume"]["end"]["material"] =
        serde_json::to_value(MaterialId::new()).unwrap();
    assert!(
        offline()
            .restore_material(bad, stored.clone(), &partial, &[])
            .is_err()
    );
    let mut bad = reference.clone();
    bad.manifest["records"] = json!([[0, 3], [5, reference.length]]);
    assert!(
        offline()
            .restore_material(bad, stored.clone(), &partial, &[])
            .is_err()
    );
    for (field, value) in [
        ("through", json!(1)),
        ("uid_validity", json!(200)),
        ("part_index", json!(4)),
        ("uid_next", json!(6)),
    ] {
        let mut current = partial.clone();
        current[field] = value;
        assert!(
            offline()
                .restore_material(reference.clone(), stored.clone(), &current, &[])
                .is_err(),
            "{field}"
        );
    }
    let mut no_body = stored;
    no_body.bodyless = true;
    assert!(
        offline()
            .restore_material(reference, no_body, &partial, &[])
            .is_err()
    );
}

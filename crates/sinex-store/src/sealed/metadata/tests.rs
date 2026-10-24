use super::*;
use serde_json::json;

fn scope() -> Scope {
    Scope::Replay {
        source: "synthetic".into(),
        material: MaterialId::new(),
        parser: "test".into(),
        parser_version: 1,
        length: 100,
        whole: false,
    }
}
#[test]
fn class_replay_is_purpose_bound_and_fully_authenticated_before_release() {
    let identity = Identity::generate();
    let scope = scope();
    let record = || ErasedRecord {
        start: 0,
        end: 10,
        raw_digest: [7; 32],
        transition: Some(json!({"count": 2})),
    };
    let mut ciphertext = Vec::new();
    seal_replay(
        &mut ciphertext,
        &identity.to_public(),
        &scope,
        [record()],
        || Ok(()),
    )
    .unwrap();
    let records = open_replay(ciphertext.as_slice(), &identity, &scope, || Ok(())).unwrap();
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].transition, record().transition);
    assert!(
        open_replay(ciphertext.as_slice(), &Identity::generate(), &scope, || Ok(
            ()
        ))
        .is_err()
    );
    let mut wrong = scope.clone();
    if let Scope::Replay { parser_version, .. } = &mut wrong {
        *parser_version = 2;
    }
    assert!(open_replay(ciphertext.as_slice(), &identity, &wrong, || Ok(())).is_err());
    assert!(super::super::unwrap(&ciphertext, &identity).is_err());
    for cut in [0, ciphertext.len() / 2, ciphertext.len() - 1] {
        assert!(open_replay(&ciphertext[..cut], &identity, &scope, || Ok(())).is_err());
    }
    let mut calls = 0;
    assert!(
        open_replay(ciphertext.as_slice(), &identity, &scope, || {
            calls += 1;
            if calls > 1 { Err(invalid()) } else { Ok(()) }
        })
        .is_err()
    );
    let decrypted = age::Decryptor::new(ciphertext.as_slice())
        .unwrap()
        .decrypt(std::iter::once(&identity as &dyn age::Identity))
        .unwrap();
    let mut text = String::new();
    std::io::Read::read_to_string(&mut { decrypted }, &mut text).unwrap();
    assert!(!text.contains("synthetic erased private text"));
    assert!(text.contains("raw_digest"));
}
#[test]
fn carry_has_its_own_scope_and_preserves_large_unrelated_state() {
    let identity = Identity::generate();
    let scope = Scope::Carry {
        source: "synthetic".into(),
        binding: "b".into(),
        parser: "p".into(),
        parser_version: 1,
        cursor: json!({"through": 20, "part": 1}),
    };
    let carry = json!({"unrelated": "retained".repeat(4096), "count": 7});
    let ciphertext = seal_carry(&carry, &identity.to_public(), &scope).unwrap();
    let mut same = scope.clone();
    if let Scope::Carry { cursor, .. } = &mut same {
        *cursor = json!({"part": 1, "through": 20});
    }
    assert_eq!(
        open_carry(ciphertext.as_slice(), &identity, &same).unwrap(),
        carry
    );
    if let Scope::Carry { cursor, .. } = &mut same {
        *cursor = json!({"through": 21, "part": 1});
    }
    assert!(open_carry(ciphertext.as_slice(), &identity, &same).is_err());
    assert!(
        open_replay(
            ciphertext.as_slice(),
            &identity,
            &super::tests::scope(),
            || Ok(())
        )
        .is_err()
    );
    assert!(super::super::unwrap(&ciphertext, &identity).is_err());
}
#[test]
fn unknown_version_wrong_purpose_and_overlapping_coordinates_refuse() {
    let identity = Identity::generate();
    let scope = scope();
    for header in [
        json!({"format":"sinex-class-metadata", "version": 2, "scope":scope}),
        json!({"format":"SXKEY", "version": 1, "scope":scope}),
    ] {
        let mut ciphertext = Vec::new();
        let encryptor = age::Encryptor::with_recipients(std::iter::once(
            &identity.to_public() as &dyn age::Recipient
        ))
        .unwrap();
        let mut out = encryptor.wrap_output(&mut ciphertext).unwrap();
        line(&mut out, &header).unwrap();
        out.finish().unwrap();
        assert!(open_replay(ciphertext.as_slice(), &identity, &scope, || Ok(())).is_err());
    }
    let r = |start, end| ErasedRecord {
        start,
        end,
        raw_digest: [0; 32],
        transition: None,
    };
    assert!(
        seal_replay(
            Vec::new(),
            &identity.to_public(),
            &scope,
            [r(0, 10), r(9, 20)],
            || Ok(())
        )
        .is_err()
    );
    let key = super::super::new_key().unwrap();
    let names = super::super::MaterialSecrets::from_data_key(&key).naming_only();
    let raw = b"synthetic erased private text";
    assert_ne!(
        names.name_raw_record(raw),
        names.name_record_hash(blake3::hash(raw).as_bytes())
    );
    assert_eq!(
        names.name_raw_record(raw),
        super::super::MaterialSecrets::from_data_key(&key).name_raw_record(raw)
    );
}

use super::*;

fn legacy(key: &[u8], recipient: &Recipient) -> Vec<u8> {
    let encryptor =
        age::Encryptor::with_recipients(std::iter::once(recipient as &dyn age::Recipient)).unwrap();
    let mut out = Vec::new();
    let mut writer = encryptor.wrap_output(&mut out).unwrap();
    writer.write_all(key).unwrap();
    writer.finish().unwrap();
    out
}

#[test]
fn legacy_data_role_and_versioned_roles_remain_distinct() {
    let identity = Identity::generate();
    let key = super::super::new_key().unwrap();
    let old = legacy(&key.key, &identity.to_public());
    let secrets = unwrap_secrets(&old, &identity, Class::Sealed).unwrap();
    assert_eq!(secrets.role(), SecretRole::DataAndRecordNaming);
    assert_eq!(super::super::unwrap(&old, &identity).unwrap(), key.key);
    let named = secrets.naming_only();
    let wrapped = wrap_secrets(&named, &identity.to_public()).unwrap();
    let decoded = unwrap_secrets(&wrapped, &identity, Class::Sealed).unwrap();
    assert_eq!(decoded.role(), SecretRole::RecordNamingOnly);
    assert_eq!(decoded.record_naming(), secrets.record_naming());
    assert!(decoded.data_key("key".into()).is_err());
    assert!(super::super::unwrap(&wrapped, &identity).is_err());
    assert!(unwrap_secrets(&old, &identity, Class::Open).is_err());
    assert!(unwrap_secrets(&wrapped, &Identity::generate(), Class::Sealed).is_err());
}

#[test]
fn rotations_retain_names_and_remove_old_decryption_capability() {
    let old = super::super::new_key().unwrap();
    let new = super::super::new_key().unwrap();
    let secrets = MaterialSecrets::from_data_key(&old)
        .naming_only()
        .with_data_key(&new);
    let identity = Identity::generate();
    let wrapped = wrap_secrets(&secrets, &identity.to_public()).unwrap();
    let retained = unwrap_secrets(&wrapped, &identity, Class::Sealed).unwrap();
    assert_eq!(retained.data_bytes().unwrap(), new.key);
    assert_eq!(retained.record_naming(), &naming_key(&old.key));
    assert_ne!(retained.record_naming(), &naming_key(&new.key));
    let value = serde_json::json!({"value":"retained"});
    let old_payload = super::super::seal_value(&value, &old).unwrap();
    assert!(super::super::unseal_payload(&old_payload, &retained.data_bytes().unwrap()).is_err());
}

#[test]
fn unknown_versions_classes_roles_and_lengths_are_rejected() {
    let key = super::super::new_key().unwrap();
    let identity = Identity::generate();
    let mut valid = Vec::from(MAGIC.as_slice());
    valid.extend_from_slice(&[VERSION, SEALED, 1]);
    valid.extend_from_slice(&key.key);
    valid.extend_from_slice(&naming_key(&key.key));
    for (index, value) in [(8, 2), (9, 0), (10, 3)] {
        let mut changed = valid.clone();
        changed[index] = value;
        assert!(
            unwrap_secrets(
                &legacy(&changed, &identity.to_public()),
                &identity,
                Class::Sealed
            )
            .is_err()
        );
    }
    valid.push(0);
    assert!(
        unwrap_secrets(
            &legacy(&valid, &identity.to_public()),
            &identity,
            Class::Sealed
        )
        .is_err()
    );
    assert!(
        unwrap_secrets(
            &legacy(&[0; 31], &identity.to_public()),
            &identity,
            Class::Sealed
        )
        .is_err()
    );
}

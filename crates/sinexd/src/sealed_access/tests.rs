use super::*;
#[test]
fn operation_grants_expire_and_lock_revokes_existing_clones() {
    let grant = Authority::new(Identity::generate(), Duration::from_secs(60)).unwrap();
    let operation = grant.clone();
    assert!(operation.check().is_ok());
    grant.revoke();
    assert!(operation.check().is_err());
    let expired = Authority::new(Identity::generate(), Duration::ZERO).unwrap();
    assert!(expired.check().is_err());
}

#[test]
fn naming_only_authority_never_becomes_a_legacy_decryption_key() {
    let identity = Identity::generate();
    let key = sinex_store::sealed::new_key().unwrap();
    let naming = sinex_store::sealed::MaterialSecrets::from_data_key(&key).naming_only();
    let wrapped = sinex_store::sealed::wrap_secrets(&naming, &identity.to_public()).unwrap();
    let authority = Authority::new(identity, Duration::from_secs(60)).unwrap();
    assert!(authority.unwrap(&wrapped).is_err());
    assert_eq!(
        authority.unwrap_secrets(&wrapped).unwrap().role(),
        sinex_store::sealed::SecretRole::RecordNamingOnly
    );
    authority.revoke();
    assert!(authority.unwrap_secrets(&wrapped).is_err());
}

#[test]
fn class_owned_metadata_requires_current_scoped_authority() {
    use sinex_store::sealed::metadata;
    let identity = Identity::generate();
    let scope = metadata::Scope::Carry {
        source: "s".into(),
        binding: "b".into(),
        parser: "p".into(),
        parser_version: 1,
        cursor: serde_json::json!({"through": 10}),
    };
    let ciphertext = metadata::seal_carry(
        &serde_json::json!({"unrelated": 3}),
        &identity.to_public(),
        &scope,
    )
    .unwrap();
    let authority = Authority::new(identity.clone(), Duration::from_secs(60)).unwrap();
    assert_eq!(
        authority
            .open_current_carry(ciphertext.as_slice(), &scope)
            .unwrap(),
        serde_json::json!({"unrelated": 3})
    );
    authority.revoke();
    assert!(
        authority
            .open_current_carry(ciphertext.as_slice(), &scope)
            .is_err()
    );
    let expired = Authority::new(identity, Duration::ZERO).unwrap();
    assert!(
        expired
            .open_current_carry(ciphertext.as_slice(), &scope)
            .is_err()
    );
}

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

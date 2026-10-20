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

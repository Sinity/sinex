use super::*;
use sinex_model::{
    MaterialId, Origin, ReadingOrder, RecordLocation, Timestamp, event_types::CommandImported,
};

#[devkit::test]
fn wrap_unwrap_and_seal_round_trip() {
    let identity = age::x25519::Identity::generate();
    let recipient = identity.to_public();
    let key = new_key().unwrap();
    let wrapped = wrap(&key, &recipient).unwrap();
    assert_eq!(unwrap(&wrapped, &identity).unwrap(), key.key);
    let other = age::x25519::Identity::generate();
    assert!(
        unwrap(&wrapped, &other).is_err(),
        "a wrong identity is refused"
    );

    let mut e = Event::build(
        &CommandImported {
            command: "gpg --decrypt diary".into(),
            hostname: "h".into(),
            started_at: Some(Timestamp::from_unix_secs(1).unwrap()),
            header_elapsed: Some(0),
            paths: vec![],
            n: 0,
        },
        "s",
        Origin::Record {
            location: RecordLocation {
                material: MaterialId::new(),
                anchor: 0,
            },
            output_index: 0,
        },
        ReadingOrder(0),
        [1; 32],
        Class::Open,
    )
    .unwrap();
    let plain = e.payload.clone();
    seal_event(&mut e, &key).unwrap();
    assert!(is_sealed(&e.payload));
    assert!(!e.payload.to_string().contains("diary"));
    assert_eq!(e.class, Class::Sealed);
    assert_eq!(key_ref(&e.payload), Some(key.key_ref.as_str()));
    assert_eq!(unseal_payload(&e.payload, &key.key).unwrap(), plain);
    assert!(unseal_payload(&e.payload, &[0; 32]).is_err());
}

#[devkit::test]
fn parse_identity_from_a_key_file() {
    let identity = age::x25519::Identity::generate();
    use age::secrecy::ExposeSecret;
    let file = format!(
        "# created: now\n# public key: {}\n{}\n",
        identity.to_public(),
        identity.to_string().expose_secret()
    );
    let parsed = parse_identity(&file).unwrap();
    assert_eq!(
        parsed.to_public().to_string(),
        identity.to_public().to_string()
    );
    assert!(parse_recipient(&identity.to_public().to_string()).is_ok());
}

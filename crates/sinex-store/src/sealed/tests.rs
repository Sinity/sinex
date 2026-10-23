use super::*;
use sinex_model::{Origin, ReadingOrder, RecordLocation, Timestamp, event_types::CommandImported};

#[test]
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
            n: 0,
        },
        "s",
        Origin::Record {
            location: RecordLocation {
                material: MaterialId::new(),
                anchor: 0,
            },
            ordinal: 0,
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

#[test]
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

#[test]
fn rotated_content_keeps_record_identity_and_unrelated_values() {
    let old = new_key().unwrap();
    let fresh = new_key().unwrap();
    let secrets = MaterialSecrets::from_data_key(&old)
        .naming_only()
        .with_data_key(&fresh);
    let event = Event::build(
        &CommandImported {
            command: "synthetic retained command".into(),
            hostname: "h".into(),
            started_at: Some(Timestamp::from_unix_secs(1).unwrap()),
            header_elapsed: Some(0),
            n: 3,
        },
        "s",
        Origin::Record {
            location: RecordLocation {
                material: MaterialId::new(),
                anchor: 24,
            },
            ordinal: 0,
        },
        ReadingOrder(7),
        [19; 32],
        Class::Open,
    )
    .unwrap();
    let mut before = event.clone();
    seal_event(&mut before, &old).unwrap();
    let mut after = event.clone();
    seal_event_with_secrets(&mut after, &fresh, &secrets).unwrap();
    assert_eq!(before.equivalence_key, after.equivalence_key);
    assert_eq!(before.record_hash, after.record_hash);
    assert_eq!(before.location(), after.location());
    assert_eq!(
        unseal_payload(&after.payload, &fresh.key).unwrap(),
        event.payload
    );
    assert!(unseal_payload(&before.payload, &fresh.key).is_err());
    assert!(unseal_payload(&after.payload, &old.key).is_err());
    assert!(seal_event_with_secrets(&mut event.clone(), &old, &secrets).is_err());
    assert!(seal_event_with_secrets(&mut event.clone(), &fresh, &secrets.naming_only()).is_err());
}

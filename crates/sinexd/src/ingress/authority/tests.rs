use super::*;
fn capability(publisher: bool) -> fetch::Grants {
    fetch::Grants {
        indexed_catalog: None,
        public_context: false,
        kind_prefixes: vec!["example".into()],
        classes: vec![Class::Open],
        write: Some(WriteGrant {
            forward_signals: false,
            author_attestation: None,
            emergency_sinex_blocked_host: None,
            emergency_lease_seconds: None,
            family: "example".into(),
            host: None,
            source_prefixes: vec![],
            source_declarations: vec![],
            canonical_kinds: vec![],
            kind_prefixes: vec!["example".into()],
            classes: vec![Class::Open],
            canonical_prefixes: if publisher {
                vec!["example.summary".into()]
            } else {
                vec![]
            },
        }),
    }
}
#[devkit::test]
fn only_the_explicit_canonical_publisher_may_publish_its_prefix() {
    let grants = fetch::ProducerGrants::from([
        ("first".into(), capability(true)),
        ("second".into(), capability(false)),
    ]);
    validate(&grants).unwrap();
    assert!(event(&grants, "first", "example.summary", Class::Open).is_ok());
    assert!(event(&grants, "second", "example.raw", Class::Open).is_ok());
    assert!(event(&grants, "second", "example.summary", Class::Open).is_err());
    assert!(event(&grants, "second", "example.raw", Class::Guarded).is_err());
    assert!(event(&grants, "second", "examples.raw", Class::Open).is_err());
    assert!(event(&grants, "unknown", "example.raw", Class::Open).is_err());
    let mut duplicate = grants;
    duplicate
        .get_mut("second")
        .unwrap()
        .write
        .as_mut()
        .unwrap()
        .canonical_prefixes = vec!["example.other_summary".into()];
    assert!(validate(&duplicate).is_err());
}
#[devkit::test]
fn read_grants_do_not_create_a_write_capability() {
    let grants = fetch::ProducerGrants::from([(
        "reader".into(),
        fetch::Grants {
            indexed_catalog: None,
            public_context: false,
            kind_prefixes: vec!["example".into()],
            classes: vec![Class::Open],
            write: None,
        },
    )]);
    assert!(write(&grants, "reader").is_err());
}

#[devkit::test]
fn signal_forwarding_requires_the_existing_unique_canonical_role() {
    let mut configured = capability(true);
    configured.write.as_mut().unwrap().forward_signals = true;
    let grants = fetch::ProducerGrants::from([("canonical".into(), configured)]);
    validate(&grants).unwrap();
    let mut capture = capability(false);
    capture.write.as_mut().unwrap().forward_signals = true;
    assert!(validate(&fetch::ProducerGrants::from([("capture".into(), capture)])).is_err());
}

#[devkit::test]
fn emergency_capability_binds_the_authenticated_producer_host() {
    let mut configured = capability(true);
    let grant = configured.write.as_mut().unwrap();
    grant.forward_signals = true;
    grant.emergency_sinex_blocked_host = Some("fixture".into());
    assert!(
        validate(&fetch::ProducerGrants::from([(
            "canonical".into(),
            configured.clone()
        )]))
        .is_err()
    );
    configured.write.as_mut().unwrap().host = Some("another-host".into());
    assert!(
        validate(&fetch::ProducerGrants::from([(
            "canonical".into(),
            configured.clone()
        )]))
        .is_err()
    );
    configured.write.as_mut().unwrap().host = Some("fixture".into());
    validate(&fetch::ProducerGrants::from([(
        "canonical".into(),
        configured,
    )]))
    .unwrap();
}

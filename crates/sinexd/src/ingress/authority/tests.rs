use super::*;
fn capability(publisher: bool) -> fetch::Grants {
    fetch::Grants {
        kind_prefixes: vec!["example".into()],
        classes: vec![Class::Open],
        write: Some(WriteGrant {
            family: "example".into(),
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
            kind_prefixes: vec!["example".into()],
            classes: vec![Class::Open],
            write: None,
        },
    )]);
    assert!(write(&grants, "reader").is_err());
}

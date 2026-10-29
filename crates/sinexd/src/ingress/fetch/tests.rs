use super::*;
#[devkit::test]
fn consumer_scope_is_canonical_and_narrowing_or_changing_identity_revokes_the_old_name() {
    let subjects = Subjects::sinex();
    let broad = Grants {
        indexed_catalog: None,
        public_context: false,
        write: None,
        kind_prefixes: vec!["polylogue.".into()],
        classes: vec![Class::Open, Class::Guarded],
    };
    let equivalent = Grants {
        indexed_catalog: None,
        public_context: false,
        write: None,
        kind_prefixes: vec![
            "polylogue.message".into(),
            "polylogue".into(),
            "polylogue".into(),
        ],
        classes: vec![Class::Guarded, Class::Open, Class::Open],
    };
    let (filters, scope) = broad.consumer_scope("host-a", &subjects);
    assert_eq!(
        equivalent.consumer_scope("host-a", &subjects),
        (filters, scope.clone())
    );
    let narrow = Grants {
        indexed_catalog: None,
        public_context: false,
        write: None,
        kind_prefixes: vec!["polylogue.message".into()],
        classes: vec![Class::Open],
    };
    let (_, narrow_scope) = narrow.consumer_scope("host-a", &subjects);
    assert_ne!(scope, narrow_scope);
    assert_ne!(scope, broad.consumer_scope("host-b", &subjects).1);
    let (capabilities, _) = subjects.producer_permissions("host-a", Some(&narrow_scope));
    assert!(
        capabilities
            .iter()
            .any(|subject| subject.ends_with(&subjects.consumer_name(&narrow_scope)))
    );
    assert!(
        !capabilities
            .iter()
            .any(|subject| subject.contains(&subjects.consumer_name(&scope)))
    );
}

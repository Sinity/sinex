use super::*;

#[test]
fn every_registered_parser_resolves_to_its_source() {
    for spec in specs() {
        let source = by_parser(spec.parser, "h").expect("registered parser resolves");
        assert_eq!(source.spec().parser, spec.parser);
    }
    assert!(by_parser("no-such-parser", "h").is_none());
}

use sinex_primitives::rpc::sources::{CaveatSeverity, caveat_codes};
use sinexd::runtime::parser::{DriftEvent, MaterialParser};

pub fn assert_required_input_keys<P>(parser: P, expected: &[&str])
where
    P: MaterialParser,
{
    let expected = expected
        .iter()
        .map(|key| (*key).to_string())
        .collect::<Vec<_>>();
    assert_eq!(parser.required_input_keys(), expected);
}

pub fn assert_required_key_blocks_readiness<P>(mut drift: DriftEvent, parser: P, required_key: &str)
where
    P: MaterialParser,
{
    drift.required_input_keys = parser.required_input_keys();
    let caveats = drift.readiness_caveats();

    assert!(
        caveats.iter().any(|caveat| {
            caveat.code == caveat_codes::PARSER_REQUIRED_FIELD_MISSING
                && caveat.severity == CaveatSeverity::Blocking
                && caveat.message.contains(required_key)
        }),
        "expected missing required input key {required_key:?} to block readiness; caveats: {caveats:?}"
    );
}

use super::*;
use sinex_primitives::domain::{EventSource, EventType};
use sinex_primitives::parser::{ParserId, SourceId, TimingConfidence};
use sinex_primitives::source_contracts::{
    AccessScope, Horizon, OccurrenceIdentity, PrivacyTier, RetentionPolicy,
};
use xtask::sandbox::prelude::sinex_test;

static EVENT_TYPES: &[(&str, &str)] = &[("terminal", "shell.command")];
static HORIZONS: &[Horizon] = &[Horizon::Historical];
fn fixture_manifest() -> ParserManifest {
    ParserManifest {
        parser_id: ParserId::from_static("fixture-parser"),
        parser_version: "1.0.0".to_string(),
        accepted_input_shapes: vec![InputShapeKind::StaticFile],
        source_id: SourceId::from_static("fixture.source"),
        declared_event_types: vec![(
            EventSource::from_static("terminal"),
            EventType::from_static("shell.command"),
        )],
        privacy_contexts: vec![ProcessingContext::Command],
        sensitivity_hints: Vec::new(),
        description: "fixture parser".to_string(),
    }
}

fn fixture_descriptor() -> SourceContract {
    SourceContract {
        id: "fixture.source",
        namespace: "fixture",
        event_types: EVENT_TYPES,
        privacy_tier: PrivacyTier::Sensitive,
        horizons: HORIZONS,
        retention: RetentionPolicy::Forever,
        occurrence_identity: OccurrenceIdentity::Natural,
        access_scope: AccessScope::Internal,
    }
}

fn acceptance_spec(assertions: Vec<FixtureAssertion>) -> FixtureSpec {
    FixtureSpec {
        name: "fixture acceptance".to_string(),
        description: "representative parser-family fixture".to_string(),
        input_shape_kind: InputShapeKind::StaticFile,
        material_bytes: Vec::new(),
        material_path: None,
        expectations: vec![FixtureExpectation {
            index: 0,
            assertions,
            golden_artifact: None,
        }],
        expect_no_intents: false,
        expect_error: false,
        expected_error_contains: None,
        tags: vec!["parser-family".to_string()],
        acceptance: Some(FixtureAcceptanceContract {
            source_id: "fixture.source".to_string(),
            require_timestamp: true,
            require_timing: true,
            require_anchor: true,
            require_occurrence_identity: true,
            require_privacy_context: true,
            require_parser_metadata: true,
        }),
    }
}

fn complete_assertions() -> Vec<FixtureAssertion> {
    vec![
        FixtureAssertion::EventSource {
            expected: "terminal".to_string(),
        },
        FixtureAssertion::EventType {
            expected: "shell.command".to_string(),
        },
        FixtureAssertion::Timestamp {
            value: Timestamp::UNIX_EPOCH,
        },
        FixtureAssertion::Timing {
            expected: TimingEvidence::Intrinsic {
                field: "fixture.timestamp".to_string(),
                confidence: TimingConfidence::Intrinsic,
            },
        },
        FixtureAssertion::Anchor {
            expected: MaterialAnchor::ByteRange { start: 0, len: 12 },
        },
        FixtureAssertion::OccurrenceKey {
            expected_fields: vec![("row".to_string(), "1".to_string())],
        },
        FixtureAssertion::PrivacyContext {
            expected: ProcessingContext::Command,
        },
        FixtureAssertion::ParserMetadata {
            parser_id: "fixture-parser".to_string(),
            parser_version: "1.0.0".to_string(),
        },
    ]
}

#[sinex_test]
async fn acceptance_contract_accepts_complete_fixture() -> xtask::sandbox::TestResult<()> {
    let spec = acceptance_spec(complete_assertions());
    let failures = spec.acceptance_failures(&fixture_manifest(), Some(&fixture_descriptor()));

    assert!(failures.is_empty(), "unexpected failures: {failures:?}");
    Ok(())
}

#[sinex_test]
async fn acceptance_contract_reports_missing_privacy_and_occurrence()
-> xtask::sandbox::TestResult<()> {
    let mut assertions = complete_assertions();
    assertions.retain(|assertion| {
        !matches!(
            assertion,
            FixtureAssertion::PrivacyContext { .. } | FixtureAssertion::OccurrenceKey { .. }
        )
    });
    let spec = acceptance_spec(assertions);
    let failures = spec.acceptance_failures(&fixture_manifest(), Some(&fixture_descriptor()));
    let rendered = format!("{failures:?}");

    assert!(rendered.contains("privacy context assertion"));
    assert!(rendered.contains("occurrence identity assertion"));
    Ok(())
}

#[sinex_test]
async fn acceptance_contract_checks_manifest_descriptor_event_pair()
-> xtask::sandbox::TestResult<()> {
    let mut assertions = complete_assertions();
    for assertion in &mut assertions {
        if let FixtureAssertion::EventType { expected } = assertion {
            *expected = "shell.unclaimed".to_string();
        }
    }
    let spec = acceptance_spec(assertions);
    let failures = spec.acceptance_failures(&fixture_manifest(), Some(&fixture_descriptor()));
    let rendered = format!("{failures:?}");

    assert!(rendered.contains("manifest declares (terminal, shell.unclaimed)"));
    assert!(rendered.contains("descriptor declares (terminal, shell.unclaimed)"));
    Ok(())
}

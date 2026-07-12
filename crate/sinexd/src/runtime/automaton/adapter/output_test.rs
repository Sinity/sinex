use super::*;
use crate::runtime::AutomatonLogicError;
use crate::runtime::Transducer;
use crate::runtime::automaton::TransducerWrapper;
use sinex_primitives::derivation::{
    AdjudicationStatus, ClaimSupportTemplate, ClaimTemporalQuality, DerivationOutputDeclaration,
    DerivationWriteSurface, InputEligibility, SourceCoverage, SupportLevel,
};

#[derive(Debug, Default, serde::Serialize, serde::Deserialize)]
struct DeclarationGateTestState;

struct DeclarationGateTestAutomaton;

const DECLARATION_GATE_TEST_OUTPUTS: &[DerivationOutputDeclaration] =
    &[DerivationOutputDeclaration {
        declaration_id: "declaration-gate-test.test.output",
        owner: "declaration-gate-test",
        product_class: DerivedProductClass::CanonicalDerivedEvent,
        write_surface: DerivationWriteSurface::DerivedOutput,
        output_source: Some("declaration-gate-test"),
        output_event_type: Some("test.output"),
        projection_kind: None,
        artifact_kind: None,
        proposal_kind: None,
        semantics_version: "1.0.0",
        input_eligibility: InputEligibility::DefaultCanonicalInput,
        default_support: ClaimSupportTemplate::UNKNOWN,
        verification_command: "xtask test -p sinexd -E 'test(derived_output_declaration_gate)'",
    }];

impl Transducer for DeclarationGateTestAutomaton {
    type State = DeclarationGateTestState;
    type Input = JsonValue;
    type Output = JsonValue;

    fn name(&self) -> &'static str {
        "declaration-gate-test"
    }

    fn input_event_type(&self) -> &'static str {
        "test.input"
    }

    fn output_event_type(&self) -> &'static str {
        "test.output"
    }

    fn output_event_source(&self) -> &'static str {
        "declaration-gate-test"
    }

    const OUTPUT_DECLARATIONS: &'static [DerivationOutputDeclaration] =
        DECLARATION_GATE_TEST_OUTPUTS;

    async fn process(
        &mut self,
        _state: &mut Self::State,
        _input: Self::Input,
        _context: &AutomatonContext,
    ) -> std::result::Result<Option<DerivedOutput<Self::Output>>, AutomatonLogicError> {
        Ok(None)
    }
}

fn declaration_gate_runtime() -> AutomatonRuntime<TransducerWrapper<DeclarationGateTestAutomaton>> {
    AutomatonRuntime::new(TransducerWrapper(DeclarationGateTestAutomaton))
}

#[test]
fn derived_output_declaration_gate_accepts_transition_shape_with_no_declaration() {
    let runtime = declaration_gate_runtime();
    assert!(
        runtime
            .validate_output_declaration(None, None, None, "test.output")
            .is_ok()
    );
}

#[test]
fn derived_output_declaration_gate_rejects_product_class_without_declaration_id() {
    let runtime = declaration_gate_runtime();
    let error = runtime
        .validate_output_declaration(
            None,
            Some(DerivedProductClass::CanonicalDerivedEvent),
            None,
            "test.output",
        )
        .expect_err("product_class without declaration_id must be rejected");
    assert!(error.to_string().contains("without a declaration_id"));
}

#[test]
fn derived_output_declaration_gate_rejects_undeclared_declaration_id() {
    let runtime = declaration_gate_runtime();
    let error = runtime
        .validate_output_declaration(
            Some("declaration-gate-test.not.registered"),
            None,
            None,
            "test.output",
        )
        .expect_err("an unregistered declaration_id must be rejected");
    assert!(error.to_string().contains("undeclared declaration_id"));
}

#[test]
fn derived_output_declaration_gate_rejects_product_class_mismatch() {
    let runtime = declaration_gate_runtime();
    let error = runtime
        .validate_output_declaration(
            Some("declaration-gate-test.test.output"),
            Some(DerivedProductClass::AnalysisClaim),
            None,
            "test.output",
        )
        .expect_err("a product_class disagreeing with the declaration must be rejected");
    assert!(error.to_string().contains("product_class disagrees"));
}

#[test]
fn derived_output_declaration_gate_rejects_event_type_mismatch() {
    let runtime = declaration_gate_runtime();
    let error = runtime
        .validate_output_declaration(
            Some("declaration-gate-test.test.output"),
            None,
            None,
            "some.other.type",
        )
        .expect_err("an event_type disagreeing with the declaration must be rejected");
    assert!(error.to_string().contains("disagrees with its declaration"));
}

#[test]
fn derived_output_declaration_gate_accepts_matching_declaration() {
    let runtime = declaration_gate_runtime();
    assert!(
        runtime
            .validate_output_declaration(
                Some("declaration-gate-test.test.output"),
                Some(DerivedProductClass::CanonicalDerivedEvent),
                None,
                "test.output",
            )
            .is_ok()
    );
}

#[test]
fn claim_support_adapter_accepts_unreviewed_vector() {
    let runtime = declaration_gate_runtime();
    let claim_support = ClaimSupport::unreviewed(
        SupportLevel::Direct,
        SourceCoverage::Covered,
        ClaimTemporalQuality::InheritParent,
        1,
        1,
        1,
        0,
    );
    assert!(
        runtime
            .validate_output_declaration(None, None, Some(&claim_support), "test.output")
            .is_ok()
    );
}

#[test]
fn claim_support_adapter_rejects_adjudicated_vector_without_judgment_id() {
    let runtime = declaration_gate_runtime();
    // A wire-deserialized ClaimSupport can carry an Accepted adjudication with
    // no judgment event id (the compile-time constructors forbid constructing
    // this directly, but deserialization bypasses them) — the adapter must
    // still reject it defensively, mirroring `ClaimSupport::is_shape_valid()`.
    let malformed: ClaimSupport = serde_json::from_value(serde_json::json!({
        "support_level": "direct",
        "source_coverage": "covered",
        "temporal_quality": "inherit_parent",
        "adjudication": "accepted",
        "evidence_event_count": 1,
        "evidence_material_count": 1,
        "support_family_count": 1,
        "counterevidence_count": 0
    }))
    .expect("malformed ClaimSupport must still deserialize");
    assert_eq!(malformed.adjudication(), AdjudicationStatus::Accepted);
    assert!(malformed.adjudication_event_id().is_none());
    assert!(!malformed.is_shape_valid());

    let error = runtime
        .validate_output_declaration(None, None, Some(&malformed), "test.output")
        .expect_err("an adjudicated claim_support without a judgment id must be rejected");
    assert!(error.to_string().contains("adjudication_event_id"));
}

#[test]
fn parent_limit_warning_limiter_suppresses_until_interval() {
    let mut limiter = ParentLimitWarnState::default();
    let key = ParentLimitWarnKey {
        automaton: "analytics-automaton",
        phase: "live processing",
        output_event_type: "activity.window.summary",
    };
    let start = Instant::now();

    assert_eq!(limiter.should_log(key.clone(), start), Some(0));
    assert_eq!(
        limiter.should_log(key.clone(), start + Duration::from_secs(1)),
        None
    );
    assert_eq!(
        limiter.should_log(key.clone(), start + Duration::from_secs(30)),
        None
    );
    assert_eq!(
        limiter.should_log(
            key,
            start + DERIVED_OUTPUT_PARENT_WARN_LOG_INTERVAL + Duration::from_secs(1)
        ),
        Some(2)
    );
}

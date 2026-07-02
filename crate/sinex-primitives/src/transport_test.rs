use super::*;
use xtask::sandbox::sinex_test;

#[sinex_test]
async fn transport_headers_include_wire_and_semantic_classes() -> TestResult<()> {
    let mut headers = async_nats::HeaderMap::new();
    insert_transport_class_headers(&mut headers, Class::SourceMaterial);

    assert!(has_transport_class_headers(&headers));
    assert_eq!(
        headers
            .get(NATS_TRAFFIC_CLASS_HEADER)
            .map(std::string::ToString::to_string)
            .as_deref(),
        Some("source_material")
    );
    assert_eq!(
        headers
            .get(SINEX_TRANSPORT_CLASS_HEADER)
            .map(std::string::ToString::to_string)
            .as_deref(),
        Some("source_material")
    );
    Ok(())
}

#[sinex_test]
async fn route_decisions_cover_admission_and_durable_boundaries() -> TestResult<()> {
    let direct = route_decision("local.staged_parser.admission")
        .expect("local staged parser admission must be classified");
    assert_eq!(direct.transport, RouteTransport::Direct);
    assert_eq!(direct.class, None);
    assert!(direct.route.contains("AdmissionService"));

    let raw = route_decision("external.raw_event_intent")
        .expect("external raw event intent must be classified");
    assert_eq!(raw.transport, RouteTransport::JetStream);
    assert_eq!(raw.class, Some(Class::Critical));
    assert_eq!(raw.route, "{env}.events.raw.{source}.{event_type}");

    let control = route_decision("source.command_control")
        .expect("source command control must be classified");
    assert_eq!(control.transport, RouteTransport::CoreNats);
    assert_eq!(control.class, Some(Class::Control));

    let checkpoint =
        route_decision("runtime.checkpoints").expect("runtime checkpoints must be classified");
    assert_eq!(checkpoint.transport, RouteTransport::JetStreamKv);
    assert_eq!(checkpoint.class, None);

    Ok(())
}

#[sinex_test]
async fn route_decisions_are_operator_explainable() -> TestResult<()> {
    for decision in CURRENT_ROUTE_DECISIONS {
        assert!(!decision.path.is_empty(), "route decision path is empty");
        assert!(
            !decision.transport.label().is_empty(),
            "route decision {} has empty transport label",
            decision.path
        );
        assert!(
            !decision.route.is_empty(),
            "route decision {} has empty route",
            decision.path
        );
        assert!(
            !decision.reason.is_empty(),
            "route decision {} has empty reason",
            decision.path
        );
        assert!(
            !decision.degraded_behavior.is_empty(),
            "route decision {} has empty degraded behavior",
            decision.path
        );
        assert!(
            !decision.verification.is_empty(),
            "route decision {} has empty verification",
            decision.path
        );
    }

    assert!(
        CURRENT_ROUTE_DECISIONS
            .iter()
            .any(|decision| decision.transport == RouteTransport::Direct),
        "transport matrix must include at least one direct in-process path"
    );
    assert!(
        CURRENT_ROUTE_DECISIONS.iter().any(|decision| {
            decision.transport == RouteTransport::JetStream
                && matches!(
                    decision.class,
                    Some(Class::Critical | Class::Derived | Class::SourceMaterial)
                )
        }),
        "transport matrix must include at least one durable JetStream path"
    );

    Ok(())
}

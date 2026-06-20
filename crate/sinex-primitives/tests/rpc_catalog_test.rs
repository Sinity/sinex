use std::collections::BTreeSet;

use sinex_primitives::rpc::{RpcDomain, RpcMutability, RpcRole, method_catalog, methods};
use xtask::sandbox::prelude::*;

#[sinex_test]
async fn rpc_catalog_contains_unique_typed_method_descriptors() -> TestResult<()> {
    let catalog = method_catalog();

    assert!(
        catalog.len() >= 90,
        "RPC catalog unexpectedly shrank to {} methods",
        catalog.len()
    );

    let names = catalog
        .iter()
        .map(|method| method.name)
        .collect::<BTreeSet<_>>();
    assert_eq!(names.len(), catalog.len(), "RPC catalog method names");

    for method in &catalog {
        assert!(
            method.name.contains('.'),
            "RPC method name should include a domain separator: {}",
            method.name
        );
        assert!(
            !method.request_type.is_empty(),
            "RPC method {} missing request type",
            method.name
        );
        assert!(
            !method.response_type.is_empty(),
            "RPC method {} missing response type",
            method.name
        );
    }

    let finalize = catalog
        .iter()
        .find(|method| method.name == "curation.finalize")
        .expect("curation.finalize should be catalogued");
    assert_eq!(finalize.role, RpcRole::Write, "curation.finalize role");
    assert_eq!(
        finalize.mutability,
        RpcMutability::Mutating,
        "curation.finalize mutability"
    );

    let route_explain = catalog
        .iter()
        .find(|method| method.name == "llm.route.explain")
        .expect("llm.route.explain should be catalogued");
    assert_eq!(
        route_explain.role,
        RpcRole::ReadOnly,
        "llm.route.explain role"
    );
    assert_eq!(
        route_explain.mutability,
        RpcMutability::ReadOnly,
        "llm.route.explain mutability"
    );

    let browser_capture = catalog
        .iter()
        .find(|method| method.name == methods::BROWSER_CAPTURE_BATCH)
        .expect("browser.capture_batch should be catalogued");
    assert_eq!(browser_capture.role, RpcRole::Write);
    assert_eq!(browser_capture.domain, RpcDomain::Browser);
    assert_eq!(browser_capture.mutability, RpcMutability::Mutating);

    Ok(())
}

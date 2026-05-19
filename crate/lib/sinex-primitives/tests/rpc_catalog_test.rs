use std::collections::BTreeSet;

use sinex_primitives::rpc::{RpcMutability, RpcRole, method_catalog};
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

    Ok(())
}

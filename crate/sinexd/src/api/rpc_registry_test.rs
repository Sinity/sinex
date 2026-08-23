use xtask::sandbox::prelude::*;

/// sinex-kke3: `method_catalog()` (the operator-facing, documented method
/// list surfaced by `sinexctl` and referenced in MCP tool docs) must not
/// advertise a method that `build_registry_impl()` never actually registers
/// a handler for — a catalog/registry gap means the documented, catalog-
/// listed surface returns JSON-RPC -32601 "Unknown method" on every call.
///
/// This is a general-mechanism guard for the whole defect class (any future
/// method added to the catalog but never wired into the registry), not just
/// the three `coordination.*` methods sinex-kke3 found.
#[sinex_test]
async fn every_cataloged_method_is_registered_in_the_gateway_registry() -> TestResult<()> {
    let registered: std::collections::BTreeSet<String> =
        crate::api::rpc_registry::list_all_methods()
            .into_iter()
            .map(|(name, _role)| name)
            .collect();

    let missing: Vec<&'static str> = sinex_primitives::rpc::method_catalog()
        .into_iter()
        .map(|info| info.name)
        .filter(|name| !registered.contains(*name))
        .collect();

    assert!(
        missing.is_empty(),
        "method_catalog() advertises methods with no registered handler in \
         build_registry_impl() (they will 500/-32601 on every call): {missing:?}"
    );
    Ok(())
}

#[sinex_test]
async fn registry_build_surface_does_not_use_raw_registration_helpers() -> TestResult<()> {
    let source = include_str!("rpc_registry.rs");
    let registry_impl = source
        .split("fn build_registry_impl() -> RpcRegistry")
        .nth(1)
        .expect("registry implementation should exist")
        .split("#[cfg(test)]")
        .next()
        .expect("test module marker should delimit registry implementation");

    let forbidden = [".register(", "pool_rpc(", "pool_auth_rpc(", "nats_rpc("];
    for pattern in forbidden {
        assert!(
            !registry_impl.contains(pattern),
            "gateway registry build surface must use RpcMethod descriptor-backed typed helpers, found `{pattern}`"
        );
    }
    Ok(())
}

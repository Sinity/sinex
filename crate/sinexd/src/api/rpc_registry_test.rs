use xtask::sandbox::prelude::*;

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

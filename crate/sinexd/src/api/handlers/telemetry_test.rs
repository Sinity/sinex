use super::throughput_component;
use xtask::sandbox::sinex_test;

#[sinex_test]
async fn throughput_component_uses_source_role_reflection_bucket() -> xtask::TestResult<()> {
    assert_eq!(throughput_component("sinex"), "reflection");
    assert_eq!(throughput_component("sinex.metric"), "reflection");
    assert_eq!(throughput_component("sinexd.event_engine"), "reflection");
    assert_eq!(throughput_component("sinexd.api.gateway"), "gateway");
    assert_eq!(throughput_component("derived.interval-lift"), "derived");
    assert_eq!(throughput_component("terminal.atuin"), "ingestion");
    Ok(())
}

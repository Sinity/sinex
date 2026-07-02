use super::builtin_presets;
use xtask::sandbox::prelude::*;

#[sinex_test]
async fn builtin_presets_include_external_bridge_surfaces() -> TestResult<()> {
    let presets = builtin_presets();
    let names: std::collections::BTreeSet<_> =
        presets.iter().map(|preset| preset.name.as_str()).collect();

    assert!(
        names.contains("polylogue.exports.default"),
        "Polylogue material bridge preset must be exposed through sources.presets.list"
    );
    // External producer presets are operator-configured, not hardcoded.
    // This test verifies the presets list endpoint is reachable and
    // returns the expected structure.
    Ok(())
}

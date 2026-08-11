//! Regression coverage for sinex-5xm8: xtask is simultaneously the schema
//! bundle generator, the drift checker, and the dev-stack seeder for the
//! checked-in PUBLIC schema files -- so any regular (non-dev) build of xtask
//! must not silently enable `sinex-primitives`'s `testing` Cargo feature,
//! which gates test-only enum variants (e.g. `CurationJudgmentActorKind::
//! TestFixture`) specifically so they cannot be constructed from ordinary
//! JSON deserialization in a production binary.
//!
//! This parses the real `xtask/Cargo.toml` (not a hand-copied excerpt), so a
//! regression re-adding `"testing"` to `[dependencies]` (or any other
//! test-only feature name) fails this test without any change here.

use std::path::PathBuf;

/// Feature names that are only meant to unlock test-only code paths and must
/// never be requested from a regular (non-dev) dependency declaration.
const TEST_ONLY_FEATURE_NAMES: &[&str] = &["testing"];

#[test]
#[ignore = "sinex-5xm8 open: sinex-primitives is declared in [dependencies] (not [dev-dependencies]) with the test-only \"testing\" feature enabled"]
fn regular_dependencies_do_not_request_test_only_features() {
    let manifest_path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("Cargo.toml");
    let raw = std::fs::read_to_string(&manifest_path)
        .expect("xtask/Cargo.toml must be readable");
    let doc: toml::Value = raw.parse().expect("xtask/Cargo.toml must be valid TOML");

    let Some(deps) = doc.get("dependencies").and_then(|d| d.as_table()) else {
        panic!("xtask/Cargo.toml must have a [dependencies] table");
    };

    let mut offenders = Vec::new();
    for (dep_name, spec) in deps {
        let Some(features) = spec.get("features").and_then(|f| f.as_array()) else {
            continue;
        };
        for feature in features {
            let Some(feature_name) = feature.as_str() else {
                continue;
            };
            if TEST_ONLY_FEATURE_NAMES.contains(&feature_name) {
                offenders.push(format!("{dep_name} requests test-only feature \"{feature_name}\""));
            }
        }
    }

    assert!(
        offenders.is_empty(),
        "[dependencies] in xtask/Cargo.toml must never request a test-only feature -- xtask \
         builds the schema-bundle generator, drift checker, and dev-stack seeder for PUBLIC \
         schema files, so this leaks test-only code paths (e.g. \
         CurationJudgmentActorKind::TestFixture) into those regular binaries. Move the \
         dependency to [dev-dependencies], or gate the feature behind a Cargo target/binary \
         that is genuinely test-only. Offenders: {offenders:?}"
    );
}

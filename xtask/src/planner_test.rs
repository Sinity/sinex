use super::*;
use crate::sandbox::sinex_test;

#[sinex_test]
async fn test_affected_packages_maps_crate_paths() -> ::xtask::sandbox::TestResult<()> {
    let dirty = &[
        "crate/sinex-db/src/repositories/events.rs",
        "crate/sinex-primitives/src/lib.rs",
        "crate/sinexd/src/main.rs",
    ];
    let pkgs = affected_packages(dirty);
    assert!(pkgs.contains(&"sinex-db".to_string()));
    assert!(pkgs.contains(&"sinex-primitives".to_string()));
    assert!(pkgs.contains(&"sinexd".to_string()));
    assert_eq!(pkgs.len(), 3);
    Ok(())
}

#[sinex_test]
async fn test_affected_packages_unknown_path_returns_workspace()
-> ::xtask::sandbox::TestResult<()> {
    let pkgs = affected_packages(&["src/main.rs"]);
    assert_eq!(pkgs, vec!["--workspace"]);
    Ok(())
}

#[sinex_test]
async fn test_affected_packages_skips_nixos() -> ::xtask::sandbox::TestResult<()> {
    let pkgs = affected_packages(&["nixos/modules/services/sinex.nix"]);
    assert!(pkgs.is_empty());
    Ok(())
}

#[sinex_test]
async fn test_plan_next_actions_produces_result() -> ::xtask::sandbox::TestResult<()> {
    let result = plan_next_actions();
    assert!(result.is_ok());
    Ok(())
}

#[sinex_test]
async fn test_planned_action_serialization() -> ::xtask::sandbox::TestResult<()> {
    let action = PlannedAction {
        command: "xtask check -p sinex-db".to_string(),
        reason: "test reason".to_string(),
        priority: Priority::Now,
        confidence: 0.95,
    };
    let json = serde_json::to_string(&action).unwrap();
    assert!(json.contains("xtask check"));
    assert!(json.contains("now"));
    Ok(())
}

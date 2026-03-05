use sinex_ingestd::MaterialReadySet;
use sinex_primitives::Uuid;
use xtask::sandbox::prelude::*;

#[sinex_test]
async fn mark_ready_makes_material_visible() -> TestResult<()> {
    let set = MaterialReadySet::new();
    let id = Uuid::now_v7();

    assert!(!set.is_ready(&id));
    set.mark_ready(id);
    assert!(set.is_ready(&id));
    assert_eq!(set.len(), 1);
    Ok(())
}

#[sinex_test]
async fn clone_shares_state() -> TestResult<()> {
    let set = MaterialReadySet::new();
    let clone = set.clone();
    let id = Uuid::now_v7();

    set.mark_ready(id);
    assert!(clone.is_ready(&id));
    Ok(())
}

#[sinex_test]
async fn unknown_material_is_not_ready() -> TestResult<()> {
    let set = MaterialReadySet::new();
    let id = Uuid::now_v7();
    assert!(!set.is_ready(&id));
    Ok(())
}

#[sinex_test]
async fn default_creates_empty_set() -> TestResult<()> {
    let set = MaterialReadySet::default();
    assert!(set.is_empty());
    assert_eq!(set.len(), 0);
    Ok(())
}

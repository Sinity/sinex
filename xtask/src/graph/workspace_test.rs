use super::*;
use crate::sandbox::sinex_test;

#[sinex_test]
async fn test_workspace_graph_new() -> TestResult<()> {
    let result = WorkspaceGraph::new();
    assert!(result.is_ok(), "Failed to create WorkspaceGraph");
    Ok(())
}

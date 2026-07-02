use super::*;
use crate::sandbox::sinex_test;

#[sinex_test]
async fn test_dot_renderer_basic() -> TestResult<()> {
    let graph = WorkspaceGraph::new()?;
    let renderer = DotRenderer::new(graph);
    let output = renderer.render()?;

    // Verify basic DOT syntax elements
    assert!(output.starts_with("digraph dependencies {"));
    assert!(output.contains("rankdir=LR;"));
    assert!(output.contains("node [shape=box];"));
    assert!(output.ends_with("}\n"));

    // Verify output is not empty
    assert!(
        output.len() > 100,
        "Output seems too short: {} bytes",
        output.len()
    );

    // Verify nodes and edges are present
    let lines: Vec<&str> = output.lines().collect();
    let node_lines: Vec<&str> = lines
        .iter()
        .filter(|l| l.trim().ends_with(';') && !l.contains("->"))
        .copied()
        .collect();

    // Should have at least some nodes
    assert!(!node_lines.is_empty(), "No nodes found in output");

    Ok(())
}

#[sinex_test]
async fn test_escape_label() -> TestResult<()> {
    // Test escaping of double quotes
    assert_eq!(DotRenderer::escape_label("test"), "test");
    assert_eq!(DotRenderer::escape_label("test\"quote"), "test\\\"quote");
    assert_eq!(DotRenderer::escape_label("\"test\""), "\\\"test\\\"");
    Ok(())
}

#[sinex_test]
async fn test_dot_renderer_with_focus() -> TestResult<()> {
    let graph = WorkspaceGraph::new()?;
    let packages = graph.workspace_packages()?;

    // Use first package as focus target
    if !packages.is_empty() {
        let focus_pkg = packages[0].name().to_string();
        let renderer = DotRenderer::new(graph.clone()).with_focus(focus_pkg.clone(), false);
        let output = renderer.render()?;

        // Verify basic DOT syntax elements
        assert!(output.starts_with("digraph dependencies {"));
        assert!(output.contains("rankdir=LR;"));
        assert!(output.contains("node [shape=box];"));
        assert!(output.ends_with("}\n"));

        // Focus output should still contain valid DOT
        assert!(
            output.contains(&focus_pkg),
            "Focus package should appear in output"
        );
    }

    Ok(())
}

#[sinex_test]
async fn test_dot_renderer_builder_pattern() -> TestResult<()> {
    let graph = WorkspaceGraph::new()?;
    let packages = graph.workspace_packages()?;

    if !packages.is_empty() {
        let focus_pkg = packages[0].name().to_string();

        // Test forward mode
        let renderer_forward = DotRenderer::new(graph.clone()).with_focus(focus_pkg.clone(), false);
        let output_forward = renderer_forward.render()?;
        assert!(output_forward.starts_with("digraph dependencies {"));
        assert!(output_forward.ends_with("}\n"));

        // Test reverse mode
        let renderer_reverse = DotRenderer::new(graph.clone()).with_focus(focus_pkg, true);
        let output_reverse = renderer_reverse.render()?;
        assert!(output_reverse.starts_with("digraph dependencies {"));
        assert!(output_reverse.ends_with("}\n"));
    }

    Ok(())
}

#[sinex_test]
async fn test_ascii_renderer_renders_focused_dependency_tree() -> TestResult<()> {
    let graph = WorkspaceGraph::new()?;
    let packages = graph.workspace_packages()?;
    let focus_pkg = packages
        .first()
        .ok_or_else(|| color_eyre::eyre::eyre!("workspace should expose at least one package"))?
        .name()
        .to_string();

    let renderer = AsciiRenderer::new(&graph, Some(focus_pkg.clone()), 3);
    let output = renderer.render()?;

    assert!(output.contains(&focus_pkg));
    assert!(output.contains("└──") || output.contains("├──"));
    assert!(!output.contains("Full tree visualization will be available in Phase 3"));
    Ok(())
}

#[sinex_test]
async fn test_ascii_renderer_marks_depth_limit() -> TestResult<()> {
    let graph = WorkspaceGraph::new()?;
    let packages = graph.workspace_packages()?;
    let focus_pkg = packages
        .first()
        .ok_or_else(|| color_eyre::eyre::eyre!("workspace should expose at least one package"))?
        .name()
        .to_string();

    let renderer = AsciiRenderer::new(&graph, Some(focus_pkg), 0);
    let output = renderer.render()?;

    assert!(output.contains("(max depth)"));
    Ok(())
}

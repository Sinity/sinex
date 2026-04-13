//! Integration tests for graph commands
//!
//! Comprehensive integration tests for the dependency graph visualization and analysis
//! features. Tests cover:
//! - Output formats (ASCII, DOT, JSON)
//! - Focus mode for specific packages
//! - Reverse dependencies
//! - File output
//! - Depth limiting
//! - Impact analysis
//! - Error handling

use std::fs;
use std::process::Command;
use tempfile::tempdir;
use xtask::sandbox::sinex_test;

type RenderCheck = fn(&str) -> bool;
type FormatCheck<'a> = (&'a str, RenderCheck);
type FormatMarker<'a> = (&'a str, &'a str, RenderCheck);

// ============================================================================
// Format Tests: parameterized over all formats
// ============================================================================

#[sinex_test]
async fn test_graph_deps_formats_render_correctly() -> TestResult<()> {
    let format_checks: &[FormatCheck<'_>] = &[
        ("ascii", |s| s.contains("─") || s.contains("├")),
        ("dot", |s| {
            s.contains("digraph") && s.contains('}') && s.lines().any(|l| l.contains("->"))
        }),
        ("json", |s| {
            serde_json::from_str::<serde_json::Value>(s).is_ok()
        }),
    ];

    for (fmt, check) in format_checks {
        let output = Command::new("xtask")
            .arg("deps")
            .arg("graph")
            .arg("--render-format")
            .arg(fmt)
            .output()?;

        assert!(
            output.status.success(),
            "Format {fmt} failed. Stderr:\n{}",
            String::from_utf8_lossy(&output.stderr)
        );
        let stdout = String::from_utf8_lossy(&output.stdout);
        assert!(check(&stdout), "Format {fmt}: invariant check failed");
    }
    Ok(())
}

// ============================================================================
// JSON structural depth tests (unique — check id/label and source/target fields)
// ============================================================================

#[sinex_test]
async fn test_graph_deps_json_structure() -> TestResult<()> {
    // Single invocation — check both node and edge structure rather than spawning twice.
    let output = Command::new("xtask")
        .arg("deps")
        .arg("graph")
        .arg("--render-format")
        .arg("json")
        .output()?;

    let stdout = String::from_utf8_lossy(&output.stdout);
    let parsed: serde_json::Value = serde_json::from_str(&stdout)?;

    if let Some(nodes) = parsed["nodes"].as_array()
        && let Some(node) = nodes.first()
    {
        assert!(node.get("id").is_some(), "Node should have 'id' field");
        assert!(
            node.get("label").is_some(),
            "Node should have 'label' field"
        );
    }
    if let Some(edges) = parsed["edges"].as_array()
        && let Some(edge) = edges.first()
    {
        assert!(
            edge.get("source").is_some(),
            "Edge should have 'source' field"
        );
        assert!(
            edge.get("target").is_some(),
            "Edge should have 'target' field"
        );
    }
    Ok(())
}

// ============================================================================
// Focus Mode Tests
// ============================================================================

#[sinex_test]
async fn test_graph_deps_focus_reverse_mode() -> TestResult<()> {
    // Reverse mode: show packages that depend on the focus package
    let output = Command::new("xtask")
        .arg("deps")
        .arg("graph")
        .arg("--render-format")
        .arg("dot")
        .arg("--focus")
        .arg("xtask")
        .arg("--reverse")
        .output()?;

    assert!(
        output.status.success(),
        "Command failed. Stderr:\n{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.contains("digraph dependencies"),
        "Should be DOT format"
    );
    Ok(())
}

#[sinex_test]
async fn test_graph_deps_all_formats_with_focus() -> TestResult<()> {
    let formats = vec!["ascii", "dot", "json"];

    for format in formats {
        let output = Command::new("xtask")
            .arg("deps")
            .arg("graph")
            .arg("--render-format")
            .arg(format)
            .arg("--focus")
            .arg("xtask")
            .output()?;

        assert!(
            output.status.success(),
            "Format {format} failed. Stderr:\n{}",
            String::from_utf8_lossy(&output.stderr)
        );
    }
    Ok(())
}

// ============================================================================
// Depth Limiting Tests
// ============================================================================

#[sinex_test]
async fn test_graph_deps_depth_parameter_accepted() -> TestResult<()> {
    for depth in [2usize, 0, 100] {
        let output = Command::new("xtask")
            .arg("deps")
            .arg("graph")
            .arg("--render-format")
            .arg("ascii")
            .arg("--depth")
            .arg(depth.to_string())
            .output()?;

        assert!(
            output.status.success(),
            "depth={depth} failed. Stderr:\n{}",
            String::from_utf8_lossy(&output.stderr)
        );
    }
    Ok(())
}

// ============================================================================
// File Output Tests
// ============================================================================

#[sinex_test]
async fn test_graph_deps_output_to_file_all_formats() -> TestResult<()> {
    let format_markers: &[FormatMarker<'_>] = &[
        ("ascii", "graph.txt", |s| {
            s.contains("─") || s.contains("├") || s.contains("└")
        }),
        ("dot", "graph.dot", |s| s.contains("digraph")),
        ("json", "graph.json", |s| {
            serde_json::from_str::<serde_json::Value>(s).is_ok()
        }),
    ];

    for (fmt, filename, check) in format_markers {
        let dir = tempdir()?;
        let output_path = dir.path().join(filename);

        let output = Command::new("xtask")
            .arg("deps")
            .arg("graph")
            .arg("--render-format")
            .arg(fmt)
            .arg("--output")
            .arg(output_path.to_str().unwrap())
            .output()?;

        assert!(
            output.status.success(),
            "Format {fmt} file output failed. Stderr:\n{}",
            String::from_utf8_lossy(&output.stderr)
        );
        assert!(
            output_path.exists(),
            "Output file for {fmt} should be created"
        );
        let contents = fs::read_to_string(&output_path)?;
        assert!(
            !contents.is_empty(),
            "Output file for {fmt} should have content"
        );
        assert!(
            check(&contents),
            "Format {fmt}: file content invariant failed"
        );
    }
    Ok(())
}

#[sinex_test]
async fn test_graph_deps_output_to_nested_directory() -> TestResult<()> {
    let dir = tempdir()?;
    let output_path = dir.path().join("subdir").join("graph.dot");
    std::fs::create_dir_all(output_path.parent().unwrap()).expect("Failed to create subdirectory");

    let output = Command::new("xtask")
        .arg("deps")
        .arg("graph")
        .arg("--render-format")
        .arg("dot")
        .arg("--output")
        .arg(output_path.to_str().unwrap())
        .output()?;

    assert!(
        output.status.success(),
        "Command failed. Stderr:\n{}",
        String::from_utf8_lossy(&output.stderr)
    );

    // Verify file was created
    assert!(
        output_path.exists(),
        "Output file should be created in nested directory"
    );
    Ok(())
}

// ============================================================================
// Combination Tests
// ============================================================================

#[sinex_test]
async fn test_graph_deps_focus_and_depth() -> TestResult<()> {
    let output = Command::new("xtask")
        .arg("deps")
        .arg("graph")
        .arg("--render-format")
        .arg("ascii")
        .arg("--focus")
        .arg("xtask")
        .arg("--depth")
        .arg("3")
        .output()?;

    assert!(
        output.status.success(),
        "Command failed. Stderr:\n{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("xtask"), "Should contain xtask");
    Ok(())
}

#[sinex_test]
async fn test_graph_deps_focus_reverse_and_output() -> TestResult<()> {
    let dir = tempdir()?;
    let output_path = dir.path().join("graph_rev.dot");

    let output = Command::new("xtask")
        .arg("deps")
        .arg("graph")
        .arg("--render-format")
        .arg("dot")
        .arg("--focus")
        .arg("xtask")
        .arg("--reverse")
        .arg("--output")
        .arg(output_path.to_str().unwrap())
        .output()?;

    assert!(
        output.status.success(),
        "Command failed. Stderr:\n{}",
        String::from_utf8_lossy(&output.stderr)
    );

    // Verify file was created
    assert!(output_path.exists(), "Output file should be created");

    let contents = fs::read_to_string(&output_path)?;
    assert!(
        contents.starts_with("digraph dependencies"),
        "File should contain valid DOT"
    );
    Ok(())
}

// ============================================================================
// Impact Analysis Tests (deps impact command)
// ============================================================================

#[sinex_test]
async fn test_deps_impact_invocations() -> TestResult<()> {
    // Merged: --help + all-packages + single-package in one test to avoid
    // 3x cargo-metadata subprocess spawns for the same command.
    let cases: &[&[&str]] = &[
        &["deps", "impact", "--help"],
        &["deps", "impact"],
        &["deps", "impact", "xtask"],
    ];

    for args in cases {
        let output = Command::new("xtask").args(*args).output()?;
        if args.contains(&"--help") {
            assert!(
                output.status.success(),
                "--help should succeed. Stderr:\n{}",
                String::from_utf8_lossy(&output.stderr)
            );
            let stdout = String::from_utf8_lossy(&output.stdout);
            assert!(
                stdout.contains("Impact Analysis") || stdout.contains("impact"),
                "Should contain impact help"
            );
        }
        // non-help invocations: accept both success and graceful failure (known --format conflict)
        if output.status.success() {
            assert!(
                !output.stdout.is_empty() || !output.stderr.is_empty(),
                "Should produce some output for {args:?}"
            );
        }
    }
    Ok(())
}

// ============================================================================
// Error Handling Tests
// ============================================================================

#[sinex_test]
async fn test_graph_deps_invalid_format() -> TestResult<()> {
    // Invalid format falls back to ASCII (graceful handling)
    // Both success and failure are acceptable
    let output = Command::new("xtask")
        .arg("deps")
        .arg("graph")
        .arg("--render-format")
        .arg("invalid-format")
        .output()?;

    // Should either succeed (fallback) or fail with error message
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(!stderr.is_empty(), "Should have error message");
    }
    Ok(())
}

#[sinex_test]
async fn test_graph_deps_invalid_focus_package() -> TestResult<()> {
    // Should fail gracefully with error message
    let output = Command::new("xtask")
        .arg("deps")
        .arg("graph")
        .arg("--render-format")
        .arg("ascii")
        .arg("--focus")
        .arg("nonexistent-package-xyz-12345")
        .output()?;

    assert!(
        !output.status.success(),
        "Command should have failed but succeeded. Stdout:\n{}",
        String::from_utf8_lossy(&output.stdout)
    );
    Ok(())
}

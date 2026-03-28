//! Graph rendering in multiple formats

use color_eyre::eyre::{Result, WrapErr};
use guppy::graph::DependencyDirection;
use serde::{Deserialize, Serialize};
use std::collections::HashSet;

use crate::graph::WorkspaceGraph;

/// Trait for graph renderers
pub trait Renderer {
    /// Render the graph
    fn render(&self) -> Result<String>;
}

/// JSON graph representation compatible with D3.js force-directed layouts.
///
/// This structure represents a dependency graph in a JSON format suitable for visualization
/// with D3.js. Each node represents a package in the workspace, and each edge represents
/// a dependency relationship.
///
/// # Schema
///
/// ```json
/// {
///   "nodes": [
///     { "id": "sinex-db", "label": "sinex-db" },
///     { "id": "sinex-gateway", "label": "sinex-gateway" }
///   ],
///   "edges": [
///     { "source": "sinex-gateway", "target": "sinex-db" }
///   ]
/// }
/// ```
///
/// # D3.js Usage
///
/// To visualize this graph with D3.js, create a force-directed simulation:
///
/// ```javascript
/// d3.json("graph.json").then(data => {
///   const svg = d3.select("svg");
///   const width = +svg.attr("width");
///   const height = +svg.attr("height");
///
///   const simulation = d3.forceSimulation(data.nodes)
///     .force("link", d3.forceLink(data.edges).id(d => d.id).distance(100))
///     .force("charge", d3.forceManyBody().strength(-300))
///     .force("center", d3.forceCenter(width / 2, height / 2))
///     .force("collision", d3.forceCollide().radius(50));
///
///   // ... render nodes and links
/// });
/// ```
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GraphJson {
    /// All packages in the dependency graph, represented as nodes.
    ///
    /// Each node corresponds to a workspace package that can be depended upon
    /// or has dependencies.
    pub nodes: Vec<NodeJson>,
    /// All dependency relationships between packages.
    ///
    /// Each edge represents a dependency: `source` depends on `target`.
    /// This is a directed graph where the arrow points to the dependency.
    pub edges: Vec<EdgeJson>,
}

/// A single package node in the dependency graph.
///
/// Nodes represent packages within the workspace. Each node has a unique identifier
/// and can be referenced by edges to represent dependencies.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeJson {
    /// Unique package identifier (package name).
    ///
    /// This is the canonical name of the package as defined in Cargo.toml.
    /// Must be unique across all nodes and match any references in `EdgeJson`.
    pub id: String,
    /// Human-readable label for display purposes.
    ///
    /// Used by visualization tools for rendering. Currently identical to `id`,
    /// but kept separate to allow future customization (e.g., adding versions).
    pub label: String,
}

/// A directed edge representing a dependency relationship between packages.
///
/// In the context of workspace dependency graphs, an edge from A to B means
/// that package A depends on (imports from) package B. The direction indicates
/// the flow of dependency: the source package requires the target package.
///
/// # Example
///
/// In a workspace where `sinex-gateway` imports from `sinex-db`:
/// ```text
/// EdgeJson {
///     source: "sinex-gateway",  // depends on
///     target: "sinex-db"        // this package
/// }
/// ```
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EdgeJson {
    /// The package that has the dependency (the depending package).
    ///
    /// This is the package that imports or uses functionality from the target package.
    pub source: String,
    /// The package being depended upon (the dependency target).
    ///
    /// This is the package that provides functionality to the source package.
    pub target: String,
}

/// DOT (Graphviz) renderer with focus and clustering support
pub struct DotRenderer {
    graph: WorkspaceGraph,
    focus: Option<String>,
    reverse: bool,
}

impl DotRenderer {
    /// Create a new DOT renderer
    ///
    /// # Arguments
    /// * `graph` - The workspace graph to render
    ///
    /// # Returns
    /// A new `DotRenderer` instance
    pub fn new(graph: WorkspaceGraph) -> Self {
        Self {
            graph,
            focus: None,
            reverse: false,
        }
    }

    /// Set focus on a specific package with optional reverse dependency mode
    ///
    /// # Arguments
    /// * `package` - Name of the package to focus on
    /// * `reverse` - If true, show packages that depend on this one; if false, show dependencies
    ///
    /// # Returns
    /// Self for method chaining
    pub fn with_focus(mut self, package: String, reverse: bool) -> Self {
        self.focus = Some(package);
        self.reverse = reverse;
        self
    }

    /// Escape special characters in labels for DOT format
    ///
    /// # Arguments
    /// * `s` - String to escape
    ///
    /// # Returns
    /// Escaped string suitable for use in DOT identifiers
    fn escape_label(s: &str) -> String {
        s.replace('\"', "\\\"")
    }
}

impl Renderer for DotRenderer {
    /// Render graph in DOT format
    ///
    /// Generates Graphviz DOT syntax with workspace packages as nodes
    /// and dependencies as edges. Supports focus mode to show:
    /// - Forward mode: a package and its direct dependencies
    /// - Reverse mode: a package and its direct dependents
    /// - Uses left-to-right ranking for readability.
    fn render(&self) -> Result<String> {
        let mut dot = String::from("digraph dependencies {\n");
        dot.push_str("  rankdir=LR;\n");
        dot.push_str("  node [shape=box];\n\n");

        // Determine which packages to render
        let packages = if let Some(focus_pkg) = &self.focus {
            // Focus mode: show focus package + dependencies/dependents
            if self.reverse {
                // Reverse mode: show packages that depend on the focus package
                let dependents = self.graph.transitive_dependents(focus_pkg)?;
                let mut focused_pkg_names = vec![focus_pkg.clone()];
                focused_pkg_names.extend(dependents);

                // Filter workspace packages to only those in our focus set
                self.graph
                    .workspace_packages()?
                    .into_iter()
                    .filter(|p| focused_pkg_names.contains(&p.name().to_string()))
                    .collect()
            } else {
                // Forward mode: show focus package + its dependencies
                let deps = self.graph.all_dependencies(focus_pkg)?;
                let mut focused_pkg_names = vec![focus_pkg.clone()];
                focused_pkg_names.extend(deps.into_iter().map(|d| d.name));

                // Filter workspace packages to only those in our focus set
                self.graph
                    .workspace_packages()?
                    .into_iter()
                    .filter(|p| focused_pkg_names.contains(&p.name().to_string()))
                    .collect()
            }
        } else {
            // No focus: render all packages
            self.graph.workspace_packages()?
        };

        // Add nodes for all packages in the filtered set
        for pkg in &packages {
            let escaped_name = Self::escape_label(pkg.name());
            dot.push_str(&format!("  \"{escaped_name}\";\n"));
        }

        dot.push('\n');

        // Build a set of visible package names for edge filtering
        let visible_packages: HashSet<String> =
            packages.iter().map(|p| p.name().to_string()).collect();

        // Add edges for dependencies (only between visible packages)
        for pkg in &packages {
            // Query forward dependencies (what this package depends on)
            let query = self
                .graph
                .graph()
                .query_forward(std::iter::once(pkg.id()))?;
            let package_set = query.resolve();

            // Iterate through all dependency links
            for link in package_set.links(DependencyDirection::Forward) {
                let from_pkg = link.from();
                let to_pkg = link.to();
                let from_name = from_pkg.name().to_string();
                let to_name = to_pkg.name().to_string();

                // Only add edge if both packages are visible
                if visible_packages.contains(&from_name)
                    && visible_packages.contains(&to_name)
                    && from_name != to_name
                {
                    let escaped_from = Self::escape_label(&from_name);
                    let escaped_to = Self::escape_label(&to_name);
                    dot.push_str(&format!("  \"{escaped_from}\" -> \"{escaped_to}\";\n"));
                }
            }
        }

        dot.push_str("}\n");
        Ok(dot)
    }
}

/// JSON renderer for D3.js-compatible output
pub struct JsonRenderer {
    graph: WorkspaceGraph,
}

impl JsonRenderer {
    /// Create a new JSON renderer for the given workspace graph
    pub fn new(graph: WorkspaceGraph) -> Self {
        Self { graph }
    }
}

impl Renderer for JsonRenderer {
    /// Render graph in JSON format compatible with D3.js
    fn render(&self) -> Result<String> {
        let packages = self.graph.workspace_packages()?;

        // Create nodes from all packages
        let nodes: Vec<NodeJson> = packages
            .iter()
            .map(|pkg| NodeJson {
                id: pkg.name().to_string(),
                label: pkg.name().to_string(),
            })
            .collect();

        // Create edges from package dependencies
        let mut edges = Vec::new();
        for pkg in &packages {
            let pkg_id = pkg.id();
            // Query forward dependencies
            let query = self
                .graph
                .graph()
                .query_forward(vec![pkg_id])
                .with_context(|| format!("Failed to query forward dependencies for '{}'", pkg.name()))?;
            let resolved = query.resolve();
            // Get all packages this one depends on
            for dep_id in resolved.package_ids(guppy::graph::DependencyDirection::Forward) {
                if pkg_id == dep_id {
                    continue;
                }
                let dep_metadata = self
                    .graph
                    .graph()
                    .metadata(dep_id)
                    .with_context(|| format!("Failed to resolve dependency metadata while rendering '{}'", pkg.name()))?;
                edges.push(EdgeJson {
                    source: pkg.name().to_string(),
                    target: dep_metadata.name().to_string(),
                });
            }
        }

        let graph_json = GraphJson { nodes, edges };
        let json = serde_json::to_string_pretty(&graph_json)
            .context("Failed to serialize graph to JSON")?;

        Ok(json)
    }
}

/// ASCII tree renderer
pub struct AsciiRenderer {
    graph: WorkspaceGraph,
    focus: Option<String>,
    depth: usize,
}

impl AsciiRenderer {
    /// Create a new ASCII renderer with optional focus and depth limit
    pub fn new(graph: &WorkspaceGraph, focus: Option<String>, depth: usize) -> Self {
        Self {
            graph: graph.clone(),
            focus,
            depth,
        }
    }

    /// Style a package name with optional focus highlight (cyan + bold)
    ///
    /// # Arguments
    /// * `name` - The package name to style
    /// * `is_focus` - Whether this is the focused package
    ///
    /// # Returns
    /// The styled string with ANSI color codes (or plain string if not focus)
    fn style_package(&self, name: &str, is_focus: bool) -> String {
        if is_focus {
            // Cyan + bold for focus package: \x1b[1;36m = bold cyan, \x1b[0m = reset
            format!("\x1b[1;36m{name}\x1b[0m")
        } else {
            name.to_string()
        }
    }

    /// Style tree structure characters in gray
    ///
    /// # Arguments
    /// * `chars` - The tree characters to style (e.g., "└──", "├──", "│")
    ///
    /// # Returns
    /// The styled string with ANSI gray color codes
    fn style_tree_chars(&self, chars: &str) -> String {
        // Gray (bright black) color: \x1b[90m = bright black, \x1b[0m = reset
        format!("\x1b[90m{chars}\x1b[0m")
    }

    fn render_tree(
        &self,
        package: &str,
        prefix: &str,
        is_last: bool,
        current_depth: usize,
        visited: &mut HashSet<String>,
    ) -> Result<String> {
        let mut output = String::new();

        let is_focus = self.focus.as_ref().is_some_and(|f| f == package);
        let tree_chars = if is_last { "└──" } else { "├──" };
        let continuation = if is_last { " " } else { "│" };

        // Prevent infinite loops from circular deps
        if visited.contains(package) {
            output.push_str(&format!(
                "{}{} {} {}\n",
                prefix,
                self.style_tree_chars(tree_chars),
                self.style_package(package, is_focus),
                self.style_tree_chars("(circular)")
            ));
            return Ok(output);
        }

        if current_depth >= self.depth {
            output.push_str(&format!(
                "{}{} {} {}\n",
                prefix,
                self.style_tree_chars(tree_chars),
                self.style_package(package, is_focus),
                self.style_tree_chars("(max depth)")
            ));
            return Ok(output);
        }

        output.push_str(&format!(
            "{}{} {}\n",
            prefix,
            self.style_tree_chars(tree_chars),
            self.style_package(package, is_focus)
        ));

        visited.insert(package.to_string());

        let deps = self.graph.all_dependencies(package)?;
        let dep_count = deps.len();

        for (i, dep) in deps.iter().enumerate() {
            let is_last_dep = i == dep_count - 1;
            let new_prefix = format!("{}{}   ", prefix, self.style_tree_chars(continuation));

            output.push_str(&self.render_tree(
                &dep.name,
                &new_prefix,
                is_last_dep,
                current_depth + 1,
                visited,
            )?);
        }

        Ok(output)
    }
}

impl Renderer for AsciiRenderer {
    /// Render graph as ASCII tree
    fn render(&self) -> Result<String> {
        let mut output = String::new();
        let mut visited = HashSet::new();

        if let Some(focus_pkg) = &self.focus {
            output.push_str(&self.render_tree(focus_pkg, "", true, 0, &mut visited)?);
        } else {
            let packages = self.graph.workspace_packages()?;
            for (i, pkg) in packages.iter().enumerate() {
                let is_last = i == packages.len() - 1;
                output.push_str(&self.render_tree(pkg.name(), "", is_last, 0, &mut visited)?);
            }
        }

        Ok(output)
    }
}

#[cfg(test)]
mod tests {
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
            let renderer_forward =
                DotRenderer::new(graph.clone()).with_focus(focus_pkg.clone(), false);
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
}

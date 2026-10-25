//! Which workspace crates may depend on which, which crates must stay free of
//! IO, and two rules about source text: the daemon states no SQL (its
//! statements are checked queries in the store), and the store names no source
//! or event type.
use serde::Deserialize;
use std::{
    collections::{HashMap, HashSet},
    path::Path,
    process::Command,
};

#[derive(Deserialize)]
struct Metadata {
    packages: Vec<Package>,
    workspace_members: Vec<String>,
    resolve: Resolve,
}
#[derive(Deserialize)]
struct Package {
    id: String,
    name: String,
    dependencies: Vec<Dependency>,
    #[serde(default)]
    features: HashMap<String, Vec<String>>,
}
#[derive(Deserialize)]
struct Dependency {
    name: String,
    kind: Option<String>,
    target: Option<String>,
    #[serde(default)]
    optional: bool,
    #[serde(default)]
    rename: Option<String>,
}
#[derive(Deserialize)]
struct Resolve {
    nodes: Vec<Node>,
}
#[derive(Deserialize)]
struct Node {
    id: String,
    deps: Vec<Edge>,
    #[serde(default)]
    features: Vec<String>,
}
#[derive(Deserialize)]
struct Edge {
    pkg: String,
    dep_kinds: Vec<Kind>,
}
#[derive(Deserialize)]
struct Kind {
    kind: Option<String>,
}

fn allowed(name: &str) -> Result<&'static [&'static str], String> {
    match name {
        "sinex-macros" | "xtask" => Ok(&[]),
        "sinex-model" => Ok(&["sinex-macros"]),
        "sinex-protocol" | "sinex-schema" | "sinex-derivations" | "sinex-analysis" => {
            Ok(&["sinex-model"])
        }
        "sinex-transport" => Ok(&["sinex-model", "sinex-protocol"]),
        // Drivers include the producer side of the NATS ingress, which speaks
        // the protocol's frames.
        "sinex-sources" => Ok(&["sinex-model", "sinex-protocol", "sinex-transport"]),
        "sinex-store" => Ok(&["sinex-model", "sinex-schema"]),
        "sinex-library" => Ok(&["sinex-model", "sinex-store"]),
        "sinex-diagnostics" => Ok(&["sinex-model", "sinex-schema", "sinex-store"]),
        "sinexd" => Ok(&[
            "sinex-model",
            "sinex-protocol",
            "sinex-transport",
            "sinex-schema",
            "sinex-store",
            "sinex-sources",
            "sinex-derivations",
            "sinex-analysis",
            "sinex-library",
        ]),
        "sinex-session" | "sinex-capture" => {
            Ok(&["sinex-model", "sinex-protocol", "sinex-sources"])
        }
        "sinexctl" => Ok(&[
            "sinex-model",
            "sinex-protocol",
            "sinex-transport",
            "sinex-diagnostics",
        ]),
        _ => Err(format!("unassigned workspace package: {name}")),
    }
}
/// Crates of pure logic over model types: no IO in them or below them.
fn pure(name: &str) -> bool {
    matches!(
        name,
        "sinex-model" | "sinex-protocol" | "sinex-derivations" | "sinex-analysis"
    )
}
fn impure(name: &str) -> bool {
    matches!(
        name,
        "sinex-transport"
            | "tempfile"
            | "tokio"
            | "async-nats"
            | "notify"
            | "rusqlite"
            | "sqlx"
            | "rustls"
            | "native-tls"
            | "openssl"
            | "cipher"
            | "tracing"
            | "log"
    ) || name.starts_with("sqlx-")
}

fn validate(metadata: Metadata) -> Result<(), String> {
    let packages: HashMap<_, _> = metadata
        .packages
        .iter()
        .map(|p| (p.id.as_str(), p))
        .collect();
    let members: Vec<_> = metadata
        .workspace_members
        .iter()
        .map(|id| {
            packages
                .get(id.as_str())
                .copied()
                .ok_or_else(|| format!("missing workspace package: {id}"))
        })
        .collect::<Result<_, _>>()?;
    let names: HashSet<_> = members.iter().map(|p| p.name.as_str()).collect();
    for package in &members {
        let permitted = allowed(&package.name)?;
        // Declarations include optional, target-specific, build and development
        // dependencies, even when the host build does not activate them.
        // Renames keep the package name.
        for dep in &package.dependencies {
            let forbidden_internal =
                names.contains(dep.name.as_str()) && !permitted.contains(&dep.name.as_str());
            let forbidden_pure = pure(&package.name) && impure(&dep.name);
            // Statements live in the store; the schema crate applies the
            // declarations and the daemon only opens connections.
            let forbidden_sql = dep.name == "sqlx"
                && !matches!(
                    package.name.as_str(),
                    "sinex-store" | "sinex-schema" | "sinexd"
                );
            if forbidden_internal
                || forbidden_pure
                || forbidden_sql
                || (dep.name.starts_with("sqlx-")
                    && !matches!(package.name.as_str(), "sinex-store" | "sinex-schema"))
            {
                return Err(format!(
                    "forbidden dependency: {} -> {} (kind={}, target={})",
                    package.name,
                    dep.name,
                    dep.kind.as_deref().unwrap_or("normal"),
                    dep.target.as_deref().unwrap_or("all")
                ));
            }
        }
    }
    // Follow actual resolved normal/build edges to catch an I/O wrapper in a pure contract.
    // Development helpers are checked at their declared boundary, not as contract runtime.
    let nodes: HashMap<_, _> = metadata
        .resolve
        .nodes
        .iter()
        .map(|n| (n.id.as_str(), n))
        .collect();
    for package in members.iter().filter(|p| pure(&p.name)) {
        let mut pending = vec![package.id.as_str()];
        let mut seen = HashSet::new();
        while let Some(id) = pending.pop() {
            if !seen.insert(id) {
                continue;
            }
            let node = nodes
                .get(id)
                .ok_or_else(|| format!("missing resolved node: {id}"))?;
            for edge in &node.deps {
                if edge
                    .dep_kinds
                    .iter()
                    .all(|k| k.kind.as_deref() == Some("dev"))
                {
                    continue;
                }
                let dependency = packages
                    .get(edge.pkg.as_str())
                    .ok_or_else(|| format!("missing resolved package: {}", edge.pkg))?;
                let dependent = packages
                    .get(id)
                    .ok_or_else(|| format!("missing resolved package: {id}"))?;
                if !activated(dependent, node, &dependency.name) {
                    continue;
                }
                if impure(&dependency.name) {
                    return Err(format!(
                        "forbidden transitive dependency: {} -> {}",
                        package.name, dependency.name
                    ));
                }
                pending.push(edge.pkg.as_str());
            }
        }
    }
    Ok(())
}

/// Whether `dependent` really builds its dependency `name`. `cargo metadata`
/// lists an optional dependency as resolved when a feature only names it
/// weakly (`log?/std`), which does not enable it; count it only when a
/// resolved feature enables it (`dep:log`, `log/std`, or the implicit `log`).
fn activated(dependent: &Package, node: &Node, name: &str) -> bool {
    let declared: Vec<&Dependency> = dependent
        .dependencies
        .iter()
        .filter(|d| d.name == name && d.kind.as_deref() != Some("dev"))
        .collect();
    if declared.is_empty() || declared.iter().any(|d| !d.optional) {
        return true;
    }
    declared.iter().any(|d| {
        let key = d.rename.as_deref().unwrap_or(&d.name);
        node.features.iter().any(|enabled| {
            enabled == key
                || dependent.features.get(enabled).is_some_and(|items| {
                    items.iter().any(|item| {
                        item == &format!("dep:{key}") || item.starts_with(&format!("{key}/"))
                    })
                })
        })
    })
}

/// The target triple `rustc` builds for by default.
fn host_triple() -> Result<String, String> {
    let output = Command::new("rustc")
        .arg("-vV")
        .output()
        .map_err(|e| format!("could not execute rustc: {e}"))?;
    String::from_utf8_lossy(&output.stdout)
        .lines()
        .find_map(|l| l.strip_prefix("host: "))
        .map(str::to_string)
        .ok_or_else(|| "rustc -vV printed no host".to_string())
}

/// Check the declared and resolved dependency graph of the workspace at `root`,
/// then the source-text rules.
pub fn check(root: &Path) -> Result<(), String> {
    graph(root)?;
    let catalog = std::fs::read_to_string(root.join("nix/catalog.json"))
        .map_err(|e| format!("nix/catalog.json: {e}"))?;
    let names = catalog_names(&catalog)?;
    let mut problems = Vec::new();
    for (path, text) in product_files(&root.join("crates/sinexd/src"))? {
        // Excision's lock and planning statements are the one exception.
        if path.components().any(|c| c.as_os_str() == "excision") || path.ends_with("excision.rs") {
            continue;
        }
        if text.contains("sqlx::query") {
            problems.push(format!(
                "{}: SQL in the daemon; add a checked query to sinex-store",
                path.display()
            ));
        }
    }
    for (path, text) in product_files(&root.join("crates/sinex-store/src"))? {
        problems.extend(named_sources(&text, &names).into_iter().map(|name| {
            format!(
                "{}: the store names the source or event type {name}",
                path.display()
            )
        }));
    }
    if problems.is_empty() {
        Ok(())
    } else {
        Err(problems.join("\n"))
    }
}

/// Every source, derivation and event type name the catalog lists.
fn catalog_names(catalog: &str) -> Result<Vec<String>, String> {
    let catalog: serde_json::Value =
        serde_json::from_str(catalog).map_err(|e| format!("nix/catalog.json: {e}"))?;
    let mut names = Vec::new();
    for entry in ["sources", "derivations"]
        .iter()
        .filter_map(|k| catalog[k].as_array())
        .flatten()
    {
        names.extend(entry["name"].as_str().map(String::from));
        for list in ["eventTypes", "inputs", "outputs"] {
            names.extend(
                entry[list]
                    .as_array()
                    .into_iter()
                    .flatten()
                    .filter_map(|v| v.as_str().map(String::from)),
            );
        }
    }
    names.sort();
    names.dedup();
    Ok(names)
}

/// The names that appear quoted in `text`, as a Rust or SQL string.
fn named_sources<'a>(text: &str, names: &'a [String]) -> Vec<&'a str> {
    names
        .iter()
        .filter(|name| text.contains(&format!("\"{name}\"")) || text.contains(&format!("'{name}'")))
        .map(String::as_str)
        .collect()
}

/// The Rust files under `dir` that are not tests (`tests.rs`, `tests/`).
fn product_files(dir: &Path) -> Result<Vec<(std::path::PathBuf, String)>, String> {
    let mut files = Vec::new();
    let mut pending = vec![dir.to_path_buf()];
    while let Some(dir) = pending.pop() {
        let entries = std::fs::read_dir(&dir).map_err(|e| format!("{}: {e}", dir.display()))?;
        for entry in entries {
            let path = entry.map_err(|e| e.to_string())?.path();
            if path
                .file_name()
                .is_some_and(|n| n == "tests" || n == "tests.rs")
            {
                continue;
            }
            if path.is_dir() {
                pending.push(path);
            } else if path.extension().is_some_and(|e| e == "rs") {
                let text = std::fs::read_to_string(&path)
                    .map_err(|e| format!("{}: {e}", path.display()))?;
                files.push((path, text));
            }
        }
    }
    files.sort();
    Ok(files)
}

fn graph(root: &Path) -> Result<(), String> {
    // The graph for the platform the crates are built for: a dependency that
    // only another platform uses (`log` under wasm32) is not built here.
    let host = host_triple()?;
    let output = devkit::cargo_command()
        .args([
            "metadata",
            "--format-version",
            "1",
            "--all-features",
            "--locked",
            "--filter-platform",
            &host,
        ])
        .current_dir(root)
        .output()
        .map_err(|e| format!("could not execute cargo metadata: {e}"))?;
    if !output.status.success() {
        return Err(format!(
            "cargo metadata failed ({}): {}",
            output.status,
            String::from_utf8_lossy(&output.stderr)
        ));
    }
    let metadata = serde_json::from_slice(&output.stdout)
        .map_err(|e| format!("invalid cargo metadata: {e}"))?;
    validate(metadata)
}

#[cfg(test)]
mod tests;

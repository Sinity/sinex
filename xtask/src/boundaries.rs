//! Which workspace crates may depend on which, and which crates must stay free of IO.
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
}
#[derive(Deserialize)]
struct Dependency {
    name: String,
    kind: Option<String>,
    target: Option<String>,
}
#[derive(Deserialize)]
struct Resolve {
    nodes: Vec<Node>,
}
#[derive(Deserialize)]
struct Node {
    id: String,
    deps: Vec<Edge>,
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
        "sinex-protocol" | "sinex-schema" | "sinex-sources" | "sinex-automata"
        | "sinex-analysis" => Ok(&["sinex-model"]),
        "sinex-store" => Ok(&["sinex-model", "sinex-schema"]),
        "sinex-library" => Ok(&["sinex-model", "sinex-store"]),
        "sinex-offline" => Ok(&["sinex-model", "sinex-schema", "sinex-store"]),
        "sinexd" => Ok(&[
            "sinex-model",
            "sinex-protocol",
            "sinex-schema",
            "sinex-store",
            "sinex-sources",
            "sinex-automata",
            "sinex-analysis",
            "sinex-library",
        ]),
        "sinex-session" | "sinex-capture" => {
            Ok(&["sinex-model", "sinex-protocol", "sinex-sources"])
        }
        "sinexctl" => Ok(&["sinex-model", "sinex-protocol", "sinex-offline"]),
        _ => Err(format!("unassigned workspace package: {name}")),
    }
}
/// Crates of pure logic over model types: no IO in them or below them.
fn pure(name: &str) -> bool {
    matches!(
        name,
        "sinex-model" | "sinex-protocol" | "sinex-automata" | "sinex-analysis"
    )
}
fn impure(name: &str) -> bool {
    matches!(
        name,
        "tokio"
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
        // Declarations include optional, target-specific, build and development dependencies,
        // even when the host build does not activate them. Renames retain the package name.
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

/// Check the declared and resolved dependency graph of the workspace at `root`.
pub fn check(root: &Path) -> Result<(), String> {
    let output = Command::new("cargo")
        .args([
            "metadata",
            "--format-version",
            "1",
            "--all-features",
            "--locked",
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

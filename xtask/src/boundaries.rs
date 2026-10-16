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
    features: Vec<String>,
    uses_default_features: bool,
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
        "sinex-model" | "xtask" => Ok(&[]),
        "sinex-protocol" | "sinex-store" => Ok(&["sinex-model"]),
        "sinexd" => Ok(&["sinex-model", "sinex-protocol", "sinex-store"]),
        "sinexctl" => Ok(&["sinex-model", "sinex-protocol"]),
        _ => Err(format!("unassigned workspace package: {name}")),
    }
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
            let forbidden_pure = matches!(package.name.as_str(), "sinex-model" | "sinex-protocol")
                && impure(&dep.name);
            let forbidden_sql = dep.name == "sqlx"
                && match package.name.as_str() {
                    "sinex-store" => false,
                    // Provider acquisition is SQLite-only. Canonical PostgreSQL belongs to store.
                    "sinexd" => {
                        dep.uses_default_features
                            || dep
                                .features
                                .iter()
                                .any(|f| !matches!(f.as_str(), "runtime-tokio" | "sqlite"))
                    }
                    _ => true,
                };
            if forbidden_internal
                || forbidden_pure
                || forbidden_sql
                || (dep.name.starts_with("sqlx-") && package.name != "sinex-store")
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
    for package in members
        .iter()
        .filter(|p| matches!(p.name.as_str(), "sinex-model" | "sinex-protocol"))
    {
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

// Keep the frontend bound to its current workspace/configuration. Cargo itself
// parses the ordinary selection/compiler/test flags after this narrow guard.
pub fn routing_features(args: &[String]) -> Result<Vec<String>, String> {
    let mut features = Vec::new();
    let mut args = args.iter();
    while let Some(arg) = args.next() {
        if arg == "--" {
            break;
        }
        if ["--manifest-path", "--config", "--lockfile-path"]
            .iter()
            .any(|key| arg == key || arg.starts_with(&format!("{key}=")))
            || arg.starts_with("-C")
        {
            return Err(format!(
                "unsupported Cargo routing override: {arg}; run xtask from the intended workspace with its own Cargo configuration"
            ));
        }
        if arg == "--features" || arg == "-F" {
            let value = args
                .next()
                .ok_or_else(|| format!("missing feature selection after {arg}"))?;
            features.extend(["--features".to_owned(), value.clone()]);
        } else if arg.starts_with("--features=") || (arg.starts_with("-F") && arg.len() > 2) {
            features.push(arg.clone());
        }
    }
    Ok(features)
}

pub fn check(root: &Path, features: &[String]) -> Result<(), String> {
    let output = Command::new("cargo")
        .args([
            "metadata",
            "--format-version",
            "1",
            "--all-features",
            "--locked",
        ])
        .args(features)
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
mod tests {
    use super::*;
    #[test]
    fn malformed_metadata_is_an_error() {
        for input in ["{}", r#"{"packages":[],"workspace_members":[]}"#] {
            assert!(serde_json::from_str::<Metadata>(input).is_err());
        }
    }
    #[test]
    fn unknown_workspace_member_is_not_implicitly_allowed() {
        assert_eq!(
            allowed("sinex-surprise").unwrap_err(),
            "unassigned workspace package: sinex-surprise"
        );
    }
}

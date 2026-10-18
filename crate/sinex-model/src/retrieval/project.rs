//! Which configured project a working directory belongs to.
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};

/// A configured project. Each root is an absolute path; a trailing `*` in the last
/// component matches any directory name with that prefix.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Project {
    pub name: String,
    pub roots: Vec<String>,
}
impl Project {
    pub fn validate_all(projects: &[Self]) -> Result<(), &'static str> {
        let mut names = BTreeSet::new();
        for p in projects {
            if p.name.is_empty()
                || !names.insert(&p.name)
                || p.roots.is_empty()
                || p.roots.iter().any(|r| {
                    !r.starts_with('/')
                        || r.contains("..")
                        || r.matches('*').count() > 1
                        || r.strip_suffix('*').unwrap_or(r).contains('*')
                        || (r.ends_with('/') && r != "/")
                })
            {
                return Err(
                    "projects require unique names and absolute roots; only a final-component trailing * is supported",
                );
            }
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "state", rename_all = "snake_case")]
pub enum ProjectEvidence {
    Known { project: String },
    Unconfigured { reason: String },
    Unknown { reason: String },
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct UnknownProjectMember {
    pub event: String,
    pub evidence: ProjectEvidence,
}
/// How a session's direct members split across projects. `rule` names how members
/// were attributed.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SessionProjectAttribution {
    pub rule: String,
    pub known_projects: BTreeMap<String, usize>,
    pub unconfigured_members: usize,
    pub unknown_members: Vec<UnknownProjectMember>,
    pub total_members: usize,
    pub complete: bool,
}

pub fn project(cwd: &str, projects: &[Project]) -> Option<String> {
    match project_evidence(Some(cwd), projects) {
        ProjectEvidence::Known { project } => Some(project),
        _ => None,
    }
}

/// The project with the deepest matching root. Two different projects matching at
/// the same depth make the answer unknown.
pub fn project_evidence(cwd: Option<&str>, projects: &[Project]) -> ProjectEvidence {
    let unknown = |reason: &str| ProjectEvidence::Unknown {
        reason: reason.into(),
    };
    let Some(cwd) = cwd.filter(|cwd| !cwd.is_empty()) else {
        return unknown("missing_cwd");
    };
    if !cwd.starts_with('/')
        || cwd.contains('\0')
        || cwd.split('/').any(|c| matches!(c, "." | ".."))
    {
        return unknown("invalid_cwd");
    }
    let mut matches: Vec<(usize, &str)> = projects
        .iter()
        .flat_map(|p| p.roots.iter().map(move |root| (root, p.name.as_str())))
        .filter(|(root, _)| root_matches(root, cwd))
        .map(|(root, name)| (root.split('/').filter(|c| !c.is_empty()).count(), name))
        .collect();
    matches.sort();
    let Some((depth, name)) = matches.pop() else {
        return ProjectEvidence::Unconfigured {
            reason: "no_configured_root".into(),
        };
    };
    if matches.iter().any(|(d, n)| *d == depth && *n != name) {
        unknown("ambiguous_roots")
    } else {
        ProjectEvidence::Known {
            project: name.into(),
        }
    }
}

fn root_matches(root: &str, cwd: &str) -> bool {
    if root == "/" {
        cwd.starts_with('/')
    } else if let Some(prefix) = root.strip_suffix('*') {
        // The wildcard component must be present, though it may equal the prefix.
        cwd.starts_with(prefix)
            && !cwd[prefix.rfind('/').unwrap_or(0) + 1..]
                .split('/')
                .next()
                .unwrap_or("")
                .is_empty()
    } else {
        cwd == root || cwd.strip_prefix(root).is_some_and(|s| s.starts_with('/'))
    }
}

#[cfg(test)]
#[path = "project_tests.rs"]
mod tests;

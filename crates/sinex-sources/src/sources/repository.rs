//! Commits in a local git repository, on `ApiCursor` (read with `gix`).
//!
//! Each poll walks from every ref's current tip and stops at the tips the
//! previous poll saw, so only commits reachable for the first time are read.
//! A commit is immutable and named by its ID, so reading it again (a ref moved
//! back, a branch recreated) is a repeat.
use crate::drivers::DriverError;
use crate::drivers::api_cursor::{Api, ApiCursor, Page};
use crate::registry::{DeclaredSource, Driver, SourceSpec};
use crate::{Parser, RecordError};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};
use sinex_model::event_types::code::GitCommit;
use sinex_model::{Class, InputRef, Timestamp};
use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;

pub const SPEC: SourceSpec = SourceSpec {
    name: "code.git",
    driver: Driver::ApiCursor,
    excisable: false,
    purpose: "work episodes, project attribution and recall",
    class: Class::Open,
    parser: "git-commits",
    parser_version: 1,
    event_types: &["git.commit"],
    framer: crate::Framer::incremental(crate::drivers::append_file::lines),
};

pub struct GitParser;

impl Parser for GitParser {
    type Record<'a> = Map<String, Value>;
    type Carry = ();
    type Payload = GitCommit;

    fn parse(
        &self,
        record: &Map<String, Value>,
        _: &(),
        _context: &crate::RecordContext<'_>,
    ) -> Result<(Vec<GitCommit>, ()), RecordError> {
        let commit: GitCommit = serde_json::from_value(Value::Object(record.clone()))
            .map_err(|e| RecordError::new(format!("not a commit record: {e}")))?;
        Ok((vec![commit], ()))
    }
}

pub fn source() -> DeclaredSource<GitParser> {
    DeclaredSource {
        spec: SPEC,
        parser: GitParser,
    }
}

/// The tips seen by the last poll.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Seen {
    pub tips: BTreeSet<String>,
}

pub struct Repository {
    pub path: PathBuf,
    pub host: String,
}

fn error(e: impl std::fmt::Display) -> DriverError {
    DriverError::Io(e.to_string())
}

fn time(t: gix::date::Time) -> Result<Timestamp, DriverError> {
    Timestamp::from_unix_secs(t.seconds).map_err(|e| error(format!("commit time: {e}")))
}

impl Api for Repository {
    fn input(&self) -> InputRef {
        InputRef {
            kind: "git".into(),
            locator: self.path.display().to_string(),
            host: self.host.clone(),
        }
    }

    fn fetch(&self, cursor: &Value) -> Result<Page, DriverError> {
        let seen: Seen = if cursor.is_null() {
            Seen::default()
        } else {
            serde_json::from_value(cursor.clone())
                .map_err(|e| DriverError::Cursor(e.to_string()))?
        };
        let repo = gix::open(&self.path).map_err(error)?;
        let repository = repo
            .workdir()
            .unwrap_or_else(|| repo.git_dir())
            .display()
            .to_string();

        // Every ref's tip commit, and which refs point at each.
        let mut refs_at: BTreeMap<gix::ObjectId, Vec<String>> = BTreeMap::new();
        let platform = repo.references().map_err(error)?;
        for reference in platform.all().map_err(error)? {
            let mut reference = reference.map_err(error)?;
            let name = reference.name().as_bstr().to_string();
            // Refs that do not lead to a commit (a tag on a tree) have no history.
            let Ok(commit) = reference.peel_to_commit() else {
                continue;
            };
            refs_at.entry(commit.id).or_default().push(name);
        }
        if let Ok(head) = repo.head_commit() {
            refs_at.entry(head.id).or_default().push("HEAD".into());
        }
        let tips: Vec<gix::ObjectId> = refs_at.keys().copied().collect();
        let hidden: Vec<gix::ObjectId> = seen
            .tips
            .iter()
            .filter_map(|hex| gix::ObjectId::from_hex(hex.as_bytes()).ok())
            // A tip that no longer exists (garbage-collected) cannot bound the walk.
            .filter(|id| repo.find_object(*id).is_ok())
            .collect();

        let mut records = Vec::new();
        if !tips.is_empty() {
            let walk = repo
                .rev_walk(tips.iter().copied())
                .with_hidden(hidden)
                .all()
                .map_err(error)?;
            for info in walk {
                let info = info.map_err(error)?;
                let commit = repo.find_commit(info.id).map_err(error)?;
                let decoded = commit.decode().map_err(error)?;
                let author = decoded.author().map_err(error)?;
                let committer = decoded.committer().map_err(error)?;
                let record = GitCommit {
                    repository: repository.clone(),
                    commit: info.id.to_string(),
                    parents: decoded.parents().map(|p| p.to_string()).collect(),
                    tree: decoded.tree().to_string(),
                    author_name: author.name.to_string(),
                    author_email: author.email.to_string(),
                    authored_at: time(author.time().map_err(error)?)?,
                    committer_name: committer.name.to_string(),
                    committer_email: committer.email.to_string(),
                    committed_at: time(committer.time().map_err(error)?)?,
                    message: String::from_utf8_lossy(decoded.message).into_owned(),
                    refs: refs_at.get(&info.id).cloned().unwrap_or_default(),
                };
                match serde_json::to_value(record).map_err(error)? {
                    Value::Object(object) => records.push(object),
                    _ => unreachable!("a struct serializes to an object"),
                }
            }
        }
        let next = Seen {
            tips: tips.iter().map(ToString::to_string).collect(),
        };
        Ok(Page {
            records,
            next: json!(next),
        })
    }
}

pub fn driver(path: PathBuf, host: String) -> ApiCursor<Repository> {
    ApiCursor {
        api: Repository { path, host },
    }
}

/// `--read repository=PATH`: a local repository's commits.
pub const FORMAT: crate::binding::InputFormat = crate::binding::InputFormat {
    name: "repository",
    specs: || vec![&SPEC],
    plan: Some(|input, settings| Ok(crate::binding::single(&SPEC, input, settings))),
    source: |_| Ok(Box::new(source())),
    reader: Some(|b| {
        Ok(crate::binding::stateless(driver(
            b.input.clone(),
            b.settings.host.clone(),
        )))
    }),
    capture: None,
};

#[cfg(test)]
mod tests;

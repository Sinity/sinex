//! Commits and ref updates in a local git repository, on `ApiCursor` (read
//! with `gix`).
//!
//! Each poll walks from the commit of every ref and every checkout's HEAD and
//! stops at the commits the previous poll saw, so only commits reachable for
//! the first time are read. A commit is immutable and named by its ID, so
//! reading it again (a ref moved back, a branch recreated) is a repeat. Each
//! commit carries what it changed against its first parent, file by file,
//! with the exact objects and modes, and the refs and checkouts at it when it
//! was read.
//!
//! Each poll also compares every ref with its target at the previous poll,
//! except `HEAD`s and `refs/stash`, whose moves are checkouts and stash
//! entries rather than history. A ref moved to a commit that does not descend
//! from its old target is a rewrite (a force-push, a reset): its update lists
//! the commits the old target reached that no ref or checkout reaches now.
//! They are recorded, never removed.
//!
//! A commit that cannot be decoded is a record failure of its own; the rest
//! of the poll is read.
use base64::Engine;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};
use sinex_model::event_types::code::{
    FileChange, GitCommit, GitFileChange, GitRecord, GitRefUpdated, RefChange,
};
use sinex_model::{Class, InputRef, Timestamp};
use sinex_source_core::drivers::DriverError;
use sinex_source_core::drivers::api_cursor::{Api, ApiCursor, Page};
use sinex_source_core::registry::{DeclaredSource, Driver, SourceSpec};
use sinex_source_core::{Parser, RecordError};
use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;

pub const SPEC: SourceSpec = SourceSpec {
    name: "code.git",
    driver: Driver::ApiCursor,
    excisable: false,
    purpose: "work episodes, project attribution and recall",
    class: Class::Open,
    parser: "git-commits",
    parser_version: 2,
    event_types: &["git.commit", "git.ref_updated"],
    framer: sinex_source_core::Framer::incremental(sinex_source_core::drivers::append_file::lines),
};

pub struct GitParser;

impl Parser for GitParser {
    type Record<'a> = Map<String, Value>;
    type Carry = ();
    type Payload = GitRecord;

    fn parse(
        &self,
        record: &Map<String, Value>,
        _: &(),
        _context: &sinex_source_core::RecordContext<'_>,
    ) -> Result<(Vec<GitRecord>, ()), RecordError> {
        let value = Value::Object(record.clone());
        let payload = match record.get("record").and_then(Value::as_str) {
            None => GitRecord::Commit(
                serde_json::from_value(value)
                    .map_err(|e| RecordError::new(format!("not a commit record: {e}")))?,
            ),
            Some("ref_updated") => GitRecord::RefUpdated(
                serde_json::from_value(value)
                    .map_err(|e| RecordError::new(format!("not a ref update: {e}")))?,
            ),
            Some("unreadable") => {
                return Err(RecordError::new(format!(
                    "commit {} could not be read: {}",
                    record.get("commit").and_then(Value::as_str).unwrap_or("?"),
                    record.get("reason").and_then(Value::as_str).unwrap_or("?"),
                )));
            }
            Some(other) => return Err(RecordError::new(format!("unknown record {other:?}"))),
        };
        Ok((vec![payload], ()))
    }
}

pub fn source() -> DeclaredSource<GitParser> {
    DeclaredSource {
        spec: SPEC,
        parser: GitParser,
    }
}

/// What the last poll saw.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Seen {
    /// The commit of every ref and checkout HEAD.
    pub tips: BTreeSet<String>,
    /// The target of each compared ref.
    #[serde(default)]
    pub refs: BTreeMap<String, String>,
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

/// Whether a ref's moves are compared between polls.
fn compared(name: &str) -> bool {
    name.starts_with("refs/") && name != "refs/stash"
}

/// Text that was meant to be UTF-8: the text, and its bytes when they were
/// not UTF-8 (kept in `original_bytes` under `pointer`).
struct Text<'a> {
    original_bytes: &'a mut BTreeMap<String, String>,
}

impl Text<'_> {
    fn read(&mut self, pointer: &str, bytes: &[u8], encoding: Option<&[u8]>) -> String {
        if let Ok(text) = std::str::from_utf8(bytes) {
            return text.to_owned();
        }
        self.original_bytes.insert(
            pointer.to_owned(),
            base64::engine::general_purpose::STANDARD.encode(bytes),
        );
        match encoding.and_then(encoding_rs::Encoding::for_label) {
            Some(encoding) => encoding.decode(bytes).0.into_owned(),
            None => String::from_utf8_lossy(bytes).into_owned(),
        }
    }
}

fn mode(mode: gix::objs::tree::EntryMode) -> String {
    format!("{:o}", mode.value())
}

/// The files `commit` changed against its first parent.
fn changes(
    repo: &gix::Repository,
    commit: &gix::Commit<'_>,
    text: &mut Text<'_>,
) -> Result<Vec<GitFileChange>, String> {
    use gix::diff::tree::recorder::Change;
    let tree = repo
        .find_tree(commit.tree_id().map_err(|e| e.to_string())?)
        .map_err(|e| e.to_string())?;
    let parent_tree = match commit.parent_ids().next() {
        Some(parent) => {
            let unreadable = |e: &dyn std::fmt::Display| format!("first parent {parent}: {e}");
            let parent = repo.find_commit(parent).map_err(|e| unreadable(&e))?;
            let tree = parent.tree_id().map_err(|e| unreadable(&e))?;
            Some(repo.find_tree(tree).map_err(|e| unreadable(&e))?)
        }
        None => None,
    };
    let empty: &[u8] = &[];
    let before = gix::objs::TreeRefIter::from_bytes(
        parent_tree.as_ref().map_or(empty, |t| &t.data),
        repo.object_hash(),
    );
    let after = gix::objs::TreeRefIter::from_bytes(&tree.data, repo.object_hash());
    let mut recorder = gix::diff::tree::Recorder::default();
    gix::diff::tree(
        before,
        after,
        gix::diff::tree::State::default(),
        &repo.objects,
        &mut recorder,
    )
    .map_err(|e| e.to_string())?;
    let mut records: Vec<(Vec<u8>, GitFileChange)> = recorder
        .records
        .into_iter()
        .filter_map(|change| {
            let (path, change) = match change {
                Change::Addition {
                    entry_mode,
                    oid,
                    path,
                    ..
                } if !entry_mode.is_tree() => (
                    path,
                    GitFileChange {
                        path: String::new(),
                        change: FileChange::Added,
                        mode: Some(mode(entry_mode)),
                        object: Some(oid.to_string()),
                        previous_mode: None,
                        previous_object: None,
                    },
                ),
                Change::Deletion {
                    entry_mode,
                    oid,
                    path,
                    ..
                } if !entry_mode.is_tree() => (
                    path,
                    GitFileChange {
                        path: String::new(),
                        change: FileChange::Deleted,
                        mode: None,
                        object: None,
                        previous_mode: Some(mode(entry_mode)),
                        previous_object: Some(oid.to_string()),
                    },
                ),
                Change::Modification {
                    previous_entry_mode,
                    previous_oid,
                    entry_mode,
                    oid,
                    path,
                } if !(entry_mode.is_tree() && previous_entry_mode.is_tree()) => (
                    path,
                    GitFileChange {
                        path: String::new(),
                        change: FileChange::Modified,
                        mode: Some(mode(entry_mode)),
                        object: Some(oid.to_string()),
                        previous_mode: Some(mode(previous_entry_mode)),
                        previous_object: Some(previous_oid.to_string()),
                    },
                ),
                _ => return None,
            };
            Some((path.into(), change))
        })
        .collect();
    records.sort_by(|a, b| a.0.cmp(&b.0));
    Ok(records
        .into_iter()
        .enumerate()
        .map(|(index, (path, mut change))| {
            change.path = text.read(&format!("/files/{index}/path"), &path, None);
            change
        })
        .collect())
}

/// The record for one commit, or why it could not be read.
fn commit_record(
    repo: &gix::Repository,
    repository: &str,
    id: gix::ObjectId,
    refs: Vec<String>,
    worktrees: Vec<String>,
) -> Result<Value, String> {
    let commit = repo.find_commit(id).map_err(|e| e.to_string())?;
    let decoded = commit.decode().map_err(|e| e.to_string())?;
    let author = decoded.author().map_err(|e| e.to_string())?;
    let committer = decoded.committer().map_err(|e| e.to_string())?;
    let mut original_bytes = BTreeMap::new();
    let mut text = Text {
        original_bytes: &mut original_bytes,
    };
    let encoding = decoded.encoding.map(|e| e.as_ref());
    let author_name = text.read("/author_name", author.name, None);
    let author_email = text.read("/author_email", author.email, None);
    let committer_name = text.read("/committer_name", committer.name, None);
    let committer_email = text.read("/committer_email", committer.email, None);
    let message = text.read("/message", decoded.message, encoding);
    let files = changes(repo, &commit, &mut text)?;
    let record = GitCommit {
        repository: repository.to_owned(),
        commit: id.to_string(),
        parents: decoded.parents().map(|p| p.to_string()).collect(),
        tree: decoded.tree().to_string(),
        author_name,
        author_email,
        authored_at: time(author.time().map_err(|e| e.to_string())?).map_err(|e| e.to_string())?,
        committer_name,
        committer_email,
        committed_at: time(committer.time().map_err(|e| e.to_string())?)
            .map_err(|e| e.to_string())?,
        message,
        refs,
        worktrees,
        files: Some(files),
        original_bytes,
    };
    serde_json::to_value(record).map_err(|e| e.to_string())
}

/// The commits reachable from `from` but from none of `hidden`, newest first.
fn reachable_only_from(
    repo: &gix::Repository,
    from: gix::ObjectId,
    hidden: &[gix::ObjectId],
) -> Result<Vec<gix::ObjectId>, DriverError> {
    let walk = repo
        .rev_walk([from])
        .with_hidden(hidden.iter().copied())
        .all()
        .map_err(error)?;
    walk.map(|info| info.map(|i| i.id).map_err(error)).collect()
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
        let observed_at = Timestamp::now();
        let repo = gix::open(&self.path).map_err(error)?;
        let repository = repo
            .workdir()
            .unwrap_or_else(|| repo.git_dir())
            .display()
            .to_string();

        // Every ref's commit, which refs point at each, and the compared refs.
        let mut refs_at: BTreeMap<gix::ObjectId, Vec<String>> = BTreeMap::new();
        let mut targets: BTreeMap<String, gix::ObjectId> = BTreeMap::new();
        let platform = repo.references().map_err(error)?;
        for reference in platform.all().map_err(error)? {
            let mut reference = reference.map_err(error)?;
            let name = reference.name().as_bstr().to_string();
            // Refs that do not lead to a commit (a tag on a tree) have no history.
            let Ok(commit) = reference.peel_to_commit() else {
                continue;
            };
            refs_at.entry(commit.id).or_default().push(name.clone());
            if compared(&name) {
                targets.insert(name, commit.id);
            }
        }
        if let Ok(head) = repo.head_commit() {
            refs_at.entry(head.id).or_default().push("HEAD".into());
        }
        // Each checkout's HEAD: the main one and every linked worktree.
        let mut checkouts_at: BTreeMap<gix::ObjectId, Vec<String>> = BTreeMap::new();
        for checkout in repo.worktrees_including_main().map_err(error)? {
            let Ok(checkout) = checkout else { continue };
            let (Some(path), Ok(head)) = (checkout.workdir(), checkout.head_id()) else {
                continue;
            };
            checkouts_at
                .entry(head.detach())
                .or_default()
                .push(path.display().to_string());
        }
        let tips: BTreeSet<gix::ObjectId> =
            refs_at.keys().chain(checkouts_at.keys()).copied().collect();
        let tip_list: Vec<gix::ObjectId> = tips.iter().copied().collect();
        let hidden: Vec<gix::ObjectId> = seen
            .tips
            .iter()
            .filter_map(|hex| gix::ObjectId::from_hex(hex.as_bytes()).ok())
            // A tip that no longer exists (garbage-collected) cannot bound the walk.
            .filter(|id| repo.find_object(*id).is_ok())
            .collect();

        let mut records = Vec::new();
        if !tip_list.is_empty() {
            let walk = repo
                .rev_walk(tip_list.iter().copied())
                .with_hidden(hidden)
                .all()
                .map_err(error)?;
            for info in walk {
                let id = info.map_err(error)?.id;
                let refs = refs_at.get(&id).cloned().unwrap_or_default();
                let worktrees = checkouts_at.get(&id).cloned().unwrap_or_default();
                let record = commit_record(&repo, &repository, id, refs, worktrees).unwrap_or_else(
                    |reason| {
                        json!({"record": "unreadable", "repository": repository,
                               "commit": id.to_string(), "reason": reason})
                    },
                );
                match record {
                    Value::Object(object) => records.push(object),
                    _ => unreachable!("a record is an object"),
                }
            }
        }

        // Ref updates since the last poll.
        let mut names: BTreeSet<&String> = seen.refs.keys().collect();
        names.extend(targets.keys());
        for name in names {
            let before = seen
                .refs
                .get(name)
                .and_then(|hex| gix::ObjectId::from_hex(hex.as_bytes()).ok());
            let after = targets.get(name).copied();
            if before == after {
                continue;
            }
            let (change, unreachable) = match (before, after) {
                (None, _) => (RefChange::Created, Vec::new()),
                (Some(old), new) => {
                    let unreachable = if repo.find_object(old).is_ok() {
                        reachable_only_from(&repo, old, &tip_list)?
                    } else {
                        Vec::new()
                    };
                    let change = match new {
                        None => RefChange::Deleted,
                        Some(_) if repo.find_object(old).is_err() => RefChange::Rewritten,
                        Some(new) => {
                            if reachable_only_from(&repo, old, &[new])?.is_empty() {
                                RefChange::FastForward
                            } else {
                                RefChange::Rewritten
                            }
                        }
                    };
                    (change, unreachable)
                }
            };
            let update = GitRefUpdated {
                repository: repository.clone(),
                ref_name: name.clone(),
                before: before.map(|id| id.to_string()),
                after: after.map(|id| id.to_string()),
                change,
                unreachable: unreachable.iter().map(ToString::to_string).collect(),
                observed_at,
            };
            match serde_json::to_value(update).map_err(error)? {
                Value::Object(mut object) => {
                    object.insert("record".into(), json!("ref_updated"));
                    records.push(object);
                }
                _ => unreachable!("a struct serializes to an object"),
            }
        }

        let next = Seen {
            tips: tips.iter().map(ToString::to_string).collect(),
            refs: targets
                .iter()
                .map(|(name, id)| (name.clone(), id.to_string()))
                .collect(),
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

/// `--read repository=PATH`: a local repository's commits and ref updates.
pub const FORMAT: sinex_source_core::binding::InputFormat =
    sinex_source_core::binding::InputFormat {
        name: "repository",
        discovery: sinex_source_core::discovery::DiscoverySpec {
            owner: sinex_source_core::discovery::Owner::System,
            locations: &[sinex_source_core::discovery::Location {
                root: "projects",
                path: "",
            }],
            capabilities: &[],
            credentials: &[],
            streams: &[],
            requests: &[],
            request_only: false,
        },
        discover: crate::discovery::discover_format,
        specs: || vec![&SPEC],
        plan: Some(|input, settings| {
            Ok(sinex_source_core::binding::single(&SPEC, input, settings))
        }),
        source: |_| Ok(Box::new(source())),
        reader: Some(|b| {
            Ok(sinex_source_core::binding::polled(driver(
                b.input.clone(),
                b.settings.host.clone(),
            )))
        }),
        capture: None,
    };

#[cfg(test)]
mod tests;

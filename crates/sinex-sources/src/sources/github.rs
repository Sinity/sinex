//! A GitHub repository's pull requests (with their reviews and the checks on
//! their newest commit), workflow runs and issues, on `ApiCursor`.
//!
//! The binding's input is the local repository; the GitHub repository is the
//! one its `origin` remote names (option `remote`, or `repository` as
//! `owner/name`). The API token is read from the file the `token_file` option
//! names, at each ask; nothing is written back to GitHub. The API is asked at
//! most once per `interval_seconds` (600 by default), and after a refusal not
//! again until GitHub's rate-limit reset, or the interval, has passed.
//!
//! Pull requests come from GraphQL, newest update first, down to the newest
//! update the previous ask read; their reviews and checks are paged to the
//! end. Checks can finish without the pull request changing, so the open pull
//! requests' checks are asked for at every ask and recorded when they differ
//! from the last recorded ones. Issues come from the REST API by update time.
//! Workflow runs come newest first, down to the oldest run that was
//! unfinished at the previous ask; a run is recorded when its attempt, status
//! or update time changes. A re-run of a run older than that is seen through
//! its pull request's checks.
use crate::drivers::DriverError;
use crate::drivers::api_cursor::{Api, ApiCursor, Page};
use crate::registry::{DeclaredSource, Driver, SourceSpec};
use crate::sources::fields;
use crate::{Parser, RecordError};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};
use sinex_model::event_types::code::{
    GithubCheck, GithubIssue, GithubPullRequest, GithubRecord, GithubReview, GithubWorkflowRun,
};
use sinex_model::{Class, InputRef, Timestamp};
use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;
use std::sync::Mutex;
use std::time::{Duration, Instant};

pub const SPEC: SourceSpec = SourceSpec {
    name: "code.github",
    driver: Driver::ApiCursor,
    excisable: false,
    purpose: "the outcome of work: pull requests, reviews, checks and CI runs beside the commits",
    class: Class::Open,
    parser: "github",
    parser_version: 1,
    event_types: &[
        "github.pull_request",
        "github.review",
        "github.check",
        "github.workflow_run",
        "github.issue",
    ],
    framer: crate::Framer::incremental(crate::drivers::append_file::lines),
};

pub struct GithubParser;

impl Parser for GithubParser {
    type Record<'a> = Map<String, Value>;
    type Carry = ();
    type Payload = GithubRecord;

    fn parse(
        &self,
        record: &Map<String, Value>,
        _: &(),
        _context: &crate::RecordContext<'_>,
    ) -> Result<(Vec<GithubRecord>, ()), RecordError> {
        let repository = fields::text(record, "repository")?;
        let object = |name: &str| {
            record
                .get(name)
                .and_then(Value::as_object)
                .ok_or_else(|| RecordError::new(format!("no `{name}` object")))
        };
        let out = match record.get("record").and_then(Value::as_str) {
            Some("pull_request") => pull_request(&repository, object("item")?)?,
            Some("checks") => {
                let commit = fields::text(record, "commit")?;
                let contexts = record
                    .get("contexts")
                    .and_then(Value::as_array)
                    .ok_or_else(|| RecordError::new("no `contexts`"))?;
                checks(&repository, &commit, contexts)?
            }
            Some("workflow_run") => vec![workflow_run(&repository, object("item")?)?],
            Some("issue") => vec![issue(&repository, object("item")?)?],
            other => return Err(RecordError::new(format!("unknown record {other:?}"))),
        };
        Ok((out, ()))
    }
}

/// The value at `path` (`author/login`, `commits/nodes/0`), when present
/// and not null.
fn at<'v>(item: &'v Map<String, Value>, path: &str) -> Option<&'v Value> {
    let mut parts = path.split('/');
    let mut value = item.get(parts.next()?)?;
    for part in parts {
        value = match value {
            Value::Object(object) => object.get(part)?,
            Value::Array(list) => list.get(part.parse::<usize>().ok()?)?,
            _ => return None,
        };
    }
    (!value.is_null()).then_some(value)
}

fn text_at(item: &Map<String, Value>, path: &str) -> Option<String> {
    at(item, path).and_then(Value::as_str).map(str::to_owned)
}

fn required_text(item: &Map<String, Value>, path: &str) -> Result<String, RecordError> {
    text_at(item, path).ok_or_else(|| RecordError::new(format!("no `{path}`")))
}

/// Text that is absent when empty (GitHub writes an empty body as `""`).
fn content_at(item: &Map<String, Value>, path: &str) -> Option<String> {
    text_at(item, path).filter(|t| !t.is_empty())
}

/// A state word, lowercased so GraphQL's `MERGED` and REST's `completed`
/// read alike.
fn word_at(item: &Map<String, Value>, path: &str) -> Option<String> {
    text_at(item, path).map(|w| w.to_ascii_lowercase())
}

fn number_at(item: &Map<String, Value>, path: &str) -> Result<u64, RecordError> {
    at(item, path)
        .and_then(Value::as_u64)
        .ok_or_else(|| RecordError::new(format!("no number `{path}`")))
}

fn time_at(item: &Map<String, Value>, path: &str) -> Result<Option<Timestamp>, RecordError> {
    text_at(item, path).map(|t| fields::rfc3339(&t)).transpose()
}

fn required_time(item: &Map<String, Value>, path: &str) -> Result<Timestamp, RecordError> {
    time_at(item, path)?.ok_or_else(|| RecordError::new(format!("no time `{path}`")))
}

fn nodes<'v>(item: &'v Map<String, Value>, path: &str) -> &'v [Value] {
    at(item, path)
        .and_then(Value::as_array)
        .map_or(&[], Vec::as_slice)
}

fn as_object(value: &Value) -> Result<&Map<String, Value>, RecordError> {
    value
        .as_object()
        .ok_or_else(|| RecordError::new("an item is not an object"))
}

fn pull_request(
    repository: &str,
    item: &Map<String, Value>,
) -> Result<Vec<GithubRecord>, RecordError> {
    let number = number_at(item, "number")?;
    let mut out = vec![GithubRecord::PullRequest(GithubPullRequest {
        repository: repository.to_owned(),
        number,
        title: required_text(item, "title")?,
        body: content_at(item, "body"),
        state: word_at(item, "state").ok_or_else(|| RecordError::new("no `state`"))?,
        draft: at(item, "isDraft").and_then(Value::as_bool) == Some(true),
        author: text_at(item, "author/login"),
        labels: nodes(item, "labels/nodes")
            .iter()
            .filter_map(|l| l.get("name").and_then(Value::as_str).map(str::to_owned))
            .collect(),
        created_at: required_time(item, "createdAt")?,
        updated_at: required_time(item, "updatedAt")?,
        closed_at: time_at(item, "closedAt")?,
        merged_at: time_at(item, "mergedAt")?,
        merge_commit: text_at(item, "mergeCommit/oid"),
        head_ref: required_text(item, "headRefName")?,
        head_commit: required_text(item, "headRefOid")?,
        base_ref: required_text(item, "baseRefName")?,
        base_commit: required_text(item, "baseRefOid")?,
        url: required_text(item, "url")?,
    })];
    for review in nodes(item, "reviews/nodes") {
        let review = as_object(review)?;
        out.push(GithubRecord::Review(GithubReview {
            repository: repository.to_owned(),
            pull_request: number,
            review_id: number_at(review, "databaseId")?,
            author: text_at(review, "author/login"),
            state: word_at(review, "state").ok_or_else(|| RecordError::new("no review `state`"))?,
            body: content_at(review, "body"),
            commit: text_at(review, "commit/oid"),
            submitted_at: time_at(review, "submittedAt")?,
            url: text_at(review, "url"),
        }));
    }
    if let Some(head) = nodes(item, "commits/nodes").first() {
        let head = as_object(head)?;
        if let Some(commit) = text_at(head, "commit/oid") {
            let contexts = nodes(head, "commit/statusCheckRollup/contexts/nodes");
            out.extend(checks(repository, &commit, contexts)?);
        }
    }
    Ok(out)
}

fn checks(
    repository: &str,
    commit: &str,
    contexts: &[Value],
) -> Result<Vec<GithubRecord>, RecordError> {
    contexts
        .iter()
        .map(|context| {
            let context = as_object(context)?;
            let check = match text_at(context, "__typename").as_deref() {
                Some("CheckRun") => GithubCheck {
                    repository: repository.to_owned(),
                    commit: commit.to_owned(),
                    reported_as: "check_run".into(),
                    name: required_text(context, "name")?,
                    check_run: Some(number_at(context, "databaseId")?),
                    status: word_at(context, "status")
                        .ok_or_else(|| RecordError::new("no check `status`"))?,
                    conclusion: word_at(context, "conclusion"),
                    app: text_at(context, "checkSuite/app/slug"),
                    workflow_run: at(context, "checkSuite/workflowRun/databaseId")
                        .and_then(Value::as_u64),
                    description: None,
                    started_at: time_at(context, "startedAt")?,
                    completed_at: time_at(context, "completedAt")?,
                    url: text_at(context, "detailsUrl"),
                },
                Some("StatusContext") => {
                    let state = word_at(context, "state")
                        .ok_or_else(|| RecordError::new("no status `state`"))?;
                    GithubCheck {
                        repository: repository.to_owned(),
                        commit: commit.to_owned(),
                        reported_as: "status".into(),
                        name: required_text(context, "context")?,
                        check_run: None,
                        conclusion: (state != "pending" && state != "expected")
                            .then(|| state.clone()),
                        status: state,
                        app: None,
                        workflow_run: None,
                        description: content_at(context, "description"),
                        started_at: time_at(context, "createdAt")?,
                        completed_at: None,
                        url: text_at(context, "targetUrl"),
                    }
                }
                other => {
                    return Err(RecordError::new(format!("unknown check kind {other:?}")));
                }
            };
            Ok(GithubRecord::Check(check))
        })
        .collect()
}

fn workflow_run(repository: &str, item: &Map<String, Value>) -> Result<GithubRecord, RecordError> {
    Ok(GithubRecord::WorkflowRun(GithubWorkflowRun {
        repository: repository.to_owned(),
        run_id: number_at(item, "id")?,
        attempt: at(item, "run_attempt").and_then(Value::as_u64).unwrap_or(1),
        run_number: number_at(item, "run_number")?,
        workflow: text_at(item, "name"),
        title: text_at(item, "display_title"),
        event: required_text(item, "event")?,
        status: word_at(item, "status"),
        conclusion: word_at(item, "conclusion"),
        head_branch: text_at(item, "head_branch"),
        head_commit: required_text(item, "head_sha")?,
        actor: text_at(item, "actor/login"),
        created_at: required_time(item, "created_at")?,
        started_at: time_at(item, "run_started_at")?,
        updated_at: required_time(item, "updated_at")?,
        url: required_text(item, "html_url")?,
    }))
}

fn issue(repository: &str, item: &Map<String, Value>) -> Result<GithubRecord, RecordError> {
    let logins = |pointer: &str| -> Vec<String> {
        nodes(item, pointer)
            .iter()
            .filter_map(|v| v.get("login").and_then(Value::as_str).map(str::to_owned))
            .collect()
    };
    Ok(GithubRecord::Issue(GithubIssue {
        repository: repository.to_owned(),
        number: number_at(item, "number")?,
        title: required_text(item, "title")?,
        body: content_at(item, "body"),
        state: word_at(item, "state").ok_or_else(|| RecordError::new("no `state`"))?,
        state_reason: word_at(item, "state_reason"),
        author: text_at(item, "user/login"),
        labels: nodes(item, "labels")
            .iter()
            .filter_map(|l| l.get("name").and_then(Value::as_str).map(str::to_owned))
            .collect(),
        assignees: logins("assignees"),
        comments: at(item, "comments").and_then(Value::as_u64).unwrap_or(0),
        created_at: required_time(item, "created_at")?,
        updated_at: required_time(item, "updated_at")?,
        closed_at: time_at(item, "closed_at")?,
        url: required_text(item, "html_url")?,
    }))
}

pub fn source() -> DeclaredSource<GithubParser> {
    DeclaredSource {
        spec: SPEC,
        parser: GithubParser,
    }
}

/// How far a list ordered by update time was read: the newest update read,
/// and the items read at exactly that time (GitHub's times are whole seconds,
/// and `since` includes its own second).
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Since {
    pub at: Option<Timestamp>,
    pub read_at: BTreeSet<u64>,
}

impl Since {
    fn is_new(&self, number: u64, updated: Timestamp) -> bool {
        match self.at {
            None => true,
            Some(at) => updated > at || updated == at && !self.read_at.contains(&number),
        }
    }

    fn advance(&mut self, number: u64, updated: Timestamp) {
        match self.at {
            Some(at) if updated < at => {}
            Some(at) if updated == at => {
                self.read_at.insert(number);
            }
            _ => {
                self.at = Some(updated);
                self.read_at = BTreeSet::from([number]);
            }
        }
    }
}

/// The workflow runs still being followed: those created at or after
/// `floor`, each with its last recorded state.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Runs {
    pub floor: Option<Timestamp>,
    pub states: BTreeMap<u64, String>,
}

/// The cursor: when the API was last asked and how far each list was read.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Position {
    pub asked_at: Option<Timestamp>,
    pub pull_requests: Since,
    pub issues: Since,
    pub runs: Runs,
    /// The hash of each open pull request's checks as last recorded.
    pub open_checks: BTreeMap<u64, String>,
}

pub struct Github {
    /// The local repository the binding names.
    pub path: PathBuf,
    pub host: String,
    /// `owner/name`, or `None` to take it from `remote`.
    pub repository: Option<String>,
    pub remote: String,
    pub token_file: PathBuf,
    pub api: String,
    pub interval: Duration,
    agent: ureq::Agent,
    /// No ask before this instant (after a refusal or failure).
    wait_until: Mutex<Option<Instant>>,
}

impl Github {
    pub fn new(
        path: PathBuf,
        host: String,
        token_file: PathBuf,
        repository: Option<String>,
    ) -> Self {
        Self {
            path,
            host,
            repository,
            remote: "origin".into(),
            token_file,
            api: "https://api.github.com".into(),
            interval: Duration::from_secs(600),
            agent: ureq::Agent::config_builder()
                .user_agent("sinex")
                .timeout_global(Some(Duration::from_secs(120)))
                .http_status_as_error(false)
                .build()
                .into(),
            wait_until: Mutex::new(None),
        }
    }
}

/// `owner/name` from a GitHub remote URL (`git@github.com:owner/name.git`,
/// `https://github.com/owner/name`, `ssh://git@github.com/owner/name.git`).
pub fn repository_of(url: &str) -> Option<String> {
    let rest = [
        "git@github.com:",
        "ssh://git@github.com/",
        "https://github.com/",
        "http://github.com/",
        "git://github.com/",
    ]
    .iter()
    .find_map(|prefix| url.strip_prefix(prefix))?;
    let rest = rest.trim_end_matches('/');
    let rest = rest.strip_suffix(".git").unwrap_or(rest);
    let (owner, name) = rest.split_once('/')?;
    (!owner.is_empty() && !name.is_empty() && !name.contains('/'))
        .then(|| format!("{owner}/{name}"))
}

/// The `rel="next"` URL of a `Link` header.
fn next_link(link: &str) -> Option<String> {
    link.split(',').find_map(|part| {
        let (url, rel) = part.split_once(';')?;
        rel.contains("rel=\"next\"").then(|| {
            url.trim()
                .trim_start_matches('<')
                .trim_end_matches('>')
                .to_owned()
        })
    })
}

fn time(raw: &str) -> Result<Timestamp, DriverError> {
    fields::rfc3339(raw).map_err(|e| DriverError::Io(format!("GitHub time {raw:?}: {}", e.0)))
}

fn item_time(item: &Value, name: &str) -> Result<Timestamp, DriverError> {
    time(
        item.get(name)
            .and_then(Value::as_str)
            .ok_or_else(|| DriverError::Io(format!("GitHub item without `{name}`")))?,
    )
}

fn item_number(item: &Value, name: &str) -> Result<u64, DriverError> {
    item.get(name)
        .and_then(Value::as_u64)
        .ok_or_else(|| DriverError::Io(format!("GitHub item without `{name}`")))
}

fn array<'a>(value: &'a Value, name: &str) -> Result<&'a Vec<Value>, DriverError> {
    value.as_array().ok_or_else(|| DriverError::Io(format!("GitHub {name} is not an array")))
}

fn next_cursor(connection: &Value, seen: &mut std::collections::BTreeSet<String>) -> Result<Option<String>, DriverError> {
    let has_next = connection.pointer("/pageInfo/hasNextPage").and_then(Value::as_bool)
        .ok_or_else(|| DriverError::Io("GitHub connection has no page information".into()))?;
    if !has_next { return Ok(None); }
    let cursor = connection.pointer("/pageInfo/endCursor").and_then(Value::as_str)
        .filter(|cursor| !cursor.is_empty())
        .ok_or_else(|| DriverError::Io("GitHub next page has no cursor".into()))?;
    if !seen.insert(cursor.to_owned()) {
        return Err(DriverError::Io("GitHub repeated a page cursor".into()));
    }
    Ok(Some(cursor.to_owned()))
}

const CONTEXT: &str = "fragment Context on StatusCheckRollupContext {
  __typename
  ... on CheckRun { databaseId name status conclusion startedAt completedAt detailsUrl
    checkSuite { app { slug } workflowRun { databaseId } } }
  ... on StatusContext { context state description targetUrl createdAt }
}";
const REVIEW: &str = "fragment Review on PullRequestReview {
  databaseId author { login } state body submittedAt url commit { oid }
}";
const HEAD: &str = "fragment Head on PullRequest {
  id number
  commits(last: 1) { nodes { commit { oid statusCheckRollup { id
    contexts(first: 100) { pageInfo { hasNextPage endCursor } nodes { ...Context } } } } } }
}";
const PULL_REQUEST: &str = "fragment PullRequest on PullRequest {
  ...Head
  title body state isDraft url createdAt updatedAt closedAt mergedAt
  author { login } mergeCommit { oid }
  headRefName headRefOid baseRefName baseRefOid
  labels(first: 100) { pageInfo { hasNextPage endCursor } nodes { name } }
  reviews(first: 100) { pageInfo { hasNextPage endCursor } nodes { ...Review } }
}";
const PULL_REQUESTS: &str = "query($owner: String!, $name: String!, $after: String) {
  repository(owner: $owner, name: $name) {
    pullRequests(first: 50, after: $after, orderBy: {field: UPDATED_AT, direction: DESC}) {
      pageInfo { hasNextPage endCursor } nodes { ...PullRequest } } } }";
const OPEN_CHECKS: &str = "query($owner: String!, $name: String!, $after: String) {
  repository(owner: $owner, name: $name) {
    pullRequests(first: 50, after: $after, states: OPEN) {
      pageInfo { hasNextPage endCursor } nodes { ...Head } } } }";
const MORE_REVIEWS: &str = "query($id: ID!, $after: String) {
  node(id: $id) { ... on PullRequest {
    reviews(first: 100, after: $after) { pageInfo { hasNextPage endCursor } nodes { ...Review } } } } }";
const MORE_LABELS: &str = "query($id: ID!, $after: String) {
  node(id: $id) { ... on PullRequest {
    labels(first: 100, after: $after) { pageInfo { hasNextPage endCursor } nodes { name } } } } }";
const MORE_CONTEXTS: &str = "query($id: ID!, $after: String) {
  node(id: $id) { ... on StatusCheckRollup {
    contexts(first: 100, after: $after) { pageInfo { hasNextPage endCursor } nodes { ...Context } } } } }";

/// Append the rest of the connection at `pointer` in `node` (`reviews`, the
/// head commit's check contexts), asking `query` for each further page, and
/// drop its page information.
fn page_to_end(
    github: &Github,
    token: &str,
    node: &mut Value,
    pointer: &str,
    id_pointer: &str,
    query: &str,
    connection: &str,
) -> Result<(), DriverError> {
    let mut seen = std::collections::BTreeSet::new();
    loop {
        let Some(current) = node.pointer(pointer).filter(|value| !value.is_null()) else {
            if pointer == CONTEXTS { return Ok(()); }
            return Err(DriverError::Io(format!("GitHub node without {pointer}")));
        };
        array(&current["nodes"], connection)?;
        let Some(after) = next_cursor(current, &mut seen)? else { break; };
        let id = node.pointer(id_pointer).and_then(Value::as_str)
            .ok_or_else(|| DriverError::Io(format!("GitHub node without {id_pointer}")))?.to_owned();
        let answer = github.graphql(token, query, json!({"id": id, "after": after}))?;
        let more = answer.pointer(&format!("/node/{connection}"))
            .ok_or_else(|| DriverError::Io(format!("GitHub answer without {connection}")))?;
        let new = array(&more["nodes"], connection)?;
        let target = node.pointer_mut(pointer).and_then(Value::as_object_mut)
            .ok_or_else(|| DriverError::Io(format!("GitHub node without {pointer}")))?;
        let have = target.get_mut("nodes").and_then(Value::as_array_mut)
            .ok_or_else(|| DriverError::Io(format!("GitHub {connection} has no nodes")))?;
        have.extend(new.iter().cloned());
        target.insert("pageInfo".into(), more["pageInfo"].clone());
    }

    if let Some(target) = node.pointer_mut(pointer).and_then(Value::as_object_mut) {
        target.remove("pageInfo");
    }
    Ok(())
}

const CONTEXTS: &str = "/commits/nodes/0/commit/statusCheckRollup/contexts";
const ROLLUP_ID: &str = "/commits/nodes/0/commit/statusCheckRollup/id";

impl Github {
    fn token(&self) -> Result<String, DriverError> {
        let token = std::fs::read_to_string(&self.token_file).map_err(|e| {
            DriverError::Io(format!("GitHub token {}: {e}", self.token_file.display()))
        })?;
        Ok(token.trim().to_owned())
    }

    fn repository(&self) -> Result<String, DriverError> {
        if let Some(name) = &self.repository {
            return Ok(name.clone());
        }
        let repo = gix::open(&self.path).map_err(|e| DriverError::Io(e.to_string()))?;
        let key = format!("remote.{}.url", self.remote);
        let url = repo
            .config_snapshot()
            .string(key.as_str())
            .map(|u| u.to_string())
            .ok_or_else(|| {
                DriverError::Io(format!(
                    "{}: no remote {}",
                    self.path.display(),
                    self.remote
                ))
            })?;
        repository_of(&url).ok_or_else(|| {
            DriverError::Io(format!(
                "remote {} ({url}) is not a GitHub repository",
                self.remote
            ))
        })
    }

    /// Read an answer; a refusal holds further asks until GitHub's reset.
    fn answer(
        &self,
        what: &str,
        response: Result<ureq::http::Response<ureq::Body>, ureq::Error>,
    ) -> Result<(Value, Option<String>), DriverError> {
        let mut response = response.map_err(|e| DriverError::Io(format!("GitHub {what}: {e}")))?;
        let header = |name: &str| {
            response
                .headers()
                .get(name)
                .and_then(|v| v.to_str().ok())
                .map(str::to_owned)
        };
        let next = header("link").and_then(|l| next_link(&l));
        let remaining = header("x-ratelimit-remaining");
        let reset = header("x-ratelimit-reset").and_then(|r| r.parse::<i64>().ok());
        let retry_after = header("retry-after").and_then(|r| r.parse::<u64>().ok());
        let status = response.status().as_u16();
        let body = response
            .body_mut()
            .with_config()
            .limit(u64::MAX)
            .read_to_string()
            .map_err(|e| DriverError::Io(format!("GitHub {what}: {e}")))?;
        if !(200..300).contains(&status) {
            let wait = if let Some(seconds) = retry_after {
                Some(Duration::from_secs(seconds))
            } else if remaining.as_deref() == Some("0") {
                reset.map(|reset| {
                    let now = Timestamp::now().unix_ns() / 1_000_000_000;
                    Duration::from_secs(u64::try_from(i128::from(reset) - now).unwrap_or(0))
                })
            } else {
                None
            };
            if let Some(wait) = wait {
                *self.wait_until.lock().expect("lock") =
                    Some(Instant::now() + wait.max(self.interval));
            }
            let message = serde_json::from_str::<Value>(&body)
                .ok()
                .and_then(|v| v.get("message").and_then(Value::as_str).map(str::to_owned))
                .unwrap_or(body);
            return Err(DriverError::Io(format!(
                "GitHub {what}: HTTP {status}: {message}"
            )));
        }
        let value = serde_json::from_str(&body)
            .map_err(|e| DriverError::Io(format!("GitHub {what}: {e}")))?;
        Ok((value, next))
    }

    fn get(&self, token: &str, url: &str) -> Result<(Value, Option<String>), DriverError> {
        if !url.starts_with(&format!("{}/", self.api.trim_end_matches('/'))) {
            return Err(DriverError::Io("GitHub next page is outside the configured API".into()));
        }
        let response = self
            .agent
            .get(url)
            .header("Authorization", &format!("Bearer {token}"))
            .header("Accept", "application/vnd.github+json")
            .call();
        self.answer(url, response)
    }

    /// One GraphQL query; the fragments it uses are added to it.
    fn graphql(&self, token: &str, query: &str, variables: Value) -> Result<Value, DriverError> {
        let mut document = query.to_owned();
        for (name, fragment) in [
            ("...PullRequest", PULL_REQUEST),
            ("...Head", HEAD),
            ("...Review", REVIEW),
            ("...Context", CONTEXT),
        ] {
            if document.contains(name) {
                document.push('\n');
                document.push_str(fragment);
            }
        }
        let body = json!({"query": document, "variables": variables}).to_string();
        let response = self
            .agent
            .post(&format!("{}/graphql", self.api))
            .header("Authorization", &format!("Bearer {token}"))
            .header("Content-Type", "application/json")
            .send(&body);
        let (answer, _) = self.answer("GraphQL", response)?;
        if let Some(errors) = answer.get("errors").and_then(Value::as_array)
            && !errors.is_empty()
        {
            return Err(DriverError::Io(format!(
                "GitHub GraphQL: {}",
                Value::Array(errors.clone())
            )));
        }
        answer
            .get("data")
            .cloned()
            .ok_or_else(|| DriverError::Io("GitHub GraphQL answer without data".into()))
    }

    /// Pull requests updated since `since`, newest first, each complete.
    fn pull_requests(
        &self,
        token: &str,
        owner: &str,
        name: &str,
        since: &Since,
    ) -> Result<Vec<Value>, DriverError> {
        let mut out = Vec::new();
        let mut after = Value::Null;
        let mut seen = std::collections::BTreeSet::new();
        'pages: loop {
            let data = self.graphql(
                token,
                PULL_REQUESTS,
                json!({"owner": owner, "name": name, "after": after}),
            )?;
            let connection = data
                .pointer("/repository/pullRequests")
                .ok_or_else(|| DriverError::Io(format!("GitHub has no repository {owner}/{name}")))?
                .clone();
            for mut node in array(&connection["nodes"], "pull requests")?.iter().cloned() {
                let number = item_number(&node, "number")?;
                let updated = item_time(&node, "updatedAt")?;
                if since.at.is_some_and(|at| updated < at) {
                    break 'pages;
                }
                if !since.is_new(number, updated) {
                    continue;
                }
                page_to_end(
                    self,
                    token,
                    &mut node,
                    "/reviews",
                    "/id",
                    MORE_REVIEWS,
                    "reviews",
                )?;
                page_to_end(self, token, &mut node, "/labels", "/id", MORE_LABELS, "labels")?;
                page_to_end(
                    self,
                    token,
                    &mut node,
                    CONTEXTS,
                    ROLLUP_ID,
                    MORE_CONTEXTS,
                    "contexts",
                )?;
                out.push(node);
            }
            let Some(cursor) = next_cursor(&connection, &mut seen)? else { break; };
            after = json!(cursor);
        }
        Ok(out)
    }

    /// Every open pull request with its head commit's checks, complete.
    fn open_checks(&self, token: &str, owner: &str, name: &str) -> Result<Vec<Value>, DriverError> {
        let mut out = Vec::new();
        let mut after = Value::Null;
        let mut seen = std::collections::BTreeSet::new();
        loop {
            let data = self.graphql(
                token,
                OPEN_CHECKS,
                json!({"owner": owner, "name": name, "after": after}),
            )?;
            let connection = data
                .pointer("/repository/pullRequests")
                .ok_or_else(|| DriverError::Io(format!("GitHub has no repository {owner}/{name}")))?
                .clone();
            for mut node in array(&connection["nodes"], "pull requests")?.iter().cloned() {
                page_to_end(
                    self,
                    token,
                    &mut node,
                    CONTEXTS,
                    ROLLUP_ID,
                    MORE_CONTEXTS,
                    "contexts",
                )?;
                out.push(node);
            }
            let Some(cursor) = next_cursor(&connection, &mut seen)? else { return Ok(out); };
            after = json!(cursor);
        }
    }

    /// Every page of a REST list.
    fn list(&self, token: &str, url: String) -> Result<Vec<Value>, DriverError> {
        let mut out = Vec::new();
        let mut next = Some(url);
        let mut seen = std::collections::BTreeSet::new();
        while let Some(url) = next {
            if !seen.insert(url.clone()) { return Err(DriverError::Io("GitHub repeated a page URL".into())); }
            let (page, link) = self.get(token, &url)?;
            out.extend(array(&page, "REST list")?.iter().cloned());
            next = link;
        }
        Ok(out)
    }

    /// Workflow runs, newest first, down to the first page reaching below
    /// `floor`.
    fn runs(
        &self,
        token: &str,
        repository: &str,
        floor: Option<Timestamp>,
    ) -> Result<Vec<Value>, DriverError> {
        let mut out = Vec::new();
        let mut next = Some(format!(
            "{}/repos/{repository}/actions/runs?per_page=100",
            self.api
        ));
        let mut seen = std::collections::BTreeSet::new();
        while let Some(url) = next {
            if !seen.insert(url.clone()) { return Err(DriverError::Io("GitHub repeated a page URL".into())); }
            let (page, link) = self.get(token, &url)?;
            let runs = array(&page["workflow_runs"], "workflow runs")?.iter().cloned();
            let mut below = false;
            for run in runs {
                if floor.is_some_and(|floor| item_time(&run, "created_at").is_ok_and(|c| c < floor))
                {
                    below = true;
                    continue;
                }
                out.push(run);
            }
            next = if below { None } else { link };
        }
        Ok(out)
    }
}

/// The hash of a pull request's head commit and its checks.
fn checks_hash(node: &Value) -> String {
    let commit = node.pointer("/commits/nodes/0/commit/oid");
    let contexts = node.pointer(&format!("{CONTEXTS}/nodes"));
    blake3::hash(json!([commit, contexts]).to_string().as_bytes())
        .to_hex()
        .to_string()
}

fn record(kind: &str, repository: &str, fields: Value) -> Map<String, Value> {
    let mut record = Map::new();
    record.insert("record".into(), json!(kind));
    record.insert("repository".into(), json!(repository));
    if let Value::Object(fields) = fields {
        record.extend(fields);
    }
    record
}

impl Api for Github {
    fn input(&self) -> InputRef {
        InputRef {
            kind: "github".into(),
            locator: self.path.display().to_string(),
            host: self.host.clone(),
        }
    }

    fn fetch(&self, cursor: &Value) -> Result<Page, DriverError> {
        let mut position: Position = if cursor.is_null() {
            Position::default()
        } else {
            serde_json::from_value(cursor.clone())
                .map_err(|e| DriverError::Cursor(e.to_string()))?
        };
        let now = Timestamp::now();
        let held = self
            .wait_until
            .lock()
            .expect("lock")
            .is_some_and(|until| Instant::now() < until);
        let recent = position.asked_at.is_some_and(|asked| {
            now.unix_ns().saturating_sub(asked.unix_ns())
                < i128::try_from(self.interval.as_nanos()).unwrap_or(i128::MAX)
        });
        if held || recent {
            return Ok(Page {
                records: Vec::new(),
                next: cursor.clone(),
            });
        }
        let result = self.ask(&mut position, now);
        if result.is_err() {
            let mut wait = self.wait_until.lock().expect("lock");
            if wait.is_none_or(|until| until <= Instant::now()) {
                *wait = Some(Instant::now() + self.interval);
            }
        }
        let records = result?;
        Ok(Page {
            records,
            next: json!(position),
        })
    }
}

impl Github {
    fn ask(
        &self,
        position: &mut Position,
        now: Timestamp,
    ) -> Result<Vec<Map<String, Value>>, DriverError> {
        let token = self.token()?;
        let repository = self.repository()?;
        let (owner, name) = repository.split_once('/').ok_or_else(|| {
            DriverError::Io(format!(
                "GitHub repository {repository:?} is not owner/name"
            ))
        })?;
        let mut records = Vec::new();

        let pull_requests = self.pull_requests(&token, owner, name, &position.pull_requests)?;
        let mut checked = BTreeMap::new();
        for node in &pull_requests {
            let number = item_number(node, "number")?;
            position
                .pull_requests
                .advance(number, item_time(node, "updatedAt")?);
            checked.insert(number, checks_hash(node));
            records.push(record("pull_request", &repository, json!({"item": node})));
        }
        let mut open_checks = BTreeMap::new();
        for node in self.open_checks(&token, owner, name)? {
            let number = item_number(&node, "number")?;
            let hash = checks_hash(&node);
            let recorded = checked.get(&number).or(position.open_checks.get(&number));
            if recorded != Some(&hash)
                && let Some(commit) = node.pointer("/commits/nodes/0/commit/oid")
            {
                let contexts = node
                    .pointer(&format!("{CONTEXTS}/nodes"))
                    .cloned()
                    .unwrap_or(json!([]));
                records.push(record(
                    "checks",
                    &repository,
                    json!({"pull_request": number, "commit": commit, "contexts": contexts}),
                ));
            }
            open_checks.insert(number, hash);
        }
        position.open_checks = open_checks;

        let mut url = format!(
            "{}/repos/{repository}/issues?state=all&sort=updated&direction=asc&per_page=100",
            self.api
        );
        if let Some(at) = position.issues.at {
            // RFC 3339 in UTC (`...Z`): nothing in it needs escaping.
            let at = serde_json::to_value(at).map_err(|e| DriverError::Io(e.to_string()))?;
            url.push_str(&format!("&since={}", at.as_str().unwrap_or_default()));
        }
        for item in self.list(&token, url)? {
            if item.get("pull_request").is_some() {
                continue;
            }
            let number = item_number(&item, "number")?;
            let updated = item_time(&item, "updated_at")?;
            if position.issues.is_new(number, updated) {
                position.issues.advance(number, updated);
                records.push(record("issue", &repository, json!({"item": item})));
            }
        }

        let runs = self.runs(&token, &repository, position.runs.floor)?;
        let mut states = BTreeMap::new();
        let mut unfinished: Option<Timestamp> = None;
        let mut newest = position.runs.floor;
        for mut run in runs {
            let id = item_number(&run, "id")?;
            let created = item_time(&run, "created_at")?;
            let state = format!(
                "{}:{}:{}:{}",
                run["run_attempt"], run["status"], run["conclusion"], run["updated_at"]
            );
            if run["status"] != json!("completed") {
                unfinished = Some(unfinished.map_or(created, |u| u.min(created)));
            }
            newest = Some(newest.map_or(created, |n| n.max(created)));
            if position.runs.states.get(&id) != Some(&state) {
                // The repository, described in full in every run, is the binding's.
                if let Some(run) = run.as_object_mut() {
                    run.remove("repository");
                    run.remove("head_repository");
                }
                records.push(record("workflow_run", &repository, json!({"item": run})));
            }
            states.insert(id, (created, state));
        }
        let floor = unfinished.or(newest);
        position.runs = Runs {
            floor,
            states: states
                .into_iter()
                .filter(|(_, (created, _))| floor.is_none_or(|f| *created >= f))
                .map(|(id, (_, state))| (id, state))
                .collect(),
        };
        position.asked_at = Some(now);
        Ok(records)
    }
}

pub fn driver(github: Github) -> ApiCursor<Github> {
    ApiCursor { api: github }
}

/// The reader of a binding: the token file is required.
fn reader(binding: &crate::binding::Binding) -> Result<ApiCursor<Github>, String> {
    let settings = &binding.settings;
    let token_file = settings
        .option("token_file")
        .ok_or("code.github needs the `token_file` option")?;
    let mut github = Github::new(
        binding.input.clone(),
        settings.host.clone(),
        PathBuf::from(token_file),
        settings.option("repository").map(str::to_owned),
    );
    if let Some(remote) = settings.option("remote") {
        github.remote = remote.to_owned();
    }
    if let Some(api) = settings.option("api") {
        github.api = api.trim_end_matches('/').to_owned();
    }
    if let Some(seconds) = settings.number("interval_seconds") {
        github.interval = Duration::from_secs(seconds);
    }
    Ok(driver(github))
}

/// `--read github=PATH`: the GitHub repository of the local repository at
/// PATH.
pub const FORMAT: crate::binding::InputFormat = crate::binding::InputFormat {
    name: "github",
    specs: || vec![&SPEC],
    plan: Some(|input, settings| Ok(crate::binding::single(&SPEC, input, settings))),
    source: |_| Ok(Box::new(source())),
    reader: Some(|b| Ok(crate::binding::stateless(reader(b)?))),
    capture: None,
};

#[cfg(test)]
mod tests;

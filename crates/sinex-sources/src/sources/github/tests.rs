use super::*;
use std::io::{BufRead, BufReader, Read, Write};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

/// One request a test server saw: method, path and query, body.
#[derive(Clone, Debug)]
struct Asked {
    path: String,
    body: String,
    authorization: String,
}

type Answer = (u16, Vec<(&'static str, String)>, String);

/// A local HTTP server answering each request with `answer`, recording what
/// it was asked.
fn serve(answer: impl Fn(&Asked) -> Answer + Send + 'static) -> (String, Arc<Mutex<Vec<Asked>>>) {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let asked = Arc::new(Mutex::new(Vec::new()));
    let seen = asked.clone();
    std::thread::spawn(move || {
        for stream in listener.incoming() {
            let Ok(mut stream) = stream else { continue };
            let mut reader = BufReader::new(stream.try_clone().unwrap());
            let mut first = String::new();
            reader.read_line(&mut first).unwrap();
            let path = first.split_whitespace().nth(1).unwrap_or("").to_owned();
            let mut length = 0;
            let mut authorization = String::new();
            loop {
                let mut header = String::new();
                reader.read_line(&mut header).unwrap();
                let header = header.trim_end();
                if header.is_empty() {
                    break;
                }
                let (name, value) = header.split_once(':').unwrap();
                match name.to_ascii_lowercase().as_str() {
                    "content-length" => length = value.trim().parse().unwrap(),
                    "authorization" => authorization = value.trim().to_owned(),
                    _ => {}
                }
            }
            let mut body = vec![0; length];
            reader.read_exact(&mut body).unwrap();
            let request = Asked {
                path,
                body: String::from_utf8(body).unwrap(),
                authorization,
            };
            seen.lock().unwrap().push(request.clone());
            let (status, headers, body) = answer(&request);
            let mut response = format!(
                "HTTP/1.1 {status} Answer\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n",
                body.len()
            );
            for (name, value) in headers {
                response.push_str(&format!("{name}: {value}\r\n"));
            }
            response.push_str("\r\n");
            response.push_str(&body);
            stream.write_all(response.as_bytes()).unwrap();
        }
    });
    (url, asked)
}

fn ok(body: Value) -> Answer {
    (200, Vec::new(), body.to_string())
}

fn check_run(status: &str, conclusion: Value) -> Value {
    json!({"__typename": "CheckRun", "databaseId": 21, "name": "test", "status": status,
           "conclusion": conclusion, "startedAt": "2026-01-01T10:01:00Z", "completedAt": null,
           "detailsUrl": "https://example.org/run/21",
           "checkSuite": {"app": {"slug": "github-actions"}, "workflowRun": {"databaseId": 100}}})
}

fn status_context() -> Value {
    json!({"__typename": "StatusContext", "context": "stacked-review-threads",
           "state": "SUCCESS", "description": "", "targetUrl": null,
           "createdAt": "2026-01-01T10:02:00Z"})
}

fn review(id: u64, state: &str) -> Value {
    json!({"databaseId": id, "author": {"login": "bob"}, "state": state, "body": "fine",
           "submittedAt": "2026-01-01T12:00:00Z", "url": "https://example.org/review",
           "commit": {"oid": "h1"}})
}

fn head(contexts: Vec<Value>) -> Value {
    json!({"nodes": [{"commit": {"oid": "h1", "statusCheckRollup": {"id": "RO_1",
        "contexts": {"pageInfo": {"hasNextPage": false, "endCursor": null}, "nodes": contexts}}}}]})
}

fn pull_request(reviews_left: bool, contexts: Vec<Value>) -> Value {
    json!({"id": "PR_7", "number": 7, "title": "Add the thing", "body": "", "state": "OPEN",
           "isDraft": false, "url": "https://github.com/o/r/pull/7",
           "createdAt": "2026-01-01T10:00:00Z", "updatedAt": "2026-01-02T00:00:00Z",
           "closedAt": null, "mergedAt": null, "author": {"login": "alice"}, "mergeCommit": null,
           "headRefName": "lane-x", "headRefOid": "h1", "baseRefName": "master",
           "baseRefOid": "b1", "labels": {"pageInfo": {"hasNextPage": false, "endCursor": null}, "nodes": [{"name": "bug"}]},
           "reviews": {"pageInfo": {"hasNextPage": reviews_left, "endCursor": "r1"},
                       "nodes": [review(11, "COMMENTED")]},
           "commits": head(contexts)})
}

fn run(id: u64, status: &str, created: &str) -> Value {
    json!({"id": id, "run_attempt": 1, "run_number": id - 90, "name": "CI",
           "display_title": "Add the thing", "event": "pull_request", "status": status,
           "conclusion": if status == "completed" { json!("success") } else { Value::Null },
           "head_branch": "lane-x", "head_sha": "h1", "actor": {"login": "alice"},
           "created_at": created, "run_started_at": created, "updated_at": created,
           "html_url": format!("https://github.com/o/r/actions/runs/{id}"),
           "repository": {"full_name": "o/r"}, "head_repository": {"full_name": "o/r"}})
}

fn issue(number: u64) -> Value {
    json!({"number": number, "title": "Broken", "body": "it breaks", "state": "closed",
           "state_reason": "completed", "user": {"login": "carol"},
           "labels": [{"name": "bug"}], "assignees": [{"login": "alice"}], "comments": 2,
           "created_at": "2026-01-01T08:00:00Z", "updated_at": "2026-01-01T09:00:00Z",
           "closed_at": "2026-01-01T09:00:00Z", "html_url": "https://github.com/o/r/issues/3"})
}

fn records(a: &crate::drivers::Acquired) -> Vec<Map<String, Value>> {
    let Some(m) = a.material.as_ref() else {
        return Vec::new();
    };
    m.records
        .iter()
        .map(|(s, e)| serde_json::from_slice(&m.bytes[*s..*e]).unwrap())
        .collect()
}

fn cursor(a: &crate::drivers::Acquired) -> Value {
    match a.material.as_ref() {
        Some(m) => (m.cursor_at)(m.bytes.len()).unwrap(),
        None => a.cursor_without_material.clone(),
    }
}

fn kinds(records: &[Map<String, Value>]) -> Vec<&str> {
    records
        .iter()
        .map(|r| r["record"].as_str().unwrap())
        .collect()
}

fn parse(record: &Map<String, Value>) -> Vec<GithubRecord> {
    GithubParser
        .parse(record, &(), &crate::RecordContext::none())
        .unwrap()
        .0
}

fn github(url: &str) -> Github {
    let token = std::env::temp_dir().join(format!("sinex-github-{}", uuid::Uuid::now_v7()));
    std::fs::write(&token, "t0ken\n").unwrap();
    let mut github = Github::new(
        PathBuf::from("/nowhere"),
        "h".into(),
        token,
        Some("o/r".into()),
    );
    github.api = url.to_owned();
    github.interval = Duration::ZERO;
    github
}

#[devkit::test]
fn remote_urls_name_their_repository() {
    for url in [
        "git@github.com:Owner/name.git",
        "https://github.com/Owner/name",
        "https://github.com/Owner/name.git/",
        "ssh://git@github.com/Owner/name.git",
    ] {
        assert_eq!(repository_of(url).as_deref(), Some("Owner/name"), "{url}");
    }
    assert_eq!(repository_of("https://gitlab.com/o/n"), None);
    assert_eq!(repository_of("https://github.com/o"), None);
}

#[devkit::test]
fn the_next_page_comes_from_the_link_header() {
    let link = r#"<https://api.github.com/x?page=2>; rel="next", <https://api.github.com/x?page=5>; rel="last""#;
    assert_eq!(
        next_link(link).as_deref(),
        Some("https://api.github.com/x?page=2")
    );
    assert_eq!(next_link(r#"<https://a/x?page=1>; rel="prev""#), None);
}

#[devkit::test]
fn an_item_updated_in_the_last_read_second_is_still_read() {
    let at = fields::rfc3339("2026-01-02T00:00:00Z").unwrap();
    let mut since = Since::default();
    since.advance(7, at);
    assert!(!since.is_new(7, at));
    assert!(
        since.is_new(8, at),
        "another item updated in the same second"
    );
    since.advance(8, at);
    assert!(!since.is_new(8, at));
    let later = fields::rfc3339("2026-01-02T00:00:01Z").unwrap();
    assert!(since.is_new(7, later));
    since.advance(7, later);
    assert_eq!(since.read_at, BTreeSet::from([7]));
}

#[devkit::test]
fn a_pull_request_with_its_reviews_and_checks() {
    let record = record(
        "pull_request",
        "o/r",
        json!({"item": pull_request(false, vec![check_run("COMPLETED", json!("FAILURE")), status_context()])}),
    );
    let out = parse(&record);
    assert_eq!(out.len(), 4);
    let GithubRecord::PullRequest(pr) = &out[0] else {
        panic!("a pull request first")
    };
    assert_eq!(pr.repository, "o/r");
    assert_eq!(pr.number, 7);
    assert_eq!(pr.state, "open");
    assert_eq!(pr.body, None, "an empty body is no body");
    assert_eq!(pr.labels, ["bug"]);
    assert_eq!(pr.head_ref, "lane-x");
    assert_eq!(pr.merge_commit, None);
    let GithubRecord::Review(review) = &out[1] else {
        panic!("a review")
    };
    assert_eq!((review.pull_request, review.review_id), (7, 11));
    assert_eq!(review.state, "commented");
    let GithubRecord::Check(run) = &out[2] else {
        panic!("a check run")
    };
    assert_eq!(run.reported_as, "check_run");
    assert_eq!(run.commit, "h1");
    assert_eq!(run.conclusion.as_deref(), Some("failure"));
    assert_eq!(run.workflow_run, Some(100));
    assert_eq!(run.app.as_deref(), Some("github-actions"));
    let GithubRecord::Check(status) = &out[3] else {
        panic!("a status")
    };
    assert_eq!(status.reported_as, "status");
    assert_eq!(status.name, "stacked-review-threads");
    assert_eq!(status.check_run, None);
    assert_eq!(status.conclusion.as_deref(), Some("success"));
    assert_eq!(status.description, None);
}

#[devkit::test]
fn runs_issues_and_open_checks_parse() {
    let out = parse(&record(
        "workflow_run",
        "o/r",
        json!({"item": run(100, "in_progress", "2026-01-02T00:00:00Z")}),
    ));
    let [GithubRecord::WorkflowRun(r)] = out.as_slice() else {
        panic!("one run")
    };
    assert_eq!((r.run_id, r.attempt, r.run_number), (100, 1, 10));
    assert_eq!(r.status.as_deref(), Some("in_progress"));
    assert_eq!(r.conclusion, None);
    assert_eq!(r.head_commit, "h1");
    let out = parse(&record("issue", "o/r", json!({"item": issue(3)})));
    let [GithubRecord::Issue(i)] = out.as_slice() else {
        panic!("one issue")
    };
    assert_eq!(i.number, 3);
    assert_eq!(i.state_reason.as_deref(), Some("completed"));
    assert_eq!(i.assignees, ["alice"]);
    assert_eq!(i.comments, 2);
    let out = parse(&record(
        "checks",
        "o/r",
        json!({"pull_request": 7, "commit": "h1", "contexts": [status_context()]}),
    ));
    assert!(matches!(out.as_slice(), [GithubRecord::Check(c)] if c.commit == "h1"));
    assert!(
        GithubParser
            .parse(
                &record("comment", "o/r", json!({})),
                &(),
                &crate::RecordContext::none()
            )
            .is_err()
    );
}

#[devkit::test]
fn asks_follow_what_changed() {
    let phase = Arc::new(AtomicUsize::new(0));
    let at = phase.clone();
    let (url, asked) = serve(move |request| {
        let phase = at.load(Ordering::SeqCst);
        if request.path == "/graphql" {
            let body = &request.body;
            return if body.contains("states: OPEN") {
                let contexts = if phase == 0 {
                    vec![check_run("IN_PROGRESS", Value::Null), status_context()]
                } else {
                    vec![check_run("COMPLETED", json!("SUCCESS")), status_context()]
                };
                let mut node = json!({"id": "PR_7", "number": 7, "commits": head(contexts)});
                node["commits"]["nodes"][0]["commit"]["oid"] = json!("h1");
                ok(json!({"data": {"repository": {"pullRequests": {
                    "pageInfo": {"hasNextPage": false, "endCursor": null}, "nodes": [node]}}}}))
            } else if body.contains("reviews(first: 100, after: $after)") {
                ok(json!({"data": {"node": {"reviews": {
                    "pageInfo": {"hasNextPage": false, "endCursor": null},
                    "nodes": [review(12, "APPROVED")]}}}}))
            } else {
                let node = pull_request(
                    true,
                    vec![check_run("IN_PROGRESS", Value::Null), status_context()],
                );
                ok(json!({"data": {"repository": {"pullRequests": {
                    "pageInfo": {"hasNextPage": false, "endCursor": null}, "nodes": [node]}}}}))
            };
        }
        if request.path.starts_with("/repos/o/r/issues") {
            let mut pull = issue(7);
            pull["pull_request"] = json!({"url": "x"});
            return ok(json!([issue(3), pull]));
        }
        if request.path.starts_with("/repos/o/r/actions/runs") {
            let newest = if phase == 0 {
                "in_progress"
            } else {
                "completed"
            };
            let mut newest = run(100, newest, "2026-01-02T00:00:00Z");
            if phase > 0 {
                newest["updated_at"] = json!("2026-01-02T00:10:00Z");
            }
            return ok(json!({"workflow_runs": [
                newest, run(99, "completed", "2026-01-01T00:00:00Z")]}));
        }
        (404, Vec::new(), json!({"message": "Not Found"}).to_string())
    });
    let d = driver(github(&url));

    let first = d.poll(&Value::Null).unwrap();
    let read = records(&first);
    assert_eq!(
        kinds(&read),
        ["pull_request", "issue", "workflow_run", "workflow_run"]
    );
    let reviews = read[0]["item"]["reviews"]["nodes"].as_array().unwrap();
    assert_eq!(reviews.len(), 2, "the reviews are paged to the end");
    assert!(read[0]["item"]["reviews"].get("pageInfo").is_none());
    assert_eq!(
        read[1]["item"]["number"],
        json!(3),
        "pull requests come from GraphQL"
    );
    assert!(read[2]["item"].get("repository").is_none());
    assert!(
        asked
            .lock()
            .unwrap()
            .iter()
            .all(|a| a.authorization == "Bearer t0ken")
    );

    phase.store(1, Ordering::SeqCst);
    let second = d.poll(&cursor(&first)).unwrap();
    let read = records(&second);
    assert_eq!(kinds(&read), ["checks", "workflow_run"], "{read:?}");
    assert_eq!(read[0]["contexts"][0]["status"], json!("COMPLETED"));
    assert_eq!(read[1]["item"]["id"], json!(100));
    let issues = asked
        .lock()
        .unwrap()
        .iter()
        .filter(|a| a.path.starts_with("/repos/o/r/issues"))
        .map(|a| a.path.clone())
        .collect::<Vec<_>>();
    assert!(
        issues[1].ends_with("&since=2026-01-01T09:00:00Z"),
        "{issues:?}"
    );

    let third = d.poll(&cursor(&second)).unwrap();
    assert!(
        records(&third).is_empty(),
        "nothing changed: {:?}",
        records(&third)
    );
}

#[devkit::test]
fn the_interval_holds_asks() {
    let (url, asked) = serve(|_| (500, Vec::new(), String::new()));
    let mut github = github(&url);
    github.interval = Duration::from_secs(3600);
    let d = driver(github);
    let position = Position {
        asked_at: Some(Timestamp::now()),
        ..Position::default()
    };
    let polled = d
        .poll(&json!({"api": position, "material": null, "part_index": 0}))
        .unwrap();
    assert!(polled.material.is_none());
    assert!(
        asked.lock().unwrap().is_empty(),
        "asked within the interval"
    );
}

#[devkit::test]
fn a_rate_limit_holds_asks_until_its_reset() {
    let reset = Timestamp::now().unix_ns() / 1_000_000_000 + 3600;
    let (url, asked) = serve(move |_| {
        (
            403,
            vec![
                ("x-ratelimit-remaining", "0".into()),
                ("x-ratelimit-reset", reset.to_string()),
            ],
            json!({"message": "API rate limit exceeded"}).to_string(),
        )
    });
    let d = driver(github(&url));
    let refused = d.poll(&Value::Null).err().expect("refused");
    assert!(refused.to_string().contains("rate limit"), "{refused}");
    assert_eq!(asked.lock().unwrap().len(), 1);
    assert!(d.poll(&Value::Null).unwrap().material.is_none());
    assert_eq!(asked.lock().unwrap().len(), 1, "held until the reset");
}

#[devkit::test]
fn labels_are_read_past_the_first_page() {
    let (url, asked) = serve(|_| {
        ok(json!({"data": {"node": {"labels": {
            "pageInfo": {"hasNextPage": false, "endCursor": null},
            "nodes": [{"name": "last-label"}],
        }}}}))
    });
    let github = github(&url);
    let mut node = pull_request(false, Vec::new());
    node["labels"] = json!({"pageInfo": {"hasNextPage": true, "endCursor": "labels-100"},
        "nodes": (0..100).map(|i| json!({"name": format!("label-{i}")})).collect::<Vec<_>>()});
    page_to_end(
        &github,
        "synthetic-token",
        &mut node,
        "/labels",
        "/id",
        MORE_LABELS,
        "labels",
    )
    .unwrap();
    let labels = node["labels"]["nodes"].as_array().unwrap();
    assert_eq!(labels.len(), 101);
    assert_eq!(labels[100]["name"], "last-label");
    assert!(asked.lock().unwrap()[0].body.contains("labels-100"));
}

#[devkit::test]
fn malformed_lists_and_page_information_are_errors() {
    let (url, _) = serve(|_| ok(json!({"data": {"repository": {"pullRequests": {}}}})));
    let github = github(&url);
    assert!(
        github
            .pull_requests("synthetic", "o", "r", &Since::default())
            .is_err()
    );
    assert!(github.open_checks("synthetic", "o", "r").is_err());
    assert!(github.list("synthetic", format!("{url}/issues")).is_err());
    assert!(github.runs("synthetic", "o/r", None).is_err());
    let mut seen = BTreeSet::new();
    assert!(
        next_cursor(
            &json!({"pageInfo": {"hasNextPage": true, "endCursor": null}}),
            &mut seen
        )
        .is_err()
    );
    let page = json!({"pageInfo": {"hasNextPage": true, "endCursor": "same"}});
    assert_eq!(
        next_cursor(&page, &mut seen).unwrap().as_deref(),
        Some("same")
    );
    assert!(next_cursor(&page, &mut seen).is_err());
}

#[devkit::test]
fn pagination_never_sends_the_token_to_another_api() {
    let (url, asked) = serve(|_| ok(json!([])));
    let github = github("https://api.github.com");
    assert!(
        github
            .list("synthetic-secret", format!("{url}/issues"))
            .is_err()
    );
    assert!(asked.lock().unwrap().is_empty());
}

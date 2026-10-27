use super::*;
use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

#[derive(Clone)]
pub(super) struct Fake {
    steps: Arc<Mutex<VecDeque<Step>>>,
}
struct Step {
    path: String,
    query: Vec<(String, String)>,
    result: Result<Value, Error>,
}
impl Fake {
    pub(super) fn new() -> Self {
        Self {
            steps: Arc::new(Mutex::new(VecDeque::new())),
        }
    }
    pub(super) fn add(&self, path: &str, query: &[(&str, &str)], result: Result<Value, Error>) {
        self.steps.lock().unwrap().push_back(Step {
            path: path.into(),
            query: query
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect(),
            result,
        });
    }
    pub(super) fn done(&self) {
        assert!(self.steps.lock().unwrap().is_empty());
    }
}
impl Transport for Fake {
    fn get(&self, path: &str, query: &[(&str, &str)]) -> Result<Value, Error> {
        let step = self
            .steps
            .lock()
            .unwrap()
            .pop_front()
            .expect("unexpected provider request");
        assert_eq!(path, step.path);
        let mut actual: Vec<_> = query
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        let mut expected = step.query;
        actual.sort();
        expected.sort();
        assert_eq!(actual, expected);
        step.result
    }
}

pub(super) fn initial() -> State {
    State {
        account: "owner@example.test".into(),
        mode: Mode::List,
        page_token: None,
        start_history_id: "184467440737095516169999".into(),
        completed_history_id: "184467440737095516169999".into(),
        initial_history_id: "184467440737095516169999".into(),
        page_index: 0,
        pending: None,
    }
}
fn gmail(fake: Fake) -> Gmail<Fake> {
    Gmail {
        transport: fake,
        account: "owner@example.test".into(),
        host: "test-host".into(),
    }
}
fn value(state: State) -> Value {
    serde_json::to_value(state).unwrap()
}
fn labels(fake: &Fake) {
    fake.add(
        "/labels",
        &[],
        Ok(json!({"labels":[{"id":"INBOX","name":"Inbox"}]})),
    );
}

#[test]
fn baseline_is_committed_before_full_mailbox_listing() {
    let fake = Fake::new();
    fake.add(
        "/profile",
        &[],
        Ok(json!({"emailAddress":"owner@example.test", "historyId":"184467440737095516169999"})),
    );
    let api = gmail(fake.clone());
    let page = api.fetch(&Value::Null).unwrap();
    assert!(page.records.is_empty());
    assert_eq!(page.next, value(initial()));
    fake.done();
}

#[test]
fn authenticated_account_and_cursor_account_are_checked() {
    let fake = Fake::new();
    fake.add(
        "/profile",
        &[],
        Ok(json!({"emailAddress":"other@example.test", "historyId":"1"})),
    );
    let api = gmail(fake.clone());
    assert!(api.fetch(&Value::Null).is_err());
    let mut state = initial();
    state.account = "other@example.test".into();
    assert!(api.fetch(&value(state)).is_err());
    fake.done();
}

#[test]
fn empty_list_pages_advance_and_then_cover_arrivals_during_listing() {
    let fake = Fake::new();
    fake.add(
        "/messages",
        &[("maxResults", "500"), ("includeSpamTrash", "true")],
        Ok(json!({"nextPageToken":"page two"})),
    );
    labels(&fake);
    fake.add(
        "/messages",
        &[
            ("maxResults", "500"),
            ("includeSpamTrash", "true"),
            ("pageToken", "page two"),
        ],
        Ok(json!({})),
    );
    labels(&fake);
    fake.add("/history", &[("maxResults","500"),("startHistoryId","184467440737095516169999")], Ok(json!({"historyId":"184467440737095516170002","history":[{"id":"184467440737095516170001","messagesAdded":[{"message":{"id":"new"}}]}]})));
    labels(&fake);
    fake.add(
        "/messages/new",
        &[("format", "raw")],
        Ok(json!({"id":"new","threadId":"t","historyId":"184467440737095516170001","raw":"YQ"})),
    );
    let api = gmail(fake.clone());
    let first = api.fetch(&value(initial())).unwrap();
    assert!(first.records.is_empty());
    assert_eq!(first.next["page_token"], "page two");
    let second = api.fetch(&first.next).unwrap();
    assert_eq!(second.next["mode"], "history");
    let history = api.fetch(&second.next).unwrap();
    assert_eq!(history.records[0]["kind"], "history");
    assert_eq!(
        history.next["completed_history_id"],
        "184467440737095516169999"
    );
    let message = api.fetch(&history.next).unwrap();
    assert_eq!(message.records[0]["response"]["id"], "new");
    assert_eq!(
        message.next["completed_history_id"],
        "184467440737095516170002"
    );
    fake.done();
}

#[test]
fn history_baseline_stays_fixed_through_multiple_empty_pages() {
    let fake = Fake::new();
    fake.add(
        "/history",
        &[
            ("maxResults", "500"),
            ("startHistoryId", "184467440737095516169999"),
        ],
        Ok(json!({"historyId":"200","nextPageToken":"next"})),
    );
    labels(&fake);
    fake.add(
        "/history",
        &[
            ("maxResults", "500"),
            ("startHistoryId", "184467440737095516169999"),
            ("pageToken", "next"),
        ],
        Ok(json!({"historyId":"201"})),
    );
    labels(&fake);
    let api = gmail(fake.clone());
    let mut state = initial();
    state.mode = Mode::History;
    let first = api.fetch(&value(state)).unwrap();
    assert!(first.records.is_empty());
    assert_eq!(first.next["start_history_id"], "184467440737095516169999");
    let second = api.fetch(&first.next).unwrap();
    assert!(second.records.is_empty());
    assert_eq!(second.next["completed_history_id"], "201");
    assert_eq!(second.next["start_history_id"], "201");
    fake.done();
}

#[test]
fn gap_is_stored_and_restart_remains_paused_until_explicit_resync() {
    let fake = Fake::new();
    fake.add(
        "/history",
        &[
            ("maxResults", "500"),
            ("startHistoryId", "184467440737095516169999"),
        ],
        Err(Error::Status(404)),
    );
    let api = gmail(fake.clone());
    let mut state = initial();
    state.mode = Mode::History;
    let gap = api.fetch(&value(state)).unwrap();
    assert_eq!(gap.records[0]["kind"], "gap");
    assert_eq!(gap.next["mode"], "gap");
    let restarted = gmail(fake.clone());
    let paused = restarted.fetch(&gap.next).unwrap();
    assert!(paused.records.is_empty());
    assert_eq!(paused.next, gap.next);
    let outer = serde_json::to_value(sinex_source_core::drivers::api_cursor::Cursor {
        api: gap.next,
        ..Default::default()
    })
    .unwrap();
    assert!(resync(&outer, "other@example.test").is_err());
    let reset = resync(&outer, "owner@example.test").unwrap();
    assert!(reset["api"].is_null());
    fake.done();
}

#[test]
fn unavailable_pages_do_not_return_progress_or_turn_into_gaps() {
    for error in [
        Error::Status(401),
        Error::Status(403),
        Error::Status(429),
        Error::Status(500),
        Error::Unavailable,
        Error::InvalidResponse,
    ] {
        let fake = Fake::new();
        fake.add(
            "/history",
            &[
                ("maxResults", "500"),
                ("startHistoryId", "184467440737095516169999"),
            ],
            Err(error),
        );
        let api = gmail(fake.clone());
        let mut state = initial();
        state.mode = Mode::History;
        let cursor = value(state);
        assert!(api.fetch(&cursor).is_err());
        assert_eq!(cursor["completed_history_id"], "184467440737095516169999");
        fake.done();
    }
}

#[test]
fn successful_messages_are_stored_before_a_later_fetch_fails() {
    let fake = Fake::new();
    fake.add(
        "/messages",
        &[("maxResults", "500"), ("includeSpamTrash", "true")],
        Ok(json!({"messages":[{"id":"one"},{"id":"two"}],"nextPageToken":"later"})),
    );
    labels(&fake);
    fake.add(
        "/messages/one",
        &[("format", "raw")],
        Ok(json!({"id":"one","threadId":"t","historyId":"h1","raw":"YQ"})),
    );
    fake.add(
        "/messages/two",
        &[("format", "raw")],
        Err(Error::Status(503)),
    );
    fake.add(
        "/messages/two",
        &[("format", "raw")],
        Err(Error::Status(404)),
    );
    let api = gmail(fake.clone());
    let listing = api.fetch(&value(initial())).unwrap();
    let first = api.fetch(&listing.next).unwrap();
    assert_eq!(first.records[0]["response"]["id"], "one");
    assert!(first.next["page_token"].is_null());
    assert!(api.fetch(&first.next).is_err());
    let restarted = gmail(fake.clone());
    let missing = restarted.fetch(&first.next).unwrap();
    assert_eq!(missing.records[0]["kind"], "missing");
    assert_eq!(missing.records[0]["response"]["provider_message_id"], "two");
    assert_eq!(missing.next["page_token"], "later");
    fake.done();
}

#[test]
fn repeated_labels_and_deletions_keep_the_exact_history_object() {
    let fake = Fake::new();
    let history = json!({"id":"h", "messages":[{"id":"gone"}],"messagesDeleted":[{"message":{"id":"gone"}},{"message":{"id":"gone"}}],"labelsAdded":[{"message":{"id":"m"},"labelIds":["a","a"]}], "labelsRemoved":[{"message":{"id":"m"},"labelIds":["b"]}]});
    fake.add(
        "/history",
        &[
            ("maxResults", "500"),
            ("startHistoryId", "184467440737095516169999"),
        ],
        Ok(json!({"historyId":"next","history":[history.clone()]})),
    );
    labels(&fake);
    let api = gmail(fake.clone());
    let mut state = initial();
    state.mode = Mode::History;
    let page = api.fetch(&value(state)).unwrap();
    assert_eq!(page.records[0]["response"], history);
    assert_eq!(page.next["pending"]["ids"], json!(["m"]));
    fake.done();
}

struct HttpExchange(std::sync::atomic::AtomicUsize);
impl Exchange for HttpExchange {
    fn exchange(&self, _: &oauth::Credentials) -> Result<oauth::Token, oauth::Error> {
        let n = self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        Ok(serde_json::from_value(json!({"access_token":format!("access-secret-{n}"),"expires_in":3600,"token_type":"Bearer"})).unwrap())
    }
}

#[test]
fn http_encodes_opaque_tokens_and_invalidates_authorization_without_echoing_bodies() {
    use std::io::{Read, Write};
    let dir = tempfile::tempdir().unwrap();
    for file in ["id", "secret", "refresh"] {
        std::fs::write(dir.path().join(file), "credential-secret").unwrap();
    }
    let files = oauth::CredentialFiles {
        client_id_file: dir.path().join("id"),
        client_secret_file: dir.path().join("secret"),
        refresh_token_file: dir.path().join("refresh"),
    };
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    let server = std::thread::spawn(move || {
        let mut requests = Vec::new();
        for status in [401, 403, 500, 200] {
            let (mut stream, _) = listener.accept().unwrap();
            stream
                .set_read_timeout(Some(std::time::Duration::from_secs(5)))
                .unwrap();
            let mut bytes = Vec::new();
            while !bytes.windows(4).any(|w| w == b"\r\n\r\n") {
                let mut chunk = [0; 4096];
                let n = stream.read(&mut chunk).unwrap();
                assert!(n > 0);
                bytes.extend_from_slice(&chunk[..n]);
            }
            requests.push(String::from_utf8(bytes).unwrap());
            let body = if status == 200 {
                r#"{"historyId":"opaque"}"#
            } else {
                "credential-secret access-secret refresh-secret"
            };
            write!(stream, "HTTP/1.1 {status} response\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}", body.len()).unwrap();
        }
        requests
    });
    let tokens = TokenProvider::new(
        files,
        HttpExchange(std::sync::atomic::AtomicUsize::new(0)),
        oauth::SystemClock,
    );
    let mut http = Http::new(tokens, "owner@example.test");
    assert!(http.endpoint.ends_with("/users/owner%40example.test"));
    http.endpoint = endpoint;
    for expected in [401, 403, 500] {
        let error = http
            .get("/history", &[("pageToken", "opaque +/&")])
            .unwrap_err();
        assert_eq!(error, Error::Status(expected));
        assert!(!format!("{error:?} {error}").contains("secret"));
    }
    assert_eq!(http.get("/history", &[]).unwrap()["historyId"], "opaque");
    let requests = server.join().unwrap();
    assert!(requests[0].contains("pageToken=opaque%20%2B%2F%26"));
    assert!(requests[0].contains("Bearer access-secret-0"));
    assert!(requests[1].contains("Bearer access-secret-1"));
    assert!(requests[2].contains("Bearer access-secret-2"));
    assert!(requests[3].contains("Bearer access-secret-2"));
}

#[test]
fn malformed_page_and_labels_do_not_advance_progress() {
    let fake = Fake::new();
    fake.add(
        "/messages",
        &[("maxResults", "500"), ("includeSpamTrash", "true")],
        Ok(Value::Null),
    );
    let api = gmail(fake.clone());
    assert!(api.fetch(&value(initial())).is_err());
    fake.done();
    let fake = Fake::new();
    fake.add(
        "/messages",
        &[("maxResults", "500"), ("includeSpamTrash", "true")],
        Ok(json!({})),
    );
    fake.add("/labels", &[], Ok(Value::Null));
    let api = gmail(fake.clone());
    assert!(api.fetch(&value(initial())).is_err());
    fake.done();
}

#[test]
fn raw_response_must_name_the_requested_message() {
    let fake = Fake::new();
    fake.add(
        "/messages",
        &[("maxResults", "500"), ("includeSpamTrash", "true")],
        Ok(json!({"messages":[{"id":"requested"}]})),
    );
    labels(&fake);
    fake.add(
        "/messages/requested",
        &[("format", "raw")],
        Ok(json!({"id":"different","historyId":"h","threadId":"t","raw":"YQ"})),
    );
    let api = gmail(fake.clone());
    let listing = api.fetch(&value(initial())).unwrap();
    assert!(api.fetch(&listing.next).is_err());
    assert_eq!(listing.next["pending"]["index"], 0);
    fake.done();
}

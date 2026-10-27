//! Gmail's list and history walks. Progress contains provider IDs, never tokens.
use super::oauth::{self, Clock, Exchange, TokenProvider};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};
use sinex_model::{InputRef, Timestamp};
use sinex_source_core::drivers::DriverError;
use sinex_source_core::drivers::api_cursor::{Api, Page};
use std::collections::BTreeSet;

#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
pub enum Error {
    #[error("Gmail HTTP status {0}")]
    Status(u16),
    #[error("Gmail transport unavailable")]
    Unavailable,
    #[error("Gmail response invalid")]
    InvalidResponse,
    #[error(transparent)]
    OAuth(#[from] oauth::Error),
}

pub trait Transport: Send + Sync {
    fn get(&self, path: &str, query: &[(&str, &str)]) -> Result<Value, Error>;
}

pub struct Http<E = oauth::GoogleExchange, C = oauth::SystemClock> {
    tokens: TokenProvider<E, C>,
    agent: ureq::Agent,
    endpoint: String,
}

impl<E: Exchange, C: Clock> Http<E, C> {
    pub fn new(tokens: TokenProvider<E, C>, account: &str) -> Self {
        // Address this mailbox so rotated credentials cannot capture another account.
        Self {
            tokens,
            agent: oauth::agent(),
            endpoint: format!(
                "https://gmail.googleapis.com/gmail/v1/users/{}",
                oauth::encode(account)
            ),
        }
    }
}

impl<E: Exchange, C: Clock> Transport for Http<E, C> {
    fn get(&self, path: &str, query: &[(&str, &str)]) -> Result<Value, Error> {
        let query = query
            .iter()
            .map(|(k, v)| format!("{}={}", oauth::encode(k), oauth::encode(v)))
            .collect::<Vec<_>>()
            .join("&");
        let url = format!("{}{path}?{query}", self.endpoint);
        let token = self.tokens.bearer()?;
        let mut response = self
            .agent
            .get(&url)
            .header("Authorization", format!("Bearer {token}"))
            .call()
            .map_err(|_| Error::Unavailable)?;
        let status = response.status().as_u16();
        if status == 401 || status == 403 {
            self.tokens.invalidate();
        }
        if status != 200 {
            return Err(Error::Status(status));
        }
        let bytes = response
            .body_mut()
            .with_config()
            .limit(u64::MAX)
            .read_to_vec()
            .map_err(|_| Error::Unavailable)?;
        serde_json::from_slice(&bytes).map_err(|_| Error::InvalidResponse)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Mode {
    List,
    History,
    Baseline,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct State {
    pub account: String,
    pub mode: Mode,
    pub page_token: Option<String>,
    pub start_history_id: String,
    pub completed_history_id: String,
    pub initial_history_id: String,
    pub page_index: u64,
    pub pending: Option<Pending>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Pending {
    ids: Vec<String>,
    index: usize,
    labels: Value,
    after: Box<State>,
}

pub struct Gmail<T> {
    pub transport: T,
    pub account: String,
    pub host: String,
}

fn invalid() -> DriverError {
    DriverError::Io("Gmail response invalid".into())
}
fn failure(e: Error) -> DriverError {
    DriverError::Io(e.to_string())
}

fn text<'a>(object: &'a Value, key: &str) -> Result<&'a str, DriverError> {
    object
        .get(key)
        .and_then(Value::as_str)
        .filter(|v| !v.is_empty())
        .ok_or_else(invalid)
}

fn array<'a>(object: &'a Value, key: &str) -> Result<&'a [Value], DriverError> {
    match object.get(key) {
        None => Ok(&[]),
        Some(Value::Array(a)) => Ok(a),
        _ => Err(invalid()),
    }
}

fn page_token(object: &Value) -> Result<Option<String>, DriverError> {
    object
        .get("nextPageToken")
        .map(|_| text(object, "nextPageToken").map(str::to_owned))
        .transpose()
}

fn record(account: &str, kind: &str, response: Value) -> Map<String, Value> {
    json!({"account": account, "kind": kind, "response": response})
        .as_object()
        .unwrap()
        .clone()
}

fn page(records: Vec<Map<String, Value>>, state: State) -> Result<Page, DriverError> {
    Ok(Page {
        records,
        next: serde_json::to_value(state).map_err(|_| invalid())?,
    })
}

impl<T: Transport> Api for Gmail<T> {
    fn input(&self) -> InputRef {
        InputRef {
            kind: "gmail".into(),
            locator: self.account.clone(),
            host: self.host.clone(),
        }
    }

    fn fetch(&self, cursor: &Value) -> Result<Page, DriverError> {
        if cursor.is_null() {
            let profile = self.transport.get("/profile", &[]).map_err(failure)?;
            if text(&profile, "emailAddress")? != self.account {
                return Err(DriverError::Io(
                    "Gmail authenticated account differs from configured account".into(),
                ));
            }
            let initial = text(&profile, "historyId")?.to_owned();
            return page(
                vec![record(&self.account, "profile", profile)],
                State {
                    account: self.account.clone(),
                    mode: Mode::List,
                    page_token: None,
                    start_history_id: initial.clone(),
                    completed_history_id: initial.clone(),
                    initial_history_id: initial,
                    page_index: 0,
                    pending: None,
                },
            );
        }
        let state: State = serde_json::from_value(cursor.clone())
            .map_err(|_| DriverError::Cursor("invalid Gmail cursor".into()))?;
        if state.account != self.account {
            return Err(DriverError::Cursor(
                "Gmail cursor belongs to another account".into(),
            ));
        }
        if state.mode == Mode::Baseline {
            return self.fetch(&Value::Null);
        }
        if state.pending.is_some() {
            return self.fetch_pending(state);
        }
        self.fetch_page(state)
    }
}

impl<T: Transport> Gmail<T> {
    fn fetch_pending(&self, mut state: State) -> Result<Page, DriverError> {
        let pending = state.pending.as_mut().unwrap();
        let id = pending
            .ids
            .get(pending.index)
            .ok_or_else(|| DriverError::Cursor("invalid Gmail pending index".into()))?;
        let response = self.transport.get(
            &format!("/messages/{}", oauth::encode(id)),
            &[("format", "raw")],
        );
        let mut stored = match response {
            Ok(response) => {
                if text(&response, "id")? != id {
                    return Err(invalid());
                }
                response
                    .get("raw")
                    .and_then(Value::as_str)
                    .ok_or_else(invalid)?;
                text(&response, "historyId")?;
                text(&response, "threadId")?;
                record(&self.account, "message", response)
            }
            Err(Error::Status(404)) => record(
                &self.account,
                "missing",
                json!({
                    "provider_message_id": id, "requested_history_id": state.start_history_id,
                    "observed_at": Timestamp::now(),
                }),
            ),
            Err(e) => return Err(failure(e)),
        };
        stored.insert("labels".into(), pending.labels.clone());
        pending.index += 1;
        if pending.index == pending.ids.len() {
            state = *pending.after.clone();
        }
        page(vec![stored], state)
    }

    fn fetch_page(&self, state: State) -> Result<Page, DriverError> {
        let mut query = vec![("maxResults", "500")];
        if let Some(token) = state.page_token.as_deref() {
            query.push(("pageToken", token));
        }
        let path = if state.mode == Mode::List {
            query.push(("includeSpamTrash", "true"));
            "/messages"
        } else {
            query.push(("startHistoryId", &state.start_history_id));
            "/history"
        };
        let response = match self.transport.get(path, &query) {
            Ok(response) => response,
            Err(Error::Status(404)) if state.mode == Mode::History => {
                let stored = record(
                    &self.account,
                    "gap",
                    json!({
                        "requested_history_id": state.start_history_id, "status": 404,
                        "reason": "history_expired_or_unavailable", "observed_at": Timestamp::now(),
                    }),
                );
                return page(
                    vec![stored],
                    State {
                        mode: Mode::Baseline,
                        ..state
                    },
                );
            }
            Err(e) => return Err(failure(e)),
        };
        if !response.is_object() {
            return Err(invalid());
        }
        let labels = self.transport.get("/labels", &[]).map_err(failure)?;
        if !labels.is_object() {
            return Err(invalid());
        }
        for label in array(&labels, "labels")? {
            text(label, "id")?;
            text(label, "name")?;
        }
        let token = page_token(&response)?;
        let mut after = State {
            page_token: token.clone(),
            page_index: if token.is_some() {
                state.page_index.checked_add(1).ok_or_else(invalid)?
            } else {
                0
            },
            ..state.clone()
        };
        let mut ids = Vec::new();
        let mut records = Vec::new();
        if state.mode == Mode::List {
            for message in array(&response, "messages")? {
                ids.push(text(message, "id")?.to_owned());
            }
            records.push(record(&self.account, "listing", response));
            if token.is_none() {
                after.mode = Mode::History;
                after.start_history_id = state.initial_history_id.clone();
            }
        } else {
            let high_water = text(&response, "historyId")?.to_owned();
            let mut seen = BTreeSet::new();
            for history in array(&response, "history")? {
                text(history, "id")?;
                // Deleted messages need no body. Added and relabelled messages do.
                for key in ["messagesAdded", "labelsAdded", "labelsRemoved"] {
                    for change in array(history, key)? {
                        let message = change.get("message").ok_or_else(invalid)?;
                        let id = text(message, "id")?;
                        if seen.insert(id.to_owned()) {
                            ids.push(id.to_owned());
                        }
                    }
                }
                // A provider may supply references without a detailed change list.
                let deleted = array(history, "messagesDeleted")?
                    .iter()
                    .map(|c| text(c.get("message").ok_or_else(invalid)?, "id"))
                    .collect::<Result<BTreeSet<_>, _>>()?;
                for message in array(history, "messages")? {
                    let id = text(message, "id")?;
                    if !deleted.contains(id) && seen.insert(id.to_owned()) {
                        ids.push(id.to_owned());
                    }
                }
                records.push(record(&self.account, "history", history.clone()));
            }
            if token.is_none() {
                after.completed_history_id = high_water.clone();
                after.start_history_id = high_water;
            }
        }
        // Keep label definitions with the page even when it has no messages.
        if !records.is_empty() {
            records[0].insert("labels".into(), labels.clone());
        }
        if ids.is_empty() {
            return page(records, after);
        }
        let next = State {
            pending: Some(Pending {
                ids,
                index: 0,
                labels,
                after: Box::new(after),
            }),
            ..state
        };
        page(records, next)
    }
}

#[cfg(test)]
mod tests;

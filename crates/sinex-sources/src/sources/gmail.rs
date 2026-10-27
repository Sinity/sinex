//! Full-mailbox Gmail capture, including sent mail, drafts, Spam and Trash.
//! API responses are stored before parsing; OAuth tokens never enter records.
pub mod api;
pub mod oauth;

use base64::Engine;
use serde::Deserialize;
use serde_json::{Map, Value, json};
use sinex_model::event_types::communication::MailMessage;
use sinex_model::event_types::gmail::{
    HistoryGap, MailChanged, MessageChange, MessageReference, ProviderMessage, ProviderMissing,
};
use sinex_model::{Class, Timestamp};
use sinex_source_core::drivers::api_cursor::ApiCursor;
use sinex_source_core::drivers::held::HeldMaterial;
use sinex_source_core::drivers::{Acquired, DriverError};
use sinex_source_core::{
    DeclaredSource, Driver, Parser, Reader, RecordContext, RecordError, SourceSpec,
};
use std::path::Path;

pub const SPEC: SourceSpec = SourceSpec {
    name: "communication.gmail",
    driver: Driver::ApiCursor,
    excisable: false,
    purpose: "mail and provider history",
    class: Class::Guarded,
    parser: "gmail",
    parser_version: 1,
    event_types: &[
        "mail.message",
        "mail.provider_message",
        "mail.changed",
        "mail.history_gap",
        "mail.provider_missing",
    ],
    framer: sinex_source_core::Framer::export(
        sinex_source_core::drivers::export_archive::frame_lines,
        sinex_source_core::drivers::export_archive::Framing::Lines,
    ),
};

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    pub account: String,
    pub credentials: oauth::CredentialFiles,
}

impl Config {
    pub fn load(path: &Path) -> Result<Self, String> {
        let bytes = std::fs::read(path).map_err(|_| "Gmail account configuration unavailable")?;
        let mut config: Self =
            serde_json::from_slice(&bytes).map_err(|_| "invalid Gmail account configuration")?;
        if config.account.trim().is_empty()
            || config.account == "me"
        {
            return Err(
                "Gmail requires an account email"
                    .into(),
            );
        }
        let parent = path.parent().unwrap_or(Path::new("."));
        for file in [
            &mut config.credentials.client_id_file,
            &mut config.credentials.client_secret_file,
            &mut config.credentials.refresh_token_file,
        ] {
            if file.is_relative() {
                *file = parent.join(&*file);
            }
        }
        Ok(config)
    }
}

sinex_model::payload_enum! {
    #[derive(Debug)]
    pub enum GmailRecord {
        Message(MailMessage), Provider(ProviderMessage), Changed(MailChanged),
        Gap(HistoryGap), Missing(ProviderMissing)
    }
}

pub struct GmailParser;
impl Parser for GmailParser {
    type Record<'a> = Map<String, Value>;
    type Carry = ();
    type Payload = GmailRecord;

    fn parse(
        &self,
        record: &Map<String, Value>,
        _: &(),
        _: &RecordContext<'_>,
    ) -> Result<(Vec<GmailRecord>, ()), RecordError> {
        parse_record(record).map(|records| (records, ()))
    }
}

fn required<'a>(value: &'a Value, key: &str) -> Result<&'a str, RecordError> {
    value
        .get(key)
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty())
        .ok_or_else(|| RecordError::new(format!("Gmail record missing {key}")))
}

fn decode<T: serde::de::DeserializeOwned>(value: Value) -> Result<T, RecordError> {
    serde_json::from_value(value).map_err(|_| RecordError::new("invalid Gmail record fields"))
}

fn strings(value: &Value, key: &str) -> Result<Vec<String>, RecordError> {
    match value.get(key) {
        None => Ok(vec![]),
        Some(value) => decode(value.clone()),
    }
}

fn record_text<'a>(record: &'a Map<String, Value>, key: &str) -> Result<&'a str, RecordError> {
    record
        .get(key)
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty())
        .ok_or_else(|| RecordError::new(format!("Gmail record missing {key}")))
}

fn parse_record(record: &Map<String, Value>) -> Result<Vec<GmailRecord>, RecordError> {
    let account = record_text(record, "account")?.to_owned();
    let response = record
        .get("response")
        .filter(|r| r.is_object())
        .ok_or_else(|| RecordError::new("Gmail record missing response"))?;
    match record_text(record, "kind")? {
        "listing" | "profile" => Ok(vec![]),
        "message" => {
            let raw = response
                .get("raw")
                .and_then(Value::as_str)
                .ok_or_else(|| RecordError::new("Gmail message missing raw bytes"))?;
            let bytes = base64::engine::general_purpose::URL_SAFE_NO_PAD
                .decode(raw)
                .or_else(|_| base64::engine::general_purpose::URL_SAFE.decode(raw))
                .map_err(|_| RecordError::new("Gmail message has invalid base64url bytes"))?;
            let mut message = super::mail::parse_message(&bytes)?;
            let provider_message_id = required(response, "id")?.to_owned();
            message.account = Some(account.clone());
            message.native_id = Some(serde_json::to_string(&(&account, &provider_message_id))
                .expect("message coordinates serialize"));
            let labels = strings(response, "labelIds")?;
            let names = record
                .get("labels")
                .and_then(|l| l.get("labels"))
                .and_then(Value::as_array);
            message.labels = labels
                .iter()
                .map(|id| {
                    names
                        .and_then(|labels| {
                            labels
                                .iter()
                                .find(|l| l.get("id").and_then(Value::as_str) == Some(id))
                        })
                        .and_then(|l| l.get("name"))
                        .and_then(Value::as_str)
                        .unwrap_or(id)
                        .to_owned()
                })
                .collect();
            let thread_id = required(response, "threadId")?.to_owned();
            message.thread_id = Some(thread_id.clone());
            let internal_date = response
                .get("internalDate")
                .map(|value| {
                    let millis = value
                        .as_str()
                        .and_then(|s| s.parse::<i128>().ok())
                        .ok_or_else(|| RecordError::new("invalid Gmail internal date"))?;
                    let ns = millis
                        .checked_mul(1_000_000)
                        .ok_or_else(|| RecordError::new("Gmail internal date out of range"))?;
                    Timestamp::from_unix_ns(ns)
                        .map_err(|_| RecordError::new("Gmail internal date out of range"))
                })
                .transpose()?;
            let provider = ProviderMessage {
                account,
                provider_message_id,
                thread_id,
                history_id: required(response, "historyId")?.into(),
                labels,
                internal_date,
                message_id: message.message_id.clone(),
            };
            Ok(vec![
                GmailRecord::Message(message),
                GmailRecord::Provider(provider),
            ])
        }
        "history" => {
            #[derive(Deserialize)]
            #[serde(rename_all = "camelCase")]
            struct History {
                id: String,
                #[serde(default)]
                messages: Vec<MessageReference>,
                #[serde(default)]
                messages_added: Vec<MessageChange>,
                #[serde(default)]
                messages_deleted: Vec<MessageChange>,
                #[serde(default)]
                labels_added: Vec<MessageChange>,
                #[serde(default)]
                labels_removed: Vec<MessageChange>,
            }
            let history: History = decode(response.clone())?;
            if history.id.is_empty() {
                return Err(RecordError::new("Gmail history ID is empty"));
            }
            Ok(vec![GmailRecord::Changed(MailChanged {
                account,
                history_id: history.id,
                messages: history.messages,
                added: history.messages_added,
                deleted: history.messages_deleted,
                labels_added: history.labels_added,
                labels_removed: history.labels_removed,
            })])
        }
        "gap" | "missing" => {
            let mut response = response.as_object().unwrap().clone();
            response.insert("account".into(), json!(account));
            if record_text(record, "kind")? == "gap" {
                Ok(vec![GmailRecord::Gap(decode(Value::Object(response))?)])
            } else {
                Ok(vec![GmailRecord::Missing(decode(Value::Object(response))?)])
            }
        }
        _ => Err(RecordError::new("unknown Gmail record kind")),
    }
}

pub fn source() -> DeclaredSource<GmailParser> {
    DeclaredSource {
        spec: SPEC,
        parser: GmailParser,
    }
}

pub struct GmailReader<T> {
    driver: ApiCursor<api::Gmail<T>>,
    held: Option<HeldMaterial>,
}

impl<T: api::Transport> GmailReader<T> {
    pub fn new(transport: T, account: String, host: String) -> Self {
        Self {
            driver: ApiCursor {
                api: api::Gmail {
                    transport,
                    account,
                    host,
                },
            },
            held: None,
        }
    }
}

impl<T: api::Transport + 'static> Reader for GmailReader<T> {
    fn poll(&mut self, cursor: &Value) -> Result<Acquired, DriverError> {
        if let Some(held) = &self.held {
            if !held.admitted(cursor) {
                return held.acquired(cursor);
            }
            self.held = None;
        }
        let mut acquired = self.driver.poll(cursor)?;
        let Some(material) = acquired.material.as_mut() else {
            if let Some(through) = cursor.get("through") {
                acquired
                    .cursor_without_material
                    .as_object_mut()
                    .expect("API cursor is an object")
                    .insert("through".into(), through.clone());
            }
            return Ok(acquired);
        };
        let at = std::mem::replace(&mut material.cursor_at, Box::new(|_| Ok(Value::Null)));
        material.cursor_at = Box::new(move |end| {
            let mut cursor = at(end)?;
            cursor
                .as_object_mut()
                .expect("API cursor is an object")
                .insert("through".into(), json!(end));
            Ok(cursor)
        });
        let held = HeldMaterial::hold(acquired, cursor)?;
        let acquired = held.acquired(cursor)?;
        self.held = Some(held);
        Ok(acquired)
    }
}

pub const FORMAT: sinex_source_core::binding::InputFormat =
    sinex_source_core::binding::InputFormat {
        name: "gmail",
        specs: || vec![&SPEC],
        plan: Some(|input, settings| {
            let config = Config::load(input)?;
            let mut settings = settings.clone();
            settings.options = json!({"account": config.account, "include_spam_trash": true});
            Ok(sinex_source_core::binding::single(&SPEC, input, &settings))
        }),
        source: |_| Ok(Box::new(source())),
        reader: Some(|binding| {
            let config = Config::load(&binding.input)?;
            if binding.settings.option("account") != Some(&config.account) {
                return Err("Gmail account configuration changed; plan the binding again".into());
            }
            let tokens = oauth::TokenProvider::new(
                config.credentials,
                oauth::GoogleExchange::default(),
                oauth::SystemClock,
            );
            Ok(sinex_source_core::binding::polled(GmailReader::new(
                api::Http::new(tokens, &config.account),
                config.account,
                binding.settings.host.clone(),
            )))
        }),
        capture: None,
    };

#[cfg(test)]
mod tests;

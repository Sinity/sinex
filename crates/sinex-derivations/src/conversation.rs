//! Conversation days: each conversation's messages on one logical day, within
//! one provider. Chat lines group by their buffer, Messenger and SMS messages
//! by their thread (its participants), and mail by its recorded thread ID. Mail
//! without a thread ID stays separate by message ID: a shared subject does not
//! identify a conversation. Mail is read as fused messages plus the mail
//! records no fused message joins, so a message several sources of one
//! account hold counts once.
//!
//! The key is the logical day: one read of the day's messages gives every
//! conversation's day, so a thread of any length costs one key per day it was
//! active.
use crate::derived_event::derived;
use crate::fusion::{Mail, mail};
use crate::{Computed, Env, InputQuery, KeyState, KeyView, Keyed, Kind};
use serde_json::{Value, json};
use sinex_model::event_types::ConversationDay;
use sinex_model::event_types::communication::{ChatLineKind, ChatMessage, ConversationMessage};
use sinex_model::event_types::fused::MailFused;
use sinex_model::{Event, EventPayload, Timestamp};
use std::collections::BTreeMap;

pub struct ConversationDays;

type Messages<'a> = Vec<(&'a Event, String, Timestamp)>;

/// The channel, conversation, sender and time of one message; mail is read
/// in the fused kind's payload.
fn message(e: &Event, payload: &Value) -> Option<(&'static str, String, String, Timestamp)> {
    match e.event_type.as_str() {
        t if t == ChatMessage::EVENT_TYPE => {
            let m: ChatMessage = e.payload_as()?;
            matches!(m.kind, ChatLineKind::Message | ChatLineKind::Action)
                .then(|| ("chat", m.buffer, m.nick.unwrap_or_default(), m.sent_at))
        }
        t if t == ConversationMessage::EVENT_TYPE => {
            let m: ConversationMessage = e.payload_as()?;
            Some((
                "messages",
                format!("{}:{}", m.service, m.thread),
                m.sender,
                m.sent_at,
            ))
        }
        t if mail().reads(t) => {
            let m: Mail = serde_json::from_value(payload.clone()).ok()?;
            let thread = match (
                m.account.as_deref(),
                m.thread_id
                    .as_deref()
                    .filter(|thread| !thread.trim().is_empty()),
            ) {
                (Some(account), Some(thread)) => {
                    json!([account, format!("thread:{thread}")]).to_string()
                }
                (Some(account), None) => json!([
                    account,
                    format!("message:{}", m.message_id.as_deref().unwrap_or(&m.record))
                ])
                .to_string(),
                (None, _) => json!([null, format!("record:{}", m.record)]).to_string(),
            };
            Some((
                "mail",
                thread,
                m.from.first().cloned().unwrap_or_default(),
                m.sent_at,
            ))
        }
        _ => None,
    }
}

fn day_of(key: &Value) -> Option<jiff::civil::Date> {
    key.get("day")?.as_str()?.parse().ok()
}

impl Keyed for ConversationDays {
    const NAME: &'static str = "conversation.days";
    const VERSION: u32 = 4;
    const USES_TIME_SETTINGS: bool = true;
    const INPUTS: &'static [&'static str] = &[
        ChatMessage::EVENT_TYPE,
        ConversationMessage::EVENT_TYPE,
        MailFused::EVENT_TYPE,
        "mail.message",
    ];
    const OUTPUTS: &'static [&'static str] = &[ConversationDay::EVENT_TYPE];

    fn keys_for(&self, input: &KeyView<'_>, env: &Env) -> Vec<Value> {
        let Some(t) = input.ts_orig else {
            return vec![];
        };
        // A message's fused record may lie in the neighbouring day.
        let margin = if mail().reads(input.event_type) {
            mail().margin()
        } else {
            0
        };
        let mut keys: Vec<Value> = Vec::new();
        for ns in [t.unix_ns() - margin, t.unix_ns() + margin] {
            let Some(day) = Timestamp::from_unix_ns(ns)
                .ok()
                .and_then(|at| env.time_settings.day(at).ok())
            else {
                continue;
            };
            let key = json!({ "day": day.to_string() });
            if !keys.contains(&key) {
                keys.push(key);
            }
        }
        keys
    }

    fn inputs(&self, key: &Value, env: &Env) -> Option<InputQuery> {
        let day = day_of(key)?;
        let margin = mail().margin();
        let mut kinds = vec![
            Kind::only(ChatMessage::EVENT_TYPE, &[("kind", "message")]),
            Kind::only(ChatMessage::EVENT_TYPE, &[("kind", "action")]),
            Kind::all(ConversationMessage::EVENT_TYPE),
        ];
        kinds.extend(mail().kinds());
        Some(InputQuery::of(
            kinds,
            Timestamp::from_unix_ns(env.time_settings.day_start(day).ok()?.unix_ns() - margin)
                .ok()?,
            Timestamp::from_unix_ns(env.time_settings.day_end(day).ok()?.unix_ns() + margin)
                .ok()?,
        ))
    }

    fn compute(&self, key: &Value, inputs: &[Event], env: &Env) -> Computed {
        let Some(day) = day_of(key) else {
            return Computed::default();
        };
        let (Ok(day_start), Ok(day_end)) = (
            env.time_settings.day_start(day),
            env.time_settings.day_end(day),
        ) else {
            return Computed::default();
        };
        let messages = mail().occurrences(inputs);
        let others = inputs
            .iter()
            .filter(|e| !mail().reads(&e.event_type))
            .map(|e| (e, std::borrow::Cow::Borrowed(&e.payload)));
        let mut days: BTreeMap<(&'static str, String), Messages<'_>> = BTreeMap::new();
        for (e, payload) in messages
            .into_iter()
            .map(|o| (o.event, o.payload))
            .chain(others)
        {
            if let Some((channel, conversation, sender, at)) = message(e, &payload)
                && day_start <= at
                && at < day_end
            {
                days.entry((channel, conversation))
                    .or_default()
                    .push((e, sender, at));
            }
        }
        let outputs = days
            .into_iter()
            .map(|((channel, conversation), mut messages)| {
                messages.sort_by_key(|(e, _, at)| {
                    (
                        *at,
                        (
                            e.source.clone(),
                            e.equivalence_key.as_ref().map(|key| key.0.clone()),
                            crate::derived_event::record_identity(e),
                        ),
                    )
                });
                let mut senders: Vec<String> = messages
                    .iter()
                    .map(|(_, s, _)| s.clone())
                    .filter(|s| !s.is_empty())
                    .collect();
                senders.sort();
                senders.dedup();
                let payload = ConversationDay {
                    channel: channel.to_owned(),
                    conversation,
                    day: day.to_string(),
                    messages: u64::try_from(messages.len()).expect("message count fits u64"),
                    senders,
                    first_at: messages[0].2,
                    last_at: messages[messages.len() - 1].2,
                };
                derived(&payload, Self::NAME, messages.iter().map(|(e, _, _)| *e))
            })
            .collect();
        Computed {
            error: None,
            outputs,
            waiting: false,
        }
    }

    fn key_state(&self, key: &Value, env: &Env) -> KeyState {
        match day_of(key).and_then(|day| env.time_settings.day_end(day).ok()) {
            Some(end) if env.now.unix_ns() >= end.unix_ns() => KeyState::Closed,
            _ => KeyState::Open,
        }
    }
}

#[cfg(test)]
mod tests;

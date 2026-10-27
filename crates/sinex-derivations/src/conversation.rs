//! Conversation days: each conversation's messages on one logical day, within
//! one provider. Chat lines group by their buffer, Messenger and SMS messages
//! by their thread (its participants), and mail by its thread ID, or by its
//! subject without reply and forward prefixes when it has none. Mail is read
//! fused, so a message several mail sources hold counts once.
//!
//! The key is the logical day: one read of the day's messages gives every
//! conversation's day, so a thread of any length costs one key per day it was
//! active.
use crate::derived_event::derived;
use crate::{Computed, Env, InputQuery, KeyState, KeyView, Keyed, Kind};
use serde_json::{Value, json};
use sinex_model::event_types::ConversationDay;
use sinex_model::event_types::communication::{ChatLineKind, ChatMessage, ConversationMessage};
use sinex_model::event_types::fused::MailFused;
use sinex_model::{Event, EventPayload, Timestamp};
use std::collections::BTreeMap;

pub struct ConversationDays;

type Messages<'a> = Vec<(&'a Event, String, Timestamp)>;

/// A subject as its thread: without leading reply and forward prefixes, case
/// and spacing ignored.
fn thread_subject(subject: &str) -> String {
    let mut s = subject.trim();
    loop {
        let lower = s.to_lowercase();
        let Some(rest) = ["re:", "fwd:", "fw:", "aw:", "odp:", "pd:"]
            .iter()
            .find_map(|p| lower.starts_with(p).then(|| &s[p.len()..]))
        else {
            break;
        };
        s = rest.trim_start();
    }
    s.split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
        .to_lowercase()
}

/// The channel, conversation, sender and time of one message.
fn message(e: &Event) -> Option<(&'static str, String, String, Timestamp)> {
    match e.event_type {
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
        t if t == MailFused::EVENT_TYPE => {
            let m: MailFused = e.payload_as()?;
            let thread = m
                .thread_id
                .filter(|t| !t.trim().is_empty())
                .or_else(|| m.subject.as_deref().map(thread_subject))
                .unwrap_or_else(|| m.message_id.clone());
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
    const VERSION: u32 = 1;
    const INPUTS: &'static [&'static str] = &[
        ChatMessage::EVENT_TYPE,
        ConversationMessage::EVENT_TYPE,
        MailFused::EVENT_TYPE,
    ];
    const OUTPUTS: &'static [&'static str] = &[ConversationDay::EVENT_TYPE];

    fn keys_for(&self, input: &KeyView<'_>, env: &Env) -> Vec<Value> {
        input
            .ts_orig
            .and_then(|t| env.time_settings.day(t).ok())
            .map(|day| json!({ "day": day.to_string() }))
            .into_iter()
            .collect()
    }

    fn inputs(&self, key: &Value, env: &Env) -> Option<InputQuery> {
        let day = day_of(key)?;
        Some(InputQuery::of(
            vec![
                Kind::only(ChatMessage::EVENT_TYPE, &[("kind", "message")]),
                Kind::only(ChatMessage::EVENT_TYPE, &[("kind", "action")]),
                Kind::all(ConversationMessage::EVENT_TYPE),
                Kind::all(MailFused::EVENT_TYPE),
            ],
            env.time_settings.day_start(day).ok()?,
            env.time_settings.day_end(day).ok()?,
        ))
    }

    fn compute(&self, key: &Value, inputs: &[Event], _: &Env) -> Computed {
        let Some(day) = day_of(key) else {
            return Computed::default();
        };
        let mut days: BTreeMap<(&'static str, String), Messages<'_>> = BTreeMap::new();
        for e in inputs {
            if let Some((channel, conversation, sender, at)) = message(e) {
                days.entry((channel, conversation))
                    .or_default()
                    .push((e, sender, at));
            }
        }
        let outputs = days
            .into_iter()
            .map(|((channel, conversation), mut messages)| {
                messages.sort_by_key(|(e, _, at)| (*at, e.id));
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

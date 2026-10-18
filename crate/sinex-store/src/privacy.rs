//! Privacy rules run in admission, once (architecture §3.5; `54y`). A rule
//! names a source, optionally an event type and a payload field, and either
//! classifies, masks or drops. Reads never apply a rule again.
use crate::{Error, Result};
use sinex_model::{Class, Event};
use sqlx::PgConnection;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Rule {
    pub source: String,
    pub event_type: Option<String>,
    pub field: Option<String>,
    pub class: Class,
    pub action: Action,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Action {
    Classify,
    Mask,
    DropField,
}

pub const MASK: &str = "[masked]";

pub async fn load<'a>(
    conn: &mut PgConnection,
    sources: impl Iterator<Item = &'a str>,
) -> Result<Vec<Rule>> {
    let mut sources: Vec<String> = sources.map(str::to_owned).collect();
    sources.sort();
    sources.dedup();
    let rows = sqlx::query!(
        "SELECT source, event_type, field, class, action FROM privacy.rules WHERE source = ANY($1)",
        &sources
    )
    .fetch_all(&mut *conn)
    .await?;
    rows.into_iter()
        .map(|r| {
            Ok(Rule {
                source: r.source,
                event_type: r.event_type,
                field: r.field,
                class: match r.class.as_str() {
                    "open" => Class::Open,
                    "guarded" => Class::Guarded,
                    "sealed" => Class::Sealed,
                    other => return Err(Error::Invalid(format!("unknown class {other}"))),
                },
                action: match r.action.as_str() {
                    "classify" => Action::Classify,
                    "mask" => Action::Mask,
                    "drop_field" => Action::DropField,
                    other => return Err(Error::Invalid(format!("unknown privacy action {other}"))),
                },
            })
        })
        .collect()
}

/// Apply every matching rule to `event`. The event's class becomes the highest
/// class any matching rule gives.
pub fn apply(rules: &[Rule], event: &mut Event) -> Result<()> {
    for rule in rules.iter().filter(|r| {
        r.source == event.source
            && r.event_type
                .as_deref()
                .is_none_or(|t| t == event.event_type)
    }) {
        event.class = event.class.max(rule.class);
        if let Some(field) = &rule.field
            && let Some(object) = event.payload.as_object_mut()
        {
            match rule.action {
                Action::Classify => {}
                Action::Mask => {
                    if let Some(v) = object.get_mut(field) {
                        *v = serde_json::Value::String(MASK.into());
                    }
                }
                Action::DropField => {
                    object.remove(field);
                }
            }
        }
    }
    if event.class == Class::Sealed {
        return Err(Error::SealedNotBuilt);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use sinex_model::{
        Event, MaterialId, Origin, ReadingOrder, RecordLocation, Timestamp, kinds::CommandImported,
    };

    fn event() -> Event {
        Event::build(
            &CommandImported {
                command: "export TOKEN=x".into(),
                hostname: "h".into(),
                started_at: Timestamp::from_unix_secs(1).unwrap(),
                header_elapsed: 0,
            },
            "terminal.zsh-history",
            Origin::Record {
                location: RecordLocation {
                    material: MaterialId::new(),
                    anchor: 0,
                },
                ordinal: 0,
            },
            ReadingOrder(0),
            [0; 32],
            Class::Open,
        )
        .unwrap()
    }

    #[test]
    fn masks_and_raises_class() {
        let mut e = event();
        let rules = vec![Rule {
            source: "terminal.zsh-history".into(),
            event_type: Some("command.imported".into()),
            field: Some("command".into()),
            class: Class::Guarded,
            action: Action::Mask,
        }];
        apply(&rules, &mut e).unwrap();
        assert_eq!(e.payload["command"], json!(MASK));
        assert_eq!(e.class, Class::Guarded);
    }

    #[test]
    fn other_sources_untouched_and_sealed_refused() {
        let mut e = event();
        let other = Rule {
            source: "x".into(),
            event_type: None,
            field: None,
            class: Class::Sealed,
            action: Action::Classify,
        };
        apply(std::slice::from_ref(&other), &mut e).unwrap();
        assert_eq!(e.class, Class::Open);
        let sealed = Rule {
            source: "terminal.zsh-history".into(),
            ..other
        };
        assert!(matches!(
            apply(&[sealed], &mut e),
            Err(Error::SealedNotBuilt)
        ));
    }
}

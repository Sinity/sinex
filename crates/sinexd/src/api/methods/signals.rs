//! Authored signal revisions are ordinary request material.
use super::*;
use crate::{Error, Result};
use sinex_model::event_types::signal::{RuleWritten, SignalRecord};
use sinex_model::{Class, Timestamp};

impl Api {
    pub(super) async fn signal(
        &self,
        caller: &Caller,
        action: &str,
        name: Option<String>,
        rule: Option<String>,
        page: Page,
    ) -> Result<Value> {
        if action == "list" {
            let mut conn = self.acquire(caller).await?;
            let answer = sinex_store::query::run(
                &mut conn,
                "events where event_type = \"signal.rule\"",
                None,
                &self.context(),
                &page,
            )
            .await?;
            return serde_json::to_value(answer).map_err(Error::from);
        }
        if page.cursor.is_some() || page.size.is_some() {
            return Err(Error::bad_request(
                "signal mutation does not take a page or cursor",
            ));
        }
        let configuration = self
            .source_config
            .signals
            .as_ref()
            .ok_or_else(|| Error::not_configured("signals are not configured"))?;
        let writer = self
            .writer
            .as_ref()
            .ok_or_else(|| Error::not_configured("signal authoring requires sinexd run"))?;
        let _serial = writer.lock_signals().await;
        let mut conn = self.acquire(caller).await?;
        let submitted = rule
            .as_deref()
            .map(serde_json::from_str::<::signals::Rule>)
            .transpose()?;
        let name = name
            .or_else(|| submitted.as_ref().map(|rule| rule.name.clone()))
            .ok_or_else(|| Error::bad_request("signal mutation requires a name"))?;
        let prior = sinex_store::query::live_payload(&mut conn, "signal.rule", "name", &name)
            .await?
            .map(|(_, payload)| serde_json::from_value::<RuleWritten>(payload).map_err(Error::from))
            .transpose()?;
        let mut active = match (action, submitted) {
            ("add", Some(rule)) => {
                if name != rule.name {
                    return Err(Error::bad_request("signal name differs from rule.name"));
                }
                rule.validate(&configuration.configuration())
                    .map_err(Error::bad_value)?;
                sinex_store::query::initialize_calendar(&mut conn).await?;
                let start = self.context();
                let seq = sinex_store::query::current_seq(&mut conn).await?;
                let answer = crate::signals::query(&mut conn, &rule.query, &start, seq).await?;
                ::signals::ActiveRule {
                    version: ::signals::RuleVersion {
                        product: "sinex".into(),
                        id: prior.as_ref().map_or_else(
                            || format!("authored:{}", uuid::Uuid::now_v7()),
                            |prior| prior.active.version.id.clone(),
                        ),
                        revision: prior.as_ref().map_or(Ok(1), |prior| {
                            prior
                                .active
                                .version
                                .revision
                                .checked_add(1)
                                .ok_or_else(|| Error::bad_request("signal revision exceeds u64"))
                        })?,
                    },
                    rule,
                    author: if caller.actor == "operator" {
                        ::signals::Author::Operator
                    } else {
                        ::signals::Author::Agent(caller.actor.into())
                    },
                    via: "api".into(),
                    live: answer.live,
                    shipped: ::signals::Shipped::default(),
                }
            }
            ("enable" | "disable" | "remove", None) => {
                let mut active = prior
                    .as_ref()
                    .ok_or_else(|| Error::not_found("no visible authored signal with this name"))?
                    .active
                    .clone();
                active.version.revision = active
                    .version
                    .revision
                    .checked_add(1)
                    .ok_or_else(|| Error::bad_request("signal revision exceeds u64"))?;
                active.rule.enabled = action == "enable";
                active
            }
            ("add", None) => {
                return Err(Error::bad_request(
                    "signal add requires --rule with the complete rule JSON",
                ));
            }
            ("enable" | "disable" | "remove", Some(_)) => {
                return Err(Error::bad_request(
                    "signal state changes do not accept a replacement rule",
                ));
            }
            _ => {
                return Err(Error::bad_request(
                    "signal action must be add, list, enable, disable or remove",
                ));
            }
        };
        // Authorship is the current authenticated act, including a new disabled revision.
        active.author = if caller.actor == "operator" {
            ::signals::Author::Operator
        } else {
            ::signals::Author::Agent(caller.actor.into())
        };
        active.via = "api".into();
        let now = Timestamp::now();
        let grants = std::iter::once(Class::Open).chain(
            caller
                .grants
                .split(',')
                .filter(|grant| !grant.is_empty())
                .map(|grant| serde_json::from_value(json!(grant)))
                .collect::<Result<Vec<_>, _>>()?,
        );
        let record = SignalRecord::Rule(Box::new(RuleWritten {
            name,
            active,
            withdrawn: action == "remove",
            grants: grants.collect(),
            written_at: now,
        }));
        let done = crate::intake::request(
            &mut conn,
            writer,
            &self.blobs,
            std::sync::Arc::new(sinex_sources::sources::signals::source()),
            &format!("signal.request:{}@{}", caller.actor, self.host),
            &self.host,
            sinex_sources::sources::signals::material(
                &[record],
                caller.actor,
                "api",
                &self.host,
                now,
            ),
        )
        .await?;
        if done.outcome.admitted != 1 || done.outcome.failures != 0 {
            return Err(Error::bad_request("signal revision was not admitted"));
        }
        let event = done
            .outcome
            .admitted_ids
            .first()
            .ok_or_else(|| Error::bad_request("signal revision has no committed reading"))?;
        Ok(json!({"citation":format!("sinex:ev/{event}"),"event":event,"material":done.material}))
    }
}

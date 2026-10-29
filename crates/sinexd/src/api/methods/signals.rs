//! Authored signal revisions are ordinary request material.
use super::*;
use sinex_model::event_types::signal::{RuleWritten, SignalRecord};
use sinex_model::{Class, Timestamp};

fn error(error: impl std::fmt::Display) -> String {
    error.to_string()
}

impl Api {
    pub(super) async fn signal(
        &self,
        caller: &Caller,
        action: &str,
        name: Option<String>,
        rule: Option<String>,
        page: Page,
    ) -> Result<Value, String> {
        if action == "list" {
            let mut conn = self.acquire(caller).await.map_err(error)?;
            let answer = sinex_store::query::run(
                &mut conn,
                "events where event_type = \"signal.rule\"",
                None,
                &self.context(),
                &page,
            )
            .await
            .map_err(error)?;
            return serde_json::to_value(answer).map_err(error);
        }
        if page.cursor.is_some() || page.size.is_some() {
            return Err("signal mutation does not take a page or cursor".into());
        }
        let configuration = self
            .source_config
            .signals
            .as_ref()
            .ok_or("signals are not configured")?;
        let writer = self
            .writer
            .as_ref()
            .ok_or("signal authoring requires sinexd run")?;
        let _serial = writer.lock_signals().await;
        let mut conn = self.acquire(caller).await.map_err(error)?;
        let submitted = rule
            .as_deref()
            .map(serde_json::from_str::<::signals::Rule>)
            .transpose()
            .map_err(error)?;
        let name = name
            .or_else(|| submitted.as_ref().map(|rule| rule.name.clone()))
            .ok_or("signal mutation requires a name")?;
        let prior = sinex_store::query::live_payload(&mut conn, "signal.rule", "name", &name)
            .await
            .map_err(error)?
            .map(|(_, payload)| serde_json::from_value::<RuleWritten>(payload).map_err(error))
            .transpose()?;
        let mut active = match (action, submitted) {
            ("add", Some(rule)) => {
                if name != rule.name {
                    return Err("signal name differs from rule.name".into());
                }
                rule.validate(&configuration.configuration())
                    .map_err(error)?;
                sinex_store::query::initialize_calendar(&mut conn)
                    .await
                    .map_err(error)?;
                let start = self.context();
                let seq = sinex_store::query::current_seq(&mut conn)
                    .await
                    .map_err(error)?;
                let answer = crate::signals::query(&mut conn, &rule.query, &start, seq)
                    .await
                    .map_err(error)?;
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
                                .ok_or("signal revision exceeds u64")
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
                }
            }
            ("enable" | "disable" | "remove", None) => {
                let mut active = prior
                    .as_ref()
                    .ok_or("no visible authored signal with this name")?
                    .active
                    .clone();
                active.version.revision = active
                    .version
                    .revision
                    .checked_add(1)
                    .ok_or("signal revision exceeds u64")?;
                active.rule.enabled = action == "enable";
                active
            }
            ("add", None) => {
                return Err("signal add requires --rule with the complete rule JSON".into());
            }
            ("enable" | "disable" | "remove", Some(_)) => {
                return Err("signal state changes do not accept a replacement rule".into());
            }
            _ => return Err("signal action must be add, list, enable, disable or remove".into()),
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
                .collect::<Result<Vec<_>, _>>()
                .map_err(error)?,
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
            return Err("signal revision was not admitted".into());
        }
        let event = done
            .outcome
            .admitted_ids
            .first()
            .ok_or("signal revision has no committed reading")?;
        Ok(json!({"citation":format!("sinex:ev/{event}"),"event":event,"material":done.material}))
    }
}

//! The privacy commands: rules, source classes, unlocking and locking the
//! sealed class, and verifying classes.
use super::*;
use crate::{Error, Result};
use sinex_store::privacy;

impl Api {
    /// The privacy commands: rules, source classes, unlocking the sealed
    /// class, and verifying classes.
    pub(super) async fn privacy(&self, request: Request, caller: &Caller) -> Result<Reply> {
        let actor = caller.actor;
        let result = match request {
            Request::PrivacyRules { history } => {
                let mut conn = self.acquire(caller).await?;
                let rows = privacy::list_rules(&mut conn, history.unwrap_or(false)).await?;
                json!({ "total": rows.len(), "rules": rows })
            }
            Request::PrivacyRule {
                name,
                source,
                event_type,
                field,
                matcher,
                pattern,
                class,
                action,
            } => {
                let class = privacy::parse_class(class.as_deref().unwrap_or("open"))?;
                let action = privacy::Action::parse(action.as_deref().unwrap_or("mask"))?;
                let rule = privacy::NewRule {
                    name,
                    source,
                    event_type,
                    field,
                    matcher: matcher.unwrap_or_else(|| "all".into()),
                    matcher_arg: pattern,
                    class,
                    action,
                    author: actor.to_string(),
                };
                let mut conn = self.acquire(caller).await?;
                let seq = privacy::add_rule(&mut conn, &rule).await?;
                json!({ "rule": rule.name, "seq": seq,
                    "applies": "to new readings; replay the source to apply it to stored readings" })
            }
            Request::PrivacyEnd { name } => {
                let mut conn = self.acquire(caller).await?;
                let seq = privacy::end_rule(&mut conn, &name, actor).await?;
                json!({ "rule": name, "ended": true, "seq": seq })
            }
            Request::PrivacyClass { source, class } => {
                let class = privacy::parse_class(&class)?;
                let mut conn = self.acquire(caller).await?;
                if actor != "operator" {
                    let current = privacy::load(&mut conn, std::iter::once(source.as_str()))
                        .await?
                        .floors
                        .get(&source)
                        .copied()
                        .unwrap_or_default();
                    if class < current {
                        return Err(Error::refused(format!(
                            "{source}'s floor is {}; lowering it weakens protection, which only the operator token may do",
                            privacy::class_name(current)
                        )));
                    }
                }
                let seq = privacy::set_source_class(&mut conn, &source, class, actor).await?;
                json!({ "source": source, "class": privacy::class_name(class), "seq": seq,
                    "applies": "to new readings; replay the source to raise stored readings" })
            }
            Request::Unlock { identity, minutes } => {
                let identity = sinex_sealing::parse_identity(&identity)?;
                let minutes = minutes.unwrap_or(480);
                let seconds = minutes
                    .checked_mul(60)
                    .ok_or_else(|| Error::bad_value("unlock duration exceeds u64 seconds"))?;
                let unlock = crate::material_access::sealing::Unlock::new(
                    identity,
                    std::time::Duration::from_secs(seconds),
                )?;
                *self.unlock.lock().await = Some(Unlocked {
                    unlock,
                    keys: Default::default(),
                });
                json!({ "unlocked": "sealed", "minutes": minutes })
            }
            Request::Lock => {
                let was = self.unlock.lock().await.take().is_some();
                json!({ "locked": "sealed", "was_unlocked": was })
            }
            Request::PrivacyVerify => {
                let mut conn = self.acquire(caller).await?;
                serde_json::to_value(privacy::verify_classes(&mut conn).await?)
                    .expect("verification serializes")
            }
            _ => unreachable!("only privacy requests reach here"),
        };
        Ok(Reply::Ok { result })
    }
}

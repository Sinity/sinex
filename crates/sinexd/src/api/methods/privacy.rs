//! The privacy commands: rules, source classes, unlocking and locking the
//! sealed class, and verifying classes.
use super::*;
use sinex_store::privacy;

impl Api {
    /// The privacy commands: rules, source classes, unlocking the sealed
    /// class, and verifying classes.
    pub(super) async fn privacy(&self, request: Request, caller: &Caller) -> Reply {
        let actor = caller.actor;
        match request {
            Request::PrivacyRules { history } => {
                let mut conn = match self.acquire(caller).await {
                    Ok(c) => c,
                    Err(e) => return refused(e.to_string()),
                };
                match privacy::list_rules(&mut conn, history.unwrap_or(false)).await {
                    Ok(rows) => Reply::Ok {
                        result: json!({ "total": rows.len(), "rules": rows }),
                    },
                    Err(e) => refused(e.to_string()),
                }
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
                let class = match privacy::parse_class(class.as_deref().unwrap_or("open")) {
                    Ok(c) => c,
                    Err(e) => return refused(e.to_string()),
                };
                let action = match privacy::Action::parse(action.as_deref().unwrap_or("mask")) {
                    Ok(a) => a,
                    Err(e) => return refused(e.to_string()),
                };
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
                let mut conn = match self.acquire(caller).await {
                    Ok(c) => c,
                    Err(e) => return refused(e.to_string()),
                };
                match privacy::add_rule(&mut conn, &rule).await {
                    Ok(seq) => Reply::Ok {
                        result: json!({ "rule": rule.name, "seq": seq,
                            "applies": "to new readings; replay the source to apply it to stored readings" }),
                    },
                    Err(e) => refused(e.to_string()),
                }
            }
            Request::PrivacyEnd { name } => {
                let mut conn = match self.acquire(caller).await {
                    Ok(c) => c,
                    Err(e) => return refused(e.to_string()),
                };
                match privacy::end_rule(&mut conn, &name, actor).await {
                    Ok(seq) => Reply::Ok {
                        result: json!({ "rule": name, "ended": true, "seq": seq }),
                    },
                    Err(e) => refused(e.to_string()),
                }
            }
            Request::PrivacyClass { source, class } => {
                let class = match privacy::parse_class(&class) {
                    Ok(c) => c,
                    Err(e) => return refused(e.to_string()),
                };
                let mut conn = match self.acquire(caller).await {
                    Ok(c) => c,
                    Err(e) => return refused(e.to_string()),
                };
                if actor != "operator" {
                    let current =
                        match privacy::load(&mut conn, std::iter::once(source.as_str())).await {
                            Ok(set) => set.floors.get(&source).copied().unwrap_or_default(),
                            Err(e) => return refused(e.to_string()),
                        };
                    if class < current {
                        return refused(format!(
                            "{source}'s floor is {}; lowering it weakens protection, which only the operator token may do",
                            privacy::class_name(current)
                        ));
                    }
                }
                match privacy::set_source_class(&mut conn, &source, class, actor).await {
                    Ok(seq) => Reply::Ok {
                        result: json!({ "source": source, "class": privacy::class_name(class), "seq": seq,
                            "applies": "to new readings; replay the source to raise stored readings" }),
                    },
                    Err(e) => refused(e.to_string()),
                }
            }
            Request::Unlock { identity, minutes } => {
                let identity = match sinex_sealing::parse_identity(&identity) {
                    Ok(i) => i,
                    Err(e) => return refused(e.to_string()),
                };
                let minutes = minutes.unwrap_or(480);
                let Some(seconds) = minutes.checked_mul(60) else {
                    return refused("unlock duration exceeds u64 seconds");
                };
                let unlock = match crate::material_access::sealing::Unlock::new(
                    identity,
                    std::time::Duration::from_secs(seconds),
                ) {
                    Ok(unlock) => unlock,
                    Err(error) => return refused(error),
                };
                *self.unlock.lock().await = Some(Unlocked {
                    unlock,
                    keys: Default::default(),
                });
                Reply::Ok {
                    result: json!({ "unlocked": "sealed", "minutes": minutes }),
                }
            }
            Request::Lock => {
                let was = self.unlock.lock().await.take().is_some();
                Reply::Ok {
                    result: json!({ "locked": "sealed", "was_unlocked": was }),
                }
            }
            Request::PrivacyVerify => {
                let mut conn = match self.acquire(caller).await {
                    Ok(c) => c,
                    Err(e) => return refused(e.to_string()),
                };
                match privacy::verify_classes(&mut conn).await {
                    Ok(v) => Reply::Ok {
                        result: serde_json::to_value(v).expect("verification serializes"),
                    },
                    Err(e) => refused(e.to_string()),
                }
            }
            _ => unreachable!("only privacy requests reach here"),
        }
    }
}

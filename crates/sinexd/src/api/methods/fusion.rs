//! Fusion decisions: `same`, `distinct` and `withdraw-decision` write an
//! authored decision naming both records by stable citation, so it survives
//! replay of either side. The fusion derivation reads decisions as inputs.
use super::*;
use sinex_derivations::derived_event::record_citation;
use sinex_derivations::fusion::{Family, families};
use sinex_model::EventPayload;
use sinex_model::Timestamp;
use sinex_model::event_types::duplicate::{Decision, Duplicate};

fn error(e: impl std::fmt::Display) -> String {
    e.to_string()
}

/// One record a decision is about, as read for the caller.
struct Side {
    event_type: String,
    source: String,
    payload: Value,
    citation: String,
    at: Option<Timestamp>,
}

async fn side(conn: &mut sqlx::PgConnection, reference: &str) -> Result<Side, String> {
    let parsed = query::parse_reference(reference).map_err(error)?;
    let shown = sinex_store::query::show(conn, &parsed)
        .await
        .map_err(error)?;
    if shown["state"] != "live" || !shown["payload"].is_object() {
        return Err(format!("{reference} is not a live, readable record"));
    }
    let text = |field: &str| shown[field].as_str().map(str::to_owned);
    let event_type = text("event_type").ok_or("record has no type")?;
    let location = match (
        text("material"),
        shown["offset"].as_u64(),
        shown["output"].as_u64(),
    ) {
        (Some(material), Some(anchor), Some(output)) => Some((material, anchor, output)),
        _ => None,
    };
    let source = text("source").ok_or("record has no source")?;
    let citation = record_citation(
        &event_type,
        &source,
        &shown["payload"],
        location,
        shown["key"].as_str(),
        &text("id").ok_or("record has no ID")?,
    );
    Ok(Side {
        source,
        payload: shown["payload"].clone(),
        at: serde_json::from_value(shown["when"].clone()).ok(),
        citation,
        event_type,
    })
}

/// The family whose members include both records, as different members.
fn family_of(a: &Side, b: &Side) -> Result<&'static Family, String> {
    let family = families()
        .iter()
        .find(|f| {
            f.member(&a.event_type, &a.source, &a.payload).is_some()
                && f.member(&b.event_type, &b.source, &b.payload).is_some()
        })
        .ok_or("no fusion family reads both records; records of other kinds stay separate")?;
    if family.member(&a.event_type, &a.source, &a.payload)
        == family.member(&b.event_type, &b.source, &b.payload)
    {
        return Err(format!(
            "both records come from one member of the {} family, which never holds two of them",
            family.name
        ));
    }
    Ok(family)
}

/// The placement a duplicate decision needs. Library items remain separate
/// identities regardless of their dates; fusion decisions must fit the
/// family's actual reading so its derivation can apply them.
struct PairContext {
    family: Option<&'static Family>,
    key: Option<Value>,
    at: Option<Timestamp>,
}

fn pair_context(a: &Side, b: &Side, decision: Option<Decision>) -> Result<PairContext, String> {
    if a.event_type == "library.item" && b.event_type == "library.item" {
        return Ok(PairContext {
            family: None,
            key: None,
            at: a.at.zip(b.at).map(|(a, b)| a.min(b)),
        });
    }
    let family = family_of(&a, &b)?;
    let scope = |s: &Side| -> Option<Vec<Value>> {
        family
            .scope
            .iter()
            .map(|f| family.field_of(&s.event_type, &s.payload, f))
            .collect()
    };
    if decision == Some(Decision::Same) && (scope(&a).is_none() || scope(&a) != scope(&b)) {
        return Err(format!(
            "the records differ in {} or lack it; records of different scopes are never one occurrence",
            family.scope.join(", ")
        ));
    }
    let (Some(at_a), Some(at_b)) = (a.at, b.at) else {
        return Err(
            "a record with an unknown time is never matched, so there is nothing to decide".into(),
        );
    };
    let key = if family.keyed_by.is_empty() {
        // Both records must fall in one fusion key's reading, or the
        // decision could never be applied.
        if (at_a.unix_ns() - at_b.unix_ns()).abs() > family.reach() {
            return Err(format!(
                "the records are {} s apart; the {} family reads pairs at most {} s apart",
                (at_a.unix_ns() - at_b.unix_ns()).abs() / 1_000_000_000,
                family.name,
                family.reach() / 1_000_000_000
            ));
        }
        None
    } else {
        let key = family.identity_key(&a.event_type, &a.payload);
        if key.is_none() || key != family.identity_key(&b.event_type, &b.payload) {
            return Err(format!(
                "the {} family decides only on copies of one identity ({})",
                family.name,
                family.keyed_by.join(", ")
            ));
        }
        key
    };
    Ok(PairContext {
        family: Some(family),
        key,
        at: Some(at_a.min(at_b)),
    })
}

/// The caller's current decisions on a pair, by the authored-record rules
/// over every decision recorded on it.
async fn current_of(
    conn: &mut sqlx::PgConnection,
    pair: &[String],
    author: &str,
) -> Result<Vec<Duplicate>, String> {
    let window = sinex_store::derivations::Window {
        range: None,
        unknown_times: true,
        kinds: vec![sinex_store::derivations::Kind {
            event_type: Duplicate::EVENT_TYPE.to_owned(),
            contains: json!({ "pair": pair }),
        }],
        start: None,
        end: None,
        before: Vec::new(),
        intervals: Vec::new(),
        after: Vec::new(),
    };
    let mut history = authored::History::new();
    let mut records = std::collections::BTreeMap::new();
    for committed in sinex_store::derivations::read_window(conn, &window)
        .await
        .map_err(error)?
    {
        let record: Duplicate = serde_json::from_value(committed.event.payload).map_err(error)?;
        let header = record.header().map_err(error)?;
        records.insert(header.revision(), record.clone());
        history.push(header, record.slots());
    }
    let current = history.current().map_err(error)?;
    Ok(current
        .into_iter()
        .filter_map(|revision| records.remove(&revision))
        .filter(|record| record.author == author && record.decision.is_some())
        .collect())
}

impl Api {
    pub(super) async fn decide_pair(
        &self,
        request: Request,
        caller: &Caller,
    ) -> Result<Value, String> {
        if !caller.grants.split(',').any(|g| g == "guarded") {
            return Err(
                "duplicate decisions cite guarded records and need the guarded grant".into(),
            );
        }
        let (decision, a, b) = match request {
            Request::Same { a, b } => (Some(Decision::Same), a, b),
            Request::Distinct { a, b } => (Some(Decision::Distinct), a, b),
            Request::WithdrawDecision { a, b } => (None, a, b),
            _ => return Err("not a duplicate decision".into()),
        };
        let mut conn = self.acquire(caller).await.map_err(error)?;
        let (a, b) = (side(&mut conn, &a).await?, side(&mut conn, &b).await?);
        if a.citation == b.citation {
            return Err("a decision names two different records".into());
        }
        let context = pair_context(&a, &b, decision)?;
        let mut pair = vec![a.citation, b.citation];
        pair.sort();
        let heads = current_of(&mut conn, &pair, caller.actor).await?;
        let now = Timestamp::now();
        let rev = uuid::Uuid::now_v7().to_string();
        let (root, revises, withdraws) = match decision {
            // A new decision revises the caller's current one on the pair:
            // every current head of that record, so a conflict resolves. A
            // revision revises one record, so the caller's other current
            // records on the pair (two roots written at once) are withdrawn
            // by the same revision, and one decision stands.
            Some(_) => {
                let root = heads
                    .first()
                    .map_or_else(|| rev.clone(), |h| h.root.clone());
                let revises = heads
                    .iter()
                    .filter(|h| h.root == root)
                    .map(|h| format!("{}@{}", h.root, h.rev))
                    .collect();
                let mut others: Vec<String> = heads
                    .iter()
                    .filter(|h| h.root != root)
                    .map(|h| h.root.clone())
                    .collect();
                others.sort();
                others.dedup();
                (root, revises, others)
            }
            // A withdrawal is a record of its own withdrawing the caller's.
            None if heads.is_empty() => {
                return Err("you have no current decision on this pair to withdraw".into());
            }
            None => {
                let mut roots: Vec<String> = heads.iter().map(|h| h.root.clone()).collect();
                roots.sort();
                roots.dedup();
                (rev.clone(), Vec::new(), roots)
            }
        };
        let record = Duplicate {
            root,
            rev,
            author: caller.actor.into(),
            via: "api".into(),
            at: i64::try_from(now.unix_ns() / 1_000_000).unwrap_or(i64::MAX),
            revises,
            withdraws,
            pair,
            decision,
            pair_at: context.at,
            family: context.family.map(|family| family.name.to_owned()),
            family_key: context.key,
        };
        let writer = self.writer.as_ref().ok_or("decisions require sinexd run")?;
        let acquired = sinex_sources::sources::duplicate_decision::request_material(
            &record,
            caller.actor,
            &record.via,
            &self.host,
            now,
        );
        let done = crate::intake::request(
            &mut conn,
            writer,
            &self.blobs,
            std::sync::Arc::new(sinex_sources::sources::duplicate_decision::request_source()),
            &format!("duplicate.request:{}@{}", caller.actor, self.host),
            &self.host,
            acquired,
        )
        .await?;
        if done.outcome.admitted != 1 || done.outcome.failures != 0 {
            return Err("the decision was not admitted as one event".into());
        }
        let material = done.material.ok_or("decision admission has no material")?;
        let id = sinex_store::live::at(
            &mut conn,
            sinex_model::RecordLocation {
                material,
                anchor: 0,
            },
            0,
        )
        .await
        .map_err(error)?
        .ok_or("the decision was archived before its response")?;
        Ok(json!({
            "event": id,
            "citation": format!("sinex:record/{material}/0"),
            "family": record.family,
            "pair": record.pair,
            "root": record.root,
            "rev": record.rev,
            "revises": record.revises,
            "withdraws": record.withdraws,
            "decision": record.decision,
            "author": record.author,
        }))
    }
}

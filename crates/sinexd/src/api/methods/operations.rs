//! Operations that change what is stored: excision, replay, recomputing a
//! derivation, and the crawl.
use super::*;

impl Api {
    /// Excise the record a citation names, with its bytes.
    pub(super) async fn excise(
        &self,
        caller: &Caller,
        reference: Option<&str>,
        material: Option<&str>,
        dry_run: bool,
        actor: &str,
    ) -> Reply {
        let Some(writer) = &self.writer else {
            return refused("this daemon has no writer; writes go through `sinexd run`");
        };
        let mut conn = match self.acquire(caller).await {
            Ok(c) => c,
            Err(e) => return refused(e.to_string()),
        };
        let target = match (reference, material) {
            (Some(reference), None) => {
                let r = match query::parse_reference(reference) {
                    Ok(r) => r,
                    Err(e) => return query_refusal(reference, e),
                };
                match sinex_store::query::resolve(&mut conn, &r).await {
                    Ok(id) => sinex_store::excision::Target::Event(EventId(id)),
                    Err(e) => return refused(e.to_string()),
                }
            }
            (None, Some(m)) => match uuid::Uuid::parse_str(m) {
                Ok(m) => sinex_store::excision::Target::Material(sinex_model::MaterialId(m)),
                Err(e) => return refused(format!("material {m}: {e}")),
            },
            _ => return refused("excise needs exactly one of an event reference or --material"),
        };
        if dry_run {
            return match sinex_store::excision::plan(&mut conn, target).await {
                Ok(plan) => Reply::Ok {
                    result: json!({ "dry_run": true, "would_excise": crate::excision::describe(&plan) }),
                },
                Err(e) => refused(e.to_string()),
            };
        }
        match crate::excision::excise_with(&mut conn, &self.blobs, writer, target, actor).await {
            Ok(summary) => Reply::Ok {
                result: json!({ "excised": summary }),
            },
            Err(e) => refused(e),
        }
    }

    pub(super) async fn replay(
        &self,
        caller: &Caller,
        o: crate::replay::Options,
        all: bool,
    ) -> Reply {
        let Some(writer) = &self.writer else {
            return refused("this daemon has no writer; writes go through `sinexd run`");
        };
        let mut conn = match self.acquire(caller).await {
            Ok(c) => c,
            Err(e) => return refused(e.to_string()),
        };
        let compare = o.compare;
        match crate::replay::run(&mut conn, &self.blobs, writer, o).await {
            Ok(report) => Reply::Ok {
                result: report.to_json(compare, all),
            },
            Err(e) => refused(e),
        }
    }

    pub(super) async fn recompute(
        &self,
        caller: &Caller,
        derivation: &str,
        compare: bool,
        range: Option<(sinex_model::Timestamp, sinex_model::Timestamp)>,
        examples: usize,
    ) -> Reply {
        let Some(writer) = &self.writer else {
            return refused("this daemon has no writer; writes go through `sinexd run`");
        };
        let conn = match self.acquire(caller).await {
            Ok(c) => c.detach(),
            Err(e) => return refused(e.to_string()),
        };
        let mut driver = match crate::derivation_runner::DerivationRunner::for_one(
            conn,
            writer.clone(),
            derivation,
            &self.host,
        )
        .await
        {
            Ok(d) => d,
            Err(e) => return refused(e),
        };
        let result = if compare {
            driver
                .compare(derivation, range, examples)
                .await
                .map(|cmp| json!({ "comparison": cmp.id, "summary": cmp.to_string() }))
        } else {
            driver
                .recompute_all(derivation, range)
                .await
                .map(|n| json!({ "derivation": derivation, "keys_marked": n }))
        };
        let ended = driver.end_runs().await;
        match (result, ended) {
            (Ok(result), Ok(())) => Reply::Ok { result },
            (Err(e), _) | (_, Err(e)) => refused(e),
        }
    }

    pub(super) async fn crawl(
        &self,
        caller: &Caller,
        allow: Vec<String>,
        urls: Vec<String>,
        limit: Option<u64>,
        pause: u64,
    ) -> Reply {
        let Some(writer) = &self.writer else {
            return refused("this daemon has no writer; writes go through `sinexd run`");
        };
        let mut conn = match self.acquire(caller).await {
            Ok(c) => c,
            Err(e) => return refused(e.to_string()),
        };
        match crate::crawl::run(
            &mut conn,
            &self.blobs,
            writer,
            &self.host,
            allow,
            urls,
            limit.map(|l| usize::try_from(l).unwrap_or(usize::MAX)),
            std::time::Duration::from_secs(pause),
        )
        .await
        {
            Ok(totals) => Reply::Ok {
                result: json!({ "crawl": totals.to_string() }),
            },
            Err(e) => refused(e),
        }
    }
}

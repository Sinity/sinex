//! Status reads source states and the recent progress of every consumer.
use super::*;
use command_surface::Verdict;
use sinex_model::Timestamp;
use sinex_model::event_types::telemetry::SinexTelemetry;
use sinex_store::telemetry::Sample;
use std::collections::BTreeMap;
use std::time::Duration;

// Fifteen minutes smooths a consumer's rate over several regular samples.
pub(crate) const HISTORY_WINDOW: Duration = Duration::from_secs(15 * 60);
// Five minutes of unchanged samples distinguishes a stall from one slow pass.
const STALL_WINDOW: Duration = Duration::from_secs(5 * 60);

pub(crate) fn status_fields(
    samples: &[Sample],
    sources: &[Value],
    context: &Context,
) -> Result<Value> {
    let mut groups: BTreeMap<(&str, &str), Vec<&Sample>> = BTreeMap::new();
    for sample in samples {
        if matches!(
            sample.component.as_str(),
            "publisher" | "search index" | "derivation"
        ) {
            groups
                .entry((&sample.component, &sample.name))
                .or_default()
                .push(sample);
        }
    }
    let mut consumers = Vec::new();
    let mut reasons = Vec::new();
    let mut measured_progress = false;
    for ((component, name), samples) in groups {
        let name = if name.is_empty() {
            component.to_owned()
        } else {
            format!("{component} {name}")
        };
        let mut samples: Vec<_> = samples
            .into_iter()
            .filter(|sample| {
                sample.measures.contains_key("position") && sample.measures.contains_key("behind")
            })
            .collect();
        if samples.is_empty() {
            consumers.push(
                json!({ "name": name, "position": null, "behind": null, "rate": null,
                "estimate_seconds": null, "last_advanced": null, "state": "unknown" }),
            );
            continue;
        }
        measured_progress = true;
        samples.sort_by_key(|sample| sample.sampled_at);
        let first = samples[0];
        let last = samples[samples.len() - 1];
        let position = last.measures["position"] as i64;
        let behind = last.measures["behind"] as i64;
        let elapsed = (last.sampled_at.unix_ns() - first.sampled_at.unix_ns()) as f64 / 1e9;
        let rate = (elapsed > 0.0)
            .then(|| (last.measures["position"] - first.measures["position"]) / elapsed);
        let gain = first.measures["behind"] - last.measures["behind"];
        let estimate_seconds = if behind == 0 {
            Some(0.0)
        } else if elapsed > 0.0 && gain > 0.0 {
            Some(behind as f64 * elapsed / gain)
        } else {
            None
        };
        let last_advanced = samples
            .windows(2)
            .rev()
            .find(|pair| pair[1].measures["position"] > pair[0].measures["position"])
            .map(|pair| formatted_time(pair[1].sampled_at, &context.zone))
            .transpose()?;
        let cutoff = context.now.as_nanosecond() - STALL_WINDOW.as_nanos() as i128;
        let stalled_since = samples
            .iter()
            .rposition(|sample| sample.sampled_at.unix_ns() <= cutoff)
            .filter(|index| {
                samples[*index..].len() >= 3
                    && samples[*index..].iter().all(|sample| {
                        sample.measures["position"] == last.measures["position"]
                            && sample.measures["behind"] > 0.0
                    })
            })
            .map(|index| samples[index].sampled_at);
        let state = if samples.len() < 2 {
            "unknown"
        } else if behind == 0 {
            "current"
        } else if stalled_since.is_some() {
            "stalled"
        } else if gain > 0.0 {
            "catching_up"
        } else if position > first.measures["position"] as i64 {
            "falling_behind"
        } else {
            "unknown"
        };
        if let Some(since) = stalled_since.filter(|_| state == "stalled") {
            reasons.push(format!(
                "{name}: {} behind, not advanced since {}",
                grouped_number(behind),
                formatted_time(since, &context.zone)?
            ));
        } else if state == "falling_behind" {
            reasons.push(format!(
                "{name}: {} behind, falling behind while advancing",
                grouped_number(behind)
            ));
        }
        consumers.push(
            json!({ "name": name, "position": position, "behind": behind, "rate": rate,
            "estimate_seconds": estimate_seconds, "last_advanced": last_advanced, "state": state }),
        );
    }
    let mut source_states: BTreeMap<&str, usize> = BTreeMap::new();
    for source in sources {
        if let Some(state) = source["state"].as_str() {
            *source_states.entry(state).or_default() += 1;
            if state == "stalled" {
                reasons.push(format!(
                    "{}: {}",
                    source["binding"].as_str().unwrap_or("source"),
                    source["reason"].as_str().unwrap_or("stalled")
                ));
            }
        }
    }
    let verdict = if let Some(reason) = reasons.first() {
        Verdict::Degraded {
            reason: reason.clone(),
        }
    } else if !measured_progress {
        Verdict::Unknown {
            reason: "no consumer position samples in the last 15 minutes".to_owned(),
        }
    } else {
        Verdict::Healthy
    };
    Ok(
        json!({ "verdict": verdict, "reasons": reasons, "consumers": consumers,
        "sources": sources, "source_states": source_states }),
    )
}

fn formatted_time(timestamp: Timestamp, zone: &jiff::tz::TimeZone) -> Result<String> {
    Ok(jiff::Timestamp::from_nanosecond(timestamp.unix_ns())?
        .to_zoned(zone.clone())
        .strftime("%Y-%m-%d %H:%M:%S%:z")
        .to_string())
}

fn grouped_number(number: i64) -> String {
    let text = number.to_string();
    let digits = text.trim_start_matches('-');
    let mut out = if number < 0 {
        "-".to_owned()
    } else {
        String::new()
    };
    for (index, digit) in digits.chars().enumerate() {
        if index > 0 && (digits.len() - index).is_multiple_of(3) {
            out.push(',');
        }
        out.push(digit);
    }
    out
}

impl Api {
    pub(super) async fn status(&self, caller: &Caller) -> Result<Reply> {
        let context = self.context();
        let mut conn = self.acquire(caller).await?;
        let row = sinex_store::diagnostics::positions(&mut conn).await?;
        let open_backup_holds = sinex_store::backup::open_summary(&mut conn).await?;
        let telemetry =
            sinex_store::telemetry::recent(&mut conn, SinexTelemetry::EVENT_TYPE, HISTORY_WINDOW)
                .await?;
        let refusals = sinex_store::diagnostics::ingress_refusals(&mut conn).await?;
        let producer_errors = sinex_store::diagnostics::producer_errors(&mut conn).await?;
        let selected = sinex_store::registry::selected_derivations(&mut conn).await?;
        let time_settings =
            sinex_model::time_settings::TimeSettings::from_parts(self.zone.clone(), self.day_start);
        let mut available: Vec<(String, u32, sinex_model::DerivationHistory)> = self
            .derivations
            .keyed
            .iter()
            .map(|d| {
                (
                    d.name().to_owned(),
                    d.version_for(&time_settings),
                    d.history_policy(),
                )
            })
            .collect();
        available.extend(
            self.derivations
                .per_event
                .iter()
                .map(|d| (d.name().to_owned(), d.version(), d.history_policy())),
        );
        let build = crate::build_identity::current()?;
        let names: Vec<_> = available.iter().map(|(name, _, _)| name.clone()).collect();
        let versions: Vec<_> = available
            .iter()
            .map(|(_, version, _)| i64::from(*version))
            .collect();
        let comparisons =
            sinex_store::registry::latest_comparisons(&mut conn, &names, &versions, build).await?;
        let producer_versions: Vec<_> = available.into_iter().map(|(name, version, history)| {
            let current = selected.iter().find(|(selected_name, _, _)| selected_name == &name);
            let compared = comparisons.iter().find(|(producer,..)| producer == &name);
            json!({"name": name, "selected": current.map(|(_,version,_)| version), "selected_build": current.map(|(_,_,build)|build), "available": version,
                "state": if current.is_some_and(|(_,selected_version,selected_build)| *selected_version == i64::from(version) && selected_build == build) { "selected" } else { "pending_history_switch" },
                "history": history, "build": build, "last_comparison": compared.map(|(_,id,made_at,keys,changed,complete)| json!({"id":id,"made_at":made_at,"keys":keys,"changed":changed,"complete":complete}))})
        }).collect();
        let rule_rows = sinex_store::signals::rule_rows(
            &mut conn,
            sinex_model::event_types::signal::RuleWritten::EVENT_TYPE,
            row.commit_seq,
        )
        .await?;
        // Ambiguity per fusion family: pairs awaiting a decision, decided
        // pairs, and fused records chosen among several candidates.
        let fusion_families: Vec<(String, String)> = sinex_derivations::fusion::families()
            .iter()
            .map(|f| (f.name.to_owned(), f.output.to_owned()))
            .collect();
        let fusion = sinex_store::diagnostics::fusion_ambiguity(
            &mut conn,
            &fusion_families,
            sinex_model::event_types::fused::ConsideredPair::EVENT_TYPE,
        )
        .await?;
        let signals = json!({"configured":self.source_config.signals.is_some(),"rules":rule_rows.into_iter().map(|(id,payload)|json!({"citation":format!("sinex:ev/{id}"),"rule":payload})).collect::<Vec<_>>()});
        let sources = sinex_store::query::source_rows(&mut conn, &context).await?;
        drop(conn);
        let mut result = status_fields(&telemetry, &sources, &context)?;
        let context = self.source_config.context(&self.host);
        let discovery =
            tokio::task::spawn_blocking(move || sinex_sources::discovery::status(&context))
                .await?
                .map_err(Error::internal)?;
        let read_failures: Vec<_> = self
            .read_failures
            .lock()
            .unwrap()
            .iter()
            .map(|(binding, failure)| {
                json!({"binding": binding, "error": failure.error, "since": failure.since})
            })
            .collect();
        let exclusions: std::collections::BTreeMap<_, _> = self
            .source_config
            .exclude
            .iter()
            .filter_map(|(name, exclusion)| exclusion.as_ref().map(|exclusion| (name, exclusion)))
            .collect();
        let details = json!({
           "commit_seq": row.commit_seq,
           "schema_version": row.schema_version,
           "failures_24h": row.failures_24h,
           // Inputs that cannot be read admit no records, so no record failure
           // counts them; each binding failing now is listed here instead.
           "read_failures": read_failures,
           "ingress_refusals_24h": refusals,
           "producer_errors": producer_errors,
           "writer": self.writer.is_some(),
           "backup_holds": { "open":open_backup_holds.0,"oldest":open_backup_holds.1 },
           "zone": self.context().zone_name(),
           "day_start": self.day_start.to_string(),
           "producer_versions": producer_versions,
           "signals":signals,
           "fusion":fusion,
           "discovery": discovery,
           "exclusions": exclusions,
        });
        result
            .as_object_mut()
            .expect("status object")
            .extend(details.as_object().expect("status details").clone());
        Ok(Reply::Ok { result })
    }
}

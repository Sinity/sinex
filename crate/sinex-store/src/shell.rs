//! The singleton branch of shell-command v1. Only Atuin is enabled: same-source
//! observations can never match, so each eligible observation owns one group.
use crate::{COUNTER, Result, WriterPool};
use serde_json::json;
use sqlx::{Connection, Row};
use uuid::Uuid;

pub(crate) fn payload_schema() -> serde_json::Value {
    let mut schema = crate::atuin::payload_schema();
    let properties = schema["properties"].as_object_mut().unwrap();
    properties.insert("canonical".into(), json!({"type":["string","null"]}));
    properties.insert(
        "observed_by".into(),
        json!({"type":"array","items":{"const":crate::ATUIN_SOURCE},"minItems":1,"maxItems":1}),
    );
    properties.insert(
        "association".into(),
        json!({"type":"object","required":["basis","rule","version","ambiguous","candidates"]}),
    );
    properties.insert(
        "field_evidence".into(),
        json!({"type":"object","required":["all"]}),
    );
    for field in ["canonical", "observed_by", "association", "field_evidence"] {
        schema["required"]
            .as_array_mut()
            .unwrap()
            .push(json!(field));
    }
    schema
}
pub(crate) async fn dirty(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    parent: Uuid,
) -> Result<()> {
    sqlx::query("INSERT INTO derivation.shell_dirty(parent_id) VALUES($1) ON CONFLICT DO NOTHING")
        .bind(parent)
        .execute(&mut **tx)
        .await?;
    Ok(())
}
fn canonical(command: &str) -> Option<String> {
    let words = shlex::split(command)?;
    let mut words = words.iter().skip_while(|w| {
        w.split_once('=').is_some_and(|(k, _)| {
            !k.is_empty() && k.chars().all(|c| c.is_ascii_alphanumeric() || c == '_')
        })
    });
    let program = words.next()?.rsplit('/').next()?.to_string();
    // A summary must not strip credential recognition context (for example a
    // URL authority passed as the program). Keep the summary unknown instead.
    if program.is_empty()
        || !program
            .chars()
            .all(|c| c.is_alphanumeric() || "_+.-".contains(c))
    {
        return None;
    }
    const ACTIONS: &[&str] = &[
        "git",
        "cargo",
        "nix",
        "systemctl",
        "xtask",
        "sinexctl",
        "agentctl",
        "bd",
        "docker",
        "npm",
        "kubectl",
    ];
    const MUTATIONS: &[(&str, &str)] = &[
        ("sed", "-i"),
        ("perl", "-i"),
        ("find", "-delete"),
        ("find", "-exec"),
        ("rsync", "--delete"),
    ];
    let args: Vec<_> = words.collect();
    let mut out = program.clone();
    if ACTIONS.contains(&program.as_str()) {
        if let Some(action) = args.first() {
            out.push(' ');
            out.push_str(action);
        }
    }
    for (p, flag) in MUTATIONS {
        if *p == program
            && args
                .iter()
                .any(|a| a.as_str() == *flag || (*flag == "-i" && a.starts_with("-i")))
        {
            out.push(' ');
            out.push_str(flag);
        }
    }
    Some(out)
}
impl WriterPool {
    pub(crate) async fn shell_enable(&mut self) -> Result<()> {
        if self.shell_producer.is_some() {
            return Ok(());
        }
        let producer = Uuid::now_v7();
        let mut tx = self.connection.begin().await?;
        sqlx::query("INSERT INTO core.producer_versions(id,name,version,run_id,configuration) VALUES($1,'fusion.shell-command','1-single-source',$2,$3)").bind(producer).bind(self.run_id).bind(json!({"rule":"shell-command","version":1,"enabled_sources":[crate::ATUIN_SOURCE],"unmatched":"single","same_source_match":false})).execute(&mut *tx).await?;
        // Admission always records durable dirty work, including while the
        // optional interpretation is disabled. Restart needs no whole-store
        // scan or a second checkpoint beside that authoritative work.
        tx.commit().await?;
        self.shell_producer = Some(producer);
        Ok(())
    }
    pub(crate) async fn shell_step(&mut self) -> Result<bool> {
        let Some(producer) = self.shell_producer else {
            return Ok(false);
        };
        let mut tx = self.connection.begin().await?;
        let parents: Vec<Uuid> = sqlx::query_scalar(
            "SELECT parent_id FROM derivation.shell_dirty ORDER BY parent_id LIMIT 64",
        )
        .fetch_all(&mut *tx)
        .await?;
        if parents.is_empty() {
            return Ok(false);
        }
        for parent in &parents {
            let old:Vec<Uuid>=sqlx::query_scalar("SELECT id FROM core.live_events WHERE event_type='command.canonical' AND source_event_ids=ARRAY[$1]::uuid[]").bind(parent).fetch_all(&mut *tx).await?;
            let input=sqlx::query("SELECT payload,equivalence_key FROM core.live_events WHERE id=$1 AND event_type='command.executed'").bind(parent).fetch_optional(&mut *tx).await?;
            // Existing exact support remains valid on enable/restart. Replay changes its
            // parent ID and dirties the former support, so no old child survives it.
            if input.is_some() && old.len() == 1 {
                sqlx::query("DELETE FROM derivation.shell_dirty WHERE parent_id=$1")
                    .bind(parent)
                    .execute(&mut *tx)
                    .await?;
                continue;
            }
            for id in old {
                let seq: i64 = sqlx::query_scalar(COUNTER).fetch_one(&mut *tx).await?;
                sqlx::query(
                    "INSERT INTO audit.archived(id,archived_seq,reason) VALUES($1,$2,'supersede')",
                )
                .bind(id)
                .bind(seq)
                .execute(&mut *tx)
                .await?;
                sqlx::query("INSERT INTO core.commit_log(commit_seq,event_id,change,store,event_type_id) VALUES($1,$2,1,0,3)").bind(seq).bind(id).execute(&mut *tx).await?;
            }
            if let Some(input) = input {
                let mut payload: serde_json::Value = input.try_get("payload")?;
                let key: String = input.try_get("equivalence_key")?;
                payload["canonical"] = json!(canonical(payload["command"].as_str().unwrap_or("")));
                payload["observed_by"] = json!([crate::ATUIN_SOURCE]);
                payload["association"] = json!({"basis":"rule","rule":"shell-command","version":1,"ambiguous":false,"candidates":0});
                payload["field_evidence"] = json!({"all":parent});
                let id = Uuid::now_v7();
                let seq: i64 = sqlx::query_scalar(COUNTER).fetch_one(&mut *tx).await?;
                let hash = blake3::hash(serde_json::to_string(&payload).unwrap().as_bytes())
                    .as_bytes()
                    .to_vec();
                sqlx::query("INSERT INTO core.events(id,source,event_type,host,payload,ts_orig,ts_orig_tiebreak,ts_quality,time_suspect,ts_precision,ts_orig_until,ts_clock,commit_seq,record_hash,source_event_ids,producer_version_id,run_id,equivalence_key,payload_schema_id) SELECT $1,'fusion.shell-command','command.canonical',host,$2,ts_orig,ts_orig_tiebreak,ts_quality,time_suspect,ts_precision,ts_orig_until,ts_clock,$3,$4,ARRAY[id],$5,$6,$7,3 FROM core.live_events WHERE id=$8").bind(id).bind(payload).bind(seq).bind(hash).bind(producer).bind(self.run_id).bind(json!(["shell-command",[key]]).to_string()).bind(parent).execute(&mut *tx).await?;
                sqlx::query("INSERT INTO core.commit_log(commit_seq,event_id,change,store,event_type_id) VALUES($1,$2,0,0,3)").bind(seq).bind(id).execute(&mut *tx).await?;
            }
            sqlx::query("DELETE FROM derivation.shell_dirty WHERE parent_id=$1")
                .bind(parent)
                .execute(&mut *tx)
                .await?;
        }
        tx.commit().await?;
        Ok(true)
    }
}

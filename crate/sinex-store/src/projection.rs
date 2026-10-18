//! One restricted read-model owner. Rows and log position commit together; the
//! complete retained log is the only supported bootstrap basis in this slice.
use crate::{Error, QualificationBoundary, Result};
use serde_json::Value;
use sinex_model::retrieval::{CONTRACT, Project, normalize, project, token_text};
use sqlx::{Connection, PgConnection, Row};
use uuid::Uuid;

pub struct Projection {
    stop: tokio::sync::watch::Sender<bool>,
    task: tokio::task::JoinHandle<Result<()>>,
}
impl Projection {
    pub async fn start(
        url: &str,
        run_id: Uuid,
        projects: Vec<Project>,
        qualification: Option<QualificationBoundary>,
    ) -> Result<Self> {
        Project::validate_all(&projects).map_err(Error::Invalid)?;
        let mut connection = PgConnection::connect(url).await?;
        let role: String = sqlx::query_scalar("SELECT current_user::text")
            .fetch_one(&mut connection)
            .await?;
        if role != crate::schema::READ_MODEL_ROLE {
            return Err(Error::Invalid(
                "projection URL must authenticate as sinex_read_model",
            ));
        }
        let paired: bool = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM core.runs WHERE id=$1 AND status='running')",
        )
        .bind(run_id)
        .fetch_one(&mut connection)
        .await?;
        if !paired {
            return Err(Error::Invalid(
                "projection connection belongs to a different store",
            ));
        }
        let owned: bool = sqlx::query_scalar("SELECT pg_try_advisory_lock(73018924014)")
            .fetch_one(&mut connection)
            .await?;
        if !owned {
            return Err(Error::Invalid("read model already owned"));
        }
        let configuration = configuration(&projects);
        let mut tx = connection.begin().await?;
        let existing: Option<String> = sqlx::query_scalar(
            "SELECT configuration FROM derivation.read_model_position WHERE name='shell-search'",
        )
        .fetch_optional(&mut *tx)
        .await?;
        if existing.as_deref() != Some(&configuration) {
            sqlx::query("DELETE FROM core.search_text")
                .execute(&mut *tx)
                .await?;
            sqlx::query("DELETE FROM core.event_project")
                .execute(&mut *tx)
                .await?;
            sqlx::query("INSERT INTO derivation.read_model_position(name,generation,configuration,position,serving,failure) VALUES('shell-search',$1,$2,0,false,NULL) ON CONFLICT(name) DO UPDATE SET generation=EXCLUDED.generation,configuration=EXCLUDED.configuration,position=0,serving=false,failure=NULL").bind(Uuid::now_v7()).bind(&configuration).execute(&mut *tx).await?;
        }
        tx.commit().await?;
        let (stop, mut stopped) = tokio::sync::watch::channel(false);
        let task = tokio::spawn(async move {
            let mut owner = Owner {
                connection,
                projects,
                qualification,
                step: 0,
            };
            loop {
                if *stopped.borrow() {
                    break;
                }
                match owner.step().await {
                    Ok(true) => continue,
                    Ok(false) => {}
                    Err(error) => {
                        eprintln!(
                            "{}",
                            serde_json::json!({"component":"sinex-read-model","phase":"unavailable","reason":error.to_string()})
                        );
                        sqlx::query("UPDATE derivation.read_model_position SET serving=false,failure='projection maintenance unavailable' WHERE name='shell-search'").execute(&mut owner.connection).await?;
                    }
                }
                tokio::select! {_=stopped.changed()=>{},_=tokio::time::sleep(std::time::Duration::from_millis(100))=>{}}
            }
            owner.connection.close().await?;
            Ok(())
        });
        Ok(Self { stop, task })
    }
    pub async fn shutdown(self) -> Result<()> {
        let _ = self.stop.send(true);
        self.task.await?
    }
}
fn caseless_version() -> String {
    sinex_model::retrieval::normalization_basis()
}
struct Owner {
    connection: PgConnection,
    projects: Vec<Project>,
    qualification: Option<QualificationBoundary>,
    step: usize,
}
impl Owner {
    async fn step(&mut self) -> Result<bool> {
        let mut tx = self.connection.begin().await?;
        sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
            .execute(&mut *tx)
            .await?;
        let state=sqlx::query("SELECT generation,position,serving FROM derivation.read_model_position WHERE name='shell-search'").fetch_one(&mut *tx).await?;
        let generation: Uuid = state.try_get("generation")?;
        let position: i64 = state.try_get("position")?;
        let current: i64 = sqlx::query_scalar("SELECT seq FROM core.commit_counter WHERE one")
            .fetch_one(&mut *tx)
            .await?;
        if position > current {
            return Err(Error::ProjectionUnavailable(
                "projection frontier exceeds committed state",
            ));
        }
        if position == current && state.try_get::<bool, _>("serving")? {
            tx.commit().await?;
            return Ok(false);
        }
        let logs=sqlx::query("SELECT commit_seq,event_id,change FROM core.commit_log WHERE commit_seq>$1 ORDER BY commit_seq LIMIT 64").bind(position).fetch_all(&mut *tx).await?;
        let more = logs.len() == 64;
        let mut next = position;
        for log in logs {
            next = log.try_get("commit_seq")?;
            let id: Uuid = log.try_get("event_id")?;
            sqlx::query("DELETE FROM core.search_text WHERE generation=$1 AND id=$2")
                .bind(generation)
                .bind(id)
                .execute(&mut *tx)
                .await?;
            sqlx::query("DELETE FROM core.event_project WHERE generation=$1 AND event_id=$2")
                .bind(generation)
                .bind(id)
                .execute(&mut *tx)
                .await?;
            if log.try_get::<i16, _>("change")? != 0 {
                continue;
            }
            let row=sqlx::query("SELECT payload,source,event_type,ts_orig::text AS at,ts_orig_tiebreak FROM core.live_events WHERE id=$1 AND event_type='command.canonical'").bind(id).fetch_optional(&mut *tx).await?;
            let Some(row) = row else { continue };
            let payload: serde_json::Value = row.try_get("payload")?;
            let strings = payload_strings(&payload);
            let tokens = token_text(&strings);
            let at: Option<String> = row.try_get("at")?;
            sqlx::query("INSERT INTO core.search_text(generation,id,ts_orig,ts_orig_tiebreak,source,event_type,tokens,text,cwd,exit_code,command,command_normalized,cwd_normalized) VALUES($1,$2,$3::text::timestamptz,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)").bind(generation).bind(id).bind(&at).bind(row.try_get::<Option<i32>,_>("ts_orig_tiebreak")?).bind(row.try_get::<String,_>("source")?).bind(row.try_get::<String,_>("event_type")?).bind(tokens).bind(serde_json::to_string(&strings).unwrap()).bind(payload["cwd"].as_str()).bind(payload["exit_code"].as_i64()).bind(payload["command"].as_str().unwrap_or("")).bind(normalize(payload["command"].as_str().unwrap_or(""))).bind(payload["cwd"].as_str().map(normalize)).execute(&mut *tx).await?;
            if let Some(project) = payload["cwd"]
                .as_str()
                .and_then(|cwd| project(cwd, &self.projects))
            {
                sqlx::query("INSERT INTO core.event_project(generation,event_id,ts_orig,project) VALUES($1,$2,$3::text::timestamptz,$4)").bind(generation).bind(id).bind(&at).bind(project).execute(&mut *tx).await?;
            }
        }
        if !more {
            next = current;
        }
        // Index the writer's whole live canonical population until its own archive
        // log arrives. Parent eligibility is the reader's current-state fence;
        // bounded replacement may leave an old canonical body live temporarily.
        // Dropping it here would turn a complete log into a false reconstruction
        // failure while older projection rows await that later archive transition.
        // Publication still verifies the complete canonical population.
        // A missing old log row therefore cannot certify a complete empty projection.
        let exact = if !more {
            sqlx::query_scalar::<_,bool>("SELECT NOT EXISTS((SELECT id FROM core.live_events e WHERE event_type='command.canonical' EXCEPT SELECT id FROM core.search_text WHERE generation=$1) UNION ALL (SELECT id FROM core.search_text WHERE generation=$1 EXCEPT SELECT id FROM core.live_events e WHERE event_type='command.canonical'))").bind(generation).fetch_one(&mut *tx).await?
        } else {
            false
        };
        if !more && !exact {
            return Err(Error::ProjectionUnavailable(
                "retained log cannot reconstruct current canonical population; bootstrap required",
            ));
        }
        sqlx::query("UPDATE derivation.read_model_position SET position=$1,serving=$2,failure=NULL WHERE name='shell-search'").bind(next).bind(exact).execute(&mut *tx).await?;
        QualificationBoundary::reach(&mut self.qualification,&format!("projection-staged-{}",self.step),serde_json::json!({"generation":generation,"from":position,"through":next,"serving":exact})).await?;
        tx.commit().await?;
        QualificationBoundary::reach(
            &mut self.qualification,
            &format!("projection-committed-{}", self.step),
            serde_json::json!({"generation":generation,"through":next,"serving":exact}),
        )
        .await?;
        self.step += 1;
        Ok(more || next != position)
    }
}

// The reader binds own-cwd attribution to the exact same configured projection.
pub(crate) fn configuration(projects: &[Project]) -> String {
    blake3::hash(
        serde_json::to_string(&(CONTRACT, projects, caseless_version()))
            .unwrap()
            .as_bytes(),
    )
    .to_hex()
    .to_string()
}

// None is a typed unavailable representation, distinct from a real empty vector.
// Metadata and original presentation strings remain complete in either case.
fn payload_strings(payload: &Value) -> Vec<String> {
    fn texts(value: &Value, out: &mut Vec<String>) {
        match value {
            Value::String(s) => out.push(s.clone()),
            Value::Array(a) => {
                for v in a {
                    texts(v, out);
                }
            }
            Value::Object(o) => {
                let mut keys: Vec<_> = o.keys().collect();
                keys.sort();
                for key in keys {
                    texts(&o[key], out);
                }
            }
            _ => {}
        }
    }
    let mut strings = Vec::new();
    if let Some(command) = payload["command"].as_str() {
        strings.push(command.to_owned());
    }
    let mut remainder = payload.clone();
    remainder
        .as_object_mut()
        .expect("canonical payload object")
        .remove("command");
    texts(&remainder, &mut strings);
    strings
}
#[cfg(test)]
#[path = "projection_text_tests.rs"]
mod text_tests;

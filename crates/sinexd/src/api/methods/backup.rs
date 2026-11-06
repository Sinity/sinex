//! Backup requests use the existing writer; a snapshot and a hold are distinct lifetimes.
use super::{Api, Caller, Reply, json, refused};
use sinex_model::OperationId;

impl Api {
    pub(super) async fn backup(
        &self,
        caller: &Caller,
        action: &str,
        hold: Option<String>,
        output: Option<String>,
        page: Option<u64>,
        cursor: Option<String>,
    ) -> Reply {
        let id = |text: &str| {
            text.parse::<uuid::Uuid>()
                .map(OperationId)
                .map_err(|e| e.to_string())
        };
        if action == "list" {
            if hold.is_some() || output.is_some() {
                return refused("backup list takes only --page and --cursor");
            }
            let after = match cursor.as_deref().map(id).transpose() {
                Ok(value) => value,
                Err(e) => return refused(e),
            };
            let size = i64::try_from(page.unwrap_or(100)).unwrap_or(i64::MAX);
            if size == 0 {
                return refused("backup list page must be positive");
            }
            let mut conn = match self.acquire(caller).await {
                Ok(conn) => conn,
                Err(e) => return refused(e.to_string()),
            };
            return match sinex_store::backup::list(&mut conn, after, size).await {
                Ok(holds) => {
                    let next = if holds.len() as u64 == size as u64 {
                        holds.last().map(|h| h.id.to_string())
                    } else {
                        None
                    };
                    Reply::Ok {
                        result: json!({"holds":holds,"page":{"cursor":next}}),
                    }
                }
                Err(e) => refused(e.to_string()),
            };
        }
        if page.is_some() || cursor.is_some() {
            return refused("backup --page and --cursor belong to list");
        }
        let Some(writer) = self.writer.as_ref() else {
            return refused("the writer is not running");
        };
        match action {
            "hold" if hold.is_none() && output.is_none() => {
                match writer.backup_hold(caller.actor).await {
                    Ok(hold) => Reply::Ok {
                        result: json!({"hold":hold}),
                    },
                    Err(e) => refused(e.to_string()),
                }
            }
            "release" if output.is_none() => {
                let hold = match hold
                    .as_deref()
                    .ok_or_else(|| "backup release needs HOLD".to_string())
                    .and_then(id)
                {
                    Ok(hold) => hold,
                    Err(e) => return refused(e.to_string()),
                };
                match writer.backup_release(hold, caller.actor).await {
                    Ok(hold) => Reply::Ok {
                        result: json!({"hold":hold}),
                    },
                    Err(e) => refused(e.to_string()),
                }
            }
            "prepare" if hold.is_none() => {
                let Some(output) = output else {
                    return refused("backup prepare needs --output PATH");
                };
                let output = std::path::PathBuf::from(output);
                let revision = match crate::build_identity::current() {
                    Ok(revision) => revision,
                    Err(e) => return refused(e),
                };
                if let Err(e) = crate::backup::create(&output) {
                    return refused(e);
                }
                let snapshot = match writer.backup_snapshot(caller.actor).await {
                    Ok(snapshot) => snapshot,
                    Err(e) => return refused(e.to_string()),
                };
                let hold = snapshot.hold.id;
                match crate::backup::prepare(
                    snapshot,
                    self.blobs.root(),
                    &output,
                    self.stop.clone(),
                    revision.into(),
                )
                .await
                {
                    Ok(artifact) => Reply::Ok {
                        result: json!({"artifact":artifact,"output":output,"hold_open":true}),
                    },
                    Err(e) => refused(format!(
                        "{e}; backup hold {hold} remains open; incomplete output {}",
                        output.display()
                    )),
                }
            }
            _ => refused("backup takes hold, release HOLD, prepare --output PATH, or list"),
        }
    }
}

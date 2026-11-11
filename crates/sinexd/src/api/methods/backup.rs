//! Backup requests use the existing writer; a snapshot and a hold are distinct lifetimes.
use super::{Api, Caller, Reply, json};
use crate::{Error, Result};
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
    ) -> Result<Reply> {
        let id = |text: &str| -> Result<OperationId> {
            text.parse::<uuid::Uuid>()
                .map(OperationId)
                .map_err(|e| Error::bad_value(format!("backup hold {text}: {e}")))
        };
        if action == "list" {
            if hold.is_some() || output.is_some() {
                return Err(Error::bad_request(
                    "backup list takes only --page and --cursor",
                ));
            }
            let after = cursor.as_deref().map(id).transpose()?;
            let size = i64::try_from(page.unwrap_or(100)).unwrap_or(i64::MAX);
            if size == 0 {
                return Err(Error::bad_value("backup list page must be positive"));
            }
            let mut conn = self.acquire(caller).await?;
            let holds = sinex_store::backup::list(&mut conn, after, size).await?;
            let next = if holds.len() as u64 == size as u64 {
                holds.last().map(|h| h.id.to_string())
            } else {
                None
            };
            return Ok(Reply::Ok {
                result: json!({"holds":holds,"page":{"cursor":next}}),
            });
        }
        if page.is_some() || cursor.is_some() {
            return Err(Error::bad_request(
                "backup --page and --cursor belong to list",
            ));
        }
        let writer = self.writer()?;
        match action {
            "hold" if hold.is_none() && output.is_none() => {
                let hold = writer.backup_hold(caller.actor).await?;
                Ok(Reply::Ok {
                    result: json!({"hold":hold}),
                })
            }
            "release" if output.is_none() => {
                let hold = id(hold
                    .as_deref()
                    .ok_or_else(|| Error::bad_request("backup release needs HOLD"))?)?;
                let hold = writer.backup_release(hold, caller.actor).await?;
                Ok(Reply::Ok {
                    result: json!({"hold":hold}),
                })
            }
            "prepare" if hold.is_none() => {
                let output = output
                    .ok_or_else(|| Error::bad_request("backup prepare needs --output PATH"))?;
                let output = std::path::PathBuf::from(output);
                let revision = crate::build_identity::current()?;
                crate::backup::create(&output)?;
                let source_publication = writer.source_publication();
                let local_guard = source_publication.pause().await;
                let mut paused = Vec::new();
                let mut producer_snapshots = Vec::new();
                if let Some((client, subjects, producers)) = &self.backup_producers {
                    let results =
                        futures::future::join_all(producers.iter().map(|producer| async move {
                            (
                                producer,
                                sinex_transport::backup::PausedProducer::pause(
                                    client.clone(),
                                    subjects.clone(),
                                    producer.clone(),
                                )
                                .await,
                            )
                        }))
                        .await;
                    for (producer, result) in results {
                        match result {
                            Ok(paused_producer) => paused.push(paused_producer),
                            Err(reason) => {
                                producer_snapshots.push(sinex_model::backup::ProducerSnapshot {
                                    producer: producer.clone(),
                                    run_id: None,
                                    files: Vec::new(),
                                    unavailable: Some(reason),
                                })
                            }
                        }
                    }
                }
                let mut snapshot = match writer.backup_snapshot(caller.actor).await {
                    Ok(snapshot) => snapshot,
                    Err(e) => {
                        for producer in &paused {
                            let _ = producer.release().await;
                        }
                        return Err(e.into());
                    }
                };
                let hold = snapshot.hold.id;
                if let Err(error) = crate::backup::record_boundary(&output, &snapshot.hold) {
                    for producer in &paused {
                        let _ = producer.release().await;
                    }
                    return Err(error.with_note(format!("backup hold {hold} remains open")));
                }
                let local_bindings = crate::binding_runner::backup_bindings(
                    snapshot.connection(),
                    &self.source_config,
                    &self.host,
                )
                .await;
                let boundary = &snapshot.hold;
                let output_root = &output;
                let external =
                    futures::future::join_all(paused.into_iter().map(|producer| async move {
                        let relative = format!("main/producers/{}", producer.producer);
                        let result = producer
                            .acquire(boundary, &output_root.join(&relative))
                            .await;
                        let _ = producer.release().await;
                        match result {
                            Ok(mut files) => {
                                for file in &mut files {
                                    file.path = format!("{relative}/{}", file.path);
                                }
                                sinex_model::backup::ProducerSnapshot {
                                    producer: producer.producer,
                                    run_id: Some(producer.run_id),
                                    files,
                                    unavailable: None,
                                }
                            }
                            Err(reason) => sinex_model::backup::ProducerSnapshot {
                                producer: producer.producer,
                                run_id: Some(producer.run_id),
                                files: Vec::new(),
                                unavailable: Some(reason),
                            },
                        }
                    }));
                let local = crate::backup::local_sources(local_bindings, output_root);
                let (acquired, local) = futures::join!(external, local);
                drop(local_guard);
                producer_snapshots.extend(acquired);
                producer_snapshots.push(local);
                match crate::backup::prepare(
                    snapshot,
                    self.blobs.root(),
                    &output,
                    self.stop.clone(),
                    revision.into(),
                    producer_snapshots,
                    self.backup_producers.as_ref().map(|(_, _, producers)| {
                        let mut required = producers.clone();
                        required.push("@daemon".into());
                        required
                    }),
                )
                .await
                {
                    Ok(artifact) => Ok(Reply::Ok {
                        result: json!({"artifact":artifact,"output":output,"hold_open":true}),
                    }),
                    Err(e) => Err(e.with_note(format!(
                        "backup hold {hold} remains open; incomplete output {}",
                        output.display()
                    ))),
                }
            }
            _ => Err(Error::bad_request(
                "backup takes hold, release HOLD, prepare --output PATH, or list",
            )),
        }
    }
}

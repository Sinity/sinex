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
            "prepare" => Err(Error::bad_request(
                "backup prepare streams its snapshot to the client that copies the backup; run sinexctl backup prepare --output PATH",
            )),
            _ => Err(Error::bad_request(
                "backup takes hold, release HOLD, prepare --output PATH, or list",
            )),
        }
    }
}

/// A backup's copy job as the daemon keeps it: the exported snapshot, held open
/// while the client copies, and its description for that client.
pub(crate) struct StartedBackup {
    pub(crate) snapshot: sinex_store::backup::Snapshot,
    pub(crate) job: sinex_model::backup::CopyJob,
}

impl Api {
    /// Start a backup's copy job for the client that copies it: pause the producers,
    /// commit a hold and export its snapshot in one writer job, and copy the
    /// producers' state into `output` while they are paused. The bulk copy
    /// (dumps and segments) is the client's.
    pub(crate) async fn backup_start(
        &self,
        caller: &Caller,
        output: String,
    ) -> Result<StartedBackup> {
        let writer = self.writer()?;
        if self.backup_producers.is_none() {
            return Err(Error::bad_request(
                "backup prepare needs the configured producer list",
            ));
        }
        let output = std::path::PathBuf::from(output);
        let revision = crate::build_identity::current()?;
        crate::request_deadline::admit()?;
        crate::backup::create(&output)?;
        // Discovery can walk historical input directories. Finish it
        // before starting the producers' paused publication lifetime.
        let config = self.source_config.clone();
        let host = self.host.clone();
        let spool = config.context(&host).spool;
        let configured = tokio::task::spawn_blocking(move || {
            crate::binding_runner::plan(&config, &host)
                .map(|bindings| bindings.into_iter().map(|(_, binding)| binding).collect())
        })
        .await??;
        let source_publication = writer.source_publication();
        let local_guard = source_publication.pause().await;
        let mut paused = Vec::new();
        let mut producer_snapshots = Vec::new();
        if let Some((client, subjects, producers)) = &self.backup_producers {
            let results = futures::future::join_all(producers.iter().map(|producer| async move {
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
                    Err(reason) => producer_snapshots.push(sinex_model::backup::ProducerSnapshot {
                        producer: producer.clone(),
                        run_id: None,
                        files: Vec::new(),
                        unavailable: Some(reason),
                    }),
                }
            }
        }
        if !producer_snapshots.is_empty() {
            for producer in &paused {
                let _ = producer.release().await;
            }
            return Err(Error::bad_request(format!(
                "required producers did not pause: {}",
                producer_snapshots
                    .iter()
                    .map(|snapshot| format!(
                        "{}: {}",
                        snapshot.producer,
                        snapshot.unavailable.as_deref().unwrap_or("unavailable")
                    ))
                    .collect::<Vec<_>>()
                    .join("; ")
            )));
        }
        let paused_at = std::time::Instant::now();
        eprintln!("sinexd backup: producers paused; taking core snapshot");
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
        eprintln!(
            "sinexd backup {hold}: core snapshot ready after {:?}; acquiring producer files",
            paused_at.elapsed()
        );
        // A failed attempt releases its own hold: nothing it wrote is a
        // usable copy, and an open hold keeps segment prefixes protected.
        let release = |hold: OperationId| async move {
            match writer.backup_release(hold, caller.actor).await {
                Ok(_) => format!("backup hold {hold} released"),
                Err(error) => {
                    format!("backup hold {hold} remains open; releasing it failed: {error}")
                }
            }
        };
        if let Err(error) = crate::backup::record_boundary(&output, &snapshot.hold) {
            for producer in &paused {
                let _ = producer.release().await;
            }
            // The snapshot's transaction ends before the writer is asked.
            drop(snapshot);
            return Err(error.with_note(release(hold).await));
        }
        let boundary = snapshot.hold.clone();
        let boundary = &boundary;
        let output_root = &output;
        let external = futures::future::join_all(paused.into_iter().map(|producer| async move {
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
        let local = async {
            let local_bindings = crate::binding_runner::backup_bindings(
                snapshot.connection(),
                configured,
                &self.host,
                &spool,
            )
            .await;
            crate::backup::local_sources(local_bindings, output_root).await
        };
        let (acquired, local) = futures::join!(external, local);
        drop(local_guard);
        producer_snapshots.extend(acquired);
        producer_snapshots.push(local);
        let job = sinex_model::backup::CopyJob {
            boundary: snapshot.hold.clone(),
            snapshot: snapshot.exported.clone(),
            database: snapshot.database.clone(),
            database_url: crate::backup::split_password(snapshot.database_url())?.public,
            store: self.blobs.root().display().to_string(),
            output: output.display().to_string(),
            source_revision: revision.into(),
            producer_snapshots,
            required_producers: self.backup_producers.as_ref().map(|(_, _, producers)| {
                let mut required = producers.clone();
                required.push("@daemon".into());
                required
            }),
        };
        // An incomplete producer set is refused here, before the client is
        // handed a job to copy.
        let refused = crate::backup::refuse_incomplete(
            &output,
            &sinex_model::backup::Manifest {
                format: 1,
                boundary: job.boundary.clone(),
                database: job.database.clone(),
                source_revision: job.source_revision.clone(),
                files: Vec::new(),
                required_producers: job.required_producers.clone(),
                producers_complete: crate::backup::producers_complete(
                    &job.producer_snapshots,
                    &job.required_producers,
                ),
                producer_snapshots: job.producer_snapshots.clone(),
            },
        );
        if let Err(error) = refused {
            // The snapshot's transaction ends before the writer is asked.
            drop(snapshot);
            return Err(error.with_note(format!(
                "{}; incomplete output {}",
                release(hold).await,
                output.display()
            )));
        }
        Ok(StartedBackup { snapshot, job })
    }
}

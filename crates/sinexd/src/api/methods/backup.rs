//! Backup requests use the existing writer; a snapshot and a hold are distinct lifetimes.
use super::{Api, Caller, Reply, json};
use crate::{Error, Result};
use futures::StreamExt;
use sinex_model::OperationId;
use tokio::sync::watch;

async fn cancelled(cancel: &mut watch::Receiver<bool>) {
    let _ = cancel.wait_for(|stopped| *stopped).await;
}

fn stopped() -> Error {
    command_surface::Failure::new(
        command_surface::FailureKind::Cancelled,
        "backup preparation stopped",
    )
    .into()
}

async fn release_producers(paused: &[(usize, sinex_transport::backup::PausedProducer)]) {
    for (_, producer) in paused {
        if let Err(error) = producer.release().await {
            eprintln!(
                "backup producer {} release failed: {error}",
                producer.producer
            );
        }
    }
}

impl Api {
    pub(super) async fn backup(&self, caller: &Caller, request: super::Request) -> Result<Reply> {
        let super::Request::Backup {
            action,
            hold,
            to,
            page_size,
            cursor,
        } = request
        else {
            unreachable!("backup request");
        };
        let id = |text: &str| -> Result<OperationId> {
            text.parse::<uuid::Uuid>()
                .map(OperationId)
                .map_err(|e| Error::bad_value(format!("backup hold {text}: {e}")))
        };
        if action == "list" {
            if hold.is_some() || to.is_some() {
                return Err(Error::bad_request(
                    "backup list takes only --page-size and --cursor",
                ));
            }
            let after = cursor.as_deref().map(id).transpose()?;
            let size = i64::try_from(page_size.unwrap_or(50)).unwrap_or(i64::MAX);
            if size == 0 {
                return Err(Error::bad_value("backup list page size must be positive"));
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
        if page_size.is_some() || cursor.is_some() {
            return Err(Error::bad_request(
                "backup --page-size and --cursor belong to list",
            ));
        }
        let writer = self.writer()?;
        match action.as_str() {
            "hold" if hold.is_none() && to.is_none() => {
                let hold = writer.backup_hold(caller.actor).await?;
                Ok(Reply::Ok {
                    result: json!({"hold":hold}),
                })
            }
            "release" if to.is_none() => {
                let hold = id(hold
                    .as_deref()
                    .ok_or_else(|| Error::bad_request("backup release needs HOLD"))?)?;
                let hold = writer.backup_release(hold, caller.actor).await?;
                Ok(Reply::Ok {
                    result: json!({"hold":hold}),
                })
            }
            "prepare" => Err(Error::bad_request(
                "backup prepare streams its snapshot to the client that copies the backup; run sinexctl backup prepare --to PATH",
            )),
            _ => Err(Error::bad_request(
                "backup takes hold, release HOLD, prepare --to PATH, or list",
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
        operation: OperationId,
        mut cancel: watch::Receiver<bool>,
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
        eprintln!("sinexd backup: waiting for running core compression policies before snapshot");
        let idle = async {
            let mut connection = self.reads.acquire().await?;
            crate::backup::policy::wait_for_idle(&mut connection).await?;
            Ok::<_, Error>(())
        };
        tokio::select! {
            result=idle => result?,
            _=cancelled(&mut cancel) => return Err(stopped()),
        }

        // Discovery can walk historical input directories. Finish it
        // before starting the producers' paused publication lifetime.
        let config = self.source_config.clone();
        let host = self.host.clone();
        let spool = config.context(&host).spool;
        let discovery = tokio::task::spawn_blocking(move || {
            crate::binding_runner::plan(&config, &host)
                .map(|bindings| bindings.into_iter().map(|(_, binding)| binding).collect())
        });
        let configured = tokio::select! {
            result = discovery => result??,
            _ = cancelled(&mut cancel) => return Err(stopped()),
        };
        let source_publication = writer.source_publication();
        let local_guard = tokio::select! {
            guard = source_publication.pause() => guard,
            _ = cancelled(&mut cancel) => return Err(stopped()),
        };
        let mut paused = Vec::new();
        let mut producer_snapshots = Vec::new();
        if let Some((client, subjects, producers)) = &self.backup_producers {
            let mut requests: futures::stream::FuturesUnordered<_> = producers
                .iter()
                .enumerate()
                .map(|(index, producer)| async move {
                    (
                        index,
                        producer,
                        sinex_transport::backup::PausedProducer::pause(
                            client.clone(),
                            subjects.clone(),
                            producer.clone(),
                        )
                        .await,
                    )
                })
                .collect();
            while !requests.is_empty() {
                tokio::select! {
                    biased;
                    response = requests.next() => {
                        let (index, producer, response) = response.expect("nonempty requests");
                        match response {
                            Ok(producer) => paused.push((index, producer)),
                            Err(reason) => producer_snapshots.push(sinex_model::backup::ProducerSnapshot {
                                producer: producer.clone(), run_id: None, files: Vec::new(), unavailable: Some(reason),
                            }),
                        }
                    }
                    _ = cancelled(&mut cancel) => {
                        drop(requests);
                        release_producers(&paused).await;
                        return Err(stopped());
                    }
                }
            }
            paused.sort_by_key(|(index, _)| *index);
        }
        if !producer_snapshots.is_empty() {
            release_producers(&paused).await;
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
        let mut snapshot = match writer.backup_snapshot(operation, caller.actor).await {
            Ok(snapshot) => snapshot,
            Err(e) => {
                release_producers(&paused).await;
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
            release_producers(&paused).await;
            // The snapshot's transaction ends before the writer is asked.
            let _ = snapshot.close().await;
            return Err(error.with_note(release(hold).await));
        }
        let boundary = snapshot.hold.clone();
        let boundary = &boundary;
        let output_root = &output;
        let acquired = {
            let external =
                futures::future::join_all(paused.iter().map(|(_, producer)| async move {
                    let relative = format!("main/producers/{}", producer.producer);
                    let result = producer
                        .acquire(boundary, &output_root.join(&relative))
                        .await;
                    match result {
                        Ok(mut files) => {
                            for file in &mut files {
                                file.path = format!("{relative}/{}", file.path);
                            }
                            sinex_model::backup::ProducerSnapshot {
                                producer: producer.producer.clone(),
                                run_id: Some(producer.run_id.clone()),
                                files,
                                unavailable: None,
                            }
                        }
                        Err(reason) => sinex_model::backup::ProducerSnapshot {
                            producer: producer.producer.clone(),
                            run_id: Some(producer.run_id.clone()),
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
            tokio::select! {
                result = async { futures::join!(external, local) } => Some(result),
                _ = cancelled(&mut cancel) => None,
            }
        };
        release_producers(&paused).await;
        drop(local_guard);
        let Some((acquired, local)) = acquired else {
            let _ = snapshot.close().await;
            return Err(stopped().with_note(release(hold).await));
        };
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
            let _ = snapshot.close().await;
            return Err(error.with_note(format!(
                "{}; incomplete output {}",
                release(hold).await,
                output.display()
            )));
        }
        let stop_requested = *cancel.borrow();
        if stop_requested {
            let _ = snapshot.close().await;
            return Err(stopped().with_note(release(hold).await));
        }
        Ok(StartedBackup { snapshot, job })
    }
}

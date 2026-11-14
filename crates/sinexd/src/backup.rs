//! One local immutable core artifact, prepared under a live exported SQL snapshot.
use crate::{Error, Result};
use sinex_model::backup::{File as ManifestFile, Manifest};
use sinex_store::backup::{self, Snapshot};
use std::fs::{File, OpenOptions};
use std::io::{Read, Write};
use std::os::unix::fs::{DirBuilderExt, OpenOptionsExt};
use std::path::{Component, Path};
use tokio::sync::watch;

fn file(path: &Path) -> Result<File> {
    OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(path)
        .map_err(Error::from)
}

fn stopped(stop: &watch::Receiver<bool>) -> Result<()> {
    if *stop.borrow() {
        Err(Error::bad_request(
            "backup preparation stopped; its hold remains open",
        ))
    } else {
        Ok(())
    }
}

/// Create the private artifact roots before any producer can populate them.
/// A partial attempt remains there, with its hold, and cannot be silently reused.
pub(crate) fn create(path: &Path) -> Result<()> {
    if !path.is_absolute() {
        return Err(Error::bad_request(
            "backup output must be an absolute new directory",
        ));
    }
    std::fs::DirBuilder::new().mode(0o700).create(path)?;
    for directory in ["main", "keys"] {
        std::fs::DirBuilder::new()
            .mode(0o700)
            .create(path.join(directory))?;
    }
    Ok(())
}

fn record<T: serde::Serialize>(path: &Path, value: &T) -> Result<()> {
    let mut out = file(path)?;
    serde_json::to_writer_pretty(&mut out, value)?;
    out.write_all(b"\n")?;
    out.sync_all().map_err(Error::from)
}

pub(crate) fn record_boundary(output: &Path, boundary: &sinex_model::backup::Hold) -> Result<()> {
    let path = output.join("boundary.json");
    if path.exists() {
        let found: sinex_model::backup::Hold = serde_json::from_reader(File::open(path)?)?;
        if &found != boundary {
            return Err(Error::bad_request(
                "output belongs to a different backup boundary",
            ));
        }
    } else {
        record(&path, boundary)?;
    }
    File::open(output)
        .and_then(|f| f.sync_all())
        .map_err(Error::from)
}

/// libpq expands a URI passed as dbname, but treats PGDATABASE as a plain name.
/// Passwords remain exclusively in the child's environment, including a URL
/// query password, which has precedence over the user-info password in SQLx.
fn pg_command(program: &str, database: &str) -> Result<tokio::process::Command> {
    let mut command = tokio::process::Command::new(program);
    let uri = database.split_once("://").is_some_and(|(scheme, _)| {
        scheme.eq_ignore_ascii_case("postgres") || scheme.eq_ignore_ascii_case("postgresql")
    });
    if !uri {
        command.env("PGDATABASE", database);
        return Ok(command);
    }
    let mut url = reqwest::Url::parse(database)
        .map_err(|_| Error::bad_value("invalid PostgreSQL connection URI"))?;
    let mut password = url
        .password()
        .map(|encoded| {
            percent_encoding::percent_decode_str(encoded)
                .decode_utf8()
                .map(|password| password.into_owned())
                .map_err(|_| Error::bad_value("PostgreSQL password is not UTF-8"))
        })
        .transpose()?;
    if url.password().is_some() {
        url.set_password(None)
            .map_err(|_| Error::bad_value("cannot remove PostgreSQL URI password"))?;
    }
    let mut public = Vec::new();
    for (key, value) in url.query_pairs() {
        if key == "password" {
            password = Some(value.into_owned());
        } else {
            public.push((key.into_owned(), value.into_owned()));
        }
    }
    // SQLx reads form-encoded pairs; libpq only percent-decodes them. Emit
    // spaces as %20 and literal plus signs as %2B for the same effective values.
    let query = public
        .iter()
        .map(|(key, value)| {
            format!(
                "{}={}",
                percent_encoding::utf8_percent_encode(key, percent_encoding::NON_ALPHANUMERIC),
                percent_encoding::utf8_percent_encode(value, percent_encoding::NON_ALPHANUMERIC)
            )
        })
        .collect::<Vec<_>>()
        .join("&");
    url.set_query((!query.is_empty()).then_some(query.as_str()));
    command
        .env_remove("PGDATABASE")
        .arg("--dbname")
        .arg(url.as_str());
    if let Some(password) = password {
        command.env("PGPASSWORD", password);
    }
    Ok(command)
}

/// Tables whose rows are rebuilt from the events after a restore, left out of
/// the main dump's data.
pub(crate) const REBUILT_TABLES: [&str; 4] = [
    "core.search_text",
    "core.search_trigram",
    "core.search_refusals",
    "core.search_position",
];

async fn dump(
    snapshot: &Snapshot,
    output: &Path,
    keys: bool,
    mut stop: watch::Receiver<bool>,
) -> Result<()> {
    stopped(&stop)?;
    let name = if keys {
        "keys/data-keys.dump"
    } else {
        "main/sinex.dump"
    };
    let stderr_path = output.join(if keys {
        "keys/pg_dump.log"
    } else {
        "main/pg_dump.log"
    });
    let stderr = file(&stderr_path)?;
    let mut command = pg_command("pg_dump", snapshot.database_url())?;
    // Uncompressed, so the backup tool's own compression and deduplication
    // see unchanged rows as unchanged; a compressed dump differs throughout
    // from night to night.
    command
        .arg("--no-password")
        .arg("--format=custom")
        .arg("--compress=0")
        .arg(format!("--snapshot={}", snapshot.exported))
        .stderr(stderr)
        .stdout(file(&output.join(name))?)
        .kill_on_drop(true);
    if keys {
        command.arg("--data-only").arg("--table=privacy.data_keys");
    } else {
        command.arg("--exclude-table-data=privacy.data_keys");
        // The search tables are rebuilt from the events; their position goes
        // with them, so a restored copy rebuilds search from the start.
        for table in REBUILT_TABLES {
            command.arg(format!("--exclude-table-data={table}"));
        }
    }
    let mut child = command
        .spawn()
        .map_err(|e| Error::from(e).within("start pg_dump"))?;
    let status = tokio::select! {
        result = child.wait() => result?,
        _ = stop.changed() => {
            child.kill().await?;
            child.wait().await?;
            return Err(Error::bad_request("backup dump stopped; its hold remains open"));
        }
    };
    if !status.success() {
        let diagnostic = std::fs::read_to_string(&stderr_path)
            .unwrap_or_else(|error| format!("cannot read private dump diagnostic: {error}"));
        return Err(Error::bad_request(format!(
            "pg_dump failed ({status}): {}; private log {}",
            diagnostic.trim(),
            stderr_path.display(),
        )));
    }
    File::open(output.join(name))
        .and_then(|f| f.sync_all())
        .map_err(Error::from)
}

fn digest(root: &Path, relative: &str, stop: &watch::Receiver<bool>) -> Result<ManifestFile> {
    let mut input = File::open(root.join(relative))?;
    let mut buffer = [0; 64 * 1024];
    let mut hasher = blake3::Hasher::new();
    let mut bytes = 0;
    loop {
        stopped(stop)?;
        let size = input.read(&mut buffer)?;
        if size == 0 {
            break;
        }
        hasher.update(&buffer[..size]);
        bytes += size as u64;
    }
    Ok(ManifestFile {
        path: relative.into(),
        bytes,
        blake3: hasher.finalize().to_hex().to_string(),
    })
}

fn segment_path(name: &str) -> Result<&Path> {
    let path = Path::new(name);
    if name.is_empty() || !path.components().all(|c| matches!(c, Component::Normal(_))) {
        return Err(Error::bad_request(
            "backup catalog contains a non-relative segment path",
        ));
    }
    Ok(path)
}

pub(crate) async fn local_sources(
    bindings: Result<Vec<sinex_source_core::Binding>>,
    output: &Path,
) -> sinex_model::backup::ProducerSnapshot {
    let result = async {
        let bindings = bindings?;
        let cancelled = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        struct Cancel(std::sync::Arc<std::sync::atomic::AtomicBool>);
        impl Drop for Cancel {
            fn drop(&mut self) {
                self.0.store(true, std::sync::atomic::Ordering::Release);
            }
        }
        let _cancel = Cancel(cancelled.clone());
        let destination = output.join("main/local-sources");
        let mut files = tokio::task::spawn_blocking(move || {
            let captured = sinex_sources::live::capture_backup(&bindings, &cancelled)
                .map_err(Error::internal)?;
            sinex_transport::backup::capture_tree(
                captured.directory.path(),
                &destination,
                &cancelled,
            )
            .map_err(Error::internal)
        })
        .await??;
        for file in &mut files {
            file.path = format!("main/local-sources/{}", file.path);
        }
        Ok::<_, Error>(files)
    }
    .await;
    match result {
        Ok(files) => sinex_model::backup::ProducerSnapshot {
            producer: "@daemon".into(),
            run_id: None,
            files,
            unavailable: None,
        },
        Err(reason) => sinex_model::backup::ProducerSnapshot {
            producer: "@daemon".into(),
            run_id: None,
            files: Vec::new(),
            unavailable: Some(reason.to_string()),
        },
    }
}

/// Populate the private artifact roots established by `create`.
pub(crate) async fn prepare(
    mut snapshot: Snapshot,
    store: &Path,
    output: &Path,
    stop: watch::Receiver<bool>,
    source_revision: String,
    producer_snapshots: Vec<sinex_model::backup::ProducerSnapshot>,
    required_producers: Option<Vec<String>>,
) -> Result<Manifest> {
    let producers_complete = required_producers.as_ref().is_some_and(|required| {
        required.len() == producer_snapshots.len()
            && required.iter().all(|producer| {
                producer_snapshots
                    .iter()
                    .filter(|snapshot| {
                        &snapshot.producer == producer && snapshot.unavailable.is_none()
                    })
                    .count()
                    == 1
            })
    });
    let mut manifest = Manifest {
        format: 1,
        boundary: snapshot.hold.clone(),
        database: snapshot.database.clone(),
        source_revision,
        producer_snapshots,
        required_producers,
        producers_complete,
        files: Vec::new(),
    };
    // This record survives failures and identifies the hold without claiming a complete artifact.
    record_boundary(output, &manifest.boundary)?;
    File::open(output).and_then(|f| f.sync_all())?;
    if !producers_complete {
        // Preserve the refusal and cut without presenting an incomplete copy
        // as a manifest. Do this before either dump or any segment copying.
        record(&output.join("producer-snapshots.json"), &manifest)?;
        File::open(output).and_then(|f| f.sync_all())?;
        let unavailable = manifest
            .producer_snapshots
            .iter()
            .filter_map(|producer| {
                producer
                    .unavailable
                    .as_ref()
                    .map(|reason| format!("{}: {reason}", producer.producer))
            })
            .collect::<Vec<_>>()
            .join("; ");
        return Err(Error::bad_request(format!(
            "required producer snapshots are incomplete: {unavailable}"
        )));
    }
    dump(&snapshot, output, false, stop.clone()).await?;
    dump(&snapshot, output, true, stop.clone()).await?;
    let hold_id = snapshot.hold.id;
    let segments = backup::segments(snapshot.connection(), hold_id).await?;
    for segment in segments {
        stopped(&stop)?;
        let relative = segment_path(&segment.name)?.to_owned();
        let input = store.join(&relative);
        let target = output.join("main").join(&relative);
        let check = stop.clone();
        let manifest_path = format!("main/{}", relative.display());
        let copied = tokio::task::spawn_blocking(move || -> Result<ManifestFile> {
            if let Some(parent) = target.parent() {
                std::fs::create_dir_all(parent)?;
            }
            let mut input = File::open(input)?;
            let mut out = file(&target)?;
            let mut remaining = segment.committed_end;
            let mut buffer = [0; 64 * 1024];
            let mut hasher = blake3::Hasher::new();
            while remaining != 0 {
                stopped(&check)?;
                let take = usize::try_from(remaining.min(buffer.len() as u64))?;
                input.read_exact(&mut buffer[..take]).map_err(|e| {
                    Error::bad_request(format!("short or unreadable committed segment: {e}"))
                })?;
                out.write_all(&buffer[..take])?;
                hasher.update(&buffer[..take]);
                remaining -= take as u64;
            }
            out.sync_all()?;
            if let Some(parent) = target.parent() {
                File::open(parent).and_then(|f| f.sync_all())?;
            }
            Ok(ManifestFile {
                path: manifest_path,
                bytes: segment.committed_end,
                blake3: hasher.finalize().to_hex().to_string(),
            })
        })
        .await??;
        manifest.files.push(copied);
    }
    snapshot.validate_copy(&output.join("main")).await?;
    stopped(&stop)?;
    let digest_root = output.to_owned();
    let digest_stop = stop.clone();
    let dumps = tokio::task::spawn_blocking(move || -> Result<_> {
        Ok([
            digest(&digest_root, "main/sinex.dump", &digest_stop)?,
            digest(&digest_root, "keys/data-keys.dump", &digest_stop)?,
        ])
    })
    .await??;
    manifest.files.extend(dumps);
    for producer in &manifest.producer_snapshots {
        manifest.files.extend(producer.files.iter().cloned());
    }
    stopped(&stop)?;
    // Both pg_dump calls imported the same still-live snapshot of this database.
    snapshot.close().await?;
    for directory in ["main", "keys"] {
        File::open(output.join(directory)).and_then(|f| f.sync_all())?;
    }
    // Each Borg repository carries the same immutable pairing identity and file digests.
    record(&output.join("main/manifest.json"), &manifest)?;
    record(&output.join("keys/manifest.json"), &manifest)?;
    for directory in [output.join("main"), output.join("keys")] {
        File::open(directory).and_then(|f| f.sync_all())?;
    }
    record(&output.join("manifest.json"), &manifest)?;
    File::open(output).and_then(|f| f.sync_all())?;
    Ok(manifest)
}

#[cfg(test)]
mod tests;

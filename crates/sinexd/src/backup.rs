//! One local immutable core artifact, prepared under a live exported SQL snapshot.
use sinex_model::backup::{File as ManifestFile, Manifest};
use sinex_store::backup::{self, Snapshot};
use std::fs::{File, OpenOptions};
use std::io::{Read, Write};
use std::os::unix::fs::{DirBuilderExt, OpenOptionsExt};
use std::path::{Component, Path};
use tokio::sync::watch;

fn file(path: &Path) -> Result<File, String> {
    OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(path)
        .map_err(|e| e.to_string())
}

fn stopped(stop: &watch::Receiver<bool>) -> Result<(), String> {
    if *stop.borrow() {
        Err("backup preparation stopped; its hold remains open".into())
    } else {
        Ok(())
    }
}

/// The caller creates one new private directory before issuing a fresh hold.
/// A partial attempt remains there, with its hold, and cannot be silently reused.
pub(crate) fn create(path: &Path) -> Result<(), String> {
    if !path.is_absolute() {
        return Err("backup output must be an absolute new directory".into());
    }
    std::fs::DirBuilder::new()
        .mode(0o700)
        .create(path)
        .map_err(|e| e.to_string())
}

fn record<T: serde::Serialize>(path: &Path, value: &T) -> Result<(), String> {
    let mut out = file(path)?;
    serde_json::to_writer_pretty(&mut out, value).map_err(|e| e.to_string())?;
    out.write_all(b"\n").map_err(|e| e.to_string())?;
    out.sync_all().map_err(|e| e.to_string())
}

async fn dump(
    snapshot: &Snapshot,
    output: &Path,
    keys: bool,
    mut stop: watch::Receiver<bool>,
) -> Result<(), String> {
    stopped(&stop)?;
    let name = if keys {
        "keys/data-keys.dump"
    } else {
        "main/sinex.dump"
    };
    let stderr = file(&output.join(if keys {
        "keys/pg_dump.log"
    } else {
        "main/pg_dump.log"
    }))?;
    let mut command = tokio::process::Command::new("pg_dump");
    command
        .env("PGDATABASE", snapshot.database_url())
        .arg("--no-password")
        .arg("--format=custom")
        .arg(format!("--snapshot={}", snapshot.exported))
        .stderr(stderr)
        .stdout(file(&output.join(name))?)
        .kill_on_drop(true);
    if keys {
        command.arg("--data-only").arg("--table=privacy.data_keys");
    } else {
        command.arg("--exclude-table-data=privacy.data_keys");
    }
    let mut child = command.spawn().map_err(|e| format!("start pg_dump: {e}"))?;
    let status = tokio::select! {
        result = child.wait() => result.map_err(|e| e.to_string())?,
        _ = stop.changed() => {
            child.kill().await.map_err(|e| e.to_string())?;
            child.wait().await.map_err(|e| e.to_string())?;
            return Err("backup dump stopped; its hold remains open".into());
        }
    };
    if !status.success() {
        return Err(format!(
            "pg_dump failed ({status}); see the artifact's private pg_dump.log"
        ));
    }
    File::open(output.join(name))
        .and_then(|f| f.sync_all())
        .map_err(|e| e.to_string())
}

fn digest(
    root: &Path,
    relative: &str,
    stop: &watch::Receiver<bool>,
) -> Result<ManifestFile, String> {
    let mut input = File::open(root.join(relative)).map_err(|e| e.to_string())?;
    let mut buffer = [0; 64 * 1024];
    let mut hasher = blake3::Hasher::new();
    let mut bytes = 0;
    loop {
        stopped(stop)?;
        let size = input.read(&mut buffer).map_err(|e| e.to_string())?;
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

fn segment_path(name: &str) -> Result<&Path, String> {
    let path = Path::new(name);
    if name.is_empty() || !path.components().all(|c| matches!(c, Component::Normal(_))) {
        return Err("backup catalog contains a non-relative segment path".into());
    }
    Ok(path)
}

pub(crate) async fn prepare(
    mut snapshot: Snapshot,
    store: &Path,
    output: &Path,
    stop: watch::Receiver<bool>,
    source_revision: String,
) -> Result<Manifest, String> {
    let mut manifest = Manifest {
        format: 1,
        boundary: snapshot.hold.clone(),
        database: snapshot.database.clone(),
        source_revision,
        files: Vec::new(),
    };
    for directory in ["main", "keys"] {
        std::fs::DirBuilder::new()
            .mode(0o700)
            .create(output.join(directory))
            .map_err(|e| e.to_string())?;
    }
    // This record survives failures and identifies the hold without claiming a complete artifact.
    record(&output.join("boundary.json"), &manifest.boundary)?;
    File::open(output)
        .and_then(|f| f.sync_all())
        .map_err(|e| e.to_string())?;
    dump(&snapshot, output, false, stop.clone()).await?;
    dump(&snapshot, output, true, stop.clone()).await?;
    let hold_id = snapshot.hold.id;
    let segments = backup::segments(snapshot.connection(), hold_id)
        .await
        .map_err(|e| e.to_string())?;
    for segment in segments {
        stopped(&stop)?;
        let relative = segment_path(&segment.name)?.to_owned();
        let input = store.join(&relative);
        let target = output.join("main").join(&relative);
        let check = stop.clone();
        let manifest_path = format!("main/{}", relative.display());
        let copied = tokio::task::spawn_blocking(move || -> Result<ManifestFile, String> {
            if let Some(parent) = target.parent() {
                std::fs::create_dir_all(parent).map_err(|e| e.to_string())?;
            }
            let mut input = File::open(input).map_err(|e| e.to_string())?;
            let mut out = file(&target)?;
            let mut remaining = segment.committed_end;
            let mut buffer = [0; 64 * 1024];
            let mut hasher = blake3::Hasher::new();
            while remaining != 0 {
                stopped(&check)?;
                let take = usize::try_from(remaining.min(buffer.len() as u64))
                    .map_err(|e| e.to_string())?;
                input
                    .read_exact(&mut buffer[..take])
                    .map_err(|e| format!("short or unreadable committed segment: {e}"))?;
                out.write_all(&buffer[..take]).map_err(|e| e.to_string())?;
                hasher.update(&buffer[..take]);
                remaining -= take as u64;
            }
            out.sync_all().map_err(|e| e.to_string())?;
            if let Some(parent) = target.parent() {
                File::open(parent)
                    .and_then(|f| f.sync_all())
                    .map_err(|e| e.to_string())?;
            }
            Ok(ManifestFile {
                path: manifest_path,
                bytes: segment.committed_end,
                blake3: hasher.finalize().to_hex().to_string(),
            })
        })
        .await
        .map_err(|e| e.to_string())??;
        manifest.files.push(copied);
    }
    snapshot
        .validate_copy(&output.join("main"))
        .await
        .map_err(|e| e.to_string())?;
    stopped(&stop)?;
    let digest_root = output.to_owned();
    let digest_stop = stop.clone();
    let dumps = tokio::task::spawn_blocking(move || -> Result<_, String> {
        Ok([
            digest(&digest_root, "main/sinex.dump", &digest_stop)?,
            digest(&digest_root, "keys/data-keys.dump", &digest_stop)?,
        ])
    })
    .await
    .map_err(|e| e.to_string())??;
    manifest.files.extend(dumps);
    stopped(&stop)?;
    // Both pg_dump calls imported the same still-live snapshot of this database.
    snapshot.close().await.map_err(|e| e.to_string())?;
    for directory in ["main", "keys"] {
        File::open(output.join(directory))
            .and_then(|f| f.sync_all())
            .map_err(|e| e.to_string())?;
    }
    // Each Borg repository carries the same immutable pairing identity and file digests.
    record(&output.join("main/manifest.json"), &manifest)?;
    record(&output.join("keys/manifest.json"), &manifest)?;
    for directory in [output.join("main"), output.join("keys")] {
        File::open(directory)
            .and_then(|f| f.sync_all())
            .map_err(|e| e.to_string())?;
    }
    record(&output.join("manifest.json"), &manifest)?;
    File::open(output)
        .and_then(|f| f.sync_all())
        .map_err(|e| e.to_string())?;
    Ok(manifest)
}

#[cfg(test)]
mod tests;

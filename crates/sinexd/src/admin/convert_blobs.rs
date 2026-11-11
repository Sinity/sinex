//! Explicit conversion from an external decoded-byte exporter. No old codec
//! runs here; every stream is verified before its extents are committed.
use crate::{Error, Result};
use sqlx::Connection;
use std::io::{Read, Seek, SeekFrom};
use std::path::Path;
use std::process::{Child, Command, Stdio};

struct Export(Child);
impl Drop for Export {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

const GROUP_INPUTS: usize = 256;
const GROUP_BYTES: u64 = 64 << 20;

pub(super) async fn run(
    database: &str,
    store: &Path,
    source: &Path,
    exporter: &Path,
) -> Result<()> {
    if !source.is_absolute() || !store.is_absolute() || !exporter.is_absolute() {
        return Err(Error::bad_request(
            "source, store and exporter paths must be absolute",
        ));
    }
    let mut conn = sqlx::PgConnection::connect(database).await?;
    if !sinex_store::writer_lock::try_claim(&mut conn).await? {
        return Err(Error::bad_request(
            "stop sinexd before converting its byte store",
        ));
    }
    let result = convert(&mut conn, store, source, exporter).await;
    let closed = conn.close().await;
    result?;
    closed?;
    Ok(())
}

async fn convert(
    conn: &mut sqlx::PgConnection,
    store: &Path,
    source: &Path,
    exporter: &Path,
) -> Result<()> {
    let blobs = sinex_store::Blobs::open(store)?;
    blobs.load(conn).await?;
    let mut after = String::new();
    let mut converted = 0u64;
    let mut bytes = 0u64;
    loop {
        let rows =
            sinex_store::blobs::migration::pending(conn, &after, GROUP_INPUTS as i64).await?;
        if rows.is_empty() {
            break;
        }
        let mut selected = Vec::new();
        let mut group_bytes = 0u64;
        for row in rows {
            let length = row.length;
            if !selected.is_empty() && group_bytes.saturating_add(length) > GROUP_BYTES {
                break;
            }
            group_bytes += length;
            selected.push((row.name, length));
        }
        let mut command = Command::new(exporter);
        command
            .arg("payloads")
            .arg(source)
            .args(selected.iter().map(|(name, _)| name));
        let mut export = Export(
            command
                .stdin(Stdio::null())
                .stdout(Stdio::piped())
                .spawn()?,
        );
        let mut output = export
            .0
            .stdout
            .take()
            .ok_or_else(|| Error::internal("exporter output missing"))?;
        let mut group = Vec::new();
        for (name, length) in selected {
            let mut decoded = tempfile::tempfile()?;
            let copied = std::io::copy(&mut output.by_ref().take(length), &mut decoded)?;
            if copied != length {
                return Err(Error::internal(format!(
                    "exporter ended before {name}: {copied} of {length} bytes"
                )));
            }
            decoded.seek(SeekFrom::Start(0))?;
            group.push(sinex_store::blobs::migration::prepare(
                &store.join("incoming"),
                &name,
                length,
                decoded,
                None,
            )?);
            after = name;
        }
        let mut extra = [0];
        if output.read(&mut extra)? != 0 {
            return Err(Error::internal(
                "exporter returned bytes beyond its catalog lengths",
            ));
        }
        let status = export.0.wait()?;
        if !status.success() {
            return Err(Error::internal(format!(
                "predecessor exporter failed: {status}"
            )));
        }
        let n = group.len() as u64;
        blobs.convert_prepared_group(conn, group).await?;
        converted += n;
        bytes += group_bytes;
        println!(
            "{}",
            serde_json::json!({"converted":converted,"decoded_bytes":bytes})
        );
    }
    // Ordinary startup must now accept the converted store, including a tail
    // left by an interrupted prior conversion.
    blobs.recover(conn).await?;
    println!(
        "{}",
        serde_json::json!({"complete":true,"converted":converted,"decoded_bytes":bytes})
    );
    Ok(())
}

#[cfg(test)]
mod tests;

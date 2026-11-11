//! Explicit conversion from an external decoded-byte exporter. No old codec
//! runs here; every stream is verified before its extents are committed.
use crate::{Error, Result};
use sqlx::Connection;
use std::io::{Seek, SeekFrom};
use std::path::Path;
use std::process::{Command, Stdio};

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
        let rows = sqlx::query!(
            "SELECT hash,size FROM core.blobs WHERE hash>$1 AND status='present' AND core.blob_descriptors(hash) IS NULL ORDER BY hash LIMIT $2",
            after, GROUP_INPUTS as i64
        ).fetch_all(&mut *conn).await?;
        if rows.is_empty() {
            break;
        }
        let mut group = Vec::new();
        let mut group_bytes = 0u64;
        for row in rows {
            let length = u64::try_from(row.size)
                .map_err(|_| Error::internal("negative stored blob size"))?;
            if !group.is_empty() && group_bytes.saturating_add(length) > GROUP_BYTES {
                break;
            }
            let mut decoded = tempfile::tempfile()?;
            let status = Command::new(exporter)
                .arg("payload")
                .arg(source)
                .arg(&row.hash)
                .stdin(Stdio::null())
                .stdout(Stdio::from(decoded.try_clone()?))
                .status()?;
            if !status.success() {
                return Err(Error::internal(format!(
                    "predecessor exporter failed for {}: {status}",
                    row.hash
                )));
            }
            decoded.seek(SeekFrom::Start(0))?;
            group.push(sinex_store::blobs::migration::prepare(
                &store.join("incoming"),
                &row.hash,
                length,
                decoded,
                None,
            )?);
            group_bytes += length;
            after = row.hash;
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

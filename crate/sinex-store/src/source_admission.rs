//! Writer steps shared by the Atuin, zsh and Chromium admissions: input and
//! producer registration, record failures, and stream parts (rows appended to a
//! file under `<root>/open` that is closed into a blob).
use crate::{BLOB_PRESENT, Error, Result, Writer};
use serde_json::Value;
use sqlx::{Connection, Row};
use std::io::{Seek, SeekFrom, Write};
use std::path::PathBuf;
use uuid::Uuid;

/// A stream part is closed once it would grow past this size.
const PART_BYTES: u64 = 100 * 1024 * 1024;

pub(crate) type Tx<'a> = sqlx::Transaction<'a, sqlx::Postgres>;

/// Observation status after a step.
pub(crate) fn observation_status(unavailable: bool, complete: bool) -> &'static str {
    if unavailable {
        "unavailable"
    } else if complete {
        "completed"
    } else {
        "reading"
    }
}

/// Record hash: the first 16 bytes of the BLAKE3 hash of the row with sorted keys.
pub(crate) fn row_hash(row: &Value) -> Result<Vec<u8>> {
    let mut row = row.clone();
    row.sort_all_objects();
    let bytes = serde_json::to_vec(&row).map_err(|_| Error::Invalid("row encoding failed"))?;
    Ok(blake3::hash(&bytes).as_bytes()[..16].to_vec())
}

/// Hash of an equivalence key, as stored in `core.live_equivalence.key_hash`.
pub(crate) fn key_hash(source: &str, event_type: &str, key: &str) -> Vec<u8> {
    blake3::hash(format!("{source}\x1f{event_type}\x1f{key}").as_bytes()).as_bytes()[..16].to_vec()
}

/// The producer row for this parser, run and configuration, created on first use.
pub(crate) async fn producer_version(
    tx: &mut Tx<'_>,
    name: &str,
    version: &str,
    run: Uuid,
    configuration: &Value,
) -> Result<Uuid> {
    let existing = sqlx::query_scalar(
        "SELECT id FROM core.producer_versions
         WHERE name=$1 AND version=$2 AND run_id=$3 AND configuration=$4 LIMIT 1",
    )
    .bind(name)
    .bind(version)
    .bind(run)
    .bind(configuration)
    .fetch_optional(&mut **tx)
    .await?;
    if let Some(id) = existing {
        return Ok(id);
    }
    let id = Uuid::now_v7();
    sqlx::query(
        "INSERT INTO core.producer_versions(id,name,version,run_id,configuration)
         VALUES($1,$2,$3,$4,$5)",
    )
    .bind(id)
    .bind(name)
    .bind(version)
    .bind(run)
    .bind(configuration)
    .execute(&mut **tx)
    .await?;
    Ok(id)
}

pub(crate) async fn record_failure(
    tx: &mut Tx<'_>,
    observation: Uuid,
    material: Uuid,
    anchor: i64,
    reason: &str,
) -> Result<()> {
    sqlx::query(
        "INSERT INTO raw.record_failures(observation_id,material_id,anchor_byte,reason)
         VALUES($1,$2,$3,$4)",
    )
    .bind(observation)
    .bind(material)
    .bind(anchor)
    .bind(reason)
    .execute(&mut **tx)
    .await?;
    Ok(())
}

/// Record the committed length of a stream part after a step appended to it.
pub(crate) async fn set_part_length(tx: &mut Tx<'_>, material: Uuid, length: i64) -> Result<()> {
    sqlx::query(
        "UPDATE raw.source_material_registry SET total_bytes=$2,durable_bytes=$2 WHERE id=$1",
    )
    .bind(material)
    .bind(length)
    .execute(&mut **tx)
    .await?;
    Ok(())
}

impl Writer {
    fn open_part_path(&self, material: Uuid) -> PathBuf {
        std::path::Path::new(&self.root)
            .join("open")
            .join(material.to_string())
    }

    /// The input row for a configured source location, created on first use.
    pub(crate) async fn register_input(
        &mut self,
        binding: &str,
        locator: &str,
        kind: &str,
    ) -> Result<Uuid> {
        let added: Option<Uuid> = sqlx::query_scalar(
            "INSERT INTO raw.inputs(id,binding,locator,kind) VALUES($1,$2,$3,$4)
             ON CONFLICT(binding,locator) DO NOTHING RETURNING id",
        )
        .bind(Uuid::now_v7())
        .bind(binding)
        .bind(locator)
        .bind(kind)
        .fetch_optional(&mut self.connection)
        .await?;
        if let Some(id) = added {
            return Ok(id);
        }
        Ok(
            sqlx::query_scalar("SELECT id FROM raw.inputs WHERE binding=$1 AND locator=$2")
                .bind(binding)
                .bind(locator)
                .fetch_one(&mut self.connection)
                .await?,
        )
    }

    /// Outcome references for a new observation of `input`, following its latest one.
    pub(crate) async fn following_outcomes(&mut self, input: Uuid) -> Result<Value> {
        let latest: Option<(Uuid, String, Option<Value>)> = sqlx::query_as(
            "SELECT id,status,metadata->'_sinex_acquisition_v1'->'outcomes'
             FROM raw.observations WHERE input_id=$1 ORDER BY seq DESC LIMIT 1",
        )
        .bind(input)
        .fetch_optional(&mut self.connection)
        .await?;
        let refs = sinex_model::acquisition::OutcomeRefs::following(latest.as_ref().map(
            |(id, status, value)| {
                (
                    *id,
                    status.as_str(),
                    value
                        .as_ref()
                        .and_then(|v| serde_json::from_value(v.clone()).ok()),
                )
            },
        ));
        serde_json::to_value(refs)
            .map_err(|_| Error::Invalid("source outcome references unavailable"))
    }

    /// Truncate parts left open by a crash to their committed length and close them.
    pub(crate) async fn recover_parts(&mut self) -> Result<()> {
        let rows = sqlx::query(
            "SELECT id,durable_bytes FROM raw.source_material_registry WHERE status='capturing'",
        )
        .fetch_all(&mut self.connection)
        .await?;
        for row in rows {
            let material: Uuid = row.try_get("id")?;
            let length: i64 = row.try_get("durable_bytes")?;
            let file = std::fs::OpenOptions::new()
                .write(true)
                .open(self.open_part_path(material))?;
            if file.metadata()?.len() < length as u64 {
                return Err(Error::Invalid(
                    "open part is shorter than its committed length",
                ));
            }
            file.set_len(length as u64)?;
            file.sync_all()?;
            self.close_part(material, "recovered_partial").await?;
        }
        sqlx::query(
            "UPDATE raw.observations
             SET status='interrupted',reason='writer restarted before completed read'
             WHERE status='reading'",
        )
        .execute(&mut self.connection)
        .await?;
        Ok(())
    }

    /// Close any part still open for `input`.
    pub(crate) async fn close_open_part(&mut self, input: Uuid) -> Result<()> {
        let open: Option<Uuid> = sqlx::query_scalar(
            "SELECT id FROM raw.source_material_registry
             WHERE input_id=$1 AND status='capturing' LIMIT 1",
        )
        .bind(input)
        .fetch_optional(&mut self.connection)
        .await?;
        if let Some(id) = open {
            self.close_part(id, "completed").await?;
        }
        Ok(())
    }

    /// Move an open part into the blob store and mark it closed with `status`.
    pub(crate) async fn close_part(&mut self, material: Uuid, status: &str) -> Result<()> {
        let path = self.open_part_path(material);
        let hash_path = path.clone();
        let (hash, length) = tokio::task::spawn_blocking(move || -> Result<_> {
            let mut file = std::fs::File::open(hash_path)?;
            let length = file.metadata()?.len() as i64;
            let mut hash = blake3::Hasher::new();
            hash.update_reader(&mut file)?;
            Ok((hashed_blobs::Hash(*hash.finalize().as_bytes()), length))
        })
        .await??;
        let name = hash.to_string();
        let blob = self.register_blob(&name, length).await?;
        let blobs = self.blobs.clone();
        let put_path = path.clone();
        tokio::task::spawn_blocking(move || -> Result<()> {
            blobs.put(std::fs::File::open(&put_path)?, 3)?;
            if blobs.verify(&hash)? != hashed_blobs::Verified::Intact {
                return Err(Error::Invalid("closed material failed verification"));
            }
            Ok(())
        })
        .await??;
        let mut tx = self.connection.begin().await?;
        sqlx::query(BLOB_PRESENT)
            .bind(blob)
            .execute(&mut *tx)
            .await?;
        sqlx::query(
            "UPDATE raw.source_material_registry
             SET blob_id=$2,status=$3,total_bytes=$4,durable_bytes=$4
             WHERE id=$1 AND status='capturing'",
        )
        .bind(material)
        .bind(blob)
        .bind(status)
        .bind(length)
        .execute(&mut *tx)
        .await?;
        tx.commit().await?;
        // Remove the open file only after the close commits; a leftover copy is harmless.
        std::fs::remove_file(path)?;
        Ok(())
    }

    /// Append one step's bytes to the input's open part and fsync them. Returns the
    /// part (None for an empty step) and the offset the bytes start at. The caller
    /// records the new length in the transaction that admits the step's events.
    pub(crate) async fn store_stream_step(
        &mut self,
        source: &str,
        input: Uuid,
        observation: Uuid,
        provenance: &Value,
        bytes: &[u8],
    ) -> Result<(Option<Uuid>, i64)> {
        if bytes.is_empty() {
            return Ok((None, 0));
        }
        let mut material = None;
        let mut start = 0i64;
        let open = sqlx::query(
            "SELECT id,durable_bytes,observation_id FROM raw.source_material_registry
             WHERE input_id=$1 AND status='capturing' ORDER BY staged_at DESC LIMIT 1",
        )
        .bind(input)
        .fetch_optional(&mut self.connection)
        .await?;
        if let Some(row) = open {
            let id = row.try_get::<Uuid, _>("id")?;
            if row.try_get::<Option<Uuid>, _>("observation_id")? == Some(observation) {
                material = Some(id);
                start = row.try_get("durable_bytes")?;
            } else {
                // An earlier failed reading left this part open. Close it rather
                // than attribute this reading's rows to it.
                self.close_part(id, "completed").await?;
            }
        }
        if let Some(id) = material
            && start as u64 + bytes.len() as u64 > PART_BYTES
        {
            self.close_part(id, "completed").await?;
            material = None;
            start = 0;
        }
        let material = match material {
            Some(id) => id,
            None => {
                self.open_part(source, input, observation, provenance)
                    .await?
            }
        };
        let path = self.open_part_path(material);
        let write_bytes = bytes.to_vec();
        tokio::task::spawn_blocking(move || -> Result<()> {
            let mut file = std::fs::OpenOptions::new().write(true).open(path)?;
            file.set_len(start as u64)?;
            file.seek(SeekFrom::Start(start as u64))?;
            file.write_all(&write_bytes)?;
            file.sync_all()?;
            Ok(())
        })
        .await??;
        Ok((Some(material), start))
    }

    async fn open_part(
        &mut self,
        source: &str,
        input: Uuid,
        observation: Uuid,
        provenance: &Value,
    ) -> Result<Uuid> {
        let id = Uuid::now_v7();
        let dir = std::path::Path::new(&self.root).join("open");
        std::fs::create_dir_all(&dir)?;
        let file = std::fs::OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(self.open_part_path(id))?;
        file.sync_all()?;
        std::fs::File::open(&dir)?.sync_all()?;
        sqlx::query(
            "INSERT INTO raw.source_material_registry
             (id,material_kind,source_identifier,status,ts_quality,blob_id,total_bytes,
              durable_bytes,input_id,observation_id,parser_provenance)
             VALUES ($1,'stream_chunk',$2,'capturing','unknown',NULL,0,0,$3,$4,$5)",
        )
        .bind(id)
        .bind(source)
        .bind(input)
        .bind(observation)
        .bind(provenance)
        .execute(&mut self.connection)
        .await?;
        Ok(id)
    }
}

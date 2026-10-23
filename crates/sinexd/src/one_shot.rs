//! What a one-shot command (backfill, replay, excision, hide) runs with: the
//! schema checked, the content store open, and the one writer task. Reads go
//! through `read_conn`, never the writer's connection.
use crate::dirty_keys::Registered;
use crate::writer::{self, WriterHandle};
use sinex_model::MaterialId;
use sinex_sources::drivers::MaterialSink;
use sinex_store::Blobs;
use sqlx::{Connection, PgConnection};
use std::path::Path;
use std::sync::Arc;
use tokio::task::JoinHandle;

pub struct OneShot {
    pub read_conn: PgConnection,
    pub blobs: Blobs,
    pub writer: WriterHandle,
    task: JoinHandle<()>,
}

impl OneShot {
    pub async fn open(database: &str, store: &Path) -> Result<Self, String> {
        let mut read_conn = PgConnection::connect(database)
            .await
            .map_err(|e| e.to_string())?;
        sinex_schema::apply(&mut read_conn)
            .await
            .map_err(|e| e.to_string())?;
        let blobs = Blobs::open(store).map_err(|e| e.to_string())?;
        let mut writer_conn = PgConnection::connect(database)
            .await
            .map_err(|e| e.to_string())?;
        writer::claim(&mut writer_conn).await?;
        let (writer, queues) = writer::channels();
        let task = tokio::spawn(writer::run(
            database.to_owned(),
            writer_conn,
            queues,
            Arc::new(Registered::new(sinex_derivations::keyed())),
        ));
        Ok(Self {
            read_conn,
            blobs,
            writer,
            task,
        })
    }

    /// Stop the writer once everything submitted is done.
    pub async fn close(self) -> Result<(), String> {
        drop(self.writer);
        self.task.await.map_err(|e| e.to_string())
    }
}

/// Drivers store a material's bytes through this.
pub struct Sink(pub Blobs);

impl MaterialSink for Sink {
    fn put_image(
        &self,
        image: &sinex_sources::drivers::sqlite_image::Image,
    ) -> Result<sinex_model::SqliteImagePublication, String> {
        let recipient = if image.metadata.class == sinex_model::Class::Sealed {
            class_key()?
        } else {
            None
        };
        crate::sqlite_images::store(&self.0, image, recipient.as_ref())
    }
    fn put(&self, bytes: &[u8]) -> Result<String, String> {
        self.0.put(bytes).map_err(|e| e.to_string())
    }

    /// An export archive is streamed into the store, never held in memory.
    fn put_file(&self, path: &std::path::Path) -> Result<String, String> {
        let file = std::fs::File::open(path).map_err(|e| format!("{}: {e}", path.display()))?;
        self.0
            .put_reader(std::io::BufReader::new(file))
            .map_err(|e| e.to_string())
    }
}

/// The class key's public half, from the file `$SINEX_CLASS_KEY_FILE` (an
/// age recipient, `age1…`). Sealed sources need it to capture; the daemon
/// never holds the secret half (spec `privacy.md` §4).
pub fn class_key() -> Result<Option<sinex_store::sealed::Recipient>, String> {
    let Some(path) = std::env::var_os("SINEX_CLASS_KEY_FILE") else {
        return Ok(None);
    };
    let text = std::fs::read_to_string(&path)
        .map_err(|e| format!("{}: {e}", std::path::Path::new(&path).display()))?;
    sinex_store::sealed::parse_recipient(&text)
        .map(Some)
        .map_err(|e| e.to_string())
}

/// Drivers of a sealed source store a material's bytes through this: each
/// material gets a fresh data key, its bytes are sealed under it, and the key
/// waits here until the batch that registers the material is sealed with it.
pub struct SealingSink {
    blobs: Blobs,
    recipient: sinex_store::sealed::Recipient,
    state: std::sync::Mutex<SealingState>,
}

#[derive(Default)]
struct SealingState {
    pending: std::collections::HashMap<String, sinex_store::sealed::DataKey>,
    current: Option<(MaterialId, String, sinex_store::sealed::DataKey)>,
}

impl SealingSink {
    pub fn new(blobs: Blobs, recipient: sinex_store::sealed::Recipient) -> Self {
        Self {
            blobs,
            recipient,
            state: Default::default(),
        }
    }

    pub fn key(&self, blob: &str) -> Result<sinex_store::sealed::DataKey, String> {
        let state = self.state.lock().expect("sealing state");
        state
            .pending
            .get(blob)
            .cloned()
            .or_else(|| {
                state
                    .current
                    .as_ref()
                    .filter(|(_, name, _)| name == blob)
                    .map(|(_, _, key)| key.clone())
            })
            .ok_or_else(|| "sealed material key is unavailable; explicit unlock is required".into())
    }

    pub fn open_carry(&self, value: &serde_json::Value) -> Result<serde_json::Value, String> {
        let Some(reference) = sinex_store::sealed::key_ref(value) else {
            return Ok(value.clone());
        };
        let state = self.state.lock().expect("sealing state");
        let key = state
            .current
            .as_ref()
            .filter(|(_, _, key)| key.key_ref == reference)
            .map(|(_, _, key)| key)
            .or_else(|| state.pending.values().find(|key| key.key_ref == reference))
            .ok_or("sealed parser carry needs explicit operator unlock")?;
        sinex_store::sealed::unseal_payload(value, &key.key).map_err(|e| e.to_string())
    }

    pub fn restore_key(&self, blob: String, key: sinex_store::sealed::DataKey) {
        self.state
            .lock()
            .expect("sealing state")
            .pending
            .insert(blob, key);
    }

    pub fn restore_current(
        &self,
        material: MaterialId,
        blob: String,
        key: sinex_store::sealed::DataKey,
    ) {
        self.state.lock().expect("sealing state").current = Some((material, blob, key));
    }

    pub fn put_reader(&self, reader: impl std::io::Read + std::io::Seek) -> Result<String, String> {
        let key = sinex_store::sealed::new_key().map_err(|e| e.to_string())?;
        let name = self
            .blobs
            .put_sealed_reader(reader, &key)
            .map_err(|e| e.to_string())?;
        self.restore_key(name.clone(), key);
        Ok(name)
    }

    /// Seal events and carry with their material key. The wrapped key travels
    /// with source progress so its row and the material commit atomically.
    pub async fn seal(
        &self,
        _read_conn: &mut PgConnection,
        batches: &mut [sinex_model::Batch],
    ) -> Result<(), String> {
        for batch in batches {
            let material = match &batch.kind {
                sinex_model::BatchKind::Source(p) => p.material.clone(),
                _ => return Err("source sealing only accepts source progress".into()),
            };
            let checkpoint = material.is_none() && batch.records.is_empty();
            if let Some(material) = material {
                let key = self.key(&material.blob)?;
                let wrapped =
                    sinex_store::sealed::wrap(&key, &self.recipient).map_err(|e| e.to_string())?;
                let wrapped = sinex_model::WrappedMaterialKey {
                    material: material.id,
                    key_ref: key.key_ref.clone(),
                    wrapped,
                };
                if let sinex_model::BatchKind::Source(progress) = &mut batch.kind {
                    progress.material_keys.push(wrapped);
                }
                let mut state = self.state.lock().expect("sealing state");
                state.pending.remove(&material.blob);
                state.current = Some((material.id, material.blob.clone(), key));
            }
            let current = self.state.lock().expect("sealing state").current.clone();
            let Some((material, _, key)) = current else {
                let sinex_model::BatchKind::Source(progress) = &batch.kind else {
                    unreachable!()
                };
                if batch.records.is_empty()
                    && (progress.carry.is_null() || sinex_store::sealed::is_sealed(&progress.carry))
                {
                    continue;
                }
                return Err(
                    "sealed source state needs its material key; explicit unlock is required"
                        .into(),
                );
            };
            for record in &mut batch.records {
                match record {
                    sinex_model::RecordOutcome::Events { events, .. } => {
                        for event in events {
                            if event
                                .location()
                                .is_none_or(|(location, _)| location.material != material)
                            {
                                return Err(
                                    "sealed batch cites a different material than its key".into()
                                );
                            }
                            sinex_store::sealed::seal_event(event, &key)
                                .map_err(|e| e.to_string())?;
                        }
                    }
                    sinex_model::RecordOutcome::Failure { location, reason } => {
                        if location.material != material {
                            return Err("sealed failure cites a different material".into());
                        }
                        *reason = sinex_store::sealed::seal_value(
                            &serde_json::Value::String(reason.clone()),
                            &key,
                        )
                        .map_err(|e| e.to_string())?
                        .to_string();
                    }
                }
            }
            if let sinex_model::BatchKind::Source(progress) = &mut batch.kind {
                let retained = checkpoint
                    && sinex_store::sealed::key_ref(&progress.carry) == Some(key.key_ref.as_str())
                    && sinex_store::sealed::unseal_payload(&progress.carry, &key.key).is_ok();
                if !retained {
                    progress.carry = sinex_store::sealed::seal_value(&progress.carry, &key)
                        .map_err(|e| e.to_string())?;
                }
            }
        }
        Ok(())
    }
}

impl MaterialSink for SealingSink {
    fn put_image(
        &self,
        image: &sinex_sources::drivers::sqlite_image::Image,
    ) -> Result<sinex_model::SqliteImagePublication, String> {
        crate::sqlite_images::store(&self.blobs, image, Some(&self.recipient))
    }
    fn put_file(&self, path: &std::path::Path) -> Result<String, String> {
        let file = std::fs::File::open(path).map_err(|e| e.to_string())?;
        self.put_reader(file)
    }

    fn put(&self, bytes: &[u8]) -> Result<String, String> {
        let key = sinex_store::sealed::new_key().map_err(|e| e.to_string())?;
        let name = self
            .blobs
            .put_sealed(bytes, &key)
            .map_err(|e| e.to_string())?;
        self.state
            .lock()
            .expect("sealing state")
            .pending
            .insert(name.clone(), key);
        Ok(name)
    }
}

/// The stored bytes of the chain ending at the cursor's last material, for
/// rebuilding a driver state after a restart; empty before the first read.
/// Only the chain's tail is read, newest material first, until it covers
/// `sinex_sources::drivers::append_file::STATE_TAIL` bytes, so a restart
/// costs bounded memory whatever the chain's length; the driver state
/// holds no more than that tail either. The last material is cut at the
/// cursor's `through`: bytes after it were stored but their readings never
/// committed, so the driver must read them again.
pub async fn stored_chain(
    read_conn: &mut PgConnection,
    blobs: &Blobs,
    cursor: &serde_json::Value,
) -> Result<Vec<(MaterialId, Vec<u8>)>, String> {
    let Some(last) = cursor
        .get("material")
        .and_then(serde_json::Value::as_str)
        .and_then(|s| uuid::Uuid::parse_str(s).ok())
    else {
        return Ok(Vec::new());
    };
    let names = sinex_store::replay::chain_ending(read_conn, last)
        .await
        .map_err(|e| e.to_string())?;
    let through = cursor
        .get("through")
        .and_then(serde_json::Value::as_u64)
        .map(|t| usize::try_from(t).unwrap_or(usize::MAX));
    let budget = sinex_sources::drivers::append_file::STATE_TAIL;
    let mut chain = Vec::new();
    let mut covered = 0usize;
    for (i, (id, name)) in names.iter().enumerate().rev() {
        let mut bytes = blobs.read(name).map_err(|e| e.to_string())?;
        if i + 1 == names.len()
            && let Some(through) = through
        {
            bytes.truncate(through);
        }
        covered += bytes.len();
        chain.push((MaterialId(*id), bytes));
        if covered >= budget {
            break;
        }
    }
    chain.reverse();
    Ok(chain)
}

#[cfg(test)]
mod tests;

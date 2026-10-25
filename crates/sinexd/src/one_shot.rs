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
        sinex_schema::require_current(&mut read_conn)
            .await
            .map_err(|e| e.to_string())?;
        let blobs = Blobs::open(store).map_err(|e| e.to_string())?;
        let mut writer_conn = PgConnection::connect(database)
            .await
            .map_err(|e| e.to_string())?;
        writer::claim(&mut writer_conn).await?;
        let (writer, queues) = writer::channels_for(database);
        let task = tokio::spawn(writer::run(
            database.to_owned(),
            writer_conn,
            queues,
            Arc::new(Registered::new(sinex_derivations::keyed())),
            Some(blobs.clone()),
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

/// The class key's public half, from the file `$SINEX_CLASS_KEY_FILE` (an age
/// recipient, `age1…`). Sealed sources need it to capture; the daemon never
/// holds the secret half.
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
    unadmitted: std::collections::HashMap<String, SealingKey>,
    current: Option<(MaterialId, String, SealingKey)>,
}

#[derive(Clone)]
struct SealingKey {
    data: sinex_store::sealed::DataKey,
    wrapped: Vec<u8>,
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
        Ok(self.held(blob)?.data)
    }

    fn held(&self, blob: &str) -> Result<SealingKey, String> {
        let state = self.state.lock().expect("sealing state");
        state
            .unadmitted
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
            .filter(|(_, _, key)| key.data.key_ref == reference)
            .map(|(_, _, key)| key)
            .or_else(|| {
                state
                    .unadmitted
                    .values()
                    .find(|key| key.data.key_ref == reference)
            })
            .ok_or("sealed parser carry needs explicit operator unlock")?;
        sinex_store::sealed::unseal_payload(value, &key.data.key).map_err(|e| e.to_string())
    }

    /// Hold `key` for the material stored as `blob`, wrapped for the class.
    pub fn restore_key(
        &self,
        blob: String,
        key: sinex_store::sealed::DataKey,
    ) -> Result<(), String> {
        let wrapped =
            sinex_store::sealed::wrap(&key, &self.recipient).map_err(|e| e.to_string())?;
        self.state
            .lock()
            .expect("sealing state")
            .unadmitted
            .insert(blob, SealingKey { data: key, wrapped });
        Ok(())
    }

    pub fn restore_current(
        &self,
        material: MaterialId,
        blob: String,
        key: sinex_store::sealed::DataKey,
    ) -> Result<(), String> {
        let wrapped =
            sinex_store::sealed::wrap(&key, &self.recipient).map_err(|e| e.to_string())?;
        self.state.lock().expect("sealing state").current =
            Some((material, blob, SealingKey { data: key, wrapped }));
        Ok(())
    }

    pub fn put_reader(&self, reader: impl std::io::Read + std::io::Seek) -> Result<String, String> {
        let key = sinex_store::sealed::new_key().map_err(|e| e.to_string())?;
        let name = self
            .blobs
            .put_sealed_reader(reader, &key)
            .map_err(|e| e.to_string())?;
        self.restore_key(name.clone(), key)?;
        Ok(name)
    }

    /// Seal events and carry with their material key. The wrapped key travels
    /// with source progress so its row and the material commit atomically.
    pub fn seal(&self, batches: &mut [sinex_model::Batch]) -> Result<(), String> {
        for batch in batches {
            let material = match &batch.kind {
                sinex_model::BatchKind::Source(p) => p.material.clone(),
                _ => return Err("source sealing only accepts source progress".into()),
            };
            let checkpoint = material.is_none() && batch.records.is_empty();
            if let Some(material) = material {
                let held = self.held(&material.blob)?;
                let key = &held.data;
                let wrapped = held.wrapped.clone();
                let wrapped = sinex_model::WrappedMaterialKey {
                    material: material.id,
                    key_ref: key.key_ref.clone(),
                    wrapped,
                };
                if let sinex_model::BatchKind::Source(progress) = &mut batch.kind {
                    progress.material_keys.push(wrapped);
                }
                let mut state = self.state.lock().expect("sealing state");
                state.unadmitted.remove(&material.blob);
                state.current = Some((material.id, material.blob.clone(), held));
            }
            let current = self.state.lock().expect("sealing state").current.clone();
            let Some((material, _, held)) = current else {
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
            let key = &held.data;
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
                            sinex_store::sealed::seal_event(event, key)
                                .map_err(|e| e.to_string())?;
                        }
                    }
                    sinex_model::RecordOutcome::Failure { location, reason } => {
                        if location.material != material {
                            return Err("sealed failure cites a different material".into());
                        }
                        *reason = sinex_store::sealed::seal_value(
                            &serde_json::Value::String(reason.clone()),
                            key,
                        )
                        .map_err(|e| e.to_string())?
                        .to_string();
                    }
                }
            }
            if let sinex_model::BatchKind::Source(progress) = &mut batch.kind {
                let already_sealed = checkpoint
                    && sinex_store::sealed::key_ref(&progress.carry) == Some(key.key_ref.as_str())
                    && sinex_store::sealed::unseal_payload(&progress.carry, &key.key).is_ok();
                // An empty carry says nothing; sealing it would make a later
                // material of a stateless parser need this material's key.
                if !already_sealed && !progress.carry.is_null() {
                    progress.carry = sinex_store::sealed::seal_value(&progress.carry, key)
                        .map_err(|e| e.to_string())?;
                }
            }
        }
        Ok(())
    }
}

impl MaterialSink for SealingSink {
    fn put_file(&self, path: &std::path::Path) -> Result<String, String> {
        let file = std::fs::File::open(path).map_err(|e| e.to_string())?;
        self.put_reader(file)
    }

    fn put(&self, bytes: &[u8]) -> Result<String, String> {
        self.put_reader(std::io::Cursor::new(bytes))
    }
}

#[cfg(test)]
mod tests;

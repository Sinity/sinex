//! What a one-shot command (backfill, replay, excision, hide) runs with: the
//! schema checked, the content store open, and the one writer task. Reads go
//! through `aux`, never the writer's connection.
use crate::keys::Registered;
use crate::writer::{self, WriterHandle};
use sinex_model::MaterialId;
use sinex_sources::drivers::MaterialSink;
use sinex_store::Blobs;
use sqlx::{Connection, PgConnection};
use std::path::Path;
use std::sync::Arc;
use tokio::task::JoinHandle;

pub struct Context {
    pub aux: PgConnection,
    pub blobs: Blobs,
    pub writer: WriterHandle,
    task: JoinHandle<()>,
}

impl Context {
    pub async fn open(database: &str, store: &Path) -> Result<Self, String> {
        let mut aux = PgConnection::connect(database)
            .await
            .map_err(|e| e.to_string())?;
        sinex_schema::apply(&mut aux)
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
            aux,
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
    keys: std::sync::Mutex<std::collections::HashMap<String, sinex_store::sealed::DataKey>>,
}

impl SealingSink {
    pub fn new(blobs: Blobs, recipient: sinex_store::sealed::Recipient) -> Self {
        Self {
            blobs,
            recipient,
            keys: Default::default(),
        }
    }

    /// Seal the batches' events with their material's key and record the
    /// wrapped key before the batches are admitted, so a sealed material row
    /// never exists without its key.
    pub async fn seal(
        &self,
        aux: &mut PgConnection,
        batches: &mut [sinex_model::Batch],
    ) -> Result<(), String> {
        for batch in batches {
            let material = match &batch.kind {
                sinex_model::BatchKind::Source(p) => p.material.clone(),
                _ => None,
            };
            let Some(material) = material else {
                // No new material: these events cite earlier parts, already
                // sealed when they were read.
                continue;
            };
            let key = self
                .keys
                .lock()
                .expect("key map")
                .get(&material.blob)
                .cloned()
                .ok_or_else(|| format!("material {}: stored without a data key", material.id))?;
            let wrapped =
                sinex_store::sealed::wrap(&key, &self.recipient).map_err(|e| e.to_string())?;
            sinex_store::sealed::store_key(aux, material.id, &key.key_ref, &wrapped)
                .await
                .map_err(|e| e.to_string())?;
            for record in &mut batch.records {
                if let sinex_model::RecordOutcome::Events { events, .. } = record {
                    for e in events {
                        sinex_store::sealed::seal_event(e, &key).map_err(|e| e.to_string())?;
                    }
                }
            }
        }
        Ok(())
    }
}

impl MaterialSink for SealingSink {
    fn put(&self, bytes: &[u8]) -> Result<String, String> {
        let key = sinex_store::sealed::new_key().map_err(|e| e.to_string())?;
        let name = self
            .blobs
            .put_sealed(bytes, &key)
            .map_err(|e| e.to_string())?;
        self.keys.lock().expect("key map").insert(name.clone(), key);
        Ok(name)
    }
}

/// The stored bytes of the chain ending at the cursor's last material, for
/// rebuilding a driver's memory after a restart; empty before the first read.
/// Only the chain's tail is read, newest material first, until it covers
/// `sinex_sources::drivers::append_file::MEMORY_TAIL` bytes, so a restart
/// costs bounded memory whatever the chain's length; the driver's memory
/// holds no more than that tail either. The last material is cut at the
/// cursor's `through`: bytes after it were stored but their readings never
/// committed, so the driver must read them again.
pub async fn stored_chain(
    aux: &mut PgConnection,
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
    let names = sinex_store::replay::chain_ending(aux, last)
        .await
        .map_err(|e| e.to_string())?;
    let through = cursor
        .get("through")
        .and_then(serde_json::Value::as_u64)
        .map(|t| usize::try_from(t).unwrap_or(usize::MAX));
    let budget = sinex_sources::drivers::append_file::MEMORY_TAIL;
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

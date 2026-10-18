//! What a one-shot command (backfill, replay, excision, hide) runs with: the
//! schema checked, the content store open, and the one writer task. Reads go
//! through `aux`, never the writer's connection.
use crate::keys::Registered;
use crate::writer::{self, WriterHandle};
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
        let writer_conn = PgConnection::connect(database)
            .await
            .map_err(|e| e.to_string())?;
        let (writer, queues) = writer::channels();
        let task = tokio::spawn(writer::run(
            database.to_owned(),
            writer_conn,
            queues,
            Arc::new(Registered(sinex_automata::keyed())),
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
}

/// The stored bytes of the chain ending at the cursor's last material, for
/// rebuilding a driver's memory after a restart; empty before the first read.
pub async fn stored_chain(
    aux: &mut PgConnection,
    blobs: &Blobs,
    cursor: &serde_json::Value,
) -> Result<Vec<Vec<u8>>, String> {
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
    names
        .iter()
        .map(|n| blobs.read(n).map_err(|e| e.to_string()))
        .collect()
}

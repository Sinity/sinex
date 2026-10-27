//! Shared context for readers and commands: the
//! schema checked, the content store open, and the one writer task. Reads go
//! through `read_conn`, never the writer's connection.
use crate::dirty_keys::Registered;
use crate::writer::{self, WriterHandle};
use sinex_store::Blobs;
use sqlx::{Connection, PgConnection};
use std::path::Path;
use std::sync::Arc;
use tokio::task::JoinHandle;

pub(crate) struct ReaderContext {
    pub read_conn: PgConnection,
    pub blobs: Blobs,
    pub writer: WriterHandle,
    task: JoinHandle<()>,
}

impl ReaderContext {
    pub(crate) async fn open(database: &str, store: &Path) -> Result<Self, String> {
        let mut read_conn = PgConnection::connect(database)
            .await
            .map_err(|e| e.to_string())?;
        sinex_schema::require_current(&mut read_conn)
            .await
            .map_err(|e| e.to_string())?;
        let blobs = Blobs::open(store).map_err(|e| e.to_string())?;
        let writer_conn = PgConnection::connect(database)
            .await
            .map_err(|e| e.to_string())?;
        let writer_conn = writer::claim(writer_conn).await?;
        let (writer, queues) = writer::channels();
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
    pub(crate) async fn close(mut self) -> Result<(), String> {
        drop(self.writer);
        self.task.await.map_err(|e| e.to_string())?;
        writer::claim(&mut self.read_conn).await?;
        sinex_store::blobs::forget_files_with_rows(&mut self.read_conn, &self.blobs)
            .await
            .map_err(|e| e.to_string())
    }
}

#[cfg(test)]
mod tests;

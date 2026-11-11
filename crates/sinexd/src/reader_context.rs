//! Shared context for readers and commands: the
//! schema checked, the content store open, and the one writer task. Reads go
//! through `read_conn`, never the writer's connection.
use crate::dirty_keys::Registered;
use crate::writer::{self, WriterHandle};
use crate::{Error, Result};
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
    pub(crate) async fn open(
        database: &str,
        store: &Path,
        time_settings: sinex_model::time_settings::TimeSettings,
    ) -> Result<Self> {
        let mut read_conn = PgConnection::connect(database).await?;
        sinex_schema::require_current(&mut read_conn).await?;
        let blobs = Blobs::open(store)?;
        let writer_conn = PgConnection::connect(database).await?;
        let mut writer_conn = writer::claim(writer_conn).await?;
        crate::catalog::install_native_sources(&mut writer_conn).await?;
        blobs.recover(&mut writer_conn).await?;
        // As at daemon start: finish removing tombstoned bytes, and no
        // operation holds keys while the writer starts.
        sinex_store::lifecycle::remove_unreferenced(&mut writer_conn, &blobs).await?;
        sinex_store::lifecycle::release_held_keys(&mut writer_conn).await?;
        sinex_store::blobs::delete_unreferenced_extents(&mut writer_conn, &blobs).await?;
        let (writer, queues) = writer::channels();
        let task = tokio::spawn(writer::run(
            database.to_owned(),
            writer_conn,
            queues,
            Arc::new(Registered::new(crate::analysis::keyed(), time_settings)),
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
    pub(crate) async fn close(self) -> Result<()> {
        drop(self.writer);
        self.task.await?;
        let mut conn = writer::claim(self.read_conn).await?;
        sinex_store::blobs::commit_pending_extents(&mut conn, &self.blobs)
            .await
            .map_err(Error::from)
    }
}

#[cfg(test)]
mod tests;

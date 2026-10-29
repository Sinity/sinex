//! A validated writer connection and its admission transactions.
use crate::{DirtyKeys, Outcome, Result, privacy};
use sinex_model::Batch;
use sqlx::{Connection, PgConnection, Postgres, Transaction};
use std::ops::{Deref, DerefMut};

/// Holds the writer claim, schema validation and privacy cache for one session.
/// A reconnect creates a new context and validates after claiming again.
pub struct AdmissionContext {
    connection: PgConnection,
    privacy: privacy::Cache,
    blobs: Option<crate::Blobs>,
}

impl AdmissionContext {
    /// Claim the session before validating or initializing admission state.
    pub async fn claim(mut connection: PgConnection) -> Result<Self> {
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(5);
        loop {
            if crate::writer_lock::try_claim(&mut connection).await? {
                break;
            }
            if tokio::time::Instant::now() >= deadline {
                return Err(crate::Error::Invalid(
                    "another sinexd writer is running against this database".into(),
                ));
            }
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }
        sinex_schema::require_current(&mut connection).await?;
        // The key must commit before any admission can cache or use it.
        let mut tx = connection.begin().await?;
        let key = privacy::hash_key(&mut tx).await?;
        tx.commit().await?;
        Ok(Self {
            connection,
            privacy: privacy::Cache::new(key),
            blobs: None,
        })
    }

    pub fn with_blobs(mut self, blobs: Option<crate::Blobs>) -> Self {
        self.blobs = blobs;
        self
    }

    pub async fn connect(url: &str) -> Result<Self> {
        Self::claim(PgConnection::connect(url).await?).await
    }

    /// All admissions in a replay share this transaction and this session cache.
    pub async fn begin(&mut self) -> Result<AdmissionTransaction<'_>> {
        Ok(AdmissionTransaction {
            transaction: Some(self.connection.begin().await?),
            committed: false,
            privacy: &mut self.privacy,
            blobs: self.blobs.clone(),
        })
    }

    pub async fn close(self) -> Result<()> {
        self.connection.close().await?;
        Ok(())
    }
}

impl Deref for AdmissionContext {
    type Target = PgConnection;
    fn deref(&self) -> &Self::Target {
        &self.connection
    }
}
impl DerefMut for AdmissionContext {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.connection
    }
}

/// An admission transaction can only be opened by a validated writer context.
pub struct AdmissionTransaction<'a> {
    transaction: Option<Transaction<'a, Postgres>>,
    committed: bool,
    privacy: &'a mut privacy::Cache,
    blobs: Option<crate::Blobs>,
}

impl AdmissionTransaction<'_> {
    pub async fn admit(&mut self, batch: &Batch, keys: &dyn DirtyKeys) -> Result<Outcome> {
        crate::admission::admit(
            self.transaction.as_mut().expect("open transaction"),
            self.privacy,
            batch,
            keys,
        )
        .await
    }
    pub async fn commit(mut self) -> Result<()> {
        let receipt = if let Some(blobs) = &self.blobs {
            let store = blobs.clone();
            tokio::task::spawn_blocking(move || store.sync_pending())
                .await
                .map_err(|error| crate::Error::Invalid(error.to_string()))??;
            Some(
                blobs
                    .commit_extents(self.transaction.as_mut().expect("open transaction"))
                    .await?,
            )
        } else {
            None
        };
        self.transaction
            .take()
            .expect("open transaction")
            .commit()
            .await?;
        self.committed = true;
        if let (Some(blobs), Some(receipt)) = (&self.blobs, receipt) {
            blobs.acknowledge(receipt)?;
        }
        Ok(())
    }
    pub async fn rollback(mut self) -> Result<()> {
        self.transaction
            .take()
            .expect("open transaction")
            .rollback()
            .await?;
        Ok(())
    }
}
impl Drop for AdmissionTransaction<'_> {
    fn drop(&mut self) {
        if !self.committed {
            // A transaction can see its own rule changes before they commit.
            self.privacy.invalidate();
        }
    }
}

impl Deref for AdmissionTransaction<'_> {
    type Target = PgConnection;
    fn deref(&self) -> &Self::Target {
        self.transaction.as_ref().expect("open transaction")
    }
}
impl DerefMut for AdmissionTransaction<'_> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.transaction.as_mut().expect("open transaction")
    }
}

impl std::fmt::Debug for AdmissionContext {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AdmissionContext").finish_non_exhaustive()
    }
}

impl<'c> sqlx::Executor<'c> for &'c mut AdmissionContext {
    type Database = Postgres;

    fn fetch_many<'e, 'q: 'e, E>(
        self,
        query: E,
    ) -> futures::stream::BoxStream<
        'e,
        std::result::Result<
            sqlx::Either<sqlx::postgres::PgQueryResult, sqlx::postgres::PgRow>,
            sqlx::Error,
        >,
    >
    where
        'c: 'e,
        E: 'q + sqlx::Execute<'q, Postgres>,
    {
        (&mut self.connection).fetch_many(query)
    }
    fn fetch_optional<'e, 'q: 'e, E>(
        self,
        query: E,
    ) -> futures::future::BoxFuture<
        'e,
        std::result::Result<Option<sqlx::postgres::PgRow>, sqlx::Error>,
    >
    where
        'c: 'e,
        E: 'q + sqlx::Execute<'q, Postgres>,
    {
        (&mut self.connection).fetch_optional(query)
    }
    fn prepare_with<'e, 'q: 'e>(
        self,
        sql: &'q str,
        parameters: &'e [sqlx::postgres::PgTypeInfo],
    ) -> futures::future::BoxFuture<
        'e,
        std::result::Result<sqlx::postgres::PgStatement<'q>, sqlx::Error>,
    >
    where
        'c: 'e,
    {
        (&mut self.connection).prepare_with(sql, parameters)
    }
    fn describe<'e, 'q: 'e>(
        self,
        sql: &'q str,
    ) -> futures::future::BoxFuture<'e, std::result::Result<sqlx::Describe<Postgres>, sqlx::Error>>
    where
        'c: 'e,
    {
        (&mut self.connection).describe(sql)
    }
}

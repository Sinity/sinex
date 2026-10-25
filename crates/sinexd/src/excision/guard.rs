//! Source locks between acquisition and excision. An acquisition holds its
//! source's lock shared until its first admission; an excision holds it
//! exclusively from planning through application.
use sqlx::{Connection, PgConnection, Postgres, Transaction, pool::PoolConnection};

pub(crate) struct ExcisionGuard {
    connection: Owned,
    source: String,
}
enum Owned {
    Pooled(Transaction<'static, Postgres>),
    #[cfg(test)]
    Direct(PgConnection),
}
impl ExcisionGuard {
    pub async fn acquire(
        mut connection: PoolConnection<Postgres>,
        source: String,
    ) -> Result<Self, String> {
        // A canceled lock wait closes the socket instead of returning a query
        // with a queued rollback to the pool. The slot stays owned until release.
        connection.close_on_drop();
        // Consumes the existing planning connection; no second pool slot is needed.
        let mut transaction: Transaction<'static, Postgres> = Transaction::begin(connection, None)
            .await
            .map_err(|e| e.to_string())?;
        let ready = check_ready(&mut transaction, &source).await;
        if let Err(error) = ready {
            transaction.rollback().await.map_err(|e| e.to_string())?;
            return Err(error);
        }
        Ok(Self {
            connection: Owned::Pooled(transaction),
            source,
        })
    }
    pub fn source(&self) -> &str {
        &self.source
    }
    pub fn connection(&mut self) -> &mut PgConnection {
        match &mut self.connection {
            Owned::Pooled(tx) => tx,
            #[cfg(test)]
            Owned::Direct(conn) => conn,
        }
    }
    #[cfg(test)]
    pub async fn connect(database: &str, source: String) -> Result<Self, String> {
        let mut connection = PgConnection::connect(database)
            .await
            .map_err(|e| e.to_string())?;
        let result = async {
            sinex_store::excision::lock_source_session(&mut connection, &source, false)
                .await
                .map_err(|e| e.to_string())?;
            check_ready_unlocked(&mut connection, &source).await
        }
        .await;
        if let Err(error) = result {
            let _ = connection.close().await;
            return Err(error);
        }
        Ok(Self {
            connection: Owned::Direct(connection),
            source,
        })
    }
    pub async fn check(&mut self) -> Result<(), String> {
        sqlx::query("SELECT 1")
            .execute(self.connection())
            .await
            .map_err(|e| e.to_string())?;
        Ok(())
    }
    pub async fn release(self) -> Result<(), String> {
        match self.connection {
            Owned::Pooled(tx) => tx.rollback().await.map_err(|e| e.to_string()),
            #[cfg(test)]
            Owned::Direct(conn) => conn.close().await.map_err(|e| e.to_string()),
        }
    }
}
async fn check_ready(conn: &mut PgConnection, source: &str) -> Result<(), String> {
    sinex_store::excision::lock_source(conn, source, false)
        .await
        .map_err(|e| e.to_string())?;
    check_ready_unlocked(conn, source).await
}
async fn check_ready_unlocked(conn: &mut PgConnection, source: &str) -> Result<(), String> {
    if sinex_store::excision::is_producer(conn, source)
        .await
        .map_err(|e| e.to_string())?
    {
        return Err("excision of a producer's records is not supported yet".into());
    }
    let partial:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM raw.positions p JOIN raw.bindings b ON b.binding=p.binding JOIN raw.materials m ON m.id::text=p.cursor->>'material' WHERE b.source=$1 AND CASE WHEN jsonb_typeof(p.cursor->'through')='number' AND p.cursor->>'through' ~ '^[0-9]+$' THEN (p.cursor->>'through')::numeric<>m.durable_bytes ELSE true END)")
        .bind(source).fetch_one(conn).await.map_err(|e|e.to_string())?;
    if partial {
        return Err(
            "the source has a partly admitted material; let its capture finish before excising"
                .into(),
        );
    }
    Ok(())
}

/// Native acquisition owns its connection until the first admission finishes.
pub(crate) struct AcquisitionGuard {
    connection: PgConnection,
    source: String,
}
impl AcquisitionGuard {
    pub async fn from_pool(pool: &sqlx::PgPool, source: &str) -> Result<Self, String> {
        let mut connection = pool.acquire().await.map_err(|e| e.to_string())?.detach();
        if let Err(error) =
            sinex_store::excision::lock_source_session(&mut connection, source, true).await
        {
            let _ = connection.close().await;
            return Err(error.to_string());
        }
        Ok(Self {
            connection,
            source: source.into(),
        })
    }

    pub async fn acquire(database: &str, source: &str) -> Result<Self, String> {
        let mut connection = PgConnection::connect(database)
            .await
            .map_err(|e| e.to_string())?;
        if let Err(error) =
            sinex_store::excision::lock_source_session(&mut connection, source, true).await
        {
            let _ = connection.close().await;
            return Err(error.to_string());
        }
        Ok(Self {
            connection,
            source: source.into(),
        })
    }
    pub fn connection(&mut self) -> &mut PgConnection {
        &mut self.connection
    }
    pub fn source(&self) -> &str {
        &self.source
    }
    pub async fn check(&mut self) -> Result<(), String> {
        self.connection.ping().await.map_err(|e| e.to_string())
    }
    pub async fn release(self) -> Result<(), String> {
        self.connection.close().await.map_err(|e| e.to_string())
    }
}

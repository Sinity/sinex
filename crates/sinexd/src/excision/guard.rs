//! The planning connection carries its source lock through queued application.
use sqlx::{Connection, PgConnection, Postgres, Transaction, pool::PoolConnection};

pub(crate) struct ExcisionGuard {
    connection: Owned,
    source: String,
}
enum Owned {
    Pooled(Transaction<'static, Postgres>),
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
        let ready = check_custody(&mut transaction, &source).await;
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
            Owned::Direct(conn) => conn,
        }
    }
    pub async fn connect(database: &str, source: String) -> Result<Self, String> {
        let mut connection = PgConnection::connect(database)
            .await
            .map_err(|e| e.to_string())?;
        let result = async {
            sinex_store::excision::lock_source_session(&mut connection, &source, false)
                .await
                .map_err(|e| e.to_string())?;
            check_custody_unlocked(&mut connection, &source).await
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
            Owned::Direct(conn) => conn.close().await.map_err(|e| e.to_string()),
        }
    }
}
async fn check_custody(conn: &mut PgConnection, source: &str) -> Result<(), String> {
    sinex_store::excision::lock_source(conn, source, false)
        .await
        .map_err(|e| e.to_string())?;
    check_custody_unlocked(conn, source).await
}
async fn check_custody_unlocked(conn: &mut PgConnection, source: &str) -> Result<(), String> {
    if sinex_store::excision::unsupported_ingress(conn, source)
        .await
        .map_err(|e| e.to_string())?
    {
        return Err(
            "source namespace has producer custody that excision cannot account for".into(),
        );
    }
    if sinex_store::native_materials::source_pending(conn, source)
        .await
        .map_err(|e| e.to_string())?
    {
        return Err("source has a pending native material publication; finish or recover that exact version before excision".into());
    }
    let partial:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM raw.positions p JOIN raw.bindings b ON b.binding=p.binding JOIN raw.materials m ON m.id::text=p.cursor->>'material' WHERE b.source=$1 AND CASE WHEN jsonb_typeof(p.cursor->'through')='number' AND p.cursor->>'through' ~ '^[0-9]+$' THEN (p.cursor->>'through')::numeric<>m.durable_bytes ELSE true END)")
        .bind(source).fetch_one(conn).await.map_err(|e|e.to_string())?;
    if partial {
        return Err("source has a partially admitted native material; finish its retained records before excision".into());
    }
    Ok(())
}

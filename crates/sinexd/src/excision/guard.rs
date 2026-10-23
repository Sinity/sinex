//! The planning connection carries its source lock through queued application.
use sqlx::{PgConnection, Postgres, Transaction, pool::PoolConnection};

pub(crate) struct ExcisionGuard {
    transaction: Transaction<'static, Postgres>,
    source: String,
}
impl ExcisionGuard {
    pub async fn acquire(
        connection: PoolConnection<Postgres>,
        source: String,
    ) -> Result<Self, String> {
        // Consumes the existing planning connection; no second pool slot is needed.
        let mut transaction: Transaction<'static, Postgres> = Transaction::begin(connection, None)
            .await
            .map_err(|e| e.to_string())?;
        sinex_store::excision::lock_source(&mut transaction, &source, false)
            .await
            .map_err(|e| e.to_string())?;
        if sinex_store::excision::unsupported_ingress(&mut transaction, &source)
            .await
            .map_err(|e| e.to_string())?
        {
            return Err(
                "source namespace has producer custody that sealed excision cannot account for"
                    .into(),
            );
        }
        Ok(Self {
            transaction,
            source,
        })
    }
    pub fn source(&self) -> &str {
        &self.source
    }
    pub fn connection(&mut self) -> &mut PgConnection {
        &mut self.transaction
    }
    pub async fn check(&mut self) -> Result<(), String> {
        sqlx::query("SELECT 1")
            .execute(&mut *self.transaction)
            .await
            .map_err(|e| e.to_string())?;
        Ok(())
    }
    pub async fn release(self) -> Result<(), String> {
        self.transaction.rollback().await.map_err(|e| e.to_string())
    }
}

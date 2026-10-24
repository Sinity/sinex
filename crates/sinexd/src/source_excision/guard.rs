//! Native acquisition owns its connection until the first admission finishes.
use sqlx::{Connection, PgConnection};

pub(crate) struct AcquisitionGuard {
    connection: PgConnection,
    source: String,
}
impl AcquisitionGuard {
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

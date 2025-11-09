use super::common::{db_error, DbResult};
use crate::types::ulid::Ulid;
use sqlx::{postgres::PgPool, Postgres, Transaction};
use uuid::Uuid;

pub struct CascadeRepository<'a> {
    pool: &'a PgPool,
}

impl<'a> CascadeRepository<'a> {
    pub fn new(pool: &'a PgPool) -> Self {
        Self { pool }
    }

    pub async fn prepare_session(
        &self,
        session_id: &str,
        drop_on_commit: bool,
    ) -> DbResult<String> {
        sqlx::query_scalar!(
            r#"SELECT core.prepare_cascade_session($1, $2) AS "table_name!""#,
            session_id,
            drop_on_commit
        )
        .fetch_one(self.pool)
        .await
        .map_err(|e| db_error(e, "prepare cascade session"))
    }

    pub async fn populate_roots(&self, table_name: &str, event_ids: &[Ulid]) -> DbResult<()> {
        let ids: Vec<Uuid> = event_ids.iter().map(|id| id.to_uuid()).collect();
        sqlx::query_scalar::<_, i64>(
            r#"SELECT core.cascade_populate_roots($1, $2::ulid[]) as inserted"#,
        )
        .bind(table_name)
        .bind(&ids)
        .fetch_one(self.pool)
        .await
        .map_err(|e| db_error(e, "populate cascade roots"))?;
        Ok(())
    }

    pub async fn expand(&self, table_name: &str, max_depth: i32) -> DbResult<usize> {
        let depth = sqlx::query_scalar!(
            r#"SELECT core.expand_cascade($1, $2)"#,
            table_name,
            max_depth
        )
        .fetch_one(self.pool)
        .await
        .map_err(|e| db_error(e, "expand cascade graph"))?
        .unwrap_or(0);
        Ok(depth as usize)
    }

    pub async fn depth_histogram(&self, table_name: &str) -> DbResult<Vec<(i32, i64)>> {
        let rows = sqlx::query!(
            r#"SELECT depth as "depth!", node_count as "node_count!" FROM core.cascade_depth_histogram($1)"#,
            table_name
        )
        .fetch_all(self.pool)
        .await
        .map_err(|e| db_error(e, "cascade depth histogram"))?;
        Ok(rows
            .into_iter()
            .map(|row| (row.depth, row.node_count))
            .collect())
    }

    pub async fn count_nodes(&self, table_name: &str) -> DbResult<i64> {
        sqlx::query_scalar!(
            r#"SELECT core.cascade_count_nodes($1) as "count!""#,
            table_name
        )
        .fetch_one(self.pool)
        .await
        .map_err(|e| db_error(e, "count cascade nodes"))
    }

    pub async fn find_integrity_violations(
        &self,
        table_name: &str,
        limit: i32,
    ) -> DbResult<Vec<(Ulid, Ulid)>> {
        sqlx::query!(
            r#"
            SELECT 
                live_event_id as "live_event_id!: Ulid",
                archived_event_id as "archived_event_id!: Ulid"
            FROM core.cascade_find_integrity_violations($1, $2)
            "#,
            table_name,
            limit
        )
        .fetch_all(self.pool)
        .await
        .map_err(|e| db_error(e, "find cascade integrity violations"))
        .map(|rows| {
            rows.into_iter()
                .map(|row| (row.live_event_id, row.archived_event_id))
                .collect()
        })
    }

    pub async fn cleanup_session(&self, table_name: &str) -> DbResult<()> {
        sqlx::query!("SELECT core.cleanup_cascade_session($1)", table_name)
            .execute(self.pool)
            .await
            .map_err(|e| db_error(e, "cleanup cascade session"))?;
        Ok(())
    }
}

pub struct CascadeRepositoryTx<'a, 't> {
    tx: &'a mut Transaction<'t, Postgres>,
}

impl<'a, 't> CascadeRepositoryTx<'a, 't> {
    pub fn new(tx: &'a mut Transaction<'t, Postgres>) -> Self {
        Self { tx }
    }

    pub async fn prepare_session(
        &mut self,
        session_id: &str,
        drop_on_commit: bool,
    ) -> DbResult<String> {
        sqlx::query_scalar::<_, String>(
            r#"SELECT core.prepare_cascade_session($1, $2) AS table_name"#,
        )
        .bind(session_id)
        .bind(drop_on_commit)
        .fetch_one(&mut **self.tx)
        .await
        .map_err(|e| db_error(e, "prepare cascade session"))
    }

    pub async fn populate_roots(&mut self, table_name: &str, event_ids: &[Ulid]) -> DbResult<()> {
        let ids: Vec<Uuid> = event_ids.iter().map(|id| id.to_uuid()).collect();
        sqlx::query_scalar::<_, i64>(
            r#"SELECT core.cascade_populate_roots($1, $2::ulid[]) as inserted"#,
        )
        .bind(table_name)
        .bind(&ids)
        .fetch_one(&mut **self.tx)
        .await
        .map_err(|e| db_error(e, "populate cascade roots"))?;
        Ok(())
    }

    pub async fn expand(&mut self, table_name: &str, max_depth: i32) -> DbResult<usize> {
        let depth = sqlx::query_scalar!(
            r#"SELECT core.expand_cascade($1, $2)"#,
            table_name,
            max_depth
        )
        .fetch_one(&mut **self.tx)
        .await
        .map_err(|e| db_error(e, "expand cascade graph"))?
        .unwrap_or(0);
        Ok(depth as usize)
    }

    pub async fn depth_histogram(&mut self, table_name: &str) -> DbResult<Vec<(i32, i64)>> {
        let rows = sqlx::query!(
            r#"SELECT depth as "depth!", node_count as "node_count!" FROM core.cascade_depth_histogram($1)"#,
            table_name
        )
        .fetch_all(&mut **self.tx)
        .await
        .map_err(|e| db_error(e, "cascade depth histogram"))?;
        Ok(rows
            .into_iter()
            .map(|row| (row.depth, row.node_count))
            .collect())
    }

    pub async fn count_nodes(&mut self, table_name: &str) -> DbResult<i64> {
        sqlx::query_scalar!(
            r#"SELECT core.cascade_count_nodes($1) as "count!""#,
            table_name
        )
        .fetch_one(&mut **self.tx)
        .await
        .map_err(|e| db_error(e, "count cascade nodes"))
    }

    pub async fn find_integrity_violations(
        &mut self,
        table_name: &str,
        limit: i32,
    ) -> DbResult<Vec<(Ulid, Ulid)>> {
        sqlx::query!(
            r#"
            SELECT 
                live_event_id as "live_event_id!: Ulid",
                archived_event_id as "archived_event_id!: Ulid"
            FROM core.cascade_find_integrity_violations($1, $2)
            "#,
            table_name,
            limit
        )
        .fetch_all(&mut **self.tx)
        .await
        .map_err(|e| db_error(e, "find cascade integrity violations"))
        .map(|rows| {
            rows.into_iter()
                .map(|row| (row.live_event_id, row.archived_event_id))
                .collect()
        })
    }

    pub async fn cleanup_session(&mut self, table_name: &str) -> DbResult<()> {
        sqlx::query!("SELECT core.cleanup_cascade_session($1)", table_name)
            .execute(&mut **self.tx)
            .await
            .map_err(|e| db_error(e, "cleanup cascade session"))?;
        Ok(())
    }
}

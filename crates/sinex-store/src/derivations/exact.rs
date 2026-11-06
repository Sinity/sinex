//! Explicit event targets, read in commit order in bounded pages.
use super::*;

pub struct ExactInputs {
    ids: Vec<Uuid>,
    after: i64,
    after_id: Option<Uuid>,
    count: i64,
    bytes: i64,
    finished: bool,
}
impl ExactInputs {
    pub fn new(ids: Vec<EventId>, count: i64, bytes: i64) -> Result<Self> {
        if count <= 0 || bytes <= 0 {
            return Err(crate::Error::Invalid(
                "exact input page budgets must be positive".into(),
            ));
        }
        let mut ids: Vec<_> = ids.into_iter().map(|id| id.0).collect();
        ids.sort_unstable();
        ids.dedup();
        Ok(Self {
            ids,
            after: 0,
            after_id: None,
            count,
            bytes,
            finished: false,
        })
    }
    /// The caller holds its input snapshot across pages. Missing or archived
    /// targets are absent. The consumer compares the returned IDs with its exact required evidence.
    pub async fn next(&mut self, conn: &mut PgConnection) -> Result<Option<Vec<Committed>>> {
        if self.finished {
            return Ok(None);
        }
        let rows = sqlx::query_file!(
            "src/derivations/sql/exact_page.sql",
            &self.ids,
            self.after,
            self.count,
            self.bytes,
            self.after_id
        )
        .fetch_all(conn)
        .await?;
        if rows.is_empty() {
            self.finished = true;
            return Ok(None);
        }
        let mut page = Vec::with_capacity(rows.len());
        for row in rows {
            page.push(committed!(row));
        }
        let last = page.last().expect("nonempty");
        self.after = last.commit_seq;
        self.after_id = Some(last.event.id.0);
        Ok(Some(page))
    }
}
#[cfg(test)]
mod tests;

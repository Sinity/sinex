//! Tables kept only to make reads fast, rebuildable, maintained by their
//! consumers through the commit-log reader, never by database triggers
//! (architecture §3.6, `nbi.3`). Filled by the query and surfaces lane.
use crate::Result;
use sqlx::PgConnection;

/// Bring the search text read model up to `commit_seq`.
pub async fn maintain_search_text(_conn: &mut PgConnection, _up_to: i64) -> Result<()> {
    todo!("architecture §3.6 read.search_text: rebuildable search read model")
}

//! Source polling evidence shared by the existing raw and interpreted readers.
use crate::Result;
use serde_json::Value;
use sqlx::PgConnection;

pub(crate) async fn read(connection: &mut PgConnection, seq: i64) -> Result<Vec<Value>> {
    Ok(sqlx::query_scalar("SELECT jsonb_build_object('input',i.id,'source',i.binding,'status',o.status,'reason',o.reason,'observed_at',o.observed_at,'record_errors',o.record_errors,'value_read_scope',coalesce(o.metadata->'_sinex_read_plan_v1'->'kind',o.metadata->'browser_read_plan_v1'->'kind'),'last_full_value_comparison',coalesce(o.metadata->'_sinex_read_plan_v1'->'checkpoint'->'full_completed_at',o.metadata->'browser_checkpoint_v1'->'full_completed_at',(SELECT c.carry->'full_completed_at' FROM raw.source_cursors c WHERE c.binding=i.binding AND c.input=i.id::text)),'full_value_comparison_due',coalesce(o.metadata->'_sinex_read_plan_v1'->'checkpoint'->'full_due_at',o.metadata->'browser_checkpoint_v1'->'full_due_at',(SELECT c.carry->'full_due_at' FROM raw.source_cursors c WHERE c.binding=i.binding AND c.input=i.id::text)),'retained_record_errors',CASE WHEN i.binding='terminal.zsh-history' THEN coalesce((SELECT (c.position->>'record_errors')::bigint FROM raw.source_cursors c WHERE c.binding=i.binding AND c.input=i.id::text),0) ELSE 0 END) FROM raw.inputs i LEFT JOIN LATERAL (SELECT status,reason,observed_at,record_errors,metadata FROM raw.observations WHERE input_id=i.id AND commit_seq<=$1 ORDER BY seq DESC LIMIT 1) o ON true ORDER BY i.id")
        .bind(seq).fetch_all(connection).await?)
}

// Raw Atuin reads count admitted events and disclose failed records separately.
// Zsh's append chain must retain known omissions even after an empty later poll.
pub(crate) fn admitted_complete(sources: &[Value]) -> bool {
    sources.iter().all(|s| {
        s["status"] == "completed"
            && (s["source"] != crate::ZSH_SOURCE || s["retained_record_errors"].as_i64() == Some(0))
    })
}

// Search and recall assemble interpretations of the input population. An omitted
// record makes that answer incomplete; successful polling is still no proof of
// continuous capture. Empty configured/retained input populations are vacuous.
pub(crate) fn interpreted_complete(sources: &[Value]) -> bool {
    admitted_complete(sources)
        && sources
            .iter()
            .all(|s| s["record_errors"].as_i64() == Some(0))
}

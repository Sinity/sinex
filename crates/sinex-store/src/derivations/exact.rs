//! Explicit event targets share the internal transformed-input boundary.
use super::*;

pub struct ExactInputs {ids:Vec<Uuid>,after:i64,count:i64,bytes:i64,finished:bool,epoch:Option<i64>}
impl ExactInputs {
    pub fn new(ids:Vec<EventId>,count:i64,bytes:i64)->Result<Self>{
        if count<=0||bytes<=0{return Err(crate::Error::Invalid("exact input page budgets must be positive".into()))}
        let mut ids:Vec<_>=ids.into_iter().map(|id|id.0).collect();ids.sort_unstable();ids.dedup();
        Ok(Self{ids,after:0,count,bytes,finished:false,epoch:None})
    }
    /// The caller holds its input snapshot across pages. Missing, archived
    /// or policy-unavailable targets are absent. The
    /// consumer compares the returned IDs with its exact required evidence.
    pub async fn next(&mut self,conn:&mut PgConnection)->Result<Option<Vec<Committed>>>{
        if self.finished{return Ok(None)}
        let rows=sqlx::query!(r#"SELECT e.id AS "id!",e.event_type AS "event_type!",e.source AS "source!",e.material_id,e.anchor,e.output_index AS "output_index!",e.parents,
          e.ts_orig,e.ts_quality AS "ts_quality!",e.payload AS "payload!",e.equivalence_key,e.reading_order AS "reading_order!",e.record_hash AS "record_hash!",
          e.record_state AS "record_state!",e.class AS "class!",e.commit_seq AS "commit_seq!",e.kind_version AS "kind_version!",e.validation AS "validation!",
          privacy.require_active_epoch() AS "policy_epoch!",
          (SELECT replace_rule FROM registry.event_types k WHERE k.name=e.event_type AND k.version=e.kind_version) AS replace_rule
          FROM core.derivation_events e JOIN (
            SELECT commit_seq FROM(SELECT commit_seq,sum(size) OVER(ORDER BY commit_seq)-size AS before
              FROM(SELECT e.commit_seq,pg_column_size(e)::bigint AS size FROM core.derivation_events e
                WHERE e.id=ANY($1) AND e.commit_seq>$2 AND NOT EXISTS(SELECT 1 FROM audit.archived a WHERE a.event_id=e.id)
                ORDER BY e.commit_seq LIMIT $3) selected) bounded WHERE before<$4::bigint) page ON page.commit_seq=e.commit_seq
          ORDER BY e.commit_seq"#,&self.ids,self.after,self.count,self.bytes).fetch_all(conn).await?;
        if rows.is_empty(){self.finished=true;return Ok(None)}
        let mut page=Vec::with_capacity(rows.len());
        for row in rows{
            if self.epoch.is_some_and(|epoch|epoch!=row.policy_epoch){return Err(crate::Error::Invalid("derivation_policy_epoch_changed".into()))}
            self.epoch=Some(row.policy_epoch);page.push(committed!(row));
        }
        self.after=page.last().expect("nonempty").commit_seq;Ok(Some(page))
    }
}
#[cfg(test)] mod tests;

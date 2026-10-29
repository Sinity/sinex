use super::*;
use crate::query::sql::SqlFragment;
use sea_query::Value as Parameter;

/// Independent scaled f64 cosine oracle, including extreme finite magnitudes.
/// Zero-direction vectors have unknown similarity.
pub fn cosine(left:&[f64],right:&[f64])->Option<f64>{
    if left.is_empty()||left.len()!=right.len()||left.iter().chain(right).any(|value|!value.is_finite()){return None}
    let a=left.iter().map(|value|value.abs()).fold(0.0,f64::max);
    let b=right.iter().map(|value|value.abs()).fold(0.0,f64::max);
    if a==0.0||b==0.0{return None}
    let mut dot=0.0;let mut aa=0.0;let mut bb=0.0;
    for (left,right)in left.iter().zip(right){let left=left/a;let right=right/b;dot+=left*right;aa+=left*left;bb+=right*right;}
    Some((dot/aa.sqrt()/bb.sqrt()).clamp(-1.0,1.0))
}

/// Exact ranking expression for one already-filtered candidate. HNSW never
/// decides set membership: absent vectors yield NULL and preserve the row.
pub(crate) fn rank_sql(target_id_sql:&str,reference:Uuid,position:i64)->SqlFragment{
    let mut sql=SqlFragment::text("(SELECT CASE WHEN p.projected IS NOT NULL AND qp.projected IS NOT NULL THEN 1-(p.projected <=> qp.projected) ELSE ");
    sql.push(r#"(WITH pairs AS MATERIALIZED(
        SELECT l.value::numeric AS a,r.value::numeric AS b
        FROM jsonb_array_elements_text(e.payload->'vector') WITH ORDINALITY l(value,n)
        JOIN jsonb_array_elements_text(q.payload->'vector') WITH ORDINALITY r(value,n) USING(n)
      ), scales AS(SELECT max(abs(a)) AS a,max(abs(b)) AS b FROM pairs), score AS(
        SELECT sum(pairs.a/nullif(scales.a,0)*pairs.b/nullif(scales.b,0)) /
          nullif(sqrt(sum(power(pairs.a/nullif(scales.a,0),2))),0) /
          nullif(sqrt(sum(power(pairs.b/nullif(scales.b,0),2))),0) AS value
        FROM pairs CROSS JOIN scales)
      SELECT CASE WHEN abs(value)<'4.9406564584124654e-324'::numeric THEN 0::double precision ELSE value::double precision END FROM score) END
      FROM core.visible_embedding_vectors p
      JOIN core.visible_events e ON e.id=p.embedding_id
      JOIN core.visible_embedding_vectors qp ON qp.embedding_id="#);
    sql.param(Parameter::Uuid(Some(reference))).push(" JOIN core.visible_events q ON q.id=qp.embedding_id WHERE p.target_id=").push(target_id_sql)
        .push(" AND p.space=qp.space AND p.coverage IN ('indexed','exact','fallback') AND e.commit_seq<=").param(Parameter::BigInt(Some(position)))
        .push(" AND NOT audit.is_archived_at(e.id,").param(Parameter::BigInt(Some(position))).push(") AND q.commit_seq<=").param(Parameter::BigInt(Some(position)))
        .push(" AND NOT audit.is_archived_at(q.id,").param(Parameter::BigInt(Some(position))).push(") ORDER BY 1 DESC NULLS LAST,e.commit_seq DESC,e.id DESC LIMIT 1)");
    sql
}

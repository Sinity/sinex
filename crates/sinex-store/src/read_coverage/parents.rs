//! A read stamp certifies its scoped inputs, never arbitrary cited event IDs.
use super::*;
use sinex_model::{Event, IngressFrame, Origin};
use std::collections::BTreeSet;

#[derive(sqlx::FromRow)]
struct Parent {
    id: uuid::Uuid,
    commit_seq: i64,
    event_type: String,
    kind_version: i64,
    source: String,
    source_version: Option<i64>,
    source_fingerprint: Option<String>,
    producer_family: Option<String>,
    class: Option<String>,
    epoch: Option<i64>,
    changed: Option<bool>,
    state: Option<String>,
}

pub(super) async fn validate(conn: &mut PgConnection, frame: &IngressFrame, events: &[Event]) -> Result<()> {
    let Some(proof) = &frame.coverage else { return Ok(()) };
    let claims = open(conn, &proof.stamp).await?;
    let offered: BTreeSet<_> = events.iter().map(|event| event.id.0).collect();
    let ids: BTreeSet<_> = events.iter().flat_map(|event| match &event.origin {
        Origin::Derived { parents } => parents.iter().map(|parent| parent.0).collect::<Vec<_>>(),
        Origin::Record { .. } => vec![],
    }).collect();
    if ids.is_empty() { return Ok(()) }
    let ids: Vec<_> = ids.into_iter().collect();
    let mut retained=checked(conn,&claims,&ids,&offered).await?;
    let referenced: BTreeSet<_> = ids.into_iter().collect();
    // Same-frame references are accepted only in dependency order, and their
    // complete ancestry must end at the certified retained inputs above.
    dag(events.iter().filter_map(|event|match &event.origin {
        Origin::Derived{parents}=>Some((event.id.0,parents.iter().map(|parent|parent.0).collect::<Vec<_>>())),
        Origin::Record{..}=>None,
    }),&mut retained,&referenced)
}

pub(super) async fn inputs(conn:&mut PgConnection,frame:&IngressFrame,ids:&[sinex_model::EventId])->Result<()> {
    if super::validate(conn,frame).await?.is_none(){return Err(Error::Invalid("coverage_inputs_uncertified".into()));}
    let proof=frame.coverage.as_ref().expect("validated above");
    let claims=open(conn,&proof.stamp).await?;
    let ids=ids.iter().map(|id|id.0).collect::<BTreeSet<_>>().into_iter().collect::<Vec<_>>();
    checked(conn,&claims,&ids,&BTreeSet::new()).await.map(drop)
}

async fn checked(conn:&mut PgConnection,claims:&Claims,ids:&[uuid::Uuid],offered:&BTreeSet<uuid::Uuid>)->Result<BTreeSet<uuid::Uuid>> {
    if ids.is_empty(){return Ok(BTreeSet::new())}
    let rows: Vec<Parent> = sqlx::query_as("SELECT e.id,e.commit_seq,e.event_type,e.kind_version,e.source,e.source_version,e.source_fingerprint,e.producer_family,o.class,o.epoch,o.changed,o.state FROM core.events e LEFT JOIN privacy.event_outputs o ON o.event_id=e.id WHERE e.id=ANY($1)")
        .bind(ids).fetch_all(&mut *conn).await?;
    if rows.len() != ids.len() { return Err(Error::Invalid("coverage_cited_parent_missing".into())) }
    let refs: Vec<_> = rows.iter().map(|row| Ok(KindRef {
        name: row.event_type.clone(),
        version: u64::try_from(row.kind_version).map_err(|_| Error::Invalid("coverage_cited_kind_unknown".into()))?,
    })).collect::<Result<_>>()?;
    let registered = sinex_schema::kinds::load(conn, &refs).await?;
    let mut retained = BTreeSet::new();
    for (row, kind) in rows.iter().zip(&refs) {
        let hash = claims.indexed_kinds.iter().find(|(known, _)| known == kind).map(|(_, hash)| hash);
        if hash.is_none() || registered.get(kind).map(|kind| kind.definition_hash()).as_ref() != hash
            || row.epoch != Some(claims.policy_epoch) || row.state.as_deref() != Some("complete") || row.changed != Some(false)
            || row.class.as_ref().is_none_or(|class| class == "sealed" || !claims.classes.contains(class))
        {
            return Err(Error::Invalid("coverage_cited_parent_outside_certified_view".into()));
        }
        if claims.kinds.iter().any(|(raw, _)| raw.name == kind.name) {
            let source = sinex_model::SourceRef { name: row.source.clone(), version: u32::try_from(row.source_version.ok_or_else(|| Error::Invalid("coverage_cited_native_source_unknown".into()))?).map_err(|_| Error::Invalid("coverage_cited_native_source_unknown".into()))? };
            let source = crate::source_declarations::load(conn, &source).await?.ok_or_else(|| Error::Invalid("coverage_cited_native_source_unknown".into()))?;
            if row.producer_family.as_deref() != Some(claims.family.as_str())
                || source.family != claims.family || row.source_fingerprint.as_deref() != Some(source.fingerprint().as_str())
                || !source.allows(kind, hash.expect("checked above"))
            {
                return Err(Error::Invalid("coverage_cited_native_source_unknown".into()));
            }
        }
        if !offered.contains(&row.id) {
            if row.commit_seq > claims.position { return Err(Error::Invalid("coverage_cited_parent_after_certified_position".into())) }
            retained.insert(row.id);
        }
    }
    Ok(retained)
}

fn dag(events:impl IntoIterator<Item=(uuid::Uuid,Vec<uuid::Uuid>)>,retained:&mut BTreeSet<uuid::Uuid>,referenced:&BTreeSet<uuid::Uuid>)->Result<()> {
    for (event,parents) in events {
            if parents.iter().any(|parent| !retained.contains(parent)) {
                return Err(Error::Invalid("coverage_same_frame_parent_not_certified_or_not_in_dag_order".into()));
            }
            if referenced.contains(&event) { retained.insert(event); }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[devkit::test]
    fn same_frame_ancestry_requires_certified_roots_and_dependency_order(){
        let (root,first,second,foreign)=(uuid::Uuid::now_v7(),uuid::Uuid::now_v7(),uuid::Uuid::now_v7(),uuid::Uuid::now_v7());
        let referenced=BTreeSet::from([first,second]);
        assert!(dag([(first,vec![root]),(second,vec![first])],&mut BTreeSet::from([root]),&referenced).is_ok());
        assert!(dag([(second,vec![first]),(first,vec![root])],&mut BTreeSet::from([root]),&referenced).is_err());
        assert!(dag([(first,vec![second]),(second,vec![first])],&mut BTreeSet::from([root]),&referenced).is_err());
        assert!(dag([(first,vec![foreign]),(second,vec![first])],&mut BTreeSet::from([root]),&referenced).is_err());
    }
}

//! Writer-owned producer orders are assigned in batch, inside the admission transaction.
use super::*;
use sinex_model::ReadingOrder;
use std::collections::{BTreeMap,HashSet};
type Key=(String,String,[u8;32]);
struct Seed {equivalence:Option<String>,material:Option<Uuid>,anchor:Option<i64>,output:Option<i64>}

pub(super) async fn assign(conn:&mut PgConnection,batch:&Batch,events:&mut[Event])->Result<()> {
    let BatchKind::Ingress(frame)=&batch.kind else{return Ok(());};
    if events.is_empty(){return Ok(());}
    let assigned:HashSet<_>=frame.assign_orders.iter().copied().collect();
    let mut seeds=BTreeMap::<Key,Seed>::new();let mut offered=Vec::with_capacity(events.len());
    for event in events.iter() {
        let record_key=match (&event.equivalence_key,&event.origin) {
            (Some(key),_)=>format!("key:{}",key.0),
            (None,Origin::Record{location,output_index})=>format!("record:{}:{}:{}",location.material,location.anchor,output_index),
            _=>return Err(crate::Error::Invalid("producer reading order needs a stable record key".into())),
        };
        // Only this digest survives excision; original record keys are not retained by the counter.
        let key=(event.source.clone(),event.event_type.clone(),*blake3::hash(record_key.as_bytes()).as_bytes());
        let (material,anchor,output)=match &event.origin {
            Origin::Record{location,output_index}=>(Some(location.material.0),Some(i64::try_from(location.anchor).map_err(|_|crate::Error::Invalid("record anchor exceeds stored range".into()))?),Some(i64::try_from(*output_index).map_err(|_|crate::Error::Invalid("record output exceeds stored range".into()))?)),
            _=>(None,None,None),
        };
        seeds.entry(key.clone()).or_insert_with(||Seed{equivalence:event.equivalence_key.as_ref().map(|key|key.0.clone()),material,anchor,output});offered.push(key);
    }
    let sources=seeds.keys().map(|key|key.0.clone()).collect::<Vec<_>>();let kinds=seeds.keys().map(|key|key.1.clone()).collect::<Vec<_>>();let hashes=seeds.keys().map(|key|key.2.to_vec()).collect::<Vec<_>>();
    let equivalence=seeds.values().map(|seed|seed.equivalence.clone()).collect::<Vec<_>>();let materials=seeds.values().map(|seed|seed.material).collect::<Vec<_>>();let anchors=seeds.values().map(|seed|seed.anchor).collect::<Vec<_>>();let outputs=seeds.values().map(|seed|seed.output).collect::<Vec<_>>();
    // The session writer claim serializes this seed and upsert with every other admission.
    // Missing counters seed from retained history through two set-based joins, not a query per output.
    let rows=sqlx::query!(r#"WITH input AS (
      SELECT * FROM unnest($1::text[],$2::text[],$3::bytea[],$4::text[],$5::uuid[],$6::bigint[],$7::bigint[])
        AS i(source,event_type,key_hash,equivalence_key,material_id,anchor,output_index)),
      missing AS (SELECT i.* FROM input i LEFT JOIN core.producer_orders c USING(source,event_type,key_hash) WHERE c.key_hash IS NULL),
      history AS (SELECT source,event_type,key_hash,max(reading_order) AS reading_order FROM (
        SELECT m.source,m.event_type,m.key_hash,e.reading_order FROM missing m JOIN core.events e
          ON e.source=m.source AND e.event_type=m.event_type AND e.equivalence_key=m.equivalence_key WHERE m.equivalence_key IS NOT NULL
        UNION ALL
        SELECT m.source,m.event_type,m.key_hash,e.reading_order FROM missing m JOIN core.events e
          ON e.source=m.source AND e.event_type=m.event_type AND e.material_id=m.material_id AND e.anchor=m.anchor AND e.output_index=m.output_index WHERE m.equivalence_key IS NULL
      ) readings GROUP BY source,event_type,key_hash)
      SELECT i.source AS "source!",i.event_type AS "event_type!",i.key_hash AS "key_hash!",coalesce(c.reading_order,h.reading_order) AS base
      FROM input i LEFT JOIN core.producer_orders c USING(source,event_type,key_hash) LEFT JOIN history h USING(source,event_type,key_hash)"#,
      &sources,&kinds,&hashes,&equivalence as &[Option<String>],&materials as &[Option<Uuid>],&anchors as &[Option<i64>],&outputs as &[Option<i64>]).fetch_all(&mut *conn).await?;
    let mut highwaters=BTreeMap::<Key,Option<i64>>::new();
    for row in rows {
        let hash:[u8;32]=row.key_hash.try_into().map_err(|_|crate::Error::Invalid("stored producer order hash has wrong length".into()))?;
        highwaters.insert((row.source,row.event_type,hash),row.base);
    }
    for (event,key) in events.iter_mut().zip(offered) {
        let previous=highwaters.get_mut(&key).ok_or_else(||crate::Error::Invalid("producer order seed is missing".into()))?;
        if let Some(coverage)=&frame.coverage
            && frame.native_source.is_none() && matches!(event.origin,Origin::Derived{..})
            && (assigned.contains(&event.id)||event.reading_order.0!=coverage.position||previous.is_some_and(|previous|event.reading_order.0<previous)) {
                return Err(crate::Error::Invalid("canonical completed position is older than its authoritative highwater or differs from its proof".into()));
        }
        if assigned.contains(&event.id){event.reading_order=ReadingOrder(previous.unwrap_or(-1).checked_add(1).ok_or_else(||crate::Error::Invalid("producer reading order exhausted its stored integer range".into()))?);}
        *previous=Some(previous.map_or(event.reading_order.0,|previous|previous.max(event.reading_order.0)));
    }
    let orders=highwaters.values().map(|order|order.expect("every distinct key was offered")).collect::<Vec<_>>();
    sqlx::query!(r#"INSERT INTO core.producer_orders(source,event_type,key_hash,reading_order)
      SELECT * FROM unnest($1::text[],$2::text[],$3::bytea[],$4::bigint[]) WHERE true
      ON CONFLICT(source,event_type,key_hash) DO UPDATE SET reading_order=greatest(core.producer_orders.reading_order,EXCLUDED.reading_order)"#,&sources,&kinds,&hashes,&orders).execute(conn).await?;
    Ok(())
}

//! Producer reads use operator grants attached to the authenticated NATS identity.
use super::*;
use serde::{Deserialize, de::DeserializeOwned};
use sinex_model::Class;
use sinex_protocol::{ConfirmedEvent, FetchEvent, FetchEventReply, FetchMaterial, FetchMaterialReply, FetchPayload, FetchPayloadReply, FetchRefusalKind, PayloadReference, Withheld};
use std::collections::BTreeMap;

#[derive(Clone, Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Grants {
    pub kind_prefixes:Vec<String>,
    pub classes:Vec<Class>,
}
pub(crate) type ProducerGrants=BTreeMap<String,Grants>;
impl Grants {
    fn kind(&self,name:&str)->bool {
        self.kind_prefixes.iter().any(|prefix| {
            let prefix=prefix.trim_end_matches('*').trim_end_matches('.');
            name==prefix || name.strip_prefix(prefix).is_some_and(|tail|tail.starts_with('.'))
        })
    }
    fn class(&self,class:Class)->bool {self.classes.contains(&class)}
    fn intersects(&self,other:&Self)->bool {
        self.kind_prefixes.iter().any(|left|other.kind(left.trim_end_matches('*').trim_end_matches('.')))
        ||other.kind_prefixes.iter().any(|right|self.kind(right.trim_end_matches('*').trim_end_matches('.')))
    }
}
pub(crate) fn load(path:Option<&std::path::Path>)->Result<ProducerGrants,String> {
    let Some(path)=path else {return Ok(BTreeMap::new());};
    let bytes=std::fs::read(path).map_err(|error|format!("{}: {error}",path.display()))?;
    let grants:ProducerGrants=serde_json::from_slice(&bytes).map_err(|error|format!("{}: {error}",path.display()))?;
    for (producer,grant) in &grants {
        if !sinex_protocol::nats::valid_producer(producer) {return Err("invalid producer in read grants".into());}
        for prefix in &grant.kind_prefixes {
            let prefix=prefix.trim_end_matches('*').trim_end_matches('.');
            if prefix.is_empty() || prefix.split('.').any(|part|part.is_empty()||!part.bytes().all(|byte|byte.is_ascii_alphanumeric()||byte==b'_'||byte==b'-')) {
                return Err(format!("invalid kind prefix in grants for {producer}"));
            }
        }
    }
    Ok(grants)
}
struct Refusal {kind:FetchRefusalKind,reason:String}
type ReadResult<T>=Result<T,Refusal>;
fn denied()->Refusal {Refusal{kind:FetchRefusalKind::Denied,reason:"producer grants do not allow this read".into()}}
fn missing()->Refusal {Refusal{kind:FetchRefusalKind::Missing,reason:"requested retained data is unavailable".into()}}
fn unavailable(error:impl std::fmt::Display)->Refusal {Refusal{kind:FetchRefusalKind::Unavailable,reason:error.to_string()}}
fn field<T:DeserializeOwned>(value:&Value,name:&str)->ReadResult<T> {serde_json::from_value(value[name].clone()).map_err(unavailable)}
impl Requests {
    fn grants(&self,producer:&str)->ReadResult<&Grants> {
        self.read_grants.get(producer).filter(|grant|!grant.kind_prefixes.is_empty()&&!grant.classes.is_empty()).ok_or_else(denied)
    }
    async fn event_for(&self,producer:&str,id:EventId)->ReadResult<ConfirmedEvent> {
        let grant=self.grants(producer)?;
        let mut conn=self.reads.acquire().await.map_err(unavailable)?;
        let classes=grant.classes.iter().map(|class|serde_json::to_value(class).expect("classes serialize").as_str().expect("class string").to_owned()).collect::<Vec<_>>().join(",");
        sinex_store::query::set_grants(&mut conn,&classes).await.map_err(unavailable)?;
        let view=sinex_store::query::show(&mut conn,&sinex_model::query::Reference::Event(id.0)).await.map_err(|error|match error {
            sinex_store::Error::Invalid(_)=>missing(),
            other=>unavailable(other),
        })?;
        let event_type:String=field(&view,"event_type")?;
        let class:Class=field(&view,"class")?;
        if !grant.kind(&event_type)||!grant.class(class) {return Err(denied());}
        if class==Class::Sealed {return Err(Refusal{kind:FetchRefusalKind::Locked,reason:"sealed producer reads require an operator unlock".into()});}
        let view=crate::api::mask::mask_json(view).map_err(unavailable)?;
        let location=if view["material"].is_null() {None} else {Some(sinex_model::RecordLocation{material:field(&view,"material")?,anchor:field(&view,"offset")?})};
        Ok(ConfirmedEvent {
            kind_version:field(&view,"kind_version")?,validation:field(&view,"validation")?,commit_seq:field(&view,"commit_seq")?,id:field(&view,"id")?,event_type,source:field(&view,"source")?,run:field(&view,"run")?,location,output_index:field(&view,"output")?,parents:field::<Option<Vec<EventId>>>(&view,"parents")?.unwrap_or_default(),
            ts_orig:field(&view,"when")?,ts_quality:field(&view,"time_quality")?,ts_coided:field(&view,"seen")?,ts_persisted:field(&view,"written")?,record_state:field(&view,"record_state")?,class,equivalence_key:field(&view,"key")?,reading_order:field(&view,"reading_order")?,record_hash:field(&view,"record_hash")?,payload:Some(view["payload"].clone()),withheld:None,
        })
    }
    pub(super) async fn fetch_event(&self,producer:&str,request:&FetchEvent)->FetchEventReply {
        match self.event_for(producer,request.event).await {
            Ok(event)=>{
                let mut reply=FetchEventReply{event:Some(event),..Default::default()};
                if serde_json::to_vec(&reply).is_ok_and(|bytes|bytes.len() as u64+1024>self.max_payload) {
                    let event=reply.event.as_mut().expect("event set");
                    let payload=serde_json::to_vec(&event.payload.take().expect("visible payload")).expect("JSON serializes");
                    event.withheld=Some(Withheld::Size{payload_bytes:payload.len() as u64,limit:self.max_payload});
                    reply.payload_ref=Some(PayloadReference{event:event.id,length:payload.len() as u64,hash:format!("blake3:{}",blake3::hash(&payload).to_hex())});
                }
                reply
            }
            Err(error)=>FetchEventReply{refusal:Some(error.reason),refusal_kind:Some(error.kind),..Default::default()},
        }
    }
    pub(super) async fn fetch_payload(&self,producer:&str,request:&FetchPayload)->FetchPayloadReply {
        let result:ReadResult<_>=async {
            let event=self.event_for(producer,request.event).await?;
            let payload=serde_json::to_vec(&event.payload.expect("visible payload")).map_err(unavailable)?;
            let hash=format!("blake3:{}",blake3::hash(&payload).to_hex());
            if hash!=request.hash {return Err(Refusal{kind:FetchRefusalKind::Changed,reason:"visible payload changed; fetch its event again".into()});}
            let start=usize::try_from(request.offset).ok().filter(|offset|*offset<=payload.len()).ok_or_else(||Refusal{kind:FetchRefusalKind::Invalid,reason:"payload offset is outside its length".into()})?;
            let transfer=self.max_payload.saturating_sub(2048)*3/4;
            if transfer==0 {return Err(unavailable("NATS message limit cannot hold a payload transfer"));}
            let length=request.length.min(transfer).min((payload.len()-start) as u64) as usize;
            Ok(FetchPayloadReply{offset:request.offset,total:payload.len() as u64,hash,data:base64::engine::general_purpose::STANDARD.encode(&payload[start..start+length]),..Default::default()})
        }.await;
        result.unwrap_or_else(|error|FetchPayloadReply{refusal:Some(error.reason),refusal_kind:Some(error.kind),..Default::default()})
    }
    pub(super) async fn fetch_material(&self,producer:&str,request:&FetchMaterial)->FetchMaterialReply {
        let result:ReadResult<_>=async {
            let grant=self.grants(producer)?;
            let mut conn=self.reads.acquire().await.map_err(unavailable)?;
            let owner=sqlx::query_scalar!("SELECT producer FROM core.ingress_frames WHERE material_id=$1 AND refusal IS NULL ORDER BY number LIMIT 1",request.material.0).fetch_optional(&mut *conn).await.map_err(unavailable)?.ok_or_else(missing)?;
            let owner_grant=self.read_grants.get(&owner).ok_or_else(denied)?;
            if !grant.intersects(owner_grant) {return Err(denied());}
            let classes=grant.classes.iter().map(|class|serde_json::to_value(class).unwrap().as_str().unwrap().to_owned()).collect::<Vec<_>>().join(",");
            sinex_store::query::set_grants(&mut conn,&classes).await.map_err(unavailable)?;
            let events=sqlx::query!(r#"SELECT event_type AS "event_type!",class AS "class!" FROM core.visible_events WHERE material_id=$1"#,request.material.0).fetch_all(&mut *conn).await.map_err(unavailable)?;
            if events.is_empty() && !(grant.class(Class::Open)&&grant.class(Class::Guarded)) {return Err(denied());}
            for event in events {
                let class:Class=serde_json::from_value(Value::String(event.class)).map_err(unavailable)?;
                if !grant.kind(&event.event_type)||!grant.class(class) {return Err(denied());}
                if class==Class::Sealed {return Err(Refusal{kind:FetchRefusalKind::Locked,reason:"sealed material requires an operator unlock".into()});}
            }
            let material=sinex_store::material_bytes::reference(&mut conn,request.material).await.map_err(unavailable)?.ok_or_else(missing)?;
            let input=sqlx::query!("SELECT i.kind,i.locator,i.host FROM raw.materials m JOIN raw.inputs i ON i.id=m.input_id WHERE m.id=$1",request.material.0).fetch_optional(&mut *conn).await.map_err(unavailable)?.ok_or_else(missing)?;
            let descriptor=sqlx::query_scalar!("SELECT descriptor FROM core.blob_extents WHERE hash=$1 AND NOT deleting",material.blob).fetch_optional(&mut *conn).await.map_err(unavailable)?.ok_or_else(missing)?;
            if self.blobs.is_sealed(&material.blob).map_err(unavailable)? {return Err(Refusal{kind:FetchRefusalKind::Locked,reason:"sealed material requires an operator unlock".into()});}
            Ok(FetchMaterialReply{material:Some(MaterialFrame{id:material.id,input:sinex_model::InputRef{kind:input.kind,locator:input.locator,host:input.host},blob:material.blob,length:material.length,parser:material.parser,parser_version:material.parser_version,part_index:material.part_index,previous_part:material.previous_part,manifest:material.manifest}),extents:vec![descriptor],..Default::default()})
        }.await;
        result.unwrap_or_else(|error|FetchMaterialReply{refusal:Some(error.reason),refusal_kind:Some(error.kind),..Default::default()})
    }
}

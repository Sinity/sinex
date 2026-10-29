//! The daemon provisions one native, grant-filtered durable per authenticated identity.
use super::*;
use sinex_protocol::{Confirmed,ConfirmedConsumer,ConfirmedConsumerReply};

impl Requests {
    pub(super) async fn confirmed_consumer(&self,producer:&str,request:&ConfirmedConsumer)->ConfirmedConsumerReply {
        let result:Result<_,String>=async {
            if request.after<0 || request.cache.is_empty() {return Err("consumer needs a nonnegative position and an index cache identity".into());}
            if let Some(basis)=&request.basis {self.verify_replay_basis(producer,&request.cache,request.after,basis).await?;}
            let grant=self.grants(producer).map_err(|_|"producer has no confirmed read grants".to_owned())?;
            let (filters,scope)=grant.consumer_scope(producer,&self.subjects);
            let cache=blake3::hash(request.cache.as_bytes()).to_hex().to_string();
            let mut conn=self.reads.acquire().await.map_err(|error|error.to_string())?;
            let head=sinex_store::query::current_seq(&mut conn).await.map_err(|error|error.to_string())?;
            drop(conn);
            let stream_name=self.subjects.confirmed_stream();let name=self.subjects.consumer_name(&scope);
            if request.status_only {return Ok(ConfirmedConsumerReply{position:Some(head),stream:stream_name,consumer:name,scope,..Default::default()});}
            if request.after>head {return Ok(ConfirmedConsumerReply{position:Some(head),scope,gap:Some("requested position exceeds durable commit history".into()),..Default::default()});}
            let _guard=self.consumer_admin.lock().await;
            let js=jetstream::new(self.client.clone());crate::publisher::ensure_stream(&js,&self.subjects).await?;
            let stream=js.get_stream(&stream_name).await.map_err(|error|error.to_string())?;
            let config=consumer::pull::Config {
                durable_name:Some(name.clone()),filter_subjects:filters,ack_policy:consumer::AckPolicy::Explicit,
                deliver_policy:consumer::DeliverPolicy::All,ack_wait:Duration::from_secs(60),max_deliver:-1,
                metadata:std::collections::HashMap::from([("sinex_scope".into(),scope.clone()),("sinex_cache".into(),cache.clone())]),
                ..Default::default()
            };
            let current=stream.get_or_create_consumer(&name,config.clone()).await.map_err(|error|error.to_string())?;
            let info=current.cached_info();
            // A restored index may be behind the native ack floor. Reuse only
            // when the exact acknowledged barrier proves the requested scalar.
            let same=info.config.metadata.get("sinex_scope")==Some(&scope)&&info.config.metadata.get("sinex_cache")==Some(&cache)&&info.config.filter_subjects==config.filter_subjects;
            let proven=if same && request.after>0 && info.ack_floor.stream_sequence>0 {
                stream.get_raw_message(info.ack_floor.stream_sequence).await.ok().and_then(|message|serde_json::from_slice::<Confirmed>(&message.payload).ok()).is_some_and(|message|matches!(message,Confirmed::PositionComplete{commit_seq} if commit_seq==request.after))
            } else {false};
            if !proven {
                stream.delete_consumer(&name).await.map_err(|error|error.to_string())?;
                stream.create_consumer(config).await.map_err(|error|error.to_string())?;
            }
            Ok(ConfirmedConsumerReply{position:Some(head),stream_instance:Some(crate::publisher::stream_instance(&stream)),stream:stream_name,consumer:name,scope,refusal:None,gap:None})
        }.await;
        result.unwrap_or_else(|error|ConfirmedConsumerReply{refusal:Some(error),..Default::default()})
    }
}

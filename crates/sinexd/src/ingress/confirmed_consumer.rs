//! The daemon provisions one native, grant-filtered durable per authenticated identity.
use super::*;
use crate::Error;
use sinex_protocol::{Confirmed, ConfirmedConsumer, ConfirmedConsumerReply};

impl Requests {
    pub(super) async fn confirmed_consumer(
        &self,
        producer: &str,
        request: &ConfirmedConsumer,
    ) -> ConfirmedConsumerReply {
        let result:Result<_>=async {
            if request.after<0 || request.cache.is_empty() {return Err(Error::internal("consumer needs a nonnegative position and an index cache identity"));}
            let grant=self.grants(producer).map_err(|_| Error::refused("producer has no confirmed read grants"))?;
            let (filters,scope)=grant.consumer_scope(producer,&self.subjects);
            let cache=blake3::hash(request.cache.as_bytes()).to_hex().to_string();
            let mut conn=self.reads.acquire().await?;
            let head=sinex_store::query::current_seq(&mut conn).await?;
            drop(conn);
            let stream_name=self.subjects.confirmed_stream();let name=self.subjects.consumer_name(&scope);
            let js=jetstream::new(self.client.clone());
            let stream=js.get_stream(&stream_name).await?;
            let origin=crate::publisher::history_origin(&stream)?;
            let mut conn=self.reads.acquire().await?;
            origin.validate(&mut conn).await?;
            drop(conn);
            let invalidated=if let Some(basis)=&request.basis {
                match self.verify_replay_basis(producer,&request.cache,request.after,basis).await {
                    Ok(())=>false,
                    Err(super::coverage::Failure::Invalidated(_))=>true,
                    Err(error)=>return Err(Error::from(error)),
                }
            } else {false};
            if invalidated || (origin.requires_projection && request.basis.as_ref().is_none_or(|basis|basis.position<origin.position)) {
                return Ok(ConfirmedConsumerReply::ProjectionRequired {origin:origin.position,position:head,scope,stream_instance:crate::publisher::stream_instance(&stream)});
            }
            if request.status_only {return Ok(ConfirmedConsumerReply::Provisioned{position:head,stream:stream_name,stream_instance:None,consumer:name,scope});}
            if request.after>head {return Ok(ConfirmedConsumerReply::Gap{position:head,scope,reason:"requested position exceeds durable commit history".into()});}
            let _guard=self.consumer_admin.lock().await;
            let config=consumer::pull::Config {
                durable_name:Some(name.clone()),filter_subjects:filters,ack_policy:consumer::AckPolicy::Explicit,
                deliver_policy:consumer::DeliverPolicy::All,ack_wait:Duration::from_secs(60),max_deliver:-1,
                metadata:std::collections::HashMap::from([("sinex_scope".into(),scope.clone()),("sinex_cache".into(),cache.clone())]),
                ..Default::default()
            };
            let current=stream.get_or_create_consumer(&name,config.clone()).await?;
            let info=current.cached_info();
            // A restored index may be behind the native ack floor. Reuse only
            // when the exact acknowledged barrier proves the requested scalar.
            let same=info.config.metadata.get("sinex_scope")==Some(&scope)&&info.config.metadata.get("sinex_cache")==Some(&cache)&&info.config.filter_subjects==config.filter_subjects;
            let proven=if same && request.after>0 && info.ack_floor.stream_sequence>0 {
                stream.get_raw_message(info.ack_floor.stream_sequence).await.ok().and_then(|message|serde_json::from_slice::<Confirmed>(&message.payload).ok()).is_some_and(|message|matches!(message,Confirmed::PositionComplete{commit_seq} if commit_seq==request.after))
            } else {false};
            if !proven {
                stream.delete_consumer(&name).await?;
                stream.create_consumer(config).await?;
            }
            Ok(ConfirmedConsumerReply::Provisioned{position:head,stream_instance:Some(crate::publisher::stream_instance(&stream)),stream:stream_name,consumer:name,scope})
        }.await;
        result.unwrap_or_else(|error| ConfirmedConsumerReply::Refused {
            reason: error.to_string(),
        })
    }
}

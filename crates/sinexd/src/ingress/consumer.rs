//! The daemon provisions one native, grant-filtered durable per authenticated identity.
use super::*;
use sinex_protocol::{Confirmed,ConfirmedConsumer,ConfirmedConsumerReply};

impl Requests {
    pub(super) async fn confirmed_consumer(&self,producer:&str,request:&ConfirmedConsumer)->ConfirmedConsumerReply {
        let result:Result<_,String>=async {
            if request.after<0 || request.cache.is_empty() {return Err("consumer needs a nonnegative position and an index cache identity".into());}
            let grant=self.grants(producer).map_err(|_|"producer has no confirmed read grants".to_owned())?;
            let mut prefixes=grant.kind_prefixes.iter().map(|prefix|prefix.trim_end_matches('*').trim_end_matches('.').to_owned()).collect::<Vec<_>>();
            prefixes.sort();prefixes.dedup();
            let all=prefixes.clone();prefixes.retain(|prefix|!all.iter().any(|other|other!=prefix&&prefix.starts_with(&format!("{other}."))));
            let mut filters=vec![self.subjects.confirmed_control()];
            for class in &grant.classes {
                let class=serde_json::to_value(class).expect("class serializes");let class=class.as_str().expect("class string");
                for prefix in &prefixes {
                    for subject in [self.subjects.confirmed_event(class,prefix),self.subjects.confirmed_lifecycle(class,prefix)] {
                        filters.push(format!("{subject}.>"));filters.push(subject);
                    }
                }
            }
            filters.sort();filters.dedup();
            let scope=blake3::hash(&serde_json::to_vec(&filters).expect("filters serialize")).to_hex().to_string();
            let cache=blake3::hash(request.cache.as_bytes()).to_hex().to_string();
            let mut conn=self.reads.acquire().await.map_err(|error|error.to_string())?;
            let head=sqlx::query_scalar!("SELECT value FROM core.counters WHERE name='commit_seq'").fetch_one(&mut *conn).await.map_err(|error|error.to_string())?;
            drop(conn);
            if request.after>head {return Ok(ConfirmedConsumerReply{scope,gap:Some("requested position exceeds durable commit history".into()),..Default::default()});}
            let stream_name=self.subjects.confirmed_stream();let name=self.subjects.consumer_name(producer);
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
            Ok(ConfirmedConsumerReply{stream:stream_name,consumer:name,scope,refusal:None,gap:None})
        }.await;
        result.unwrap_or_else(|error|ConfirmedConsumerReply{refusal:Some(error),..Default::default()})
    }
}

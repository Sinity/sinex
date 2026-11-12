//! The daemon provisions one native, grant-filtered durable per authenticated identity.
use super::*;
use crate::Error;
use sinex_protocol::{ConfirmedConsumer, ConfirmedConsumerReply};

impl Requests {
    pub(super) async fn confirmed_consumer(
        &self,
        producer: &str,
        request: &ConfirmedConsumer,
    ) -> ConfirmedConsumerReply {
        let result: Result<_> = async {
            if request.after < 0 || request.cache.is_empty() {
                return Err(Error::internal(
                    "consumer needs a nonnegative position and an index cache identity",
                ));
            }
            let grant = self
                .grants(producer)
                .map_err(|_| Error::refused("producer has no confirmed read grants"))?;
            let (filters, scope) = grant.consumer_scope(producer, &self.subjects);
            let cache = blake3::hash(request.cache.as_bytes()).to_hex().to_string();
            let stream_name = self.subjects.confirmed_stream();
            let name = self.subjects.consumer_name(&scope);
            let js = jetstream::new(self.client.clone());
            let stream = js.get_stream(&stream_name).await?;
            let origin = crate::publisher::history_origin(&stream)?;
            let instance = crate::publisher::stream_instance(&stream);
            let mut conn = self.reads.acquire().await?;
            origin.validate(&mut conn).await?;
            let head = sinex_store::query::current_seq(&mut conn).await?;
            drop(conn);
            let projection_required = || ConfirmedConsumerReply::ProjectionRequired {
                origin: origin.position,
                position: head,
                scope: scope.clone(),
                stream_instance: instance.clone(),
            };
            let basis = if let Some(basis) = &request.basis {
                match self
                    .verify_replay_basis(producer, &request.cache, request.after, basis)
                    .await
                {
                    Ok(claims) => Some(claims),
                    Err(super::coverage::Failure::Invalidated(_)) => {
                        return Ok(projection_required());
                    }
                    Err(error) => return Err(Error::from(error)),
                }
            } else {
                None
            };
            if origin.requires_projection
                && basis
                    .as_ref()
                    .is_none_or(|basis| basis.position < origin.position)
            {
                return Ok(projection_required());
            }
            if request.status_only {
                return Ok(ConfirmedConsumerReply::Provisioned {
                    position: head,
                    stream: stream_name,
                    stream_instance: None,
                    consumer: name,
                    scope,
                });
            }
            if request.after > head {
                return Ok(ConfirmedConsumerReply::Gap {
                    position: head,
                    scope,
                    reason: "requested position exceeds durable commit history".into(),
                });
            }
            let start = if let Some(receipt) = &request.completion {
                match super::native_prefix::completion(
                    &stream,
                    receipt,
                    request.after,
                    &self.subjects.confirmed_control(),
                )
                .await
                {
                    Ok(()) => receipt.stream_sequence,
                    Err(super::coverage::Failure::Invalidated(_)) => {
                        return Ok(projection_required());
                    }
                    Err(error) => return Err(Error::from(error)),
                }
            } else if let Some(basis) = &basis {
                if basis.position != request.after {
                    return Ok(projection_required());
                }
                match super::native_prefix::projection_start(&stream, &basis.native_prefix).await {
                    Ok(start) => start,
                    Err(super::coverage::Failure::Invalidated(_)) => {
                        return Ok(projection_required());
                    }
                    Err(error) => return Err(Error::from(error)),
                }
            } else if request.after == 0 {
                1
            } else {
                return Ok(projection_required());
            };
            let _guard = self.consumer_admin.lock().await;
            let config = consumer::pull::Config {
                durable_name: Some(name.clone()),
                filter_subjects: filters,
                ack_policy: consumer::AckPolicy::Explicit,
                deliver_policy: consumer::DeliverPolicy::ByStartSequence {
                    start_sequence: start,
                },
                ack_wait: Duration::from_secs(60),
                max_deliver: -1,
                metadata: std::collections::HashMap::from([
                    ("sinex_scope".into(), scope.clone()),
                    ("sinex_cache".into(), cache.clone()),
                ]),
                ..Default::default()
            };
            let current = match stream.consumer_info(&name).await {
                Ok(info) => Some(info),
                Err(error)
                    if error.kind() == jetstream::context::ConsumerInfoErrorKind::NotFound =>
                {
                    None
                }
                Err(error) => return Err(Error::nats(error)),
            };
            // Reoffer the completed marker itself. A seeded native floor or a
            // previously ACKed marker is not a delivery in this connection.
            if current.is_some() {
                stream.delete_consumer(&name).await?;
            }
            stream.create_consumer(config).await?;
            Ok(ConfirmedConsumerReply::Provisioned {
                position: head,
                stream_instance: Some(instance),
                stream: stream_name,
                consumer: name,
                scope,
            })
        }
        .await;
        result.unwrap_or_else(|error| ConfirmedConsumerReply::Refused {
            reason: error.to_string(),
        })
    }
}

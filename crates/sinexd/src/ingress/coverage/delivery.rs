//! Validate a delivered barrier independently of consumer acknowledgments.
use super::*;
use crate::Result;

pub(super) async fn verify(
    client: &async_nats::Client,
    subjects: &Subjects,
    grant: &fetch::Grants,
    claims: &Claims,
    completion: &sinex_protocol::DeliveredCompletion,
    position: i64,
) -> Result<(), Failure> {
    let js = crate::publisher::context(client.clone());
    let stream = js
        .get_stream(subjects.confirmed_stream())
        .await
        .map_err(Error::nats)?;
    if completion.stream_instance != claims.instance
        || crate::publisher::stream_instance(&stream) != claims.instance
        || completion.stream_sequence == 0
    {
        return Err(Failure::Invalidated(
            "coverage_native_incarnation_changed".into(),
        ));
    }
    let consumer = stream
        .get_consumer::<consumer::pull::Config>(&subjects.consumer_name(&claims.scope))
        .await
        .map_err(Error::nats)?;
    let info = consumer.cached_info();
    let (filters, scope) = grant.consumer_scope(&claims.producer, subjects);
    let cache = blake3::hash(claims.cache.as_bytes()).to_hex().to_string();
    if scope != claims.scope
        || info.config.metadata.get("sinex_scope") != Some(&scope)
        || info.config.metadata.get("sinex_cache") != Some(&cache)
        || info.config.filter_subjects != filters
        || !matches!(info.config.deliver_policy, consumer::DeliverPolicy::ByStartSequence { start_sequence } if start_sequence <= completion.stream_sequence)
        || info.delivered.consumer_sequence == 0
        || info.config.ack_policy != consumer::AckPolicy::Explicit
        || completion.stream_sequence > info.delivered.stream_sequence
    {
        return Err(Failure::Invalidated(
            "coverage_native_delivery_window_unproven".into(),
        ));
    }
    super::super::native_prefix::completion(
        &stream,
        completion,
        position,
        &subjects.confirmed_control(),
    )
    .await?;
    Ok(())
}

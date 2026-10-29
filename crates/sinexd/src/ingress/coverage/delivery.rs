//! Validate a delivered barrier independently of consumer acknowledgments.
use super::*;

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
        .map_err(|error| error.to_string())?;
    if completion.stream_instance != claims.instance
        || crate::publisher::stream_instance(&stream) != claims.instance
        || completion.stream_sequence == 0
    {
        return Err(Failure::Invalidated("coverage_native_incarnation_changed".into()));
    }
    let consumer = stream
        .get_consumer::<consumer::pull::Config>(&subjects.consumer_name(&claims.scope))
        .await
        .map_err(|error| error.to_string())?;
    let info = consumer.cached_info();
    let (filters, scope) = grant.consumer_scope(&claims.producer, subjects);
    let cache = blake3::hash(claims.cache.as_bytes()).to_hex().to_string();
    if scope != claims.scope
        || info.config.metadata.get("sinex_scope") != Some(&scope)
        || info.config.metadata.get("sinex_cache") != Some(&cache)
        || info.config.filter_subjects != filters
        || info.config.deliver_policy != consumer::DeliverPolicy::All
        || info.config.ack_policy != consumer::AckPolicy::Explicit
        || completion.stream_sequence > info.delivered.stream_sequence
    {
        return Err(Failure::Invalidated("coverage_native_delivery_window_unproven".into()));
    }
    let message = stream
        .get_raw_message(completion.stream_sequence)
        .await
        .map_err(|error| error.to_string())?;
    if message.subject.as_str() != subjects.confirmed_control()
        || !matches!(serde_json::from_slice::<sinex_protocol::Confirmed>(&message.payload),
            Ok(sinex_protocol::Confirmed::PositionComplete { commit_seq }) if commit_seq == position)
    {
        return Err(Failure::Invalidated("coverage_native_completed_position_unproven".into()));
    }
    Ok(())
}

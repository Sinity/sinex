use super::*;

#[devkit::test]
async fn context_capability_is_default_deny_and_rejects_forged_identity(db:devkit::pg::TestDatabase) {
    let grants=crate::ingress::fetch::ProducerGrants::from([("allowed".into(),crate::ingress::fetch::Grants{indexed_catalog:None, public_context:true,..Default::default()})]);
    let harness=Harness::start_with_grants(db,grants).await;
    // The synthetic server has no configured policy, so an authorized caller
    // receives Unavailable. A different authenticated identity is Refused.
    let allowed:sinex_protocol::PublicContext=sinex_transport::replies::request_json(&harness.client,harness.subjects.public_context("allowed"),"allowed",&sinex_protocol::PublicContextRequest::default(),Duration::from_secs(5)).await.unwrap();
    assert!(matches!(allowed,sinex_protocol::PublicContext::Unavailable{..}));
    let denied:sinex_protocol::PublicContext=sinex_transport::replies::request_json(&harness.client,harness.subjects.public_context("denied"),"denied",&sinex_protocol::PublicContextRequest::default(),Duration::from_secs(5)).await.unwrap();
    assert!(matches!(denied,sinex_protocol::PublicContext::Refused{..}));
    let forged:sinex_protocol::PublicContext=sinex_transport::replies::request_json(&harness.client,harness.subjects.public_context("allowed"),"allowed",&json!({"producer":"denied"}),Duration::from_secs(5)).await.unwrap();
    assert!(matches!(forged,sinex_protocol::PublicContext::Refused{..}));
    drop(harness);
}

//! Private owner-reference indexes. Indexed values route identity lookups and
//! invalidation; computations still read the current transformed projection.
pub(crate) const TARGETS:&str="CREATE INDEX model_request_targets ON core.events USING gin ((payload->'targets')) WHERE event_type='model.request' AND kind_version=2 AND validation='validated'";
pub(crate) const POLICY:&str="CREATE INDEX model_request_policy ON core.events ((payload->>'policy'),commit_seq,id) WHERE event_type='model.request' AND kind_version=2 AND validation='validated'";
pub(crate) const RESPONSE:&str="CREATE INDEX model_request_response ON core.events ((payload->>'recorded_response'),commit_seq,id) WHERE event_type='model.request' AND kind_version=2 AND validation='validated'";
pub(crate) const KIND:&str="CREATE INDEX events_reference_kind ON core.events(event_type,kind_version,id)";
pub(crate) fn statements()->Vec<String>{[TARGETS,POLICY,RESPONSE,KIND].into_iter().map(String::from).collect()}
pub(crate) fn before(statement:&str)->bool{!["model_request_targets","model_request_policy","model_request_response","events_reference_kind"].iter().any(|name|statement.contains(name))}

//! Explicit operator authoring and supplied effects. The model-call entry
//! returns before acquiring a connection or constructing any provider.
use super::*;
use sinex_model::event_types::model_call::{
    ModelPolicy, ModelReceipt, ModelRecord, ModelRequest, RecordedResponse,
};
use sinex_model::model_calls::{
    Action, Capabilities, Outcome as ModelOutcome, Policy, ReceiptOnly, Refusal,
};
use sinex_model::{Class, Timestamp};
use sinex_sources::sources::model_calls as records;
use sqlx::PgConnection;

impl Api {
    pub(super) fn model_call_disabled(&self) -> Reply {
        data(json!({"model_call":ModelOutcome::Refused{refusal:Refusal::ProviderDisabled}}))
    }

    pub(super) async fn model_policy(&self, caller: &Caller, definition: &str) -> Reply {
        if caller.actor != "operator" {
            return refused("model policy authoring requires the operator");
        }
        let result = async {
            let policy: Policy = serde_json::from_str(definition)
                .map_err(|error| format!("model policy: {error}"))?;
            policy.validate()?;
            let at = Timestamp::from_unix_ns(self.context().now.as_nanosecond())
                .map_err(|error| error.to_string())?;
            let policy = ModelPolicy {
                policy_name: policy.name.clone(),
                policy,
                configured_at: at,
                configured_by: caller.actor.into(),
            };
            let mut conn = self
                .acquire(caller)
                .await
                .map_err(|error| error.to_string())?;
            let original = format!(
                "{{\"policy_name\":{},\"policy\":{},\"configured_at\":{},\"configured_by\":{}}}",
                serde_json::to_string(&policy.policy_name).expect("name serializes"),
                definition,
                serde_json::to_string(&at).expect("time serializes"),
                serde_json::to_string(caller.actor).expect("actor serializes")
            );
            let (event, material) = self
                .model_record_event_original(
                    &mut conn,
                    caller,
                    ModelRecord::Policy(policy.clone()),
                    Class::Open,
                    at,
                    Some(&original),
                )
                .await?;
            let event = match event {
                Some(id) => id,
                None => self.model_existing(caller, &policy, material, true).await?,
            };
            let outcome = self
                .model_action(caller, Action::AuthorizePolicy { policy: event, at })
                .await?;
            Ok::<_, String>(json!({"policy":format!("sinex:ev/{event}"),"model_call":outcome}))
        }
        .await;
        match result {
            Ok(value) => data(value),
            Err(reason) => refused(reason),
        }
    }

    pub(super) async fn model_record(
        &self,
        caller: &Caller,
        policy_reference: &str,
        definition: &str,
    ) -> Reply {
        if caller.actor != "operator" {
            return refused("recording model expenditure requires the operator");
        }
        let result=async {
            let mut response:RecordedResponse=serde_json::from_str(definition).map_err(|error|format!("recorded model response: {error}"))?;
            response.validate()?;
            let at=Timestamp::from_unix_ns(self.context().now.as_nanosecond()).map_err(|error|error.to_string())?;
            response.received_at=at;
            let mut conn=self.acquire(caller).await.map_err(|error|error.to_string())?;
            let reference=query::parse_reference(policy_reference).map_err(|error|error.to_string())?;
            let policy=EventId(sinex_store::query::resolve(&mut conn,&reference).await.map_err(|error|error.to_string())?);
            let ids:std::collections::BTreeSet<_>=response.request.inputs.iter().map(|target|target.event).collect();
            let Some((class,targets_current))=sinex_store::model_calls::recording_class(&mut conn,&ids.into_iter().collect::<Vec<_>>()).await.map_err(|error|error.to_string())? else {
                return Err("recorded response needs known input classification before retention".into());
            };
            let (response_event,material)=self.model_record_event_original(&mut conn,caller,ModelRecord::Response(response.clone()),class,at,Some(definition)).await?;
            let response_event=match response_event{Some(id)=>id,None=>self.model_existing(caller,&response,material,false).await?};
            let lookup=self.model_action(caller,Action::FindRequest{policy,response:response_event}).await?;
            let ModelOutcome::RequestLookup{request:previous,policy_current}=lookup else {
                return Ok(json!({"response":format!("sinex:ev/{response_event}"),"model_call":lookup}));
            };
            // Reconcile a retained request before attempting any new binding.
            // Current display or target availability cannot erase incurred cost.
            let accounted=if let Some(request)=previous {
                Some(self.model_action(caller,Action::Reconcile{request,response:response_event,at}).await?)
            } else {None};
            if accounted.as_ref().is_some_and(|result|!matches!(result,ModelOutcome::Accepted{..})) {
                return Ok(json!({"request":previous.map(|id|format!("sinex:ev/{id}")),"response":format!("sinex:ev/{response_event}"),"model_call":accounted}));
            }
            let binding=if targets_current&&policy_current {
                let request=ModelRequest{request_id:response.request_id.clone(),policy,request:response.request,recorded_response:Some(response_event),requested_at:at,requested_by:caller.actor.into()};
                self.model_bind_response(&mut conn,caller,request,class,at).await
            } else {Err("current input or authorized policy is unavailable for effect binding".into())};
            match binding {
                Ok(request)=>{
                    let outcome=match accounted {Some(outcome)=>outcome,None=>self.model_action(caller,Action::Reconcile{request,response:response_event,at}).await?};
                    Ok::<_,String>(json!({"request":format!("sinex:ev/{request}"),"accounting_request":previous.map(|id|format!("sinex:ev/{id}")),"response":format!("sinex:ev/{response_event}"),"binding":"available","model_call":outcome}))
                }
                Err(reason)=>{
                    let outcome=accounted.unwrap_or_else(||ModelOutcome::Refused{refusal:Refusal::Invalid{detail:"no retained matching request; current input proof is required before a new request can be admitted".into()}});
                    Ok(json!({"request":previous.map(|id|format!("sinex:ev/{id}")),"response":format!("sinex:ev/{response_event}"),"binding":"unavailable","binding_reason":reason,"model_call":outcome}))
                }
            }
        }.await;
        match result {
            Ok(value) => data(value),
            Err(reason) => refused(reason),
        }
    }

    pub(super) async fn model_receipt(
        &self,
        caller: &Caller,
        policy_reference: &str,
        definition: &str,
        capabilities: Capabilities,
    ) -> Reply {
        if caller.actor != "operator" {
            return refused("actual receipt accounting requires the operator");
        }
        if !capabilities.actual_receipts {
            return data(
                json!({"model_call":ModelOutcome::Refused{refusal:Refusal::ReceiptAccountingDisabled}}),
            );
        }
        let result = async {
            let receipt: ReceiptOnly = serde_json::from_str(definition)
                .map_err(|error| format!("actual model receipt: {error}"))?;
            receipt.validate()?;
            let at = Timestamp::from_unix_ns(self.context().now.as_nanosecond())
                .map_err(|error| error.to_string())?;
            let mut conn = self
                .acquire(caller)
                .await
                .map_err(|error| error.to_string())?;
            let reference =
                query::parse_reference(policy_reference).map_err(|error| error.to_string())?;
            let policy = EventId(
                sinex_store::query::resolve(&mut conn, &reference)
                    .await
                    .map_err(|error| error.to_string())?,
            );
            let record = ModelReceipt {
                policy,
                receipt,
                recorded_at: at,
                recorded_by: caller.actor.into(),
            };
            let original = format!(
                "{{\"policy\":{},\"receipt\":{},\"recorded_at\":{},\"recorded_by\":{}}}",
                serde_json::to_string(&policy).expect("policy reference serializes"),
                definition,
                serde_json::to_string(&at).expect("time serializes"),
                serde_json::to_string(caller.actor).expect("actor serializes")
            );
            let (captured, material) = self
                .model_record_event_original(
                    &mut conn,
                    caller,
                    ModelRecord::Receipt(record.clone()),
                    Class::Open,
                    at,
                    Some(&original),
                )
                .await?;
            let receipt = match captured {
                Some(id) => id,
                None => {
                    self.model_existing(caller, &record, material, false)
                        .await?
                }
            };
            let outcome = self
                .model_action_with(caller, Action::RecordActual { receipt, at }, capabilities)
                .await?;
            Ok::<_, String>(json!({"receipt":format!("sinex:ev/{receipt}"),"model_call":outcome}))
        }
        .await;
        match result {
            Ok(value) => data(value),
            Err(reason) => refused(reason),
        }
    }

    async fn model_bind_response(
        &self,
        conn: &mut PgConnection,
        caller: &Caller,
        request: ModelRequest,
        class: Class,
        at: Timestamp,
    ) -> Result<EventId, String> {
        match self
            .model_record_event(
                conn,
                caller,
                ModelRecord::Request(request.clone()),
                class,
                at,
            )
            .await?
        {
            (Some(id), _) => Ok(id),
            (None, material) => self.model_existing(caller, &request, material, true).await,
        }
    }

    async fn model_existing<P: sinex_model::EventPayload>(
        &self,
        caller: &Caller,
        payload: &P,
        material: sinex_model::MaterialId,
        current: bool,
    ) -> Result<EventId, String> {
        let sinex_model::KeyKind::Fields(fields) = P::KEY else {
            return Err("model record needs a native key".into());
        };
        let value = serde_json::to_value(payload).map_err(|error| error.to_string())?;
        let key = sinex_model::EquivalenceKey::of(P::EVENT_TYPE, fields, &value).0;
        let outcome = self
            .model_action(
                caller,
                Action::FindRecord {
                    // Source policy belongs to this owner declaration, never
                    // to a requested source name or a store-side catalog copy.
                    source: records::RECORDS.name.into(),
                    kind: sinex_model::KindRef {
                        name: P::EVENT_TYPE.into(),
                        version: P::VERSION,
                    },
                    material,
                    key,
                    current,
                },
            )
            .await?;
        match outcome {
            ModelOutcome::RecordLookup { event: Some(id) } => Ok(id),
            _ => Err("retained model record is unavailable".into()),
        }
    }

    async fn model_record_event(
        &self,
        conn: &mut PgConnection,
        caller: &Caller,
        record: ModelRecord,
        minimum: Class,
        at: Timestamp,
    ) -> Result<(Option<EventId>, sinex_model::MaterialId), String> {
        self.model_record_event_original(conn, caller, record, minimum, at, None)
            .await
    }
    async fn model_record_event_original(
        &self,
        conn: &mut PgConnection,
        caller: &Caller,
        record: ModelRecord,
        minimum: Class,
        at: Timestamp,
        original: Option<&str>,
    ) -> Result<(Option<EventId>, sinex_model::MaterialId), String> {
        let writer = self
            .writer
            .as_ref()
            .ok_or("model recording requires sinexd run")?;
        let acquired = match original {
            Some(value) => records::record_original(record, value, caller.actor, &self.host, at),
            None => records::record_material(record, caller.actor, &self.host, at),
        };
        let mut source = records::record_source();
        source.spec.class = source.spec.class.max(minimum);
        let admitted = crate::intake::request(
            conn,
            writer,
            &self.blobs,
            std::sync::Arc::new(source),
            &format!("model.records:{}@{}", caller.actor, self.host),
            &self.host,
            acquired,
        )
        .await?;
        if admitted.outcome.failures != 0 {
            return Err("model record was not admitted".into());
        }
        let material = admitted
            .material
            .ok_or("model record has no retained material")?;
        Ok((admitted.outcome.admitted_ids.first().copied(), material))
    }

    async fn model_action(&self, caller: &Caller, action: Action) -> Result<ModelOutcome, String> {
        self.model_action_with(caller, action, Capabilities::default())
            .await
    }
    async fn model_action_with(
        &self,
        caller: &Caller,
        action: Action,
        capabilities: Capabilities,
    ) -> Result<ModelOutcome, String> {
        let writer = self
            .writer
            .as_ref()
            .ok_or("model accounting requires sinexd run")?;
        writer
            .model_call(caller.actor.into(), action, capabilities)
            .await
            .map_err(|error| error.to_string())
    }
}

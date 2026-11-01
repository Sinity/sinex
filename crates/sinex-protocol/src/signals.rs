//! Authenticated host completion proof for the one compiled primary-mode exception.
pub use sinex_model::event_types::signal::EmergencyCompleted as EmergencyCompletion;
/// Deployment authority is scoped by the authenticated producer's configured
/// family. Merely publishing canonical facts grants no privileged author role.
#[derive(Clone,Debug,Default,PartialEq,Eq,serde::Serialize,serde::Deserialize,schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct AuthorAttestation {
    pub operator:bool,
    pub shipped:Vec<ShippedRule>,
}
#[derive(Clone,Debug,PartialEq,Eq,serde::Serialize,serde::Deserialize,schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ShippedRule {
    pub id:String,
    pub revision:u64,
    pub fingerprint:String,
}
pub fn rule_fingerprint(active:&::signals::ActiveRule)->String {
    sinex_model::json_identity::hash(&serde_json::json!({"version":active.version,"rule":active.rule,"live":active.live}))
}
pub fn authorize_author(intent:&::signals::FiredIntent,active:Option<&::signals::ActiveRule>,family:&str,capability:Option<&AuthorAttestation>)->Result<(),String> {
    if intent.delivery.trigger.rule.product!=family {return Err("signal author belongs to another authenticated family".into());}
    let prefix=match intent.author{::signals::Author::Shipped=>"shipped:",_=>"authored:"};
    if intent.delivery.trigger.rule.id.strip_prefix(prefix).is_none_or(str::is_empty){return Err("signal rule identity is outside its author namespace".into());}

    let channel=|active:&::signals::ActiveRule|match active.rule.deliver {
        ::signals::Deliver::Notice if active.rule.interrupt=>::signals::Channel::Interrupt,
        ::signals::Deliver::Notice=>::signals::Channel::Notice,
        ::signals::Deliver::Value=>::signals::Channel::Value,
        ::signals::Deliver::Action=>::signals::Channel::Action,
    };
    if let Some(active)=active && (active.version!=intent.delivery.trigger.rule || active.author!=intent.author || active.current()!=intent.current || active.rule.quiet_hours!=intent.quiet_hours || active.rule.rate_limit!=intent.rate_limit || channel(active)!=intent.delivery.channel || active.rule.scope!=intent.delivery.scope || intent.delivery.downgrade.is_some() || !active.rule.enabled) {
        return Err("signal active rule differs from its frozen intent".into());
    }
    if matches!(intent.author,::signals::Author::Agent(_)){return Ok(());}
    let capability=capability.ok_or("forwarded elevated author lacks configured attestation")?;
    let active=active.ok_or("forwarded elevated author lacks its frozen active rule")?;
    match intent.author {
        ::signals::Author::Operator if capability.operator=>Ok(()),
        ::signals::Author::Shipped if capability.shipped.iter().any(|rule|rule.id==active.version.id && rule.revision==active.version.revision && rule.fingerprint==rule_fingerprint(active))=>Ok(()),
        _=>Err("forwarded elevated author is outside its configured attestation".into()),
    }
}

pub const EMERGENCY_PRODUCT:&str="polylogue";
pub const EMERGENCY_RULE_ID: &str = "shipped:sinex-blocked";
pub const EMERGENCY_RULE_REVISION: u64 = 1;
pub const EMERGENCY_OCCURRENCE_ID:&str="sinex-blocked";

/// Mint the episode once when an outage is actually observed and persist it
/// with the ordinary signal checkpoint. Polls and restarts reuse that identity.
pub fn emergency_trigger(episode:sinex_model::EventId)->::signals::Trigger {
    ::signals::Trigger{rule: ::signals::RuleVersion{product:EMERGENCY_PRODUCT.into(),id:EMERGENCY_RULE_ID.into(),revision:EMERGENCY_RULE_REVISION},occurrence: ::signals::Occurrence{id:EMERGENCY_OCCURRENCE_ID.into(),episode:episode.to_string()}}
}


/// The compiled outage alert carries no dynamic error, path or captured work.
/// Products use this one builder before the shared desktop worker freezes it.
pub fn emergency_notice(delivery_id:&str)->::signals::notifications::Notice {
    ::signals::notifications::Notice {
        delivery_id:delivery_id.into(),product:EMERGENCY_PRODUCT.into(),icon:"polylogue".into(),
        rule:"Sinex blocked".into(),occurrence:delivery_id.into(),summary:"Sinex blocked".into(),
        body:"Polylogue cannot confirm new work while Sinex is unavailable or refusing this producer. See Polylogue status for details.".into(),
        reference:"status".into(),command_hint:"polylogue status".into(),interrupt:true,current:true,data_loss:false,budget_passive_count:None,
    }
}


/// Exact compiled contract identity, including its static nonsensitive notice
/// and sole permitted local route. Authored rules cannot inherit this identity.
pub fn emergency_rule() -> ShippedRule {
    ShippedRule {
        id: EMERGENCY_RULE_ID.into(),
        revision: EMERGENCY_RULE_REVISION,
        fingerprint: sinex_model::json_identity::hash(&serde_json::json!({
            "contract": "sinex.offline_emergency.v1",
            "product": EMERGENCY_PRODUCT,
            "rule": EMERGENCY_RULE_ID,
            "revision": EMERGENCY_RULE_REVISION,
            "notice": emergency_notice("compiled-delivery-placeholder"),
            "route": "desktop",
        })),
    }
}

/// Transport authority and the owner-issued context token are checked by Sinex.
/// This checks only the shared immutable completion identity.
pub fn validate_emergency(completion: &EmergencyCompletion, family: &str) -> Result<(), String> {
    let intent = &completion.intent;
    let episode=serde_json::from_value::<sinex_model::EventId>(serde_json::Value::String(intent.delivery.trigger.occurrence.episode.clone())).ok();
    if episode.is_none_or(|episode|episode.0.get_version_num()!=7 || intent.delivery.trigger!=emergency_trigger(episode)) {
        return Err("emergency occurrence is not an opaque compiled outage episode".into());
    }

    if completion.notice!=emergency_notice(&completion.delivery_id)
        || !intent.current || intent.quiet_hours.is_some() || intent.rate_limit.is_some()
        || !completion.inputs.is_empty() || !intent.delivery.basis.is_empty() || intent.delivery.change.is_some() || intent.delivery.value.is_some() || intent.delivery.scope.is_some() || intent.delivery.downgrade.is_some()
        || intent.delivery.trigger.rule.product != family
        || intent.delivery.trigger.rule.id != EMERGENCY_RULE_ID
        || intent.delivery.trigger.rule.revision != EMERGENCY_RULE_REVISION
        || intent.author != ::signals::Author::Shipped
        || intent.delivery.id != intent.delivery.trigger.delivery_id()
        || completion.delivery_id != intent.delivery.id
        || completion.notice.delivery_id != completion.delivery_id
        || completion.notice.product != family
        || completion.notice.current != intent.current
        || completion.receipts.is_empty()
        || completion.attempt.dbus.is_none()
        || completion.attempt.command.is_some()
        || completion.receipts.iter().any(|receipt| {
            receipt.delivery_id != completion.delivery_id
                || receipt.event.is_some()
                || receipt.via.is_some()
                || receipt.route != ::signals::notifications::Route::Dbus
        })
        || intent.delivery.channel != ::signals::Channel::Interrupt
        || !completion.notice.interrupt
        || completion.notice.data_loss
        || completion.notice.budget_passive_count.is_some()
    {
        return Err("emergency completion is not the exact compiled shipped handoff".into());
    }
    if completion.attempt.notice != completion.notice {
        return Err("emergency attempt differs from its frozen notice".into());
    }
    for (route, outcome) in [
        (
            ::signals::notifications::Route::Dbus,
            &completion.attempt.dbus,
        ),
        (
            ::signals::notifications::Route::Command,
            &completion.attempt.command,
        ),
    ] {
        let receipt = completion
            .receipts
            .iter()
            .find(|receipt| receipt.route == route);
        if receipt.map(|receipt| &receipt.outcome) != outcome.as_ref() {
            return Err("emergency receipts differ from the original route attempts".into());
        }
    }
    let mut routes = std::collections::BTreeSet::new();
    for receipt in &completion.receipts {
        if !routes.insert(format!("{:?}", receipt.route)) {
            return Err("emergency completion repeats a route receipt".into());
        }
    }
    let at = i64::try_from(completion.invoked_at.unix_ns() / 1_000_000)
        .map_err(|_| "emergency invocation clock exceeds milliseconds")?;
    if completion.completed_at < completion.invoked_at
        || at < intent.delivery.at
        || intent.delivery.at < intent.delivery.matched
        || intent
            .delivery
            .basis
            .iter()
            .any(|row| row.observed > intent.delivery.matched)
    {
        return Err("emergency completion precedes its observed occurrence".into());
    }
    if completion.host.is_empty() || completion.context_token.is_empty() {
        return Err("emergency completion lacks owner policy provenance".into());
    }
    Ok(())
}

#[cfg(test)]
mod tests;

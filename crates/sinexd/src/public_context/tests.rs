use super::*;
use serde_json::json;
use sqlx::Connection;

fn day()->DaySettingWire {
    serde_json::from_value(json!({"zone":"UTC","start":"04:00:00"})).unwrap()
}
fn config()->crate::signals::Config {
    serde_json::from_value(json!({"interrupt_budget_per_hour":2,"quiet_hours":{"from":1320,"to":480},"notifications":null,"outlet_timeout_ms":1000})).unwrap()
}
fn available()->PublicContext {
    PublicContext::Available{context_token:String::new(),settings_identity:"configured".into(),owner_revision:7,day:day(),quiet_hours:Some(signals::QuietHours{from:1320,to:480}),interrupt_budget:PublicInterruptBudget{per_hour:2,used:1,remaining:1,window_ms:3600000},observed_at:Timestamp::from_unix_secs(1800000000).unwrap(),coverage:CoverageStatus::NotRequested,offline_emergency:None}
}
#[test]
fn owner_token_binds_identity_host_and_exact_snapshot() {
    let key=[17;32];
    let token=sign(&key,"producer","host-a",&available()).unwrap();
    let mut expected=available();
    if let PublicContext::Available{context_token,..}=&mut expected{*context_token=token.clone();}
    assert_eq!(open(&key,"producer","host-a",&token).unwrap(),expected);
    assert!(open(&key,"other","host-a",&token).is_err());
    assert!(open(&key,"producer","host-b",&token).is_err());
    assert!(open(&[18;32],"producer","host-a",&token).is_err());
    let (body,tag)=token.split_once('.').unwrap();
    let mut claims:serde_json::Value=serde_json::from_slice(&URL_SAFE_NO_PAD.decode(body).unwrap()).unwrap();
    claims["context"]["interrupt_budget"]["remaining"]=json!(99);
    let forged=format!("{}.{}",URL_SAFE_NO_PAD.encode(serde_json::to_vec(&claims).unwrap()),tag);
    assert!(open(&key,"producer","host-a",&forged).is_err());
}
#[test]
fn identity_tracks_day_and_public_policy_without_private_outlets() {
    let day=day();let mut config=config();
    let baseline=settings_identity(&day,"host-a",&config).unwrap();
    config.private=vec![signals::Interval{start:1,end:2}];
    config.outlet_timeout_ms=2000;
    assert_eq!(baseline,settings_identity(&day,"host-a",&config).unwrap());
    config.interrupt_budget_per_hour=3;
    assert_ne!(baseline,settings_identity(&day,"host-a",&config).unwrap());
    let mut other_day=day.clone();other_day.start=jiff::civil::Time::constant(5,0,0,0);
    assert_ne!(settings_identity(&day,"host-a",&config).unwrap(),settings_identity(&other_day,"host-a",&config).unwrap());
    assert_ne!(settings_identity(&day,"host-a",&config).unwrap(),settings_identity(&day,"host-b",&config).unwrap());
}
#[devkit::test]
async fn unconfigured_and_uninitialized_context_stay_explicit(db:devkit::pg::TestDatabase) {
    let mut conn=sqlx::PgConnection::connect(&db.url(None)).await.unwrap();
    let at=Timestamp::from_unix_secs(1800000000).unwrap();
    let provider=Provider::new(day(),"host-a".into(),None).unwrap();
    assert!(matches!(provider.snapshot(&mut conn,"producer",&PublicContextRequest::default(),at).await.unwrap(),PublicContext::Unavailable{..}));
    let provider=Provider::new(day(),"host-a".into(),Some(&config())).unwrap();
    assert!(matches!(provider.snapshot(&mut conn,"producer",&PublicContextRequest::default(),at).await.unwrap(),PublicContext::Pending{..}));
}

#[test]
fn offline_permission_requires_deployment_lease_and_is_inside_signed_context() {
    let provider = Provider::new(day(), "host-a".into(), Some(&config())).unwrap();
    let mut context = available();
    let at = Timestamp::from_unix_secs(1_800_000_000).unwrap();
    provider.grant_offline_emergency(&mut context, "primary", None, at).unwrap();
    assert!(matches!(&context, PublicContext::Available { offline_emergency: None, .. }));
    let mut write: crate::ingress::authority::WriteGrant = serde_json::from_value(json!({
        "family":"polylogue", "host":"host-a", "kind_prefixes":["polylogue"], "classes":["open"],
        "canonical_prefixes":["polylogue"], "forward_signals":true, "emergency_sinex_blocked_host":"host-a"
    })).unwrap();
    provider.grant_offline_emergency(&mut context, "primary", Some(&write), at).unwrap();
    assert!(matches!(&context, PublicContext::Available { offline_emergency: None, .. }));
    write.emergency_lease_seconds = Some(900);
    provider.grant_offline_emergency(&mut context, "primary", Some(&write), at).unwrap();
    let PublicContext::Available { offline_emergency: Some(permission), .. } = &context else { panic!("explicit lease"); };
    assert!(permission.permits("primary", "host-a", "configured", at));
    assert_eq!(permission.expires_at.unix_ns() - at.unix_ns(), 900_000_000_000);
    let key = [22; 32];
    let token = sign(&key, "primary", "host-a", &context).unwrap();
    let signed = open(&key, "primary", "host-a", &token).unwrap();
    assert!(matches!(signed, PublicContext::Available { offline_emergency: Some(_), .. }));
    let (body, tag) = token.split_once('.').unwrap();
    let mut claims: serde_json::Value = serde_json::from_slice(&URL_SAFE_NO_PAD.decode(body).unwrap()).unwrap();
    claims["context"]["offline_emergency"]["producer"] = json!("foreign");
    let forged = format!("{}.{}", URL_SAFE_NO_PAD.encode(serde_json::to_vec(&claims).unwrap()), tag);
    assert!(open(&key, "primary", "host-a", &forged).is_err());
    write.emergency_lease_seconds = Some(0);
    assert!(provider.grant_offline_emergency(&mut context, "primary", Some(&write), at).is_err());
    write.emergency_lease_seconds = Some(3601);
    assert!(provider.grant_offline_emergency(&mut context, "primary", Some(&write), at).is_err());
}

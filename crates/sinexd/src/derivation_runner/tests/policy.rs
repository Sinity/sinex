//! Rule changes re-derive from transformed inputs through ordinary scheduling.
use super::*;
use sinex_store::privacy::{self,Action,NewRule};
fn rule()->NewRule{NewRule{name:"configured command".into(),source:"terminal.zsh".into(),event_type:Some("command.imported".into()),field:Some("/command".into()),matcher:"pattern".into(),matcher_arg:Some("leafsecret".into()),class:Class::Open,action:Action::Mask,author:"fixture".into()}}

#[devkit::test]
async fn policy_refresh_resets_per_event_inputs_and_recovers_on_rule_end(db:devkit::pg::TestDatabase){
    let w=World::with_keys(db,vec![]).await;let source=zsh("leafsecret",0,1);let source_id=source.id;w.admit(vec![source]).await;
    let mut runner=w.driver_with(vec![],vec![Box::new(Echo)]).await;runner.catch_up().await.unwrap();
    assert_eq!(w.live::<CommandFused>().await[0].command,"LEAFSECRET");
    let mut conn=PgConnection::connect(&w.url).await.unwrap();privacy::add_rule(&mut conn,&rule()).await.unwrap();privacy::projection::ensure_current(&mut conn).await.unwrap();
    runner.catch_up().await.unwrap();let current=w.live::<CommandFused>().await;assert_eq!(current.len(),1);assert_eq!(current[0].command,"[MASKED]");
    let (state,basis,active):(String,Option<i64>,i64)=sqlx::query_as("SELECT o.state,p.basis_epoch,s.active_epoch FROM privacy.event_outputs o JOIN privacy.original_events p ON p.event_id=o.event_id JOIN privacy.policy_state s ON s.id JOIN core.events e ON e.id=o.event_id WHERE e.source='test.echo' AND NOT EXISTS(SELECT 1 FROM audit.archived a WHERE a.event_id=e.id)").fetch_one(&mut conn).await.unwrap();assert_eq!(state,"complete");assert_eq!(basis,Some(active));
    drop(runner);let mut restarted=w.driver_with(vec![],vec![Box::new(Echo)]).await;restarted.catch_up().await.unwrap();assert_eq!(w.live::<CommandFused>().await,current);
    privacy::end_rule(&mut conn,"configured command","fixture").await.unwrap();privacy::projection::ensure_current(&mut conn).await.unwrap();restarted.catch_up().await.unwrap();assert_eq!(w.live::<CommandFused>().await[0].command,"LEAFSECRET");
    let raw:String=sqlx::query_scalar("SELECT payload->>'command' FROM core.events WHERE id=$1").bind(source_id.0).fetch_one(&mut conn).await.unwrap();assert_eq!(raw,"leafsecret");
}

struct CommandKeys;
impl Keyed for CommandKeys {
    const NAME:&'static str="fixture.command_keys";const VERSION:u32=1;const INPUTS:&'static[&'static str]=&["command.imported"];const OUTPUTS:&'static[&'static str]=&["command.fused"];
    fn keys_for(&self,input:&KeyView<'_>,_:&Env)->Vec<Value>{input.payload.get("command").cloned().into_iter().collect()}
    fn inputs(&self,key:&Value,_:&Env)->Option<InputQuery>{Some(InputQuery::all(vec![sinex_derivations::Kind{event_type:"command.imported".into(),equal:vec![("command".into(),key.clone())]}]))}
    fn compute(&self,key:&Value,inputs:&[Event],_:&Env)->Computed{
        if inputs.is_empty(){return Computed::default()}
        Computed{error:None,waiting:false,outputs:vec![sinex_derivations::derived_event::derived(&CommandFused{record:key.as_str().unwrap().into(),command:key.as_str().unwrap().into(),host:"fixture".into(),started_at:at(0),cwd:None,session:None,duration_ns:None,exit_code:None,observed_by:vec![]},Self::NAME,inputs)]}
    }
    fn key_state(&self,_:&Value,_:&Env)->KeyState{KeyState::Closed}
}
#[devkit::test]
async fn policy_refresh_marks_transformed_and_old_keys_and_new_admission_keeps_them_current(db:devkit::pg::TestDatabase){
    let w=World::with_keys(db,vec![Box::new(CommandKeys)]).await;w.admit(vec![zsh("leafsecret",0,1)]).await;
    let mut runner=w.driver_with(vec![Box::new(CommandKeys)],vec![]).await;runner.catch_up().await.unwrap();assert_eq!(w.live::<CommandFused>().await[0].command,"leafsecret");
    let mut conn=PgConnection::connect(&w.url).await.unwrap();privacy::add_rule(&mut conn,&rule()).await.unwrap();privacy::projection::ensure_current(&mut conn).await.unwrap();runner.catch_up().await.unwrap();
    let current=w.live::<CommandFused>().await;assert_eq!(current.len(),1);assert_eq!(current[0].command,"[masked]");
    let old:i64=sqlx::query_scalar("SELECT count(*) FROM derivation.outputs o JOIN core.events e ON e.id=o.event_id WHERE o.derivation='fixture.command_keys' AND o.key='\"leafsecret\"'::jsonb AND NOT EXISTS(SELECT 1 FROM audit.archived a WHERE a.event_id=e.id)").fetch_one(&mut conn).await.unwrap();assert_eq!(old,0);
    w.admit(vec![zsh("second leafsecret",1,2)]).await;runner.catch_up().await.unwrap();
    let mut commands:Vec<_>=w.live::<CommandFused>().await.into_iter().map(|row|row.command).collect();commands.sort();assert_eq!(commands,["[masked]","second [masked]"]);
}

#[devkit::test]
async fn live_comparison_applies_configured_output_rules_and_enforces_input_grants(db:devkit::pg::TestDatabase){
    let w=World::with_keys(db,vec![]).await;let mut source=zsh("leafsecret",0,1);source.class=Class::Guarded;w.admit(vec![source]).await;
    let mut runner=w.driver_with(vec![],vec![Box::new(Echo)]).await;
    let error=runner.compare("test.echo",None,5).await.unwrap_err();assert!(error.contains("comparison_grants_unavailable"));assert_eq!(w.count("SELECT count(*) FROM derivation.comparisons").await,0);
    let mut conn=PgConnection::connect(&w.url).await.unwrap();privacy::add_rule(&mut conn,&NewRule{name:"configured candidate".into(),source:"test.echo".into(),event_type:Some("command.fused".into()),field:Some("/command".into()),matcher:"pattern".into(),matcher_arg:Some("LEAFSECRET".into()),class:Class::Open,action:Action::Mask,author:"fixture".into()}).await.unwrap();privacy::projection::ensure_current(&mut conn).await.unwrap();
    sinex_store::query::set_grants(&mut runner.read,"guarded").await.unwrap();let comparison=runner.compare("test.echo",None,5).await.unwrap();let summary=comparison.to_string();assert!(summary.contains("[masked]"));assert!(!summary.contains("LEAFSECRET"));
    sinex_store::query::set_grants(&mut conn,"guarded").await.unwrap();let kept=derivations::comparisons(&mut conn,Some("test.echo"),20).await.unwrap();assert!(kept[0].examples.to_string().contains("[masked]"));assert!(!kept[0].examples.to_string().contains("LEAFSECRET"));
}

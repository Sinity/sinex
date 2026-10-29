use super::*;
use crate::privacy::{Action,Matcher,Rule,RuleSet};
use serde_json::json;
use sinex_model::{Class,Origin,ReadingOrder};
use std::collections::HashMap;

fn event()->Event {
    Event::submitted(KindRef{name:"fixture.input".into(),version:2},
        json!({"reference":"task:17","number":42,"text":"[masked]","unrelated":"prose","kind":"screen","width":100}),
        "fixture".into(),Origin::Derived{parents:vec![]},ReadingOrder(0),[0;32],Class::Open)
}
fn apply(event:&mut Event,path:&str,action:Action)->Vec<AffectedPath>{
    let rules=RuleSet::new(vec![Rule{id:uuid::Uuid::nil(),name:"configured".into(),source:"fixture".into(),event_type:Some("fixture.input".into()),
        field:Some(path.into()),matcher:Matcher::All,class:Class::Open,action}],HashMap::new(),[0;32]);
    let result=rules.apply(event).unwrap();
    assert_eq!(result.changed,action.transforms());result.trace
}
fn contract(paths:&[&str])->Vec<InputRequirement>{vec![InputRequirement::new(KindRef{name:"fixture.input".into(),version:2},paths.iter().copied())]}

#[devkit::test]
fn action_trace_distinguishes_required_reference_numeric_and_same_value_masks(){
    for path in ["/reference","/number","/text"] {
        let mut input=event();let before=input.payload.clone();let trace=apply(&mut input,path,Action::Mask);
        if path=="/text"{assert_eq!(input.payload,before,"same-value mask still has transformation provenance")}
        assert_eq!(trace[0].path,path);
        assert_eq!(reason(&input,&trace,Some(&contract(&[path]))),Some(InputUnavailableReason::RequiredPathTransformed));
    }
}
#[devkit::test]
fn unrelated_fields_and_classification_remain_estimable_but_unknown_contract_does_not(){
    let mut input=event();let trace=apply(&mut input,"/unrelated",Action::Mask);
    assert_eq!(reason(&input,&trace,Some(&contract(&["/number"]))),None);
    assert_eq!(reason(&input,&trace,None),Some(InputUnavailableReason::UndeclaredAffectedInput));
    let wrong=vec![InputRequirement::new(KindRef{name:input.event_type.clone(),version:1},["/number"])];
    assert_eq!(reason(&input,&trace,Some(&wrong)),Some(InputUnavailableReason::UndeclaredAffectedInput));
    let trace=apply(&mut input,"/number",Action::Classify);
    assert_eq!(reason(&input,&trace,None),None);
}
#[devkit::test]
fn required_paths_cover_ancestors_arrays_escaped_names_and_branch_selectors(){
    assert!(overlaps("/calls/*/at","/calls/12/at"));
    assert!(overlaps("/calls/*/at","/calls"));
    assert!(overlaps("/calls","/calls/12/at"));
    assert!(!overlaps("/calls/*/at","/calls/12/text"));
    assert!(overlaps("/a~1b/~0","/a~1b/~0"));
    let mut requirements=contract(&["/kind"]);
    let mut geometry=InputRequirement::new(requirements[0].kind.clone(),["/width"]);
    geometry.when.push(("/kind".into(),json!("screen")));requirements.push(geometry);
    let mut input=event();input.payload["kind"]=json!("audio");
    let trace=apply(&mut input,"/width",Action::Mask);
    assert_eq!(reason(&input,&trace,Some(&requirements)),None,"audio does not use OCR geometry");
    input.payload["kind"]=json!("screen");
    assert_eq!(reason(&input,&trace,Some(&requirements)),Some(InputUnavailableReason::RequiredPathTransformed));
    let trace=apply(&mut input,"/kind",Action::Mask);
    assert_eq!(reason(&input,&trace,Some(&requirements)),Some(InputUnavailableReason::RequiredPathTransformed),"masked selector cannot select a harmless branch");
}

#[devkit::test]
async fn projection_trace_survives_same_value_mask_and_rule_loosening(db:devkit::pg::TestDatabase){
    use crate::{AdmissionContext,NoDerivations};
    use sinex_model::{Batch,BatchKind,RecordOutcome,RunId,MaterialId,RecordLocation,event_types::operator::LogEntry};
    let mut conn=AdmissionContext::connect(&db.url(None)).await.unwrap();
    let raw=Event::build(&LogEntry{written_at:None,label:Some("unrelated".into()),text:"[masked]".into()},"fixture",
        Origin::Record{location:RecordLocation{material:MaterialId::new(),anchor:0},output_index:0},ReadingOrder(1),[0;32],Class::Open).unwrap();
    let mut tx=conn.begin().await.unwrap();tx.admit(&Batch{run:RunId::new(),kind:BatchKind::Api{actor:"fixture".into()},records:vec![RecordOutcome::Events{location:None,events:vec![raw.clone()]}],archive:vec![]},&NoDerivations).await.unwrap();tx.commit().await.unwrap();
    let required=vec![InputRequirement::new(KindRef{name:raw.event_type.clone(),version:raw.kind_version},["/text"])];
    crate::privacy::add_rule(&mut conn,&crate::privacy::NewRule{name:"mask".into(),source:"fixture".into(),event_type:Some(raw.event_type.clone()),field:Some("/text".into()),matcher:"all".into(),matcher_arg:None,class:Class::Open,action:Action::Mask,author:"fixture".into()}).await.unwrap();
    assert!(unavailable(&mut conn,std::slice::from_ref(&raw),Some(&required)).await.is_err(),"stale projection is unavailable before build");
    projection::ensure_current(&mut conn).await.unwrap();
    let visible:serde_json::Value=sqlx::query_scalar("SELECT payload FROM core.derivation_events WHERE id=$1").bind(raw.id.0).fetch_one(&mut *conn).await.unwrap();
    assert_eq!(visible,raw.payload);
    let issues=unavailable(&mut conn,std::slice::from_ref(&raw),Some(&required)).await.unwrap();
    assert_eq!(issues[0].reason,InputUnavailableReason::RequiredPathTransformed);
    register(&mut conn,"fixture.consumer",2,"build",Some(&required)).await.unwrap();
    assert!(register(&mut conn,"fixture.consumer",2,"build",None).await.is_err(),"a build's input footprint cannot change");
    crate::privacy::end_rule(&mut conn,"mask","fixture").await.unwrap();projection::ensure_current(&mut conn).await.unwrap();
    assert!(unavailable(&mut conn,std::slice::from_ref(&raw),Some(&required)).await.unwrap().is_empty());
}

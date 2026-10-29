//! Prior-only rhythm comparisons over coverage-qualified daily input products.
use crate::derived_event::derived;
use crate::{Computed,Env,InputQuery,KeyState,KeyView,Keyed};
use serde_json::{Value,json};
use sinex_model::Event;
use sinex_model::event_types::portfolio::{InputWorkload,RhythmDeviation};
use std::collections::BTreeMap;

#[derive(Clone,Debug,PartialEq,serde::Serialize,serde::Deserialize)]
#[serde(default,deny_unknown_fields)]
pub struct RhythmProfile {pub window_days:u32,pub minimum_days:u32}
impl Default for RhythmProfile {fn default()->Self{Self{window_days:28,minimum_days:14}}}
pub struct RhythmDeviations(pub RhythmProfile);
fn earlier(mut day:jiff::civil::Date,count:u32)->Option<jiff::civil::Date>{for _ in 0..count{day=day.yesterday().ok()?}Some(day)}
fn value(workload:&InputWorkload,measure:&str)->Option<f64>{match measure{
    "input.key_presses"=>Some(workload.key_presses as f64),"input.typing_seconds"=>workload.typing_seconds,_=>None}}
impl Keyed for RhythmDeviations {
    const NAME:&'static str="rhythm.deviations";const VERSION:u32=1;const USES_TIME_SETTINGS:bool=true;
    const INPUTS:&'static [&'static str]=&["input.workload"];const OUTPUTS:&'static [&'static str]=&["rhythm.deviation"];
    fn version(&self)->u32{let hash=blake3::hash(&serde_json::to_vec(&(Self::VERSION,&self.0)).unwrap());1+(u32::from_le_bytes(hash.as_bytes()[..4].try_into().unwrap())>>2)}
    fn keys_for(&self,input:&KeyView<'_>,env:&Env)->Vec<Value>{
        let Some(scope)=input.payload["scope"].as_str()else{return vec![]};
        let Some(mut day)=input.payload["day"].as_str().and_then(|day|day.parse::<jiff::civil::Date>().ok())else{return vec![]};
        let Ok(today)=env.time_settings.day(env.now)else{return vec![]};
        let mut keys=vec![];
        for _ in 0..=self.0.window_days {if day>today{break}keys.push(json!({"scope":scope,"day":day.to_string()}));
            let Ok(next)=day.tomorrow()else{break};day=next;} keys
    }
    fn inputs(&self,key:&Value,env:&Env)->Option<InputQuery>{
        let day=key["day"].as_str()?.parse().ok()?;let first=earlier(day,self.0.window_days)?;
        Some(InputQuery::window(Self::INPUTS,env.time_settings.day_start(first).ok()?,env.time_settings.day_end(day).ok()?)
            .only("scope",key.get("scope")?.clone()))
    }
    fn key_state(&self,_:&Value,_:&Env)->KeyState{KeyState::Closed}
    fn compute(&self,key:&Value,inputs:&[Event],env:&Env)->Computed{
        let Some(day)=key["day"].as_str()else{return Computed::default()};
        let Some(scope)=key["scope"].as_str()else{return Computed::default()};
        let Ok(date)=day.parse()else{return Computed::default()};
        let Some(first)=earlier(date,self.0.window_days)else{return Computed::default()};
        let (Ok(start),Ok(end),Ok(from))=(env.time_settings.day_start(date),env.time_settings.day_end(date),env.time_settings.day_start(first))else{return Computed::default()};
        let rows:Vec<_>=inputs.iter().filter_map(|event|Some((event,event.payload_as::<InputWorkload>()?)))
            .filter(|(_,row)|row.scope==scope&&row.period_start>=from&&row.period_start<end).collect();
        let current:Vec<_>=rows.iter().filter(|(_,row)|row.day==day).collect();
        if current.len()!=1{return Computed::default()}
        let mut outputs=vec![];
        for measure in ["input.key_presses","input.typing_seconds"]{
            let mut by_day=BTreeMap::<String,Vec<f64>>::new();
            for (_,row) in &rows {if row.period_start<start&&row.coverage=="observed"&&let Some(value)=value(row,measure){by_day.entry(row.day.clone()).or_default().push(value)}}
            let mut prior:Vec<_>=by_day.values_mut().filter_map(|values|{values.sort_by(f64::total_cmp);values.dedup_by(|a,b|a.to_bits()==b.to_bits());(values.len()==1).then(||values[0])}).collect();
            prior.sort_by(f64::total_cmp);
            let current_value=value(&current[0].1,measure);
            let valid=self.0.window_days>0&&self.0.minimum_days>0&&self.0.minimum_days<=self.0.window_days;
            let reason=if !valid{"invalid_profile"}else if current[0].1.coverage!="observed"{"current_day_coverage_incomplete"}
                else if current_value.is_none(){"current_measure_not_estimable"}else if prior.len()<self.0.minimum_days as usize{"insufficient_prior_observed_days"}else{"estimated"};
            let baseline=(reason=="estimated").then(||{let middle=prior.len()/2;if prior.len()%2==0{(prior[middle-1]+prior[middle])/2.0}else{prior[middle]}});
            outputs.push(derived(&RhythmDeviation{scope:scope.into(),day:day.into(),measure:measure.into(),period_start:start,period_end:end,
                baseline_from:from,baseline_until:start,prior_observed_days:prior.len()as u64,value:current_value,baseline,
                deviation:baseline.zip(current_value).map(|(base,value)|value-base),estimation:reason.into(),profile:format!("rhythm/prior-median-v1:{}:{}",self.0.window_days,self.0.minimum_days)},
                Self::NAME,rows.iter().map(|(event,_)|*event)));
        }
        Computed{error:None,outputs,waiting:false}
    }
}

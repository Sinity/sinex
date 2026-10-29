//! Validation and reuse of supplied vectors. This deterministic derivation
//! never constructs a model provider or reads credentials.
use crate::{Env,Input,PerEvent};
use crate::derived_event::derived;
use sinex_model::{Event,Origin};
use sinex_model::event_types::embedding::{SuppliedEmbedding,TextEmbedding};

pub struct Embeddings;
impl PerEvent for Embeddings {
    const NAME:&'static str="text.embeddings";
    const VERSION:u32=1;
    const INPUTS:&'static [&'static str]=&["embedding.supplied"];
    const OUTPUTS:&'static [&'static str]=&["text.embedding"];
    fn derive(&self,input:Input<'_>,_:&Env)->Result<Vec<Event>,String>{
        let supplied:SuppliedEmbedding=serde_json::from_value(input.event.payload.clone()).map_err(|error|format!("invalid supplied embedding: {error}"))?;
        supplied.validate()?;
        if !matches!(&input.event.origin,Origin::Derived{parents} if parents.contains(&supplied.target)) {
            return Err("supplied embedding must cite its target as an exact parent".into())
        }
        Ok(vec![derived(&TextEmbedding{target:supplied.target,supplied:input.event.id,input_record:supplied.input_record,
            input_hash:supplied.input_hash,space:supplied.space,vector:supplied.vector,embedded_at:supplied.embedded_at},Self::NAME,[input.event])])
    }
}
#[cfg(test)]
mod tests;

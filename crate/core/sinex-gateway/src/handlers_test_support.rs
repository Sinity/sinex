//! Test-only helpers for handler validation logic.

use color_eyre::eyre::{Result, WrapErr};
use sinex_db::models::Entity;
use sinex_db::replay::state_machine::ReplayState;
use sinex_primitives::Id;
use uuid::Uuid;

use crate::handlers::{
    decode_blob_content as decode_blob_content_inner,
    decode_note_content as decode_note_content_inner,
    parse_replay_state as parse_replay_state_inner,
    validate_entity_link_ids as validate_entity_link_ids_inner,
    validate_entity_name as validate_entity_name_inner,
};
pub fn decode_note_content(base64_content: &str) -> Result<String> {
    decode_note_content_inner(base64_content).map_err(Into::into)
}

pub fn validate_entity_name(name: &str) -> Result<()> {
    validate_entity_name_inner(name).map_err(Into::into)
}

pub fn validate_entity_link(from_id: &str, to_id: &str) -> Result<()> {
    let from = from_id
        .parse::<Uuid>()
        .map(Id::<Entity>::from_uuid)
        .wrap_err("Invalid or missing from_entity_id")?;
    let to = to_id
        .parse::<Uuid>()
        .map(Id::<Entity>::from_uuid)
        .wrap_err("Invalid or missing to_entity_id")?;
    validate_entity_link_ids_inner(&from, &to).map_err(Into::into)
}

pub fn decode_blob_content(content_b64: &str, limit: usize) -> Result<Vec<u8>> {
    decode_blob_content_inner(content_b64, limit).map_err(Into::into)
}

pub fn parse_replay_state(value: &str) -> Result<ReplayState> {
    parse_replay_state_inner(value).map_err(Into::into)
}

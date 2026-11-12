//! Receipt authority and diagnostics use their requested keys, not all frames.
pub(crate) const MATERIAL: &str = "CREATE INDEX ingress_frames_material ON core.ingress_frames (material_id, number) WHERE material_id IS NOT NULL";
pub(crate) const REFUSALS: &str = "CREATE INDEX ingress_frames_refused_at ON core.ingress_frames (received_at) WHERE refusal IS NOT NULL";
pub(crate) const MALFORMED: &str = "CREATE INDEX ingress_frames_malformed_producer ON core.ingress_frames (producer, number DESC) WHERE refusal IS NOT NULL AND outcome->>'kind' = 'malformed'";

pub(crate) const INDEXES: [&str; 3] = [MATERIAL, REFUSALS, MALFORMED];

pub(crate) fn statements() -> Vec<String> {
    INDEXES.into_iter().map(String::from).collect()
}

pub(crate) fn previous(tables: &mut Vec<String>) {
    tables.retain(|statement| !INDEXES.contains(&statement.as_str()));
}

#[cfg(test)]
mod tests;

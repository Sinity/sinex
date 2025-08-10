//! Knowledge graph models
//!
//! Marker types and domain models for the knowledge graph system.

use serde::{Deserialize, Serialize};

/// Marker type for Entity IDs
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct Entity;

/// Marker type for EntityRelation IDs
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct EntityRelation;

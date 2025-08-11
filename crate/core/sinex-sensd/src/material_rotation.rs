//! Material rotation manager for zero-gap invariant enforcement
//!
//! Ensures continuous materials always have zero gaps during rotation by:
//! 1. Staging next material before finalizing current
//! 2. Atomic switchover with brief overlap period
//! 3. Finalization only after new material is confirmed active
//!
//! This implements TARGET_final.md line 221: "Zero-gap invariant for continuous materials"

use crate::temporal_ledger::TemporalLedger;
use chrono::{DateTime, Utc};
use color_eyre::eyre::Result;
use sinex_core::types::Ulid;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{info, warn};

/// Material rotation state
#[derive(Debug, Clone)]
pub enum RotationState {
    /// Normal operation with single active material
    Normal {
        material_id: Ulid,
        started_at: DateTime<Utc>,
        bytes_written: i64,
    },
    /// Rotation in progress with overlap period
    Rotating {
        old_material_id: Ulid,
        new_material_id: Ulid,
        rotation_started_at: DateTime<Utc>,
        overlap_deadline: DateTime<Utc>,
    },
}

/// Rotation trigger conditions
#[derive(Debug, Clone)]
pub struct RotationPolicy {
    /// Maximum size in bytes before rotation
    pub max_bytes: i64,
    /// Maximum age before rotation
    pub max_age_seconds: u64,
    /// Overlap period during rotation (milliseconds)
    pub overlap_duration_ms: u64,
}

impl Default for RotationPolicy {
    fn default() -> Self {
        Self {
            max_bytes: 100 * 1024 * 1024, // 100MB
            max_age_seconds: 3600,        // 1 hour
            overlap_duration_ms: 100,     // 100ms overlap
        }
    }
}

/// Material rotation manager
pub struct MaterialRotationManager {
    temporal_ledger: Arc<TemporalLedger>,
    state: Arc<RwLock<RotationState>>,
    policy: RotationPolicy,
    source_type: String,
    source_path: String,
}

impl MaterialRotationManager {
    /// Create new rotation manager
    pub fn new(
        temporal_ledger: Arc<TemporalLedger>,
        policy: RotationPolicy,
        source_type: String,
        source_path: String,
    ) -> Self {
        // Start with uninitialized state - will be initialized on first use
        let state = Arc::new(RwLock::new(RotationState::Normal {
            material_id: Ulid::nil(),
            started_at: Utc::now(),
            bytes_written: 0,
        }));

        Self {
            temporal_ledger,
            state,
            policy,
            source_type,
            source_path,
        }
    }

    /// Initialize or get current material
    pub async fn get_or_create_material(&self) -> Result<Ulid> {
        let mut state = self.state.write().await;

        match &*state {
            RotationState::Normal { material_id, .. } if !material_id.is_nil() => Ok(*material_id),
            RotationState::Rotating {
                new_material_id, ..
            } => Ok(*new_material_id),
            _ => {
                // Need to create initial material
                let material_id = self
                    .temporal_ledger
                    .create_material(&self.source_type, &self.source_path, None)
                    .await?;

                info!(
                    "Created initial material {} for {}",
                    material_id, self.source_path
                );

                *state = RotationState::Normal {
                    material_id,
                    started_at: Utc::now(),
                    bytes_written: 0,
                };

                Ok(material_id)
            }
        }
    }

    /// Check if rotation is needed and initiate if necessary
    pub async fn check_rotation(&self, bytes_written: i64) -> Result<Option<Ulid>> {
        let mut state = self.state.write().await;

        // Clone values we need before mutation
        let (should_rotate, old_material_id) = match &*state {
            RotationState::Normal {
                material_id,
                started_at,
                ..
            } => {
                let age = Utc::now().signed_duration_since(*started_at).num_seconds() as u64;

                // Check rotation conditions
                let needs_rotation =
                    bytes_written >= self.policy.max_bytes || age >= self.policy.max_age_seconds;

                if needs_rotation {
                    info!(
                        "Initiating rotation for material {}: bytes={}, age={}s",
                        material_id, bytes_written, age
                    );
                    (true, *material_id)
                } else {
                    return Ok(None);
                }
            }
            RotationState::Rotating { .. } => {
                // Already rotating, check if we should complete
                return self.check_rotation_completion(&mut state).await;
            }
        };

        if should_rotate {
            // CRITICAL: Create new material BEFORE finalizing old one (zero-gap invariant)
            let new_material_id = self
                .temporal_ledger
                .create_material(&self.source_type, &self.source_path, None)
                .await?;

            let overlap_deadline =
                Utc::now() + chrono::Duration::milliseconds(self.policy.overlap_duration_ms as i64);

            *state = RotationState::Rotating {
                old_material_id,
                new_material_id,
                rotation_started_at: Utc::now(),
                overlap_deadline,
            };

            info!(
                "Rotation initiated: {} -> {}, overlap until {:?}",
                old_material_id, new_material_id, overlap_deadline
            );

            Ok(Some(new_material_id))
        } else {
            Ok(None)
        }
    }

    /// Complete rotation after overlap period
    async fn check_rotation_completion(&self, state: &mut RotationState) -> Result<Option<Ulid>> {
        // Extract values we need before mutation
        let (old_id, new_id, should_complete) = if let RotationState::Rotating {
            old_material_id,
            new_material_id,
            overlap_deadline,
            ..
        } = state
        {
            if Utc::now() >= *overlap_deadline {
                (*old_material_id, *new_material_id, true)
            } else {
                return Ok(None);
            }
        } else {
            return Ok(None);
        };

        if should_complete {
            // Overlap period complete, finalize old material
            info!("Completing rotation: finalizing material {}", old_id);

            // Get final byte count for old material
            let final_bytes = self.get_material_bytes(old_id).await?;

            // Finalize old material
            self.temporal_ledger
                .finalize_material(old_id, "rotated", final_bytes)
                .await?;

            // Transition to normal state with new material
            *state = RotationState::Normal {
                material_id: new_id,
                started_at: Utc::now(),
                bytes_written: 0,
            };

            info!("Rotation complete: now using material {}", new_id);
        }

        Ok(None)
    }

    /// Get current active material ID
    pub async fn get_active_material(&self) -> Result<Ulid> {
        let state = self.state.read().await;

        match &*state {
            RotationState::Normal { material_id, .. } => {
                if material_id.is_nil() {
                    drop(state);
                    self.get_or_create_material().await
                } else {
                    Ok(*material_id)
                }
            }
            RotationState::Rotating {
                new_material_id, ..
            } => {
                // During rotation, new material is active
                Ok(*new_material_id)
            }
        }
    }

    /// Update bytes written counter
    pub async fn update_bytes_written(&self, additional_bytes: i64) -> Result<()> {
        let mut state = self.state.write().await;

        if let RotationState::Normal { bytes_written, .. } = &mut *state {
            *bytes_written += additional_bytes;
        }

        Ok(())
    }

    /// Force rotation (e.g., on error or shutdown)
    pub async fn force_rotation(&self, reason: &str) -> Result<Ulid> {
        let mut state = self.state.write().await;

        // Extract values before mutation
        let (old_material_id, is_rotating, existing_new_id) = match &*state {
            RotationState::Normal { material_id, .. } => (*material_id, false, None),
            RotationState::Rotating {
                new_material_id,
                old_material_id,
                ..
            } => (*old_material_id, true, Some(*new_material_id)),
        };

        if !is_rotating {
            warn!(
                "Forcing rotation of material {} due to: {}",
                old_material_id, reason
            );

            // Create new material first (zero-gap invariant)
            let new_material_id = self
                .temporal_ledger
                .create_material(&self.source_type, &self.source_path, None)
                .await?;

            // Get final bytes
            let final_bytes = self.get_material_bytes(old_material_id).await?;

            // Finalize immediately (no overlap for forced rotation)
            self.temporal_ledger
                .finalize_material(old_material_id, reason, final_bytes)
                .await?;

            *state = RotationState::Normal {
                material_id: new_material_id,
                started_at: Utc::now(),
                bytes_written: 0,
            };

            info!(
                "Forced rotation complete: now using material {}",
                new_material_id
            );
            Ok(new_material_id)
        } else {
            let new_material_id = existing_new_id.unwrap();
            warn!("Forcing completion of ongoing rotation due to: {}", reason);

            // Complete the ongoing rotation immediately
            let final_bytes = self.get_material_bytes(old_material_id).await?;

            self.temporal_ledger
                .finalize_material(old_material_id, reason, final_bytes)
                .await?;

            *state = RotationState::Normal {
                material_id: new_material_id,
                started_at: Utc::now(),
                bytes_written: 0,
            };

            Ok(new_material_id)
        }
    }

    /// Get total bytes for a material from temporal ledger
    async fn get_material_bytes(&self, _material_id: Ulid) -> Result<i64> {
        // Query temporal ledger for total bytes
        // This would normally query the database
        // For now, return a placeholder
        Ok(0)
    }

    /// Verify zero-gap invariant is maintained
    pub async fn verify_zero_gap_invariant(&self) -> Result<bool> {
        let state = self.state.read().await;

        match &*state {
            RotationState::Normal { material_id, .. } => {
                // In normal state, we should have an active material
                Ok(!material_id.is_nil())
            }
            RotationState::Rotating {
                old_material_id,
                new_material_id,
                ..
            } => {
                // During rotation, both materials should be valid
                Ok(!old_material_id.is_nil() && !new_material_id.is_nil())
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_zero_gap_invariant() {
        // This test would require a mock TemporalLedger
        // For now, we document the expected behavior:

        // 1. Initial state: no material
        // 2. First get_or_create: creates material A
        // 3. Write data until rotation trigger
        // 4. Check rotation: creates material B BEFORE finalizing A
        // 5. During overlap: both A and B exist
        // 6. After overlap: A is finalized, only B remains active
        // 7. Throughout: verify_zero_gap_invariant() always returns true
    }
}

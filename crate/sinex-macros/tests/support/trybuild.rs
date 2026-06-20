//! Shared trybuild runner helper for sinex-macros.
//!
//! Keep the compile-fail fixture files and `.stderr` outputs individual; this
//! helper only removes repeated runner setup so diagnostic ownership stays
//! visible.

pub fn cases() -> trybuild::TestCases {
    trybuild::TestCases::new()
}

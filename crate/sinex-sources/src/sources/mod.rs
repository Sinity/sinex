//! Every source, one module per family, registered in one list.
pub mod atuin;
pub mod hyprland;
pub mod zsh;

use crate::registry::SourceSpec;

/// Every source's declaration.
pub fn specs() -> Vec<&'static SourceSpec> {
    vec![&atuin::SPEC, &zsh::SPEC, &hyprland::SPEC]
}

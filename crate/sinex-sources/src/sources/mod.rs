//! Every source, one module per family, registered in one list.
pub mod atuin;
pub mod hyprland;
pub mod journald;
pub mod zsh;

use crate::registry::{Source, SourceSpec};

/// Every source's declaration.
pub fn specs() -> Vec<&'static SourceSpec> {
    vec![&atuin::SPEC, &zsh::SPEC, &hyprland::SPEC, &journald::SPEC]
}

/// The source whose parser read a stored material, as a replay re-reads it
/// with the current version of that parser. `host` is the material's input
/// host, for parsers that record it.
pub fn by_parser(parser: &str, host: &str) -> Option<Box<dyn Source>> {
    match parser {
        p if p == atuin::SPEC.parser => Some(Box::new(atuin::source())),
        p if p == zsh::SPEC.parser => Some(Box::new(zsh::source(host.to_owned()))),
        p if p == hyprland::SPEC.parser => Some(Box::new(hyprland::source())),
        p if p == journald::SPEC.parser => Some(Box::new(journald::source())),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_registered_parser_resolves_to_its_source() {
        for spec in specs() {
            let source = by_parser(spec.parser, "h").expect("registered parser resolves");
            assert_eq!(source.spec().name, spec.name);
        }
        assert!(by_parser("no-such-parser", "h").is_none());
    }
}

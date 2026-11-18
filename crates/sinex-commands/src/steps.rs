//! One typed declaration of each verb's CLI fields and wire arguments.
use command_surface::{ArgumentForm, ArgumentSpec, StepDeclarations};
use sinex_protocol::rows::{Read, Save, Step};
use std::sync::LazyLock;

/// Step argument ownership is shared by all parsers. The CLI's
/// splitter passes already-separated words to this declaration owner.
pub fn steps() -> &'static StepDeclarations {
    static STEPS: LazyLock<StepDeclarations> = LazyLock::new(|| {
        let mut steps = StepDeclarations::default().cli_selection("selection", "references");
        steps
            .declare::<Read>(
                "read",
                vec![
                    ArgumentSpec::new(
                        "view",
                        ArgumentForm::Named,
                        "full content or provenance trace",
                    ),
                    ArgumentSpec::new("page_size", ArgumentForm::Named, "rows in a trace page"),
                    ArgumentSpec::new("cursor", ArgumentForm::Named, "continue a trace page"),
                    ArgumentSpec::new("at", ArgumentForm::Named, "exact accepted commit position"),
                ],
            )
            .expect("read step fields match their typed declaration");
        steps
            .declare::<Save>(
                "save",
                vec![
                    ArgumentSpec::new("name", ArgumentForm::Positional, "saved question name"),
                    ArgumentSpec::new(
                        "parameter",
                        ArgumentForm::Repeated,
                        "repeatable typed parameter declaration",
                    ),
                ],
            )
            .expect("save fields match their declaration");
        steps
    });
    &STEPS
}

/// Required credentials come from the typed steps, independently of tool names.
pub fn required_access(steps: &[Step]) -> command_surface::Access {
    steps
        .iter()
        .map(|step| match step {
            Step::Read(_) => command_surface::Access::Read,
            Step::Save(_) => command_surface::Access::Write,
        })
        .max()
        .unwrap_or(command_surface::Access::Read)
}

#[cfg(test)]
mod tests;

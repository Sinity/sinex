//! One typed declaration of each verb's CLI fields and wire arguments.
use command_surface::{ArgumentForm, ArgumentSpec, StepDeclarations};
use sinex_protocol::rows::{Lifecycle, Note, Read, Save, Step};
use std::sync::LazyLock;

/// Step argument ownership is shared by all parsers. The CLI's
/// splitter passes already-separated words to this declaration owner.
pub fn steps() -> &'static StepDeclarations {
    static STEPS: LazyLock<StepDeclarations> = LazyLock::new(|| {
        let mut steps = StepDeclarations::default().cli_selection("selection", "references");
        for verb in ["archive", "restore"] {
            steps
                .declare::<Lifecycle>(
                    verb,
                    vec![
                        ArgumentSpec::new("all", ArgumentForm::Flag, "authorize all selected rows"),
                        ArgumentSpec::new(
                            "dry_run",
                            ArgumentForm::Flag,
                            "show the cascade and refusals without writing",
                        ),
                    ],
                )
                .expect("lifecycle fields match their declaration");
        }
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
            .declare::<Note>(
                "note",
                vec![
                    ArgumentSpec::new(
                        "text",
                        ArgumentForm::Positional,
                        "text about the moment; empty text keeps context only",
                    ),
                    ArgumentSpec::new("at", ArgumentForm::Named, "RFC3339 moment; default now"),
                    ArgumentSpec::new(
                        "cwd",
                        ArgumentForm::Named,
                        "directory observed by the client",
                    ),
                    ArgumentSpec::new(
                        "command",
                        ArgumentForm::Named,
                        "command observed by the client",
                    ),
                    ArgumentSpec::new(
                        "app",
                        ArgumentForm::Named,
                        "application observed by the client",
                    ),
                    ArgumentSpec::new(
                        "title",
                        ArgumentForm::Named,
                        "window title observed by the client",
                    ),
                    ArgumentSpec::new(
                        "command_record",
                        ArgumentForm::Named,
                        "the shell's ID for the command",
                    ),
                    ArgumentSpec::new(
                        "focus_record",
                        ArgumentForm::Named,
                        "the window manager's ID for the focus change",
                    ),
                ],
            )
            .expect("note fields match their declaration");
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
            Step::Note(_) => command_surface::Access::Permanent,
            Step::Archive(_) | Step::Restore(_) => command_surface::Access::Write,
        })
        .max()
        .unwrap_or(command_surface::Access::Read)
}

#[cfg(test)]
mod tests;

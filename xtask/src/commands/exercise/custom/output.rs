use std::path::Path;

use crate::commands::exercise::builders::{v_empty, v_has, v_json, v_lines};
use crate::commands::exercise::runner::exec_step;
use crate::commands::exercise::types::{ExpectedExit, StepOutcome, Validation};

pub fn custom_output_format_matrix(dir: &Path, verbose: bool) -> Vec<StepOutcome> {
    let mut steps = Vec::new();

    let formats: &[(&str, ExpectedExit, Vec<Validation>)] = &[
        ("human", ExpectedExit::Success, vec![v_lines(Some(1), None)]),
        (
            "json",
            ExpectedExit::Success,
            vec![v_json(), v_has(&["status"])],
        ),
        (
            "compact",
            ExpectedExit::Success,
            vec![v_lines(Some(1), Some(5))],
        ),
        ("silent", ExpectedExit::Success, vec![v_empty()]),
    ];

    for (i, (fmt, expected, validations)) in formats.iter().enumerate() {
        let (outcome, _) = exec_step(
            dir,
            i,
            &format!("format_{fmt}"),
            &["status", "--doctor", "--format", fmt],
            *expected,
            validations,
            verbose,
        );
        steps.push(outcome);
    }

    steps
}

use super::*;
use crate::cell::Missing;
use crate::panel::Unit;
use crate::span::Span;

fn cell(c: Cell<f64>) -> PanelCell {
    PanelCell {
        metric: "shell.commands",
        metric_version: 1,
        unit: Unit {
            label: "2026-09-14".into(),
            span: Span::new(0, 1),
        },
        dimension: String::new(),
        dimension_value: String::new(),
        cell: c,
        numerator: None,
        denominator: None,
        n_rows: 0,
        observed_seconds: 0.0,
    }
}

#[test]
fn a_missing_day_is_rendered_as_unknown_never_zero() {
    let f = from_cell(
        &cell(Cell::Missing {
            reason: Missing::NoOpportunity,
        }),
        "Commands run",
        &["zsh"],
        false,
    );
    assert_eq!(
        f.markdown(),
        "shell.commands on 2026-09-14 is not known: no opportunity"
    );
    assert_eq!(f.status, Support::Unknown);
}

#[test]
fn caveats_render_with_their_claim() {
    let f = from_cell(
        &cell(Cell::Partial {
            value: 6.0,
            observed_fraction: 0.83,
        }),
        "Commands run",
        &["atuin", "zsh"],
        true,
    );
    assert_eq!(
        f.markdown(),
        "Commands run (shell.commands) on 2026-09-14: 6 [the day is not over] [only 83% of the unit was observed] [on atuin, zsh]"
    );
    assert_eq!(f.checks.coverage, Check::Qualified);
    let rec = from_cell(
        &cell(Cell::Recorded { value: 3.0 }),
        "Commands run",
        &["zsh"],
        false,
    );
    assert!(rec.markdown().contains(": at least 3 [a lower bound"));
}

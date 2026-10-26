use super::*;
use sinex_derivations::figures::metric;

const DAY: i64 = 86_400 * MICROS_PER_SECOND;

fn day_units(n: i64) -> Vec<Unit> {
    (0..n)
        .map(|i| Unit {
            label: format!("d{i}"),
            span: Span::new(i * DAY, (i + 1) * DAY),
        })
        .collect()
}

fn covered(source: &str, a: i64, b: i64) -> Coverage {
    Coverage {
        source: source.into(),
        span: Span::new(a, b),
        completeness: Completeness::Sample,
        bounded_by: "test".into(),
    }
}

fn figure(metric: &str, day: &str, dimension: (&str, &str), value: Option<f64>) -> DayFigure {
    DayFigure {
        metric: metric.into(),
        day: day.into(),
        dimension: dimension.0.into(),
        dimension_value: dimension.1.into(),
        value,
        numerator: None,
        denominator: None,
        rows: value.map_or(0, |v| v as u64),
        reference: format!("sinex:ev/{day}{}", dimension.1),
    }
}

fn totals(cells: &[PanelCell]) -> Vec<&Cell<f64>> {
    cells
        .iter()
        .filter(|c| c.dimension.is_empty())
        .map(|c| &c.cell)
        .collect()
}

/// The Kestrel example: partial first day, an observed day, a day whose last
/// hours the poll could not see, and a day with no poll at all, which is
/// missing, never zero.
#[devkit::test]
fn kestrel_week_tells_zero_from_missing() {
    let commands = metric("shell.commands").unwrap();
    let figures = [
        figure("shell.commands", "d0", ("", ""), Some(2.0)),
        figure("shell.commands", "d1", ("", ""), Some(3.0)),
    ];
    let coverage = [covered(
        "terminal.atuin-history",
        DAY - DAY / 4,
        2 * DAY + DAY / 2,
    )];
    let cells = cells(commands, &day_units(4), &figures, &coverage);
    assert_eq!(
        totals(&cells),
        [
            &Cell::Partial {
                value: 2.0,
                observed_fraction: 0.25
            },
            &Cell::Observed { value: 3.0 },
            &Cell::Partial {
                value: 0.0,
                observed_fraction: 0.5
            },
            &Cell::Missing {
                reason: Missing::NoCoverage
            },
        ]
    );
    assert_eq!(cells[1].reference.as_deref(), Some("sinex:ev/d1"));
    assert_eq!(cells[2].reference, None, "a zero has no figure");
}

#[devkit::test]
fn any_source_of_the_metric_covers_it() {
    let commands = metric("shell.commands").unwrap();
    let coverage = [
        covered("terminal.atuin-history", 0, DAY / 2),
        covered("terminal.zsh-history", DAY / 4, DAY),
        covered("desktop.hyprland", 0, 2 * DAY),
    ];
    let cells = cells(commands, &day_units(2), &[], &coverage);
    assert_eq!(
        totals(&cells),
        [
            &Cell::Observed { value: 0.0 },
            &Cell::Missing {
                reason: Missing::NoCoverage
            }
        ],
        "another metric's source covers nothing here"
    );
}

#[devkit::test]
fn figures_without_known_coverage_are_lower_bounds() {
    let commands = metric("shell.commands").unwrap();
    let unknown = Coverage {
        completeness: Completeness::Unknown,
        ..covered("terminal.zsh-history", 0, DAY)
    };
    let cells = cells(
        commands,
        &day_units(2),
        &[figure("shell.commands", "d0", ("", ""), Some(1.0))],
        &[unknown],
    );
    assert_eq!(
        totals(&cells),
        [
            &Cell::Recorded { value: 1.0 },
            &Cell::Missing {
                reason: Missing::CoverageUnknown
            }
        ]
    );
}

#[devkit::test]
fn a_ratio_of_nothing_is_not_applicable() {
    let failed = metric("shell.failed_ratio").unwrap();
    let cells = cells(
        failed,
        &day_units(1),
        &[],
        &[covered("terminal.atuin-history", 0, DAY)],
    );
    assert_eq!(
        totals(&cells),
        [&Cell::Missing {
            reason: Missing::NotApplicable
        }]
    );
}

#[devkit::test]
fn breakdowns_share_the_day_coverage() {
    let commands = metric("shell.commands").unwrap();
    let figures = [
        figure("shell.commands", "d0", ("", ""), Some(2.0)),
        figure("shell.commands", "d0", ("host", "b"), Some(1.0)),
        figure("shell.commands", "d0", ("host", "a"), Some(1.0)),
        figure("web.visits", "d0", ("", ""), Some(9.0)),
    ];
    let cells = cells(
        commands,
        &day_units(1),
        &figures,
        &[covered("terminal.zsh-history", 0, DAY)],
    );
    let hosts: Vec<&str> = cells
        .iter()
        .filter(|c| c.dimension == "host")
        .map(|c| c.dimension_value.as_str())
        .collect();
    assert_eq!(hosts, ["a", "b"]);
    assert_eq!(cells.len(), 3, "another metric's figures are not read");
    assert!(
        cells
            .iter()
            .all(|c| matches!(c.cell, Cell::Observed { value } if value >= 1.0))
    );
}

#[devkit::test]
fn logical_days_follow_the_day_setting() {
    let civil = Civil::new("Europe/Warsaw", 4, 0).unwrap();
    // 2026-10-25 is the autumn DST change in Warsaw: a 25-hour day.
    let from: jiff::civil::Date = "2026-10-24".parse().unwrap();
    let to: jiff::civil::Date = "2026-10-26".parse().unwrap();
    let days = days(&civil, from, to).unwrap();
    assert_eq!(days.len(), 3);
    assert_eq!(days[0].label, "2026-10-24");
    assert_eq!(days[0].span.len(), 25 * 3600 * MICROS_PER_SECOND);
    assert_eq!(days[1].span.len(), DAY);
}

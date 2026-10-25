use super::*;
use crate::metric::{AggLaw, Allocation, Quantity, SourceRole};
use serde_json::json;

const DAY: i64 = 86_400 * MICROS_PER_SECOND;

fn metric() -> MetricDef {
    MetricDef {
        id: "shell.commands",
        version: 1,
        description: "",
        relation: "command.fused",
        filter: None,
        unit: UnitGrain::Day,
        quantity: Quantity::Count,
        law: AggLaw::Count,
        interval: None,
        allocation: Allocation::Instant,
        coverage: CoverageRule::AnyOf(&["atuin", "zsh"]),
        dimensions: &["host"],
        role: SourceRole::Measured,
        deviation: true,
    }
}

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

/// The Kestrel example: partial first day, an observed day, a day whose last
/// hours the poll could not see, and a day with no poll at all, which is
/// missing, never zero.
#[devkit::test]
fn kestrel_week_tells_zero_from_missing() {
    let payloads: Vec<Value> = (0..5).map(|_| json!({"host": "h"})).collect();
    let mut rows = Vec::new();
    for p in &payloads[..2] {
        rows.push(Row {
            source: "atuin",
            at: DAY - 1000,
            payload: p,
        });
    }
    for p in &payloads[2..] {
        rows.push(Row {
            source: "atuin",
            at: DAY + 1000,
            payload: p,
        });
    }
    let coverage = [covered("atuin", DAY - DAY / 4, 2 * DAY + DAY / 2)];
    let cells = compute(
        &metric(),
        &day_units(4),
        &rows,
        &coverage,
        &SpanSet::default(),
    );
    let totals: Vec<_> = cells.iter().filter(|c| c.dimension.is_empty()).collect();
    assert_eq!(
        totals[0].cell,
        Cell::Partial {
            value: 2.0,
            observed_fraction: 0.25
        }
    );
    assert_eq!(totals[1].cell, Cell::Observed { value: 3.0 });
    assert_eq!(
        totals[2].cell,
        Cell::Partial {
            value: 0.0,
            observed_fraction: 0.5
        }
    );
    assert_eq!(
        totals[3].cell,
        Cell::Missing {
            reason: Missing::NoCoverage
        }
    );
}

#[devkit::test]
fn any_of_unions_and_sources_intersects() {
    let rows = [covered("atuin", 0, 10), covered("zsh", 5, 20)];
    let (u, _) = covered_spans(&CoverageRule::AnyOf(&["atuin", "zsh"]), &rows);
    assert_eq!(u.len(), 20);
    let (i, _) = covered_spans(&CoverageRule::Sources(&["atuin", "zsh"]), &rows);
    assert_eq!(i.len(), 5);
}

#[devkit::test]
fn gaps_turn_coverage_into_missing() {
    let cells = compute(
        &metric(),
        &day_units(1),
        &[],
        &[covered("zsh", 0, DAY)],
        &SpanSet::from_spans([Span::new(0, DAY)]),
    );
    assert_eq!(
        cells[0].cell,
        Cell::Missing {
            reason: Missing::Gap
        }
    );
}

#[devkit::test]
fn rows_without_known_coverage_are_lower_bounds() {
    let p = json!({"host": "h"});
    let rows = [Row {
        source: "zsh",
        at: 10,
        payload: &p,
    }];
    let unknown = Coverage {
        completeness: Completeness::Unknown,
        ..covered("zsh", 0, DAY)
    };
    let cells = compute(
        &metric(),
        &day_units(2),
        &rows,
        &[unknown],
        &SpanSet::default(),
    );
    assert_eq!(cells[0].cell, Cell::Recorded { value: 1.0 });
    assert_eq!(
        cells[2].cell,
        Cell::Missing {
            reason: Missing::CoverageUnknown
        }
    );
}

#[devkit::test]
fn breakdowns_share_the_unit_coverage() {
    let a = json!({"host": "a"});
    let b = json!({});
    let rows = [
        Row {
            source: "zsh",
            at: 10,
            payload: &a,
        },
        Row {
            source: "zsh",
            at: 20,
            payload: &b,
        },
    ];
    let cells = compute(
        &metric(),
        &day_units(1),
        &rows,
        &[covered("zsh", 0, DAY)],
        &SpanSet::default(),
    );
    let hosts: Vec<&str> = cells
        .iter()
        .filter(|c| c.dimension == "host")
        .map(|c| c.dimension_value.as_str())
        .collect();
    assert_eq!(hosts, ["a", "unknown"]);
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
    let start = 1_792_814_400 * MICROS_PER_SECOND; // 2026-10-24T02:00:00Z = 04:00 CEST
    let days = units(&civil, UnitGrain::Day, Span::new(start, start + 3 * DAY)).unwrap();
    assert_eq!(days[0].label, "2026-10-24");
    assert_eq!(days[0].span.len(), 25 * 3600 * MICROS_PER_SECOND);
    assert_eq!(days[1].span.len(), DAY);
}

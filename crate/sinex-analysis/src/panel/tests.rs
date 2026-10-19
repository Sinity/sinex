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
        opportunity: OpportunityRule::AnyOf(&["atuin", "zsh"]),
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

fn opp(source: &str, a: i64, b: i64) -> Opportunity {
    Opportunity {
        source: source.into(),
        span: Span::new(a, b),
        completeness: Completeness::Sample,
        basis: "test".into(),
    }
}

/// The Kestrel example (`analysis.md` §3.3): partial first day, an
/// observed day, a day whose last hours the poll could not see, and a day
/// with no poll at all, which is missing, never zero.
#[test]
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
    let opportunity = [opp("atuin", DAY - DAY / 4, 2 * DAY + DAY / 2)];
    let cells = compute(
        &metric(),
        &day_units(4),
        &rows,
        &opportunity,
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
            reason: Missing::NoOpportunity
        }
    );
}

#[test]
fn any_of_unions_and_sources_intersects() {
    let rows = [opp("atuin", 0, 10), opp("zsh", 5, 20)];
    let (u, _) = opportunity_for(&OpportunityRule::AnyOf(&["atuin", "zsh"]), &rows);
    assert_eq!(u.len(), 20);
    let (i, _) = opportunity_for(&OpportunityRule::Sources(&["atuin", "zsh"]), &rows);
    assert_eq!(i.len(), 5);
}

#[test]
fn gaps_turn_opportunity_into_missing() {
    let cells = compute(
        &metric(),
        &day_units(1),
        &[],
        &[opp("zsh", 0, DAY)],
        &SpanSet::from_spans([Span::new(0, DAY)]),
    );
    assert_eq!(
        cells[0].cell,
        Cell::Missing {
            reason: Missing::Gap
        }
    );
}

#[test]
fn rows_without_known_opportunity_are_lower_bounds() {
    let p = json!({"host": "h"});
    let rows = [Row {
        source: "zsh",
        at: 10,
        payload: &p,
    }];
    let unknown = Opportunity {
        completeness: Completeness::Unknown,
        ..opp("zsh", 0, DAY)
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

#[test]
fn breakdowns_share_the_unit_opportunity() {
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
        &[opp("zsh", 0, DAY)],
        &SpanSet::default(),
    );
    assert_eq!(
        dimension_values(&cells, "host")
            .into_iter()
            .collect::<Vec<_>>(),
        ["a", "unknown"]
    );
    assert!(
        cells
            .iter()
            .all(|c| matches!(c.cell, Cell::Observed { value } if value >= 1.0))
    );
}

#[test]
fn logical_days_follow_the_day_setting() {
    let civil = Civil::new("Europe/Warsaw", 4, 0).unwrap();
    // 2026-10-25 is the autumn DST change in Warsaw: a 25-hour day.
    let start = 1_792_814_400 * MICROS_PER_SECOND; // 2026-10-24T02:00:00Z = 04:00 CEST
    let days = units(&civil, UnitGrain::Day, Span::new(start, start + 3 * DAY)).unwrap();
    assert_eq!(days[0].label, "2026-10-24");
    assert_eq!(days[0].span.len(), 25 * 3600 * MICROS_PER_SECOND);
    assert_eq!(days[1].span.len(), DAY);
}

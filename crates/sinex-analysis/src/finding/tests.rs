use super::*;
use crate::cell::Missing;
use crate::panel::Unit;
use crate::span::Span;

fn cell(c: Cell<f64>) -> PanelCell {
    PanelCell {
        metric: "shell.commands",
        unit: Unit {
            label: "2026-09-14".into(),
            span: Span::new(0, 1),
        },
        dimension: String::new(),
        dimension_value: String::new(),
        cell: c,
        numerator: None,
        denominator: None,
        rows: 0,
        observed_seconds: 0.0,
        reference: Some("sinex:ev/f".into()),
    }
}

#[devkit::test]
fn a_missing_day_is_rendered_as_unknown_never_zero() {
    let f = from_cell(
        &cell(Cell::Missing {
            reason: Missing::NoCoverage,
        }),
        "Commands run",
        &["zsh"],
        false,
    );
    assert_eq!(
        f.markdown(),
        "shell.commands on 2026-09-14 is not known: no coverage"
    );
    assert_eq!(f.rank().0, 2, "unknown values rank last");
}

#[devkit::test]
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
    assert_eq!(f.evidence, ["sinex:ev/f"]);
    assert_eq!(f.rank().0, 1);
    let rec = from_cell(
        &cell(Cell::Recorded { value: 3.0 }),
        "Commands run",
        &["zsh"],
        false,
    );
    assert!(rec.markdown().contains(": at least 3 [a lower bound"));
}

#[devkit::test]
fn multiline_observations_are_one_citable_presentation_line() {
    let finding = Finding {
        template: Template::Observation {
            event_type: "synthetic.record".into(),
            source: "synthetic".into(),
            when: None,
            description: "first\r\nsecond\nthird\u{2028}fourth".into(),
        },
        caveats: vec![],
        evidence: vec![],
    };
    let text = finding.markdown();
    assert!(!text.contains('\n') && !text.contains('\r') && !text.contains('\u{2028}'));
    assert!(text.contains("first\\r\\nsecond\\nthird\\u{2028}fourth"));
}

#[devkit::test]
fn num_keeps_small_effects() {
    for (value, expected) in [
        (0.0, "0"),
        (-0.0, "0"),
        (1.0, "1"),
        (-12.0, "-12"),
        (12.345, "12.35"),
        (-1.234, "-1.23"),
        (0.0031, "0.0031"),
        (-0.003123, "-0.00312"),
        (0.0000000031, "0.0000000031"),
        (0.9999, "1"),
    ] {
        assert_eq!(number(value), expected, "{value}");
    }
    let association = Finding {
        template: Template::Association {
            x: "x".into(),
            y: "y".into(),
            lag: 0,
            r: 0.0031,
            ci95: Some([-0.00012, 0.00632]),
            n: 20,
            n_eff: 18.0,
            q: Some(0.000031),
            family_size: 2,
        },
        caveats: vec![Caveat::SmallN {
            n: 20,
            min_detectable_r: Some(0.00042),
        }],
        evidence: vec![],
    };
    assert_eq!(
        association.markdown(),
        "x and y (lag 0) are associated, r = 0.0031 over 20 units (effective 18.0), 95% interval -0.00012 to 0.00632, q = 0.000031 across 2 tested pairs [20 units; effects below r = 0.00042 would go undetected]"
    );
    let trend = Finding {
        template: Template::Trend {
            metric: "small.effect".into(),
            slope_per_unit: -0.0031,
            p: 0.00021,
            from: "2026-01-01".into(),
            to: "2026-01-20".into(),
            units: 20,
        },
        caveats: vec![],
        evidence: vec![],
    };
    assert_eq!(
        trend.markdown(),
        "small.effect fell by 0.0031 per day from 2026-01-01 to 2026-01-20 (20 days, Mann-Kendall p = 0.00021)"
    );
}

use super::super::observed;
use super::*;

fn run(x: &[f64]) -> serde_json::Value {
    let mut input = MethodInput::default();
    input.series.insert("m".into(), observed(x));
    let p = ChangePointParams {
        metric: "m".into(),
        min_segment: 7,
        max_points: 10,
    };
    ChangePoints::run(&p, &input).unwrap().output
}

#[test]
fn a_level_shift_is_found_where_it_happened() {
    let noise = [0.3, -0.2, 0.1, -0.4, 0.2, 0.0, -0.1, 0.4, -0.3, 0.1];
    let x: Vec<f64> = (0..30)
        .map(|i| if i < 18 { 5.0 } else { 9.0 } + noise[i % 10])
        .collect();
    let out = run(&x);
    let cps = out["change_points"].as_array().unwrap();
    assert_eq!(cps.len(), 1);
    assert_eq!(cps[0]["index"], 18);
    assert_eq!(cps[0]["unit"], "u18");
}

#[test]
fn noise_alone_gives_no_change_point() {
    let x: Vec<f64> = (0..40)
        .map(|i| 5.0 + [0.3, -0.2, 0.1, -0.4, 0.2][i % 5])
        .collect();
    assert!(run(&x)["change_points"].as_array().unwrap().is_empty());
}

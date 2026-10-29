use super::*;
use serde_json::Map;
use sinex_model::{Class, MaterialId, Origin, ReadingOrder, RecordLocation};

/// 2024-06-15 01:00 UTC, 03:00 in Warsaw: before the 04:00 day start, so in
/// the logical day 2024-06-14.
const T: i64 = 1_718_413_200;

fn at(secs: i64) -> Timestamp {
    Timestamp::from_unix_secs(T + secs).unwrap()
}

fn point(provider: &str, source: &str, secs: i64, values: Value) -> Event {
    let start = at(secs);
    let mut fields = Map::new();
    fields.insert("fitValue".into(), values);
    Event::build(
        &HealthRecord::new(
            provider.into(),
            "com.google.heart_rate.bpm".into(),
            format!("{source}:{}:{}", start.unix_ns(), start.unix_ns() + 1),
            Some(start),
            Some(start),
            None,
            fields,
            vec![],
        )
        .unwrap(),
        "google.fit",
        Origin::Record {
            location: RecordLocation {
                material: MaterialId::new(),
                anchor: 0,
            },
            output_index: 0,
        },
        ReadingOrder(0),
        [0; 32],
        Class::Open,
    )
    .unwrap()
}

fn fit(source: &str, secs: i64, value: f64) -> Event {
    point(
        "google-fit",
        source,
        secs,
        json!([{"fpVal": value}, {"value": {"intVal": 1}}]),
    )
}

fn env() -> Env {
    Env::at(at(10 * 86_400), sinex_model::time_settings::TimeSettings::new("Europe/Warsaw", 4, 0).expect("fixture day setting"))
}

fn keys(d: &dyn crate::AnyKeyed, e: &Event) -> Vec<Value> {
    d.keys_for(
        &KeyView {
            event_type: &e.event_type,
            ts_orig: e.ts_orig.at,
            payload: &e.payload,
        },
        &env(),
    )
}

fn figures(d: &dyn crate::AnyKeyed, key: &Value, inputs: &[Event]) -> Vec<(HealthFigure, Event)> {
    d.compute(key, inputs, &env())
        .outputs
        .into_iter()
        .map(|e| (e.payload_as::<HealthFigure>().unwrap(), e))
        .collect()
}

fn parents(e: &Event) -> Vec<sinex_model::EventId> {
    match &e.origin {
        Origin::Derived { parents } => parents.clone(),
        Origin::Record { .. } => vec![],
    }
}

#[devkit::test]
fn a_point_dirties_its_civil_hour_and_only_fit_points_count() {
    let p = fit("raw:heart", 1_800, 60.0);
    assert_eq!(
        keys(&HourlyFigures, &p),
        vec![json!({"hour": "2024-06-15T01:00:00Z"})]
    );
    let other = point("samsung-health", "raw:heart", 1_800, json!([]));
    assert!(keys(&HourlyFigures, &other).is_empty());
    let query = HourlyFigures
        .inputs(&json!({"hour": "2024-06-15T01:00:00Z"}), &env())
        .unwrap();
    assert_eq!(query.window, Some((at(0), at(3_600))));
    assert_eq!(
        query.kinds,
        vec![crate::Kind {
            event_type: "health.record".into(),
            equal: vec![("provider".into(), json!("google-fit"))],
        }]
    );
}

#[devkit::test]
fn an_hour_sums_each_series_and_value_position_from_its_points() {
    let points = [
        fit("raw:heart", 10, 60.0),
        fit("raw:heart", 20, 80.0),
        fit("merged:heart", 30, 70.0),
        point("google-fit", "raw:heart", 40, json!([{"stringVal": "x"}])),
    ];
    let out = figures(
        &HourlyFigures,
        &json!({"hour": "2024-06-15T01:00:00Z"}),
        &points,
    );
    assert_eq!(out.len(), 2);
    let (merged, _) = &out[0];
    assert_eq!(merged.series, "merged:heart");
    assert_eq!(merged.points, 1);
    let (raw, event) = &out[1];
    assert_eq!(raw.series, "raw:heart");
    assert_eq!(raw.grain, "hour");
    assert_eq!(raw.day, "2024-06-14");
    assert_eq!(raw.category, "com.google.heart_rate.bpm");
    assert_eq!((raw.period_start, raw.period_end), (at(0), at(3_600)));
    assert_eq!(raw.points, 3);
    assert_eq!(
        raw.values,
        vec![
            FigureValue {
                position: 0,
                count: 2,
                sum: 140.0,
                min: 60.0,
                max: 80.0
            },
            FigureValue {
                position: 1,
                count: 2,
                sum: 2.0,
                min: 1.0,
                max: 1.0
            },
        ]
    );
    assert_eq!(
        parents(event),
        vec![points[0].id, points[1].id, points[3].id]
    );
}

#[devkit::test]
fn a_day_combines_its_hourly_figures_on_the_logical_day() {
    let night = figures(
        &HourlyFigures,
        &json!({"hour": "2024-06-15T01:00:00Z"}),
        &[fit("raw:heart", 10, 50.0)],
    );
    let evening = figures(
        &HourlyFigures,
        &json!({"hour": "2024-06-14T18:00:00Z"}),
        &[
            fit("raw:heart", -25_200, 90.0),
            fit("raw:heart", -25_100, 70.0),
        ],
    );
    let hourly: Vec<Event> = night
        .iter()
        .chain(&evening)
        .map(|(_, e)| e.clone())
        .collect();
    assert_eq!(
        keys(&DailyFigures, &hourly[0]),
        vec![json!({"day": "2024-06-14"})]
    );
    let out = figures(&DailyFigures, &json!({"day": "2024-06-14"}), &hourly);
    assert_eq!(out.len(), 1);
    let (day, event) = &out[0];
    assert_eq!(day.grain, "day");
    assert_eq!(day.points, 3);
    assert_eq!(
        day.values[0],
        FigureValue {
            position: 0,
            count: 3,
            sum: 210.0,
            min: 50.0,
            max: 90.0
        }
    );
    assert_eq!(parents(event), vec![hourly[0].id, hourly[1].id]);
    // A daily figure dirties nothing, so the daily derivation never reads its
    // own outputs.
    assert!(keys(&DailyFigures, event).is_empty());
}

//! `bench output-ranges`: what admission's lookup of the stored outputs an
//! input's range touches reads, as one derivation's stored outputs grow. It
//! runs the store's two lookup statements against generated ranges and
//! records the rows each read from the range table, from its plan.
use super::*;
use measurements::{query, save};

const AFFECTED_KEYS: &str =
    include_str!("../../../crates/sinex-store/src/output_ranges/affected_keys.sql");
const OPEN_KEYS: &str = include_str!("../../../crates/sinex-store/src/output_ranges/open_keys.sql");

/// Stored outputs of the one derivation at each measurement.
const SIZES: [u64; 3] = [10_000, 100_000, 1_000_000];
/// The derivation's keys; every other key has open outputs.
const KEYS: u64 = 256;

/// Rows a plan read from the range table, and the blocks its statement touched.
fn read(plan: &Value) -> Value {
    fn rows(node: &Value) -> f64 {
        let number = |name: &str| node[name].as_f64().unwrap_or(0.0);
        let own = if node["Relation Name"] == "output_ranges" {
            (number("Actual Rows")
                + number("Rows Removed by Filter")
                + number("Rows Removed by Index Recheck"))
                * number("Actual Loops")
        } else {
            0.0
        };
        own + node["Plans"]
            .as_array()
            .map_or(0.0, |plans| plans.iter().map(rows).sum())
    }
    let top = &plan[0]["Plan"];
    json!({
        "rows_read": rows(top),
        "rows_returned": top["Actual Rows"],
        "blocks": top["Shared Hit Blocks"].as_f64().unwrap_or(0.0)
            + top["Shared Read Blocks"].as_f64().unwrap_or(0.0),
        "execution_ms": plan[0]["Execution Time"],
    })
}

pub(super) fn run(parent: &Path) -> Result<(), String> {
    let out = measurements::run_directory(parent)?;
    let tool = schema_tool()?;
    let mut cluster = Cluster::start(&out.join("pg"), true, &[])?;
    let applied = Command::new(&tool)
        .arg("apply")
        .arg(cluster.url())
        .output()
        .map_err(|e| e.to_string())?;
    if !applied.status.success() {
        return Err(format!(
            "applying the schema: {}",
            String::from_utf8_lossy(&applied.stderr)
        ));
    }
    save(&out, "environment.json", &measurements::environment(&out))?;
    // Output `n` belongs to key `n % KEYS`. An odd one covers ten seconds
    // from second `10 n`; an even one is open from there. The asked range is
    // 25 seconds in the first size's middle, so every size holds the same
    // overlapping outputs and the same keys with an earlier open output.
    let asked = "'2026-01-01T13:53:20Z'::timestamptz";
    let mut stored = 0;
    let mut sizes = Vec::new();
    for size in SIZES {
        cluster
            .psql(&format!(
                "INSERT INTO derivation.output_ranges (derivation, key, event_id, event_type, started_at, ended_at)
                 SELECT 'bench.ranges', jsonb_build_object('key', n % {KEYS}), uuidv7(), 'bench.interval',
                        '2026-01-01T00:00:00Z'::timestamptz + n * interval '10 seconds',
                        CASE WHEN n % 2 = 1 THEN '2026-01-01T00:00:10Z'::timestamptz + n * interval '10 seconds' END
                 FROM generate_series({}, {size}) n",
                stored + 1
            ))
            .ok_or("storing generated ranges failed")?;
        cluster
            .psql("VACUUM (ANALYZE) derivation.output_ranges")
            .ok_or("analyzing generated ranges failed")?;
        stored = size;
        let overlapping = query(
            &cluster,
            &format!(
                "PREPARE overlapping(text[], text[], timestamptz[], timestamptz[]) AS {AFFECTED_KEYS};
                 EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) EXECUTE overlapping(
                   '{{bench.ranges}}', '{{bench.interval}}', ARRAY[{asked}], ARRAY[{asked} + interval '25 seconds'])"
            ),
        )?;
        let open = query(
            &cluster,
            &format!(
                "PREPARE open(text, text, timestamptz) AS {OPEN_KEYS};
                 EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) EXECUTE open(
                   'bench.ranges', 'bench.interval', {asked} + interval '25 seconds')"
            ),
        )?;
        save(&out, &format!("overlapping-{size}.json"), &overlapping)?;
        save(&out, &format!("open-{size}.json"), &open)?;
        sizes.push(json!({
            "stored_outputs": size,
            "ended_outputs_overlapping": read(&overlapping),
            "keys_with_an_open_output": read(&open),
        }));
    }
    let teardown_s = cluster.stop()?;
    let metrics = json!({"complete": true, "keys": KEYS, "sizes": sizes, "teardown_s": teardown_s,
        "artifacts": out,
        "scope": "The two statements of admission's affected-output lookup over generated ranges of one derivation; no events are stored."});
    save(&out, "metrics.json", &metrics)?;
    let store = Store::open().map_err(|e| e.to_string())?;
    let checkout = env::current_dir().map_err(|e| e.to_string())?;
    let id = bench::record(
        &store,
        &checkout,
        "sinex",
        "output-range-lookup",
        "generated-10k-100k-1m",
        &out,
        &json!({"metrics": metrics}),
    )
    .map_err(|e| e.to_string())?;
    println!("{}", json!({"run": id, "metrics": metrics}));
    Ok(())
}

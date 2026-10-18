//! `src/shell-count.sql` counts, for each record, the other source's records
//! that could pair with it. Compare it with a plain Rust count over edge cases,
//! and check that it uses the (source, command_hash, at_ns) index.
use serde_json::Value;
use sqlx::{Connection, PgConnection};
use uuid::Uuid;

type Result<T = ()> = std::result::Result<T, Box<dyn std::error::Error>>;

const COUNT_SQL: &str = include_str!("../src/shell-count.sql");
const TABLE: [&str; 2] = [
    "CREATE TEMP TABLE sinex_shell_nodes(id uuid PRIMARY KEY,source smallint NOT NULL,host text,command text NOT NULL,command_hash text NOT NULL,at_ns numeric,paired boolean NOT NULL DEFAULT false,count2 bigint NOT NULL DEFAULT 0,count60 bigint NOT NULL DEFAULT 0)",
    "CREATE INDEX ON sinex_shell_nodes(source,command_hash,at_ns)",
];

struct Node {
    id: Uuid,
    source: i16,
    /// Half nanoseconds, so a fractional numeric time is exact.
    at: Option<i128>,
    host: Option<&'static str>,
    command: &'static str,
    hash: &'static str,
    paired: bool,
}

/// What the SQL must compute: other-source records with equal host, hash and
/// command, both times known and at most `bound` ns apart; when `fallback`,
/// only records that are not yet paired.
fn expected(nodes: &[Node], n: &Node, bound: i64, fallback: bool) -> i64 {
    nodes
        .iter()
        .filter(|x| {
            x.source != n.source
                && x.host.is_some()
                && x.host == n.host
                && x.hash == n.hash
                && x.command == n.command
                && matches!((x.at, n.at), (Some(a), Some(b)) if (a - b).abs() <= 2 * i128::from(bound))
                && (!fallback || (!x.paired && !n.paired))
        })
        .count() as i64
}

fn numeric(half_ns: i128) -> String {
    match half_ns % 2 {
        0 => (half_ns / 2).to_string(),
        _ => {
            assert!(half_ns > 0, "fractional fixtures are positive");
            format!("{}.5", half_ns / 2)
        }
    }
}

fn fixture() -> Vec<Node> {
    let mut nodes = Vec::new();
    let mut add = |source, at: Option<i128>, host, command, hash, paired| {
        nodes.push(Node {
            id: Uuid::from_u128(nodes.len() as u128 + 1),
            source,
            at,
            host,
            command,
            hash,
            paired,
        })
    };
    let max = i128::from(i64::MAX);
    let min = i128::from(i64::MIN);
    // Window edges, both i64 extremes and beyond, and an unknown time.
    for at in [
        Some(0),
        Some(2_000_000_000),
        Some(-2_000_000_000),
        Some(2_000_000_001),
        Some(-2_000_000_001),
        Some(60_000_000_000),
        Some(-60_000_000_000),
        Some(60_000_000_001),
        Some(min),
        Some(max),
        Some(min - 60_000_000_000),
        Some(max + 60_000_000_000),
        Some(10_i128.pow(30)),
        Some(-(10_i128.pow(30))),
        None,
    ] {
        for source in [0, 1] {
            add(source, at.map(|t| t * 2), Some("h"), "a", "same", false);
        }
    }
    add(1, Some(0), Some("h"), "a", "same", false);
    add(1, Some(0), Some("h"), "a", "same", false);
    add(0, Some(0), None, "a", "same", false);
    add(1, Some(0), None, "a", "same", false);
    // Equal hash, different command.
    add(
        1,
        Some(0),
        Some("h"),
        "different full command",
        "same",
        false,
    );
    add(1, Some(0), Some("other"), "a", "same", false);
    add(1, Some(0), Some("h"), "a", "same", true);
    add(0, Some(0), Some("h"), "a", "same", true);
    // Fractional times catch integer casts.
    add(0, Some(1), Some("h"), "a", "same", false);
    add(1, Some(4_000_000_001), Some("h"), "a", "same", false);
    let mut state = 8429_u64;
    let mut next = |n: u64| {
        // splitmix64
        state = state.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let mut z = state;
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        (z ^ (z >> 31)) % n
    };
    for _ in 0..96 {
        let source = next(2) as i16;
        let at = [None, Some(0), Some(2_000_000_000), Some(max)][next(4) as usize];
        let host = [Some("h"), Some("other"), None][next(3) as usize];
        let command = ["a", "b"][next(2) as usize];
        let hash = ["same", "different"][next(2) as usize];
        let paired = next(2) == 1;
        add(source, at.map(|t| t * 2), host, command, hash, paired);
    }
    nodes
}

fn plan_nodes(plan: &Value) -> Vec<&Value> {
    let mut out = vec![plan];
    for child in plan["Plans"].as_array().into_iter().flatten() {
        out.extend(plan_nodes(child));
    }
    out
}

#[tokio::test]
#[ignore = "requires SINEX_TEST_ADMIN_URL"]
async fn shell_count_sql_matches_a_plain_count_and_uses_its_index() -> Result {
    let mut db = PgConnection::connect(&std::env::var("SINEX_TEST_ADMIN_URL")?).await?;
    for statement in TABLE {
        sqlx::query(statement).execute(&mut db).await?;
    }
    let nodes = fixture();
    for n in &nodes {
        sqlx::query("INSERT INTO sinex_shell_nodes(id,source,host,command,command_hash,at_ns,paired) VALUES($1,$2,$3,$4,$5,$6::text::numeric,$7)")
            .bind(n.id).bind(n.source).bind(n.host).bind(n.command).bind(n.hash).bind(n.at.map(numeric)).bind(n.paired)
            .execute(&mut db).await?;
    }
    for bound in [0, 2_000_000_000, 60_000_000_000, i64::MAX] {
        for fallback in [false, true] {
            let field = if fallback { "count60" } else { "count2" };
            let update = COUNT_SQL.replace("{field}", field);
            // The product updates at most 64 records per statement.
            for chunk in nodes.chunks(64) {
                let ids: Vec<Uuid> = chunk.iter().map(|n| n.id).collect();
                sqlx::query(&update)
                    .bind(&ids)
                    .bind(bound)
                    .bind(fallback)
                    .execute(&mut db)
                    .await?;
            }
            let actual: Vec<(Uuid, i64)> = sqlx::query_as(&format!(
                "SELECT id,{field} FROM sinex_shell_nodes ORDER BY id"
            ))
            .fetch_all(&mut db)
            .await?;
            let wanted: Vec<(Uuid, i64)> = nodes
                .iter()
                .map(|n| (n.id, expected(&nodes, n, bound, fallback)))
                .collect();
            assert_eq!(actual, wanted, "bound {bound}, fallback {fallback}");
        }
    }

    // Plan on a larger table, indexed before it is filled and not analyzed, as
    // the product's work tables are.
    sqlx::query("DROP TABLE sinex_shell_nodes")
        .execute(&mut db)
        .await?;
    for statement in TABLE {
        sqlx::query(statement).execute(&mut db).await?;
    }
    sqlx::query("INSERT INTO sinex_shell_nodes(id,source,host,command,command_hash,at_ns) SELECT md5(n::text)::uuid,0,'h','command '||n,md5(n::text),n::numeric*1000000000 FROM generate_series(1,20000) n")
        .execute(&mut db).await?;
    let keys: Vec<String> = (1..=64).map(|n| format!("md5('{n}')::uuid")).collect();
    let update = COUNT_SQL
        .replace("{field}", "count2")
        .replace("$1", &format!("ARRAY[{}]::uuid[]", keys.join(",")))
        .replace("$2", "2000000000")
        .replace("$3", "false");
    let plan: Value = sqlx::query_scalar(&format!("EXPLAIN (ANALYZE,FORMAT JSON) {update}"))
        .fetch_one(&mut db)
        .await?;
    let conditions: Vec<&str> = plan_nodes(&plan[0]["Plan"])
        .into_iter()
        .filter(|node| {
            node["Index Name"]
                .as_str()
                .is_some_and(|name| name.contains("source_command_hash_at_ns"))
        })
        .filter_map(|node| node["Index Cond"].as_str())
        .collect();
    assert!(
        conditions
            .iter()
            .any(|c| ["source", "command_hash", "at_ns >=", "at_ns <="]
                .iter()
                .all(|part| c.contains(part))),
        "{plan}"
    );
    Ok(())
}

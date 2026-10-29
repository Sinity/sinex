//! Independent custody proof for the two generated native histories.
use super::*;
use serde_json::{Value, json};
use std::collections::{BTreeSet, HashMap, HashSet};
use std::os::unix::fs::MetadataExt;
use uuid::Uuid;

type Check<T> = Result<T, String>;
fn require(ok: bool, reason: &str) -> Check<()> {
    if ok { Ok(()) } else { Err(reason.into()) }
}
fn number(value: &Value, name: &str) -> Check<u64> {
    value[name]
        .as_u64()
        .ok_or_else(|| format!("missing unsigned {name}"))
}
fn id(value: &Value, name: &str) -> Check<Uuid> {
    value[name]
        .as_str()
        .and_then(|v| Uuid::parse_str(v).ok())
        .ok_or_else(|| format!("missing material {name}"))
}
fn digest(value: &impl serde::Serialize) -> String {
    blake3::hash(&serde_json::to_vec(value).unwrap())
        .to_hex()
        .to_string()
}
#[derive(Clone)]
struct Part {
    id: Uuid,
    input: Uuid,
    index: u32,
    previous: Option<Uuid>,
    manifest: Value,
    bytes: Vec<u8>,
}
#[derive(Clone)]
struct Fact {
    material: Uuid,
    anchor: u64,
    hash: [u8; 32],
    payload: Value,
    order: i64,
}
#[derive(Clone)]
struct Snapshot {
    scope: Value,
    input: Uuid,
    cursor: Value,
    carry: Value,
    parts: HashMap<Uuid, Part>,
    facts: Vec<Fact>,
    failures: BTreeSet<(Uuid, u64)>,
    memory: Option<(Uuid, Vec<u8>)>,
}
#[derive(Clone)]
enum Fixture {
    Atuin {
        rows: usize,
        generation: String,
    },
    Zsh {
        records: usize,
        bytes: Vec<u8>,
        inode: u64,
        mtime: i128,
        ctime: i128,
    },
}

/// Validate both stores independently before discarding any physical locator.
pub(super) async fn positions(target: &Target, inputs: &Inputs) -> Vec<(String, Value, Value)> {
    let mut conn = target.connect().await;
    let blobs = sinex_store::Blobs::open(&target.store).unwrap();
    blobs.load(&mut conn).await.unwrap();
    let a = std::fs::metadata(&inputs.atuin).unwrap();
    let z = std::fs::metadata(&inputs.zsh).unwrap();
    let fixtures = [
        (
            format!("terminal.atuin-history:{}@{HOST}", inputs.atuin.display()),
            "terminal.atuin-history",
            "atuin-history",
            1u32,
            &inputs.atuin,
            Fixture::Atuin {
                rows: inputs.atuin_rows,
                generation: format!("{HOST}:{}:{}", a.dev(), a.ino()),
            },
        ),
        (
            inputs.zsh_binding(),
            "terminal.zsh-history",
            "zsh-extended-history",
            3u32,
            &inputs.zsh,
            Fixture::Zsh {
                records: inputs.zsh_records,
                bytes: inputs.zsh_bytes(),
                inode: z.ino(),
                mtime: i128::from(z.mtime()) * 1_000_000_000 + i128::from(z.mtime_nsec()),
                ctime: i128::from(z.ctime()) * 1_000_000_000 + i128::from(z.ctime_nsec()),
            },
        ),
    ];
    let mut out = Vec::new();
    for (binding, source, parser, version, path, fixture) in fixtures {
        let started = Instant::now();
        let reference = sinex_model::SourceRef {
            name: source.into(),
            version,
        };
        let declaration = sinex_store::source_declarations::load(&mut conn, &reference)
            .await
            .unwrap()
            .expect("native declaration");
        let fingerprint = declaration.fingerprint();
        // Definitions are immutable for this proof snapshot; every event below
        // still validates its exact kind, hash and native provenance.
        let mut allowed = BTreeMap::new();
        for kind in &declaration.kinds {
            let hash = kind.definition_hash();
            let previous = allowed.insert(kind.kind.clone(), hash.clone());
            assert!(previous.is_none_or(|previous| previous == hash));
        }
        let saved_binding = sinex_store::bindings::load(&mut conn, &binding)
            .await
            .unwrap()
            .expect("native binding authority");
        assert_eq!(saved_binding.source, source);
        assert_eq!(saved_binding.input, path.to_string_lossy().as_ref());
        assert_eq!(saved_binding.settings["host"], HOST);
        let (cursor, carry) = sinex_store::positions::load(&mut conn, &binding)
            .await
            .unwrap()
            .expect("complete native position");
        let rows = sqlx::query("SELECT m.id,m.input_id,m.part_index,m.previous_part_id,m.blob,m.length,m.manifest,m.parser,m.parser_version,i.locator,i.host,i.kind FROM raw.materials m JOIN raw.inputs i ON i.id=m.input_id WHERE i.binding=$1 ORDER BY m.part_index")
            .bind(&binding).fetch_all(&mut conn).await.unwrap();
        assert!(!rows.is_empty(), "native cursor has no actual materials");
        let input = rows[0].get::<Uuid, _>("input_id");
        let mut parts = HashMap::new();
        for row in rows {
            assert_eq!(
                row.get::<Uuid, _>("input_id"),
                input,
                "one native input incarnation"
            );
            assert_eq!(
                row.get::<String, _>("locator"),
                path.to_string_lossy().as_ref()
            );
            assert_eq!(row.get::<String, _>("host"), HOST);
            assert_eq!(row.get::<String, _>("parser"), parser);
            assert_eq!(row.get::<i32, _>("parser_version"), version as i32);
            assert_eq!(
                row.get::<String, _>("kind"),
                if source == "terminal.atuin-history" {
                    "sqlite"
                } else {
                    "file"
                }
            );
            let blob = row.get::<String, _>("blob");
            let bytes = blobs.read(&blob).unwrap();
            assert_eq!(bytes.len() as i64, row.get::<i64, _>("length"));
            assert_eq!(format!("blake3:{}", blake3::hash(&bytes).to_hex()), blob);
            let part = Part {
                id: row.get("id"),
                input,
                index: u32::try_from(row.get::<i32, _>("part_index")).unwrap(),
                previous: row.get("previous_part_id"),
                manifest: row.get("manifest"),
                bytes,
            };
            parts.insert(part.id, part);
        }
        // The schema associates observations with the immutable input, not
        // individual material rows. For these fixed fixtures EVERY retained
        // observation must attest the same device/inode, with no unavailable look.
        let metadata = std::fs::metadata(path).unwrap();
        let observations = sqlx::query_as::<_, (Option<String>, Option<String>, Option<String>)>(
            "SELECT device::text,inode::text,unavailable FROM raw.observations WHERE input_id=$1",
        )
        .bind(input)
        .fetch_all(&mut conn)
        .await
        .unwrap();
        assert!(
            !observations.is_empty(),
            "native input has no captured generation observation"
        );
        for (device, inode, unavailable) in observations {
            assert_eq!(
                device,
                Some(metadata.dev().to_string()),
                "captured device differs"
            );
            assert_eq!(
                inode,
                Some(metadata.ino().to_string()),
                "captured inode differs"
            );
            assert!(
                unavailable.is_none(),
                "immutable fixture had an unavailable acquisition"
            );
        }
        let events = sqlx::query("SELECT e.material_id,e.anchor,e.output_index,e.reading_order,e.producer_host,e.record_hash,e.payload,e.source,e.source_version,e.source_fingerprint,k.definition_hash,e.event_type,e.kind_version FROM core.events e JOIN raw.materials m ON m.id=e.material_id JOIN registry.event_types k ON k.name=e.event_type AND k.version=e.kind_version WHERE m.input_id=$1")
            .bind(input).fetch_all(&mut conn).await.unwrap();
        eprintln!("native proof {source} loaded after {:?}", started.elapsed());
        let mut facts = Vec::new();
        for event in events {
            assert_eq!(event.get::<String, _>("source"), source);
            assert_eq!(
                event.get::<Option<i64>, _>("source_version"),
                Some(i64::from(version))
            );
            assert_eq!(
                event.get::<Option<String>, _>("source_fingerprint"),
                Some(fingerprint.clone())
            );
            assert_eq!(event.get::<i64, _>("output_index"), 0);
            assert_eq!(
                event.get::<Option<String>, _>("producer_host").as_deref(),
                Some(HOST)
            );
            let kind = sinex_model::KindRef {
                name: event.get("event_type"),
                version: u64::try_from(event.get::<i64, _>("kind_version")).unwrap(),
            };
            assert_eq!(
                allowed.get(&kind),
                Some(&event.get::<String, _>("definition_hash")),
                "exact native kind schema"
            );
            let hash: Vec<u8> = event.get("record_hash");
            facts.push(Fact {
                material: event.get("material_id"),
                anchor: u64::try_from(event.get::<i64, _>("anchor")).unwrap(),
                hash: hash.try_into().unwrap(),
                payload: event.get("payload"),
                order: event.get("reading_order"),
            });
        }
        let failures = sqlx::query_as::<_,(Uuid,i64)>("SELECT f.material_id,f.anchor FROM raw.failures f JOIN raw.materials m ON m.id=f.material_id WHERE m.input_id=$1")
            .bind(input).fetch_all(&mut conn).await.unwrap().into_iter().map(|(m,a)|(m,u64::try_from(a).unwrap())).collect();
        let memory = sinex_store::positions::reader_memory(&mut conn, &binding)
            .await
            .unwrap()
            .map(|m| (m.material.0, m.bytes.to_vec()));
        let snapshot = Snapshot {
            scope: json!({"binding":binding,"source":reference,"fingerprint":fingerprint,"locator":path.to_string_lossy(),"host":HOST,"device":metadata.dev(),"inode":metadata.ino()}),
            input,
            cursor,
            carry,
            parts,
            facts,
            failures,
            memory,
        };
        eprintln!(
            "native proof {source} event identities after {:?}",
            started.elapsed()
        );
        let logical = validate(&snapshot, &fixture).unwrap_or_else(|reason| {
            panic!("native progress custody failed for {source}: {reason}")
        });
        eprintln!(
            "native proof {source} complete after {:?}",
            started.elapsed()
        );
        out.push((binding, logical, snapshot.carry));
    }
    out.sort_by(|a, b| a.0.cmp(&b.0));
    out
}

fn frames(bytes: &[u8], zsh: bool) -> Vec<(usize, usize)> {
    let mut start = 0;
    let mut out = Vec::new();
    for (i, b) in bytes.iter().enumerate() {
        if *b == b'\n' && (!zsh || i == 0 || bytes[i - 1] != b'\\') {
            out.push((start, i + 1));
            start = i + 1;
        }
    }
    out
}
fn chain(snapshot: &Snapshot, zsh: bool) -> Check<Vec<(&Part, usize)>> {
    let mut at = Some(id(&snapshot.cursor, "material")?);
    let mut seen = HashSet::new();
    let mut ordered = Vec::new();
    while let Some(current) = at {
        require(seen.insert(current), "cyclic material chain")?;
        let part = snapshot
            .parts
            .get(&current)
            .ok_or("cursor or predecessor material is missing")?;
        require(
            part.input == snapshot.input,
            "material belongs to another input",
        )?;
        at = part.previous;
        ordered.push(part);
    }
    ordered.reverse();
    require(
        ordered.len() == snapshot.parts.len(),
        "registered material outside cursor chain",
    )?;
    require(
        number(&snapshot.cursor, "part_index")?
            == u64::from(ordered.last().ok_or("empty material chain")?.index),
        "cursor part index differs from actual material",
    )?;
    let mut out = Vec::new();
    for (i, part) in ordered.iter().enumerate() {
        require(part.index == i as u32, "noncontiguous actual part indices")?;
        require(
            part.manifest["path"] == snapshot.scope["locator"]
                && part.manifest["host"] == snapshot.scope["host"],
            "material manifest scope differs",
        )?;
        require(
            part.manifest.get("members").is_none_or(|v| v == &json!([])),
            "unproved container member ranges",
        )?;
        require(
            part.manifest.get("records").is_none_or(Value::is_null),
            "unproved selective record ranges",
        )?;
        let cut = if let Some(next) = ordered.get(i + 1) {
            number(&next.manifest, "previous_through")?
        } else {
            number(&snapshot.cursor, "through")?
        };
        let cut = usize::try_from(cut).map_err(|_| "cut exceeds address space")?;
        require(cut <= part.bytes.len(), "through exceeds actual material")?;
        require(
            cut == 0 || frames(&part.bytes, zsh).iter().any(|(_, end)| *end == cut),
            "through is inside a native record",
        )?;
        out.push((*part, cut));
    }
    Ok(out)
}
struct Authorities<'a> {
    located: HashMap<(Uuid, u64, [u8; 32]), Vec<&'a Value>>,
    repeated: HashMap<[u8; 32], Vec<&'a Value>>,
    failures: &'a BTreeSet<(Uuid, u64)>,
}
impl<'a> Authorities<'a> {
    fn new(snapshot: &'a Snapshot) -> Self {
        let mut located: HashMap<_, Vec<_>> = HashMap::new();
        let mut repeated: HashMap<_, Vec<_>> = HashMap::new();
        for fact in &snapshot.facts {
            located
                .entry((fact.material, fact.anchor, fact.hash))
                .or_default()
                .push(&fact.payload);
            repeated.entry(fact.hash).or_default().push(&fact.payload);
        }
        Self {
            located,
            repeated,
            failures: &snapshot.failures,
        }
    }
    fn record(&self, part: &Part, start: usize, raw: &[u8], expected: &Value) -> Check<bool> {
        // Native record hashes include the framed newline. SQLite reader
        // memory instead hashes the row's JSON line before adding newline.
        let hash = *blake3::hash(raw).as_bytes();
        let matches = |payload: &&Value| {
            expected
                .as_object()
                .unwrap()
                .iter()
                .all(|(k, v)| payload.get(k) == Some(v))
        };
        let located = self
            .located
            .get(&(part.id, start as u64, hash))
            .is_some_and(|values| values.iter().any(matches));
        // Snapshot loading bounds every candidate to the same actual immutable
        // input and owner source/schema; a hash alone is never the authority.
        let repeated = self
            .repeated
            .get(&hash)
            .is_some_and(|values| values.iter().any(matches));
        let failed = self.failures.contains(&(part.id, start as u64));
        require(
            located || repeated || failed,
            "consumed complete record has no event, failure or exact repeat authority",
        )?;
        Ok(failed && !located && !repeated)
    }
}
fn validate(snapshot: &Snapshot, fixture: &Fixture) -> Check<Value> {
    let is_zsh = matches!(fixture, Fixture::Zsh { .. });
    let allowed: &[&str] = if is_zsh {
        &[
            "offset",
            "size",
            "mtime_ns",
            "ctime_ns",
            "inode",
            "looked_ns",
            "window",
            "unrecorded_rewrite",
            "material",
            "part_index",
            "through",
        ]
    } else {
        &[
            "database_generation",
            "max_rowid",
            "last_full_pass_ns",
            "last_order",
            "material",
            "part_index",
            "open",
            "through",
            "memory_part",
            "changed_since_memory",
        ]
    };
    require(
        snapshot
            .cursor
            .as_object()
            .is_some_and(|fields| fields.keys().all(|key| allowed.contains(&key.as_str()))),
        "unproved native cursor field",
    )?;
    require(
        snapshot.scope["source"]
            == json!({"name":if is_zsh {"terminal.zsh-history"} else {"terminal.atuin-history"},"version":if is_zsh {3} else {1}}),
        "wrong exact native source",
    )?;
    require(snapshot.scope["host"] == HOST, "wrong native host scope")?;
    let parts = chain(snapshot, is_zsh)?;
    let authority = Authorities::new(snapshot);
    let mut admitted = BTreeSet::new();
    for (part, cut) in &parts {
        for (start, _) in frames(&part.bytes, is_zsh)
            .into_iter()
            .filter(|(_, end)| end <= cut)
        {
            admitted.insert((part.id, start as u64));
        }
    }
    require(
        snapshot
            .facts
            .iter()
            .all(|f| admitted.contains(&(f.material, f.anchor))),
        "event cites an unconsumed prefix record",
    )?;
    require(
        snapshot.failures.iter().all(|key| admitted.contains(key)),
        "failure cites an unconsumed prefix record",
    )?;
    match fixture {
        Fixture::Atuin { rows, generation } => {
            require(
                snapshot.cursor["database_generation"] == *generation,
                "wrong current database generation",
            )?;
            require(snapshot.carry.is_null(), "unexpected Atuin parser carry")?;
            require(
                snapshot.cursor["last_full_pass_ns"]
                    .as_i64()
                    .is_some_and(|at| at > 0),
                "missing actual full-pass acquisition clock",
            )?;
            require(
                snapshot.cursor["last_order"]
                    .as_i64()
                    .is_some_and(|order| snapshot.facts.iter().all(|fact| fact.order <= order)),
                "cursor reading-order frontier regressed",
            )?;
            let mut hashes = BTreeMap::<i64, [u8; 32]>::new();
            let mut saved_hashes = None;
            let mut since_memory = 0u64;
            let mut failed = 0;
            let memory_part = snapshot.cursor["memory_part"].as_u64();
            for (part, cut) in &parts {
                require(
                    part.manifest["database_generation"] == *generation,
                    "material database generation differs",
                )?;
                require(
                    part.manifest["media_type"] == "application/x-ndjson"
                        && part.manifest["table"] == "history",
                    "wrong captured SQLite grain",
                )?;
                for (start, end) in frames(&part.bytes, false)
                    .into_iter()
                    .filter(|(_, end)| end <= cut)
                {
                    let raw = &part.bytes[start..end];
                    let row: Value =
                        serde_json::from_slice(raw).map_err(|_| "invalid captured SQLite row")?;
                    let rowid = row["_rowid"].as_i64().ok_or("missing captured rowid")?;
                    require(
                        rowid > 0 && rowid <= *rows as i64,
                        "captured row outside known native frontier",
                    )?;
                    let i = rowid - 1;
                    require(
                        row == json!({"_rowid":rowid,"id":format!("a{i:06}"),"timestamp":1_600_000_000_000_000_000i64+i*1_000_000_000,"duration":1000,"exit":0,"command":format!("atuin {i}"),"cwd":"/","session":"s","hostname":"h","deleted_at":null}),
                        "captured row differs from independent generated native contents",
                    )?;
                    failed+=u64::from(authority.record(part,start,raw,&json!({"native_id":format!("a{i:06}"),"command":format!("atuin {i}"),"duration_ns":1000,"exit_code":0}))?);
                    hashes.insert(rowid, *blake3::hash(&raw[..raw.len() - 1]).as_bytes());
                    since_memory += 1;
                }
                if memory_part == Some(u64::from(part.index)) {
                    saved_hashes = Some((part.id, hashes.clone()));
                    since_memory = 0;
                }
            }
            require(
                hashes.keys().copied().eq(1..=*rows as i64),
                "native frontier has missing rows despite its maximum",
            )?;
            require(
                number(&snapshot.cursor, "max_rowid")? == *rows as u64,
                "cursor row frontier differs",
            )?;
            require(
                snapshot.cursor.get("open").is_none_or(|v| v == &json!([])),
                "settled fixture contains open native rows",
            )?;
            let (saved_id, saved) =
                saved_hashes.ok_or("reader memory cursor does not name an actual part")?;
            let (memory_id, bytes) = snapshot.memory.as_ref().ok_or("reader memory is missing")?;
            require(
                *memory_id == saved_id,
                "reader memory belongs to another material",
            )?;
            require(
                bytes.len() % 40 == 0,
                "reader memory has an incomplete entry",
            )?;
            let mut actual = BTreeMap::new();
            for entry in bytes.chunks_exact(40) {
                let rowid = i64::from_le_bytes(entry[..8].try_into().unwrap());
                let hash: [u8; 32] = entry[8..].try_into().unwrap();
                require(
                    actual.insert(rowid, hash).is_none(),
                    "duplicate reader memory row",
                )?;
            }
            require(
                actual == saved,
                "reader memory does not reproduce captured admitted row hashes",
            )?;
            require(
                number(&snapshot.cursor, "changed_since_memory")? == since_memory,
                "reader memory frontier differs",
            )?;
            require(
                failed == 0,
                "known valid SQLite grains were recorded as failures",
            )?;
            Ok(
                json!({"scope":snapshot.scope,"generation":generation,"max_rowid":rows,"open":[],"rows_digest":digest(&hashes),"reader_state_digest":digest(&hashes),"complete_records":rows}),
            )
        }
        Fixture::Zsh {
            records,
            bytes,
            inode,
            mtime,
            ctime,
        } => {
            require(
                snapshot
                    .cursor
                    .get("unrecorded_rewrite")
                    .is_none_or(Value::is_null),
                "unrecorded rewrite is not complete progress",
            )?;
            require(
                snapshot.cursor["looked_ns"]
                    .as_i64()
                    .is_some_and(|at| at > 0),
                "missing acquisition look clock",
            )?;
            let mut captured = Vec::new();
            let mut counts = BTreeMap::<(i64, [u8; 32]), u32>::new();
            let mut grains = 0usize;
            let mut failed = 0;
            for (part, cut) in &parts {
                require(
                    number(&part.manifest, "file_offset")? == captured.len() as u64,
                    "noncontiguous captured file ranges",
                )?;
                require(
                    part.manifest["media_type"] == "text/plain",
                    "wrong captured append grain",
                )?;
                for (start, end) in frames(&part.bytes, true)
                    .into_iter()
                    .filter(|(_, end)| end <= cut)
                {
                    let i = grains;
                    let second = 1_600_000_000 + i as i64;
                    let command = if i % 97 == 0 {
                        format!("printf '%s' \nline {i}")
                    } else {
                        format!("cmd {i}")
                    };
                    let raw = &part.bytes[start..end];
                    failed += u64::from(authority.record(
                        part,
                        start,
                        raw,
                        &json!({"command":command,"hostname":HOST,"n":0}),
                    )?);
                    let key =
                        blake3::derive_key("Sinex zsh command occurrence v1", command.as_bytes());
                    *counts.entry((second, key)).or_default() += 1;
                    grains += 1;
                }
                captured.extend_from_slice(&part.bytes[..*cut]);
            }
            require(
                captured == *bytes,
                "captured contiguous prefixes differ from known complete native bytes",
            )?;
            require(grains == *records, "complete record grain count differs")?;
            require(
                number(&snapshot.cursor, "offset")? == bytes.len() as u64
                    && number(&snapshot.cursor, "size")? == bytes.len() as u64,
                "cursor native file frontier differs",
            )?;
            require(
                number(&snapshot.cursor, "inode")? == *inode,
                "wrong native file generation",
            )?;
            require(
                snapshot.cursor["mtime_ns"] == json!(mtime)
                    && snapshot.cursor["ctime_ns"] == json!(ctime),
                "native file observation differs",
            )?;
            let window = blake3::hash(&captured).to_hex().to_string();
            require(
                snapshot.cursor["window"] == window,
                "cursor prefix digest differs from actual captured ranges",
            )?;
            let expected = json!({"latest":1_600_000_000i64+*records as i64-1,"counts":counts.into_iter().map(|((at,hash),n)|(at,hash,n)).collect::<Vec<_>>()});
            require(
                snapshot.carry == expected,
                "full parser occurrence carry differs from consumed fixture grains",
            )?;
            require(
                failed == 0,
                "known valid shell grains were recorded as failures",
            )?;
            require(snapshot.memory.is_none(), "unexpected append reader memory")?;
            Ok(
                json!({"scope":snapshot.scope,"offset":bytes.len(),"size":bytes.len(),"inode":inode,"mtime_ns":mtime,"ctime_ns":ctime,"window":window,"complete_records":grains}),
            )
        }
    }
}

#[cfg(test)]
#[path = "progress/tests.rs"]
mod tests;

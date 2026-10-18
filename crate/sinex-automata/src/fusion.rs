//! Command fusion (architecture §6.4): one fused command from every source that
//! saw it (Atuin, zsh, later kitty), by declared rules, with every source
//! record as a parent. Without a rule match records stay separate; the operator
//! can still decide `same A B`.
//!
//! The rule is data ([`FusionRule`]). Two records match when their trimmed
//! command text and host are equal, they come from different sources, and:
//!
//! 1. strong pass: the globally closest pairs within `window` seconds, closest
//!    first (ties by source order, then commit order);
//! 2. fallback pass: among records still alone, pairs within `fallback` where
//!    each side has exactly one candidate;
//! 3. every other record is its own fused command.
//!
//! A group holds at most one record per source. Unknown times never match and
//! never fuse (they have no key).
//!
//! The key is the host and the UTC hour the command started. A command near an
//! hour edge also dirties the neighbouring hour, and each hour reads a margin
//! of `fallback` on both sides but emits only groups whose earliest member
//! starts inside it, so a group straddling the edge is emitted exactly once.
use crate::output::derived;
use crate::{Computed, Env, InputQuery, KeyState, KeyView, Keyed};
use serde_json::{Value, json};
use sinex_model::kinds::{CommandExecuted, CommandFused, CommandImported};
use sinex_model::{Event, EventPayload, RecordState, Timestamp};

/// Matching and field preference for one family of records.
#[derive(Clone, Copy, Debug)]
pub struct FusionRule {
    /// Input event types in preference order: earlier wins a field.
    pub sources: &'static [&'static str],
    pub window_ns: i128,
    pub fallback_ns: i128,
    pub bucket_secs: i64,
}

pub const COMMAND_RULE: FusionRule = FusionRule {
    sources: &[CommandExecuted::EVENT_TYPE, CommandImported::EVENT_TYPE],
    window_ns: 2_000_000_000,
    fallback_ns: 60_000_000_000,
    bucket_secs: 3600,
};

pub struct CommandFusion;

/// One input record, normalized for matching.
#[derive(Clone, Debug)]
struct Record<'a> {
    event: &'a Event,
    source_rank: usize,
    host: String,
    command: String,
    at_ns: i128,
    cwd: Option<String>,
    session: Option<String>,
    duration_ns: Option<i64>,
    exit_code: Option<i64>,
}

/// The host part of an Atuin `host:user` hostname.
fn host_of(hostname: &str) -> &str {
    hostname.split(':').next().unwrap_or(hostname)
}

fn record<'a>(event: &'a Event, rule: &FusionRule) -> Option<Record<'a>> {
    if event.record_state == RecordState::Removed {
        return None;
    }
    let source_rank = rule.sources.iter().position(|t| *t == event.event_type)?;
    let at_ns = event.ts_orig.at?.unix_ns();
    if let Some(a) = event.payload_as::<CommandExecuted>() {
        return Some(Record {
            event,
            source_rank,
            host: host_of(&a.hostname).to_owned(),
            command: a.command.trim().to_owned(),
            at_ns,
            cwd: Some(a.cwd),
            session: Some(a.session),
            duration_ns: a.duration_ns,
            exit_code: a.exit_code,
        });
    }
    if let Some(z) = event.payload_as::<CommandImported>() {
        return Some(Record {
            event,
            source_rank,
            host: host_of(&z.hostname).to_owned(),
            command: z.command.trim().to_owned(),
            at_ns,
            cwd: None,
            session: None,
            duration_ns: None,
            exit_code: None,
        });
    }
    None
}

/// Group records by the rule. Returns groups of indices into `records`, each
/// sorted by source rank.
fn group(records: &[Record<'_>], rule: &FusionRule) -> Vec<Vec<usize>> {
    let n = records.len();
    let mut group_of: Vec<usize> = (0..n).collect();
    let mut members: Vec<Vec<usize>> = (0..n).map(|i| vec![i]).collect();
    let compatible = |a: &Record<'_>, b: &Record<'_>| {
        a.source_rank != b.source_rank && a.host == b.host && a.command == b.command
    };

    // Candidate pairs, closest first. Records are sorted by time, so the inner
    // loop stops once the gap exceeds the fallback window.
    let mut pairs: Vec<(i128, usize, usize)> = Vec::new();
    for i in 0..n {
        for j in i + 1..n {
            let dt = records[j].at_ns - records[i].at_ns;
            if dt > rule.fallback_ns {
                break;
            }
            if compatible(&records[i], &records[j]) {
                pairs.push((dt, i, j));
            }
        }
    }
    pairs.sort_by_key(|&(dt, i, j)| (dt, records[i].source_rank.min(records[j].source_rank), i, j));

    let merge = |i: usize, j: usize, members: &mut Vec<Vec<usize>>, group_of: &mut Vec<usize>| {
        let (gi, gj) = (group_of[i], group_of[j]);
        if gi == gj {
            return false;
        }
        let ranks_i: Vec<usize> = members[gi]
            .iter()
            .map(|&m| records[m].source_rank)
            .collect();
        if members[gj]
            .iter()
            .any(|&m| ranks_i.contains(&records[m].source_rank))
        {
            return false;
        }
        let all: Vec<usize> = members[gi].iter().chain(&members[gj]).copied().collect();
        let lo = all.iter().map(|&m| records[m].at_ns).min().unwrap_or(0);
        let hi = all.iter().map(|&m| records[m].at_ns).max().unwrap_or(0);
        if hi - lo > rule.window_ns && all.len() > 2 {
            return false;
        }
        let moved = std::mem::take(&mut members[gj]);
        for m in &moved {
            group_of[*m] = gi;
        }
        members[gi].extend(moved);
        true
    };

    // Strong pass.
    for &(dt, i, j) in &pairs {
        if dt <= rule.window_ns {
            merge(i, j, &mut members, &mut group_of);
        }
    }

    // Fallback pass: only between records still alone, and only when each
    // side has exactly one such candidate.
    let alone = |i: usize, members: &Vec<Vec<usize>>, group_of: &Vec<usize>| {
        members[group_of[i]].len() == 1
    };
    let mut candidates: Vec<usize> = vec![0; n];
    let fallback: Vec<(usize, usize)> = pairs
        .iter()
        .filter(|&&(dt, i, j)| {
            dt > rule.window_ns && alone(i, &members, &group_of) && alone(j, &members, &group_of)
        })
        .map(|&(_, i, j)| (i, j))
        .collect();
    for &(i, j) in &fallback {
        candidates[i] += 1;
        candidates[j] += 1;
    }
    for &(i, j) in &fallback {
        if candidates[i] == 1 && candidates[j] == 1 {
            merge(i, j, &mut members, &mut group_of);
        }
    }

    let mut groups: Vec<Vec<usize>> = members.into_iter().filter(|g| !g.is_empty()).collect();
    for g in &mut groups {
        g.sort_by_key(|&m| (records[m].source_rank, records[m].at_ns));
    }
    groups
}

fn fused(group: &[usize], records: &[Record<'_>]) -> Event {
    // Members are in source-preference order; the first with a value wins.
    let first = &records[group[0]];
    let pick = |f: fn(&Record<'_>) -> Option<String>| group.iter().find_map(|&m| f(&records[m]));
    let payload = CommandFused {
        command: first.command.clone(),
        host: first.host.clone(),
        started_at: first.event.ts_orig.at.expect("fused records have a time"),
        cwd: pick(|r| r.cwd.clone()),
        session: pick(|r| r.session.clone()),
        duration_ns: group.iter().find_map(|&m| records[m].duration_ns),
        exit_code: group.iter().find_map(|&m| records[m].exit_code),
        observed_by: group
            .iter()
            .map(|&m| records[m].event.source.clone())
            .collect(),
    };
    derived(
        &payload,
        CommandFusion::NAME,
        group.iter().map(|&m| records[m].event),
    )
}

fn bucket_start(key: &Value) -> Option<(String, i64)> {
    Some((
        key.get("host")?.as_str()?.to_owned(),
        key.get("hour")?.as_i64()?,
    ))
}

impl Keyed for CommandFusion {
    const NAME: &'static str = "command.fusion";
    const VERSION: u32 = 1;
    const INPUTS: &'static [&'static str] = COMMAND_RULE.sources;
    const OUTPUTS: &'static [&'static str] = &[CommandFused::EVENT_TYPE];

    fn keys_for(&self, input: &KeyView<'_>, _env: &Env) -> Vec<Value> {
        let rule = COMMAND_RULE;
        let host = input
            .payload
            .get("hostname")
            .and_then(Value::as_str)
            .map(|h| host_of(h).to_owned());
        let (Some(host), Some(t)) = (host, input.ts_orig) else {
            // No time or no host: nothing to fuse with; the record stays separate.
            return vec![];
        };
        let ns = t.unix_ns();
        let bucket_ns = i128::from(rule.bucket_secs) * 1_000_000_000;
        let hour = |ns: i128| i64::try_from(ns.div_euclid(bucket_ns)).unwrap_or(i64::MAX);
        let mut hours = vec![hour(ns)];
        for edge in [ns - rule.fallback_ns, ns + rule.fallback_ns] {
            if !hours.contains(&hour(edge)) {
                hours.push(hour(edge));
            }
        }
        hours
            .into_iter()
            .map(|h| json!({"host": host, "hour": h}))
            .collect()
    }

    fn inputs(&self, key: &Value, _env: &Env) -> Option<InputQuery> {
        let rule = COMMAND_RULE;
        let (_, hour) = bucket_start(key)?;
        let start = i128::from(hour) * i128::from(rule.bucket_secs) * 1_000_000_000;
        let end = start + i128::from(rule.bucket_secs) * 1_000_000_000;
        Some(InputQuery::window(
            rule.sources,
            Timestamp::from_unix_ns(start - rule.fallback_ns).ok()?,
            Timestamp::from_unix_ns(end + rule.fallback_ns).ok()?,
        ))
    }

    fn compute(&self, key: &Value, inputs: &[Event], _env: &Env) -> Computed {
        let rule = COMMAND_RULE;
        let Some((host, hour)) = bucket_start(key) else {
            return Computed::default();
        };
        let start = i128::from(hour) * i128::from(rule.bucket_secs) * 1_000_000_000;
        let end = start + i128::from(rule.bucket_secs) * 1_000_000_000;
        let mut records: Vec<Record<'_>> = inputs
            .iter()
            .filter_map(|e| record(e, &rule))
            .filter(|r| r.host == host)
            .collect();
        records.sort_by_key(|r| (r.at_ns, r.source_rank));
        let outputs = group(&records, &rule)
            .into_iter()
            .filter(|g| {
                let earliest = g
                    .iter()
                    .map(|&m| records[m].at_ns)
                    .min()
                    .unwrap_or(i128::MIN);
                (start..end).contains(&earliest)
            })
            .map(|g| fused(&g, &records))
            .collect();
        Computed {
            outputs,
            waiting: false,
        }
    }

    fn key_state(&self, key: &Value, env: &Env) -> KeyState {
        let rule = COMMAND_RULE;
        let Some((_, hour)) = bucket_start(key) else {
            return KeyState::Closed;
        };
        // Atuin fills duration and exit when a command ends; give long
        // commands a quarter of an hour past the margin before calling it settled.
        let settled = (i128::from(hour) + 1) * i128::from(rule.bucket_secs) * 1_000_000_000
            + rule.fallback_ns
            + 900 * 1_000_000_000;
        if env.now.unix_ns() >= settled {
            KeyState::Closed
        } else {
            KeyState::Open
        }
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use sinex_model::{Class, MaterialId, Origin, ReadingOrder, RecordLocation};

    pub(crate) fn atuin(command: &str, secs: f64, exit: i64) -> Event {
        let at = Timestamp::from_unix_ns((secs * 1e9) as i128).unwrap();
        Event::build(
            &CommandExecuted {
                native_id: format!("{secs}"),
                command: command.into(),
                cwd: "/home/u/proj".into(),
                session: "s1".into(),
                hostname: "box:u".into(),
                started_at: at,
                duration_ns: Some(1_000_000_000),
                exit_code: Some(exit),
                deleted_at: None,
                author: None,
                intent: None,
                shell: None,
                extra: Default::default(),
            },
            "terminal.atuin",
            Origin::Record {
                location: RecordLocation {
                    material: MaterialId::new(),
                    anchor: 0,
                },
                ordinal: 0,
            },
            ReadingOrder(0),
            [0; 32],
            Class::Open,
        )
        .unwrap()
    }

    pub(crate) fn zsh(command: &str, secs: f64) -> Event {
        let at = Timestamp::from_unix_ns((secs * 1e9) as i128).unwrap();
        Event::build(
            &CommandImported {
                command: command.into(),
                hostname: "box".into(),
                started_at: at,
                header_elapsed: 0,
            },
            "terminal.zsh",
            Origin::Record {
                location: RecordLocation {
                    material: MaterialId::new(),
                    anchor: 0,
                },
                ordinal: 0,
            },
            ReadingOrder(0),
            [0; 32],
            Class::Guarded,
        )
        .unwrap()
    }

    const H: f64 = 1_700_002_800.0; // an hour boundary (1700002800 / 3600 = 472223)

    fn run(inputs: &[Event]) -> Vec<CommandFused> {
        let env = Env::at(Timestamp::from_unix_secs(2_000_000_000).unwrap());
        let mut keys: Vec<Value> = Vec::new();
        for e in inputs {
            for k in CommandFusion.keys_for(
                &KeyView {
                    event_type: e.event_type,
                    ts_orig: e.ts_orig.at,
                    payload: &e.payload,
                },
                &env,
            ) {
                if !keys.contains(&k) {
                    keys.push(k);
                }
            }
        }
        let mut out = Vec::new();
        for k in keys {
            let q = CommandFusion.inputs(&k, &env).unwrap();
            let mut window: Vec<Event> = inputs
                .iter()
                .filter(|e| {
                    e.ts_orig
                        .at
                        .is_some_and(|t| t >= q.window.0 && t < q.window.1)
                        && q.event_types.contains(&e.event_type)
                })
                .cloned()
                .collect();
            window.sort_by_key(|e| e.ts_orig.at);
            out.extend(
                CommandFusion
                    .compute(&k, &window, &env)
                    .outputs
                    .iter()
                    .map(|e| e.payload_as::<CommandFused>().unwrap()),
            );
        }
        out.sort_by(|a, b| {
            a.started_at
                .cmp(&b.started_at)
                .then(a.command.cmp(&b.command))
        });
        out
    }

    #[test]
    fn close_readings_fuse_and_atuin_supplies_fields() {
        let out = run(&[atuin("ls -la", H + 10.0, 2), zsh("  ls -la ", H + 10.5)]);
        assert_eq!(out.len(), 1);
        let f = &out[0];
        assert_eq!(f.observed_by, vec!["terminal.atuin", "terminal.zsh"]);
        assert_eq!(f.exit_code, Some(2));
        assert_eq!(f.cwd.as_deref(), Some("/home/u/proj"));
        assert_eq!(f.host, "box");
    }

    #[test]
    fn closest_pairs_win_and_extras_stay_alone() {
        // Two zsh readings of the same command near one Atuin reading: the
        // closer one fuses, the other stays its own command.
        let out = run(&[
            atuin("make", H + 100.0, 0),
            zsh("make", H + 100.4),
            zsh("make", H + 101.5),
        ]);
        assert_eq!(out.len(), 2);
        assert_eq!(out.iter().filter(|f| f.observed_by.len() == 2).count(), 1);
    }

    #[test]
    fn fallback_needs_a_unique_candidate() {
        // 30 s apart: only the fallback pass can match, and it does when unique.
        assert_eq!(
            run(&[atuin("git push", H + 200.0, 0), zsh("git push", H + 230.0)]).len(),
            1
        );
        // Two zsh candidates for one Atuin record: ambiguous, nothing fuses.
        let out = run(&[
            atuin("git push", H + 300.0, 0),
            zsh("git push", H + 320.0),
            zsh("git push", H + 340.0),
        ]);
        assert_eq!(out.len(), 3);
        assert!(out.iter().all(|f| f.observed_by.len() == 1));
    }

    #[test]
    fn different_commands_or_far_apart_never_fuse() {
        assert_eq!(
            run(&[atuin("ls", H + 10.0, 0), zsh("ls -l", H + 10.0)]).len(),
            2
        );
        assert_eq!(
            run(&[atuin("ls", H + 10.0, 0), zsh("ls", H + 100.0)]).len(),
            2
        );
    }

    #[test]
    fn a_pair_across_an_hour_edge_is_emitted_once() {
        let out = run(&[atuin("cargo test", H - 0.5, 0), zsh("cargo test", H + 0.5)]);
        assert_eq!(out.len(), 1, "{out:?}");
        assert_eq!(out[0].observed_by.len(), 2);
    }

    #[test]
    fn privacy_class_is_the_strictest_parent() {
        let a = atuin("ls", H + 10.0, 0);
        let z = zsh("ls", H + 10.2);
        let env = Env::at(Timestamp::now());
        let key = json!({"host": "box", "hour": (H as i64) / 3600});
        let out = CommandFusion.compute(&key, &[a, z], &env).outputs;
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].class, Class::Guarded);
        assert!(matches!(&out[0].origin, Origin::Derived { parents } if parents.len() == 2));
    }

    #[test]
    fn removed_atuin_records_do_not_fuse() {
        let mut a = atuin("rm -rf x", H + 10.0, 0);
        a.record_state = RecordState::Removed;
        let out = run(&[a, zsh("rm -rf x", H + 10.1)]);
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].observed_by, vec!["terminal.zsh"]);
    }

    #[test]
    fn result_does_not_depend_on_input_order() {
        let inputs = vec![
            atuin("a", H + 1.0, 0),
            zsh("a", H + 1.1),
            atuin("b", H + 2.0, 1),
            zsh("b", H + 30.0),
            zsh("c", H + 40.0),
        ];
        let mut reversed = inputs.clone();
        reversed.reverse();
        assert_eq!(run(&inputs), run(&reversed));
    }
}

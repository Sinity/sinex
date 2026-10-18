//! zsh extended-history admission. The configured file is treated as
//! append-only: each step continues from the stored offset, and a detected
//! rewrite fences the input. Bytes are stored as stream parts like Atuin rows.
use crate::source_admission::{
    key_hash, observation_status, producer_version, record_failure, set_part_length,
};
use crate::{Error, Result, Writer, coordinate_hash, next_commit_seq};
use serde_json::{Value, json};
use sqlx::Connection;
use time::{OffsetDateTime, format_description::well_known::Rfc3339};
use uuid::Uuid;
pub const SOURCE: &str = "terminal.zsh-history";
pub const PROTOCOL: &str = "append_only_extended_v1";
const PARSER: &str = "zsh-extended-v2";
const PARSER_NAME: &str = "parser.terminal.zsh-history";
#[derive(Clone)]
pub struct ZshStep {
    pub observation: Uuid,
    pub observed_at: String,
    pub locator: String,
    pub host: String,
    /// None registers the reading and returns the stored cursor position.
    pub start: Option<i64>,
    pub prefix_blake3: String,
    pub bytes: Vec<u8>,
    pub complete: bool,
    pub unavailable: Option<String>,
    /// The file was rewritten: refuse further reads of this input.
    pub chain_broken: bool,
    pub privacy_rules: Vec<crate::PrivacyRule>,
}
pub(crate) fn payload_schema() -> Value {
    json!({
        "type": "object",
        "required": ["native_id", "command", "hostname", "timestamp_ns", "identity", "continuity"],
        "properties": {
            "native_id": {"type": "string"},
            "command": {"type": "string"},
            "hostname": {"type": "string"},
            "timestamp_ns": {"type": "integer"},
            "cwd": {"type": "null"},
            "exit_code": {"type": "null"},
            "duration_ns": {"type": "null"},
            "session": {"type": "null"},
            "completion": {"const": "end_not_recorded"},
            "identity": {"type": "object"},
            "continuity": {"type": "object"}
        }
    })
}
pub fn unmetafy(bytes: &[u8]) -> std::result::Result<Vec<u8>, &'static str> {
    let mut result = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == 0x83 {
            i += 1;
            if i == bytes.len() {
                return Err("incomplete zsh meta escape");
            };
            result.push(bytes[i] ^ 0x20)
        } else {
            result.push(bytes[i])
        }
        i += 1;
    }
    Ok(result)
}
/// Byte ranges of the records in `bytes`. A trailing partial record is an error.
pub fn frame(bytes: &[u8]) -> std::result::Result<Vec<(usize, usize)>, &'static str> {
    let mut ranges = Vec::new();
    let mut start = 0;
    for (i, byte) in bytes.iter().enumerate() {
        if *byte == b'\n' && (i == 0 || bytes[i - 1] != b'\\') {
            ranges.push((start, i + 1));
            start = i + 1;
        }
    }
    if start != bytes.len() {
        return Err("unfinished zsh record");
    };
    Ok(ranges)
}
pub(crate) fn parse(bytes: &[u8]) -> std::result::Result<(i64, String), &'static str> {
    let decoded = unmetafy(bytes)?;
    let text = std::str::from_utf8(&decoded).map_err(|_| "invalid UTF-8 after zsh unmetafy")?;
    if text.contains('\0') {
        return Err("NUL-containing zsh record cannot be represented by the native payload store");
    }
    let text = text.strip_suffix('\n').ok_or("unfinished zsh record")?;
    let text = text
        .strip_prefix(": ")
        .ok_or("only extended zsh history is supported")?;
    let (start, rest) = text.split_once(':').ok_or("invalid extended zsh header")?;
    let (elapsed, command) = rest.split_once(';').ok_or("invalid extended zsh header")?;
    let start: i64 = start.parse().map_err(|_| "invalid zsh start")?;
    let elapsed: i64 = elapsed.parse().map_err(|_| "invalid zsh elapsed")?;
    if start < 0 || elapsed < 0 {
        return Err("invalid zsh time");
    };
    // elapsed is not used: with SHARE_HISTORY zsh writes 0 before the command ends.
    let timestamp = start
        .checked_mul(1_000_000_000)
        .ok_or("zsh time out of range")?;
    OffsetDateTime::from_unix_timestamp(start).map_err(|_| "zsh time out of range")?;
    // zsh appends one space when the command's last non-space byte is a
    // backslash. Remove only that space, then decode escaped newlines.
    let command = if command.trim_end_matches(' ').ends_with('\\') {
        command.strip_suffix(' ').unwrap_or(command)
    } else {
        command
    };
    Ok((timestamp, command.replace("\\\n", "\n")))
}
impl Writer {
    pub(crate) async fn admit_zsh(&mut self, step: ZshStep) -> Result<Value> {
        if step.locator.is_empty()
            || step.host.is_empty()
            || step.bytes.len() > crate::MAX_STEP_BYTES
            || (step.unavailable.is_some() && !step.bytes.is_empty())
        {
            return Err(Error::Invalid("invalid bounded zsh step"));
        }
        for rule in &step.privacy_rules {
            rule.validate()
                .map_err(|_| Error::Invalid("invalid zsh privacy rule"))?;
        }
        let input = self.register_input(SOURCE, &step.locator, "file").await?;
        let position: Option<Value> = sqlx::query_scalar(
            "SELECT position FROM raw.source_cursors WHERE binding=$1 AND input=$2",
        )
        .bind(SOURCE)
        .bind(input.to_string())
        .fetch_optional(&mut self.connection)
        .await?;
        let position = position.unwrap_or_else(|| {
            json!({
                "offset": 0,
                "prefix_blake3": blake3::hash(b"").to_hex().to_string(),
                "protocol": PROTOCOL,
                "host": step.host,
            })
        });
        if position["protocol"] != PROTOCOL || position["host"] != step.host {
            return Err(Error::Invalid(
                "zsh protocol or execution host changed for retained chain",
            ));
        }
        let provenance = json!({
            "source": SOURCE,
            "event_type": "command.imported",
            "parser": PARSER,
            "representation": "original_zsh_history",
            "privacy_class": "plain",
            "privacy_rules": step.privacy_rules,
            "protocol": PROTOCOL,
            "continuity": {
                "basis": "operator_declared_append_only_protocol",
                "scope": "configured input chain",
                "prefix_check": "full retained prefix digest",
                "unobserved_rewrite_excluded": false
            },
            "execution_host": step.host,
            "shell_field_basis": crate::atuin::shell_field_config(&step.privacy_rules),
        });
        sqlx::query(
            "INSERT INTO raw.observations
               (id,input_id,seq,observed_at,source_provenance,metadata,status)
             SELECT $1,$2,coalesce(max(seq),0)+1,$3::text::timestamptz,$4,$5,'reading'
             FROM raw.observations WHERE input_id=$2
             ON CONFLICT(id) DO NOTHING",
        )
        .bind(step.observation)
        .bind(input)
        .bind(&step.observed_at)
        .bind(&provenance)
        .bind(json!({"path": step.locator, "execution_host": step.host}))
        .execute(&mut self.connection)
        .await?;
        let Some(source_start) = step.start else {
            return Ok(position);
        };
        if position["chain_broken"].as_bool() == Some(true) && step.unavailable.is_none() {
            return Err(Error::Invalid(
                "zsh input chain fenced after observed rewrite; declare a new input chain",
            ));
        }
        if position["offset"].as_i64() != Some(source_start) {
            return Err(Error::Invalid(
                "zsh cursor changed before bounded admission",
            ));
        }
        let read_order = OffsetDateTime::parse(&step.observed_at, &Rfc3339)
            .map_err(|_| Error::Invalid("invalid zsh observation time"))?
            .unix_timestamp_nanos() as i64;
        let ranges = frame(&step.bytes).map_err(Error::Invalid)?;
        if ranges.len() > crate::MAX_STEP_ROWS {
            return Err(Error::Invalid("zsh step exceeds record bound"));
        }
        let (material, base) = self
            .store_stream_step(SOURCE, input, step.observation, &provenance, &step.bytes)
            .await?;
        let mut tx = self.connection.begin().await?;
        let producer =
            producer_version(&mut tx, PARSER_NAME, PARSER, self.run_id, &provenance).await?;
        let mut seq: i64 = next_commit_seq(&mut tx).await?;
        let mut admitted = 0i64;
        let mut failures = 0i64;
        if let Some(material) = material {
            set_part_length(&mut tx, material, base + step.bytes.len() as i64).await?;
        }
        for (anchor, end) in &ranges {
            let material = material.ok_or(Error::Invalid("zsh retained material missing"))?;
            let record = &step.bytes[*anchor..*end];
            let anchor = base + *anchor as i64;
            let (timestamp, command) = match parse(record) {
                Ok(v) => v,
                Err(reason) => {
                    record_failure(&mut tx, step.observation, material, anchor, reason).await?;
                    failures += 1;
                    continue;
                }
            };
            // zsh records have no ID: the key is the time and command, plus a
            // counter for repeats of the same command in the same second.
            let n: i64 = sqlx::query_scalar(
                "SELECT coalesce(max((e.equivalence_key::jsonb->>3)::bigint),-1)+1
                 FROM core.events e
                 JOIN raw.source_material_registry m ON m.id=e.source_material_id
                 WHERE m.input_id=$1 AND e.event_type='command.imported' AND e.source=$2
                   AND (e.equivalence_key::jsonb->>1)::bigint=$3
                   AND e.equivalence_key::jsonb->>2=$4",
            )
            .bind(input)
            .bind(SOURCE)
            .bind(timestamp)
            .bind(&command)
            .fetch_one(&mut *tx)
            .await?;
            // The native ID is the record's offset in the file, never the command
            // text, so it cannot leak text that privacy rules mask.
            let native = format!("chain-record:{}", source_start + (anchor - base));
            let key = json!([input, timestamp, &command, n]).to_string();
            let mut payload = json!({
                "native_id": native,
                "command": command,
                "hostname": step.host,
                "timestamp_ns": timestamp,
                "cwd": null,
                "session": null,
                "exit_code": null,
                "duration_ns": null,
                "completion": "end_not_recorded",
                "identity": {
                    "start_ns": timestamp,
                    "n": n,
                    "basis": "record_fields_and_append_chain",
                    "source_anchor": source_start + (anchor - base),
                },
                "continuity": provenance["continuity"],
            });
            crate::atuin::mask(&mut payload, &step.privacy_rules)?;
            let id = Uuid::now_v7();
            let at = OffsetDateTime::from_unix_timestamp_nanos(timestamp as i128)
                .map_err(|_| Error::Invalid("invalid zsh event time"))?
                .format(&Rfc3339)
                .map_err(|_| Error::Invalid("invalid zsh event time"))?;
            let hash = blake3::hash(record).as_bytes()[..16].to_vec();
            sqlx::query(
                "INSERT INTO core.events(id,source,event_type,host,payload,ts_orig,
                   ts_orig_tiebreak,ts_quality,ts_precision,ts_clock,commit_seq,
                   source_material_id,anchor_byte,offset_start,offset_end,offset_kind,ordinal,
                   record_hash,run_id,equivalence_key,producer_version_id,payload_schema_id)
                 VALUES($1,$2,'command.imported',$3,$4,$5::text::timestamptz,
                   0,'intrinsic','second','producer',$6,
                   $7,$8,$8,$9,'bytes',0,
                   $10,$11,$12,$13,4)",
            )
            .bind(id)
            .bind(SOURCE)
            .bind(&step.host)
            .bind(payload)
            .bind(at)
            .bind(seq)
            .bind(material)
            .bind(anchor)
            .bind(base + *end as i64)
            .bind(&hash)
            .bind(self.run_id)
            .bind(&key)
            .bind(producer)
            .execute(&mut *tx)
            .await?;
            sqlx::query(
                "INSERT INTO core.live_coordinates(store,occ_hash,event_id) VALUES(0,$1,$2)",
            )
            .bind(coordinate_hash(material, anchor, "command.imported", 0))
            .bind(id)
            .execute(&mut *tx)
            .await?;
            sqlx::query(
                "INSERT INTO core.live_equivalence(store,key_hash,event_id,read_order,record_hash)
                 VALUES(0,$1,$2,$3,$4)",
            )
            .bind(key_hash(SOURCE, "command.imported", &key))
            .bind(id)
            .bind(read_order)
            .bind(hash)
            .execute(&mut *tx)
            .await?;
            sqlx::query(
                "INSERT INTO core.commit_log(commit_seq,event_id,change,store,event_type_id)
                 VALUES($1,$2,0,0,4)",
            )
            .bind(seq)
            .bind(id)
            .execute(&mut *tx)
            .await?;
            crate::shell::dirty(&mut tx, id).await?;
            seq = next_commit_seq(&mut tx).await?;
            admitted += 1;
        }
        let status = observation_status(step.unavailable.is_some(), step.complete);
        let summary: Value = sqlx::query_scalar(
            "UPDATE raw.observations SET rows_read=rows_read+$2+$3,admitted=admitted+$2,
               record_errors=record_errors+$3,status=$4,reason=$5,commit_seq=$6
             WHERE id=$1
             RETURNING jsonb_build_object('observation',id,'status',status,
               'rows_read',rows_read,'admitted',admitted,'record_errors',record_errors,
               'commit_seq',commit_seq)",
        )
        .bind(step.observation)
        .bind(admitted)
        .bind(failures)
        .bind(status)
        .bind(&step.unavailable)
        .bind(seq)
        .fetch_one(&mut *tx)
        .await?;
        let next = if step.chain_broken {
            let mut fenced = position.clone();
            fenced["chain_broken"] = json!(true);
            Some(fenced)
        } else if step.unavailable.is_none() {
            Some(json!({
                "offset": source_start + step.bytes.len() as i64,
                "prefix_blake3": step.prefix_blake3,
                "protocol": PROTOCOL,
                "host": step.host,
                "record_errors": position["record_errors"].as_i64().unwrap_or(0) + failures,
            }))
        } else {
            None
        };
        if let Some(next) = next {
            sqlx::query(
                "INSERT INTO raw.source_cursors(binding,input,position,carry)
                 VALUES($1,$2,$3,'{}'::jsonb)
                 ON CONFLICT(binding,input) DO UPDATE
                 SET position=EXCLUDED.position,updated_at=now()",
            )
            .bind(SOURCE)
            .bind(input.to_string())
            .bind(next)
            .execute(&mut *tx)
            .await?;
        }
        tx.commit().await?;
        if step.complete {
            self.close_open_part(input).await?;
        }
        Ok(summary)
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn installed_zsh_sentinel_keeps_original_backslashes_spaces_and_tabs() {
        let records: &[(&[u8], &str)] = &[
            (b": 1:0;echo slash\\ \n", "echo slash\\"),
            (b": 1:0;echo slashspace\\  \n", "echo slashspace\\ "),
            (b": 1:0;echo slashspaces\\   \n", "echo slashspaces\\  "),
            (b": 1:0;echo slashtab\\\t\n", "echo slashtab\\\t"),
            (
                b": 1:0;echo slashtabspace\\\t \n",
                "echo slashtabspace\\\t ",
            ),
            (
                b": 1:0;echo slashnewline\\\\\nnext\n",
                "echo slashnewline\\\nnext",
            ),
            (b": 1:0;echo plainspace  \n", "echo plainspace  "),
        ];
        for (record, expected) in records {
            assert_eq!(frame(record).unwrap(), vec![(0, record.len())]);
            assert_eq!(parse(record).unwrap().1, *expected);
        }
    }
    #[test]
    fn frame_and_unmetafy_preserve_distinct_records() {
        let bytes =
            b": 3600:0;echo \xc4\x83\x85\n: 3600:0;echo \xc4\x83\x85\n: 3601:0;printf a\\\nb\n";
        let r = frame(bytes).unwrap();
        assert_eq!(r.len(), 3);
        assert_eq!(parse(&bytes[r[0].0..r[0].1]).unwrap().1, "echo ĥ");
        assert_eq!(parse(&bytes[r[2].0..r[2].1]).unwrap().1, "printf a\nb");
        assert!(frame(b": 1:0;unfinished").is_err());
        assert!(parse(b"plain\n").is_err());
        assert!(unmetafy(b"\x83").is_err());
    }
}

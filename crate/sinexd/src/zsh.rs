//! A bounded original-byte reader. Append continuity is an explicit configured
//! source protocol, never inferred from inode or equal prefix bytes alone.
use crate::config::ZshConfig;
use sinex_store::{Admission, MAX_STEP_BYTES, MAX_STEP_ROWS, PrivacyRule, ZshStep};
use std::io::{BufRead, BufReader, Read};
use time::{OffsetDateTime, format_description::well_known::Rfc3339};
use uuid::Uuid;

pub async fn run(
    config: ZshConfig,
    rules: Vec<PrivacyRule>,
    admission: Admission,
    mut stop: tokio::sync::watch::Receiver<bool>,
) -> Result<(), String> {
    loop {
        let base = ZshStep {
            observation: Uuid::now_v7(),
            observed_at: OffsetDateTime::now_utc()
                .format(&Rfc3339)
                .map_err(|_| "zsh observation clock")?,
            locator: config.path.to_string_lossy().into_owned(),
            host: config.execution_host.clone(),
            start: None,
            prefix_blake3: String::new(),
            bytes: vec![],
            complete: false,
            unavailable: None,
            chain_broken: false,
            privacy_rules: rules.clone(),
        };
        let position = admission
            .submit_zsh(base.clone())
            .await
            .map_err(|_| "zsh observation registration failed")?;
        let offset = position["offset"]
            .as_i64()
            .ok_or("zsh retained cursor invalid")?;
        let result = if position["chain_broken"].as_bool() == Some(true) {
            Err(ReadFailure::Unavailable(
                "zsh input chain fenced after observed rewrite; declare a new input chain",
            ))
        } else {
            read(
                &config,
                &base,
                offset,
                position["prefix_blake3"]
                    .as_str()
                    .ok_or("zsh retained prefix invalid")?,
                &admission,
                &stop,
            )
            .await
        };
        if let Err(reason) = result {
            // Earlier chunks may have committed. Read the current cursor instead of
            // reusing the poll's old offset for the unavailable outcome.
            let current = admission
                .submit_zsh(ZshStep {
                    start: None,
                    ..base.clone()
                })
                .await
                .map_err(|_| "zsh cursor recovery failed")?;
            admission
                .submit_zsh(ZshStep {
                    start: current["offset"].as_i64(),
                    unavailable: Some(reason.message().into()),
                    chain_broken: matches!(reason, ReadFailure::Chain(_)),
                    ..base
                })
                .await
                .map_err(|_| "zsh unavailable outcome failed")?;
        }
        tokio::select! {_=stop.changed()=>return Ok(()),_=tokio::time::sleep(std::time::Duration::from_millis(config.poll_interval_ms))=>{}}
    }
}
#[derive(Debug)]
enum ReadFailure {
    Unavailable(&'static str),
    Chain(&'static str),
}
impl ReadFailure {
    fn message(&self) -> &'static str {
        match self {
            Self::Unavailable(s) | Self::Chain(s) => s,
        }
    }
}
impl From<&'static str> for ReadFailure {
    fn from(s: &'static str) -> Self {
        Self::Unavailable(s)
    }
}
async fn read(
    config: &ZshConfig,
    base: &ZshStep,
    mut offset: i64,
    expected: &str,
    admission: &Admission,
    stop: &tokio::sync::watch::Receiver<bool>,
) -> Result<(), ReadFailure> {
    let file = std::fs::File::open(&config.path)
        .map_err(|_| "zsh history unavailable (read-only open failed)")?;
    let mut reader = BufReader::new(file);
    let mut hash = blake3::Hasher::new();
    let mut left = offset;
    let mut block = [0u8; 65536];
    while left > 0 {
        let n = usize::try_from(left.min(block.len() as i64)).unwrap();
        reader.read_exact(&mut block[..n]).map_err(|_| {
            ReadFailure::Chain("zsh retained chain truncated; rewrite correspondence unsupported")
        })?;
        hash.update(&block[..n]);
        left -= n as i64;
        if *stop.borrow() {
            return Err("zsh read interrupted".into());
        };
        tokio::task::yield_now().await;
    }
    if hash.finalize().to_hex().as_str() != expected {
        return Err(ReadFailure::Chain(
            "zsh retained prefix differs; rewrite correspondence unsupported",
        ));
    }
    let mut batch = Vec::new();
    let mut record = Vec::new();
    let mut count = 0;
    loop {
        if *stop.borrow() {
            return Err("zsh read interrupted; committed prefix retained".into());
        }
        let mut line = Vec::new();
        let n = Read::by_ref(&mut reader)
            .take((MAX_STEP_BYTES - record.len() + 1) as u64)
            .read_until(b'\n', &mut line)
            .map_err(|_| "zsh read unavailable")?;
        if n == 0 {
            if !record.is_empty() {
                return Err("zsh trailing incomplete record; committed prefix retained".into());
            };
            break;
        }
        record.extend_from_slice(&line);
        if record.len() > MAX_STEP_BYTES {
            return Err("zsh record exceeds bounded byte budget".into());
        }
        if !line.ends_with(b"\n") {
            return Err("zsh trailing incomplete record; committed prefix retained".into());
        }
        if line.len() > 1 && line[line.len() - 2] == b'\\' {
            continue;
        }
        if !batch.is_empty()
            && (batch.len() + record.len() > MAX_STEP_BYTES || count == MAX_STEP_ROWS)
        {
            let length = batch.len() as i64;
            admission
                .submit_zsh(ZshStep {
                    start: Some(offset),
                    prefix_blake3: hash.finalize().to_hex().to_string(),
                    bytes: std::mem::take(&mut batch),
                    ..base.clone()
                })
                .await
                .map_err(|_| "zsh bounded admission failed")?;
            offset += length;
            count = 0;
        }
        hash.update(&record);
        batch.append(&mut record);
        count += 1;
        tokio::task::yield_now().await;
    }
    admission
        .submit_zsh(ZshStep {
            start: Some(offset),
            prefix_blake3: hash.finalize().to_hex().to_string(),
            bytes: batch,
            complete: true,
            ..base.clone()
        })
        .await
        .map_err(|_| "zsh EOF admission failed")?;
    Ok(())
}

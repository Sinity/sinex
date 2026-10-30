use super::*;
use sinex_source_core::{DrainInput, Drainer, Limits, Parser, RecordContext, RecordError};
use std::io::{Read, Seek, SeekFrom};

struct JsonParser;
impl Parser for JsonParser {
    type Record<'a> = serde_json::Map<String, serde_json::Value>;
    type Carry = ();
    type Payload = sinex_model::event_types::CommandImported;
    fn parse(
        &self,
        record: &Self::Record<'_>,
        _: &(),
        _: &RecordContext<'_>,
    ) -> Result<(Vec<Self::Payload>, ()), RecordError> {
        Ok((
            vec![sinex_model::event_types::CommandImported {
                command: record["command"].as_str().unwrap().into(),
                hostname: "synthetic".into(),
                started_at: None,
                header_elapsed: None,
                paths: vec![],
                n: 0,
            }],
            (),
        ))
    }
}
struct PaddedJson {
    padding: u64,
    at: u64,
}
impl PaddedJson {
    const PREFIX: &'static [u8] = b"{\"command\":\"retained\",\"ignored\":[";
    const SUFFIX: &'static [u8] = b"]}\n";
    fn len(&self) -> u64 {
        Self::PREFIX.len() as u64 + self.padding + Self::SUFFIX.len() as u64
    }
}
impl Read for PaddedJson {
    fn read(&mut self, out: &mut [u8]) -> std::io::Result<usize> {
        let count = out.len().min(self.len().saturating_sub(self.at) as usize);
        for (index, byte) in out[..count].iter_mut().enumerate() {
            let at = self.at + index as u64;
            *byte = if at < (Self::PREFIX.len() as u64) {
                Self::PREFIX[at as usize]
            } else if at < (Self::PREFIX.len() as u64 + self.padding) {
                b' '
            } else {
                Self::SUFFIX[(at - Self::PREFIX.len() as u64 - self.padding) as usize]
            };
        }
        self.at += count as u64;
        Ok(count)
    }
}
impl Seek for PaddedJson {
    fn seek(&mut self, from: SeekFrom) -> std::io::Result<u64> {
        let at = match from {
            SeekFrom::Start(n) => i128::from(n),
            SeekFrom::Current(n) => i128::from(self.at) + i128::from(n),
            SeekFrom::End(n) => i128::from(self.len()) + i128::from(n),
        };
        self.at = u64::try_from(at).map_err(|_| std::io::Error::other("negative input seek"))?;
        Ok(self.at)
    }
}
fn peak_rss_kib() -> u64 {
    let status = std::fs::read_to_string("/proc/self/status").unwrap();
    status
        .lines()
        .find_map(|line| line.strip_prefix("VmHWM:"))
        .unwrap()
        .split_whitespace()
        .next()
        .unwrap()
        .parse()
        .unwrap()
}
#[devkit::test]
fn oversized_stored_json_replays_with_bounded_raw_record_memory() {
    const ENV: &str = "SINEX_SYNTHETIC_RECORD_PADDING";
    if let Ok(padding) = std::env::var(ENV) {
        let padding = padding.parse().unwrap();
        let input = PaddedJson { padding, at: 0 };
        let length = input.len();
        let dir = tempfile::tempdir().unwrap();
        let blobs = sinex_store::Blobs::open(dir.path()).unwrap();
        let name = blobs.put_seekable(input).unwrap();
        let reader = blobs.read_plain_seek(&name, length, || Ok(())).unwrap();
        let stored = StoredReader::new(reader, length);
        let manifest = sinex_model::Manifest::default();
        let records = [(0, length as usize)];
        let chunks = Drainer::new(
            &JsonParser,
            DrainInput {
                source: "synthetic.json",
                class: sinex_model::Class::Open,
                material: sinex_model::MaterialId::new(),
                bytes: sinex_source_core::drain::RecordBytes::Reader(&stored),
                records: &records,
                carry: serde_json::Value::Null,
                reading_order: &|_| sinex_model::ReadingOrder(0),
                limits: Limits::default(),
                manifest: &manifest,
            },
        )
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
        assert_eq!(chunks.len(), 1);
        assert_eq!(chunks[0].events, 1);
        assert_eq!(chunks[0].failures, 0);
        let sinex_model::RecordOutcome::Events { events, .. } = &chunks[0].outcomes[0] else {
            panic!()
        };
        assert_eq!(events[0].payload["command"], "retained");
        assert_eq!(
            name,
            format!(
                "blake3:{}",
                blake3::Hash::from_bytes(events[0].record_hash).to_hex()
            )
        );
        println!(
            "SYNTHETIC_RECORD_RSS {}",
            serde_json::json!({"record_bytes":length,"peak_rss_kib":peak_rss_kib(),"events":1})
        );
        return;
    }
    let test = format!(
        "{}::oversized_stored_json_replays_with_bounded_raw_record_memory",
        module_path!().split_once("::").unwrap().1
    );
    let mut peaks = Vec::new();
    for padding in [32u64 << 20, 64u64 << 20] {
        let output = std::process::Command::new(std::env::current_exe().unwrap())
            .args(["--exact", &test, "--nocapture"])
            .env(ENV, padding.to_string())
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "{} {}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        let stdout = String::from_utf8(output.stdout).unwrap();
        let measurement: serde_json::Value = stdout
            .lines()
            .find_map(|line| line.strip_prefix("SYNTHETIC_RECORD_RSS "))
            .map(|json| serde_json::from_str(json).unwrap())
            .expect("child printed a measurement");
        eprintln!("{measurement}");
        peaks.push(measurement["peak_rss_kib"].as_u64().unwrap());
    }
    assert!(
        peaks[1] < peaks[0] + 16 * 1024,
        "doubling a record must not retain an additional whole raw record: {peaks:?}"
    );
}

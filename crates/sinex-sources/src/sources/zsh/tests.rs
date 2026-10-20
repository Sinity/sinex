use super::*;
use crate::{DrainInput, Limits, drain};
use proptest::prelude::*;
use sinex_model::{MaterialId, ReadingOrder, RecordOutcome};

fn parse(bytes: &[u8]) -> Result<String, RecordError> {
    ZshParser { host: "h".into() }
        .parse(&bytes, &Seen::default(), &crate::RecordContext::none())
        .map(|(p, _)| p[0].command.clone())
}

#[test]
fn identical_runs_in_one_second_are_numbered_in_file_order() {
    let parser = ZshParser { host: "h".into() };
    let lines: [&[u8]; 4] = [
        b": 10:0;ls\n",
        b": 10:0;pwd\n",
        b": 10:0;ls\n",
        b": 11:0;ls\n",
    ];
    let mut seen = Seen::default();
    let mut ns = Vec::new();
    for line in lines {
        let (out, next) = parser
            .parse(&line, &seen, &crate::RecordContext::none())
            .unwrap();
        ns.push(out[0].n);
        seen = next;
    }
    assert_eq!(ns, [0, 0, 1, 0]);
    // A re-read from the start numbers the same records the same way.
    let mut again = Seen::default();
    let mut ns2 = Vec::new();
    for line in lines {
        let (out, next) = parser
            .parse(&line, &again, &crate::RecordContext::none())
            .unwrap();
        ns2.push(out[0].n);
        again = next;
    }
    assert_eq!(ns, ns2);
}

#[test]
fn keeps_original_backslashes_spaces_and_tabs() {
    let records: &[(&[u8], &str)] = &[
        (b": 1:0;echo slash\\ \n", "echo slash\\"),
        (b": 1:0;echo slashspace\\  \n", "echo slashspace\\ "),
        (b": 1:0;echo slashtab\\\t\n", "echo slashtab\\\t"),
        (
            b": 1:0;echo slashnewline\\\\\nnext\n",
            "echo slashnewline\\\nnext",
        ),
        (b": 1:0;echo plainspace  \n", "echo plainspace  "),
    ];
    for (record, expected) in records {
        assert_eq!(frame(record), (vec![(0, record.len())], record.len()));
        assert_eq!(parse(record).unwrap(), *expected);
    }
}

#[test]
fn metafied_bytes_and_continuations() {
    let bytes = b": 3600:0;echo \xc4\x83\x85\n: 3601:0;printf a\\\nb\n: 3602:0;partial";
    let (r, consumed) = frame(bytes);
    assert_eq!(r.len(), 2);
    assert_eq!(consumed, bytes.len() - b": 3602:0;partial".len());
    assert_eq!(parse(&bytes[r[0].0..r[0].1]).unwrap(), "echo ĥ");
    assert_eq!(parse(&bytes[r[1].0..r[1].1]).unwrap(), "printf a\nb");
    assert!(parse(b"plain\n").is_err());
    assert!(unmetafy(b"\x83").is_err());
}

fn commands(bytes: &[u8], base: usize) -> Vec<(usize, String)> {
    let (records, _) = frame(bytes);
    let out = drain(
        &ZshParser { host: "h".into() },
        DrainInput {
            manifest: &serde_json::Value::Null,
            source: SPEC.name,
            class: Class::Open,
            material: MaterialId(uuid::Uuid::nil()),
            bytes,
            records: &records,
            carry: serde_json::Value::Null,
            reading_order: &|a| ReadingOrder(a as i64),
            limits: Limits::default(),
        },
    )
    .unwrap();
    out.iter()
        .flat_map(|c| &c.outcomes)
        .map(|o| match o {
            RecordOutcome::Events { location, events } => (
                base + location.unwrap().anchor as usize,
                events[0].payload["command"].as_str().unwrap().to_owned(),
            ),
            RecordOutcome::Failure { location, reason } => {
                (base + location.anchor as usize, format!("FAIL {reason}"))
            }
        })
        .collect()
}

proptest! {
    /// Source law: reading the file in pieces gives the same records, at the
    /// same file offsets, as reading it at once.
    #[test]
    fn pieces_equal_whole(cmds in prop::collection::vec("[a-z \\\\]{0,6}", 1..12), cut in 0usize..400) {
        let mut file = Vec::new();
        for (i, c) in cmds.iter().enumerate() {
            file.extend_from_slice(format!(": {}:0;{}\n", 100 + i, c.replace('\\', "\\\\")).as_bytes());
        }
        let whole = commands(&file, 0);
        let cut = cut.min(file.len());
        let (_, consumed) = frame(&file[..cut]);
        let mut pieces = commands(&file[..consumed], 0);
        pieces.extend(commands(&file[consumed..], consumed));
        prop_assert_eq!(whole, pieces);
    }

    /// Source law: an anchor reads back exactly its record.
    #[test]
    fn anchors_read_back_records(cmds in prop::collection::vec("[a-z]{1,5}", 1..8)) {
        let mut file = Vec::new();
        for (i, c) in cmds.iter().enumerate() {
            file.extend_from_slice(format!(": {}:0;{}\n", i, c).as_bytes());
        }
        for (anchor, command) in commands(&file, 0) {
            let (r, _) = frame(&file[anchor..]);
            prop_assert_eq!(parse(&file[anchor..anchor + r[0].1]).unwrap(), command);
        }
    }
}

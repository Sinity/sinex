use super::*;

fn integer(v: i32) -> Vec<u8> {
    let mut out = vec![0];
    out.extend(v.to_be_bytes());
    out
}

fn big(v: u64) -> Vec<u8> {
    let digits: Vec<u8> = v
        .to_le_bytes()
        .into_iter()
        .rev()
        .skip_while(|b| *b == 0)
        .collect();
    let digits: Vec<u8> = digits.into_iter().rev().collect();
    let mut out = vec![1, 1];
    out.extend((digits.len() as u64).to_be_bytes());
    out.extend(digits);
    out
}

fn literal(s: &str) -> Vec<u8> {
    let mut out = vec![0];
    out.extend((s.chars().count() as u64).to_be_bytes());
    out.extend(s.as_bytes());
    out
}

fn sample(windows: &[(bool, Vec<u8>, Vec<u8>)], desktop: Vec<u8>) -> Vec<u8> {
    let mut out = vec![1];
    out.extend(integer(59_772)); // 2022-07-12
    out.extend(big(154_965_627_369)); // numerator
    out.extend(integer(5_000_000)); // denominator: 30993.125 s = 08:36:33.125
    out.extend(integer(60_000));
    out.push(3);
    out.extend((windows.len() as u64).to_be_bytes());
    for (active, t, p) in windows {
        out.push(u8::from(*active));
        out.extend(t);
        out.extend(p);
    }
    out.extend(integer(2_390));
    out.extend(desktop);
    out
}

#[devkit::test]
fn samples_decode_with_back_references_and_utc_times() {
    let mut file = MAGIC.to_vec();
    file.extend(sample(
        &[
            (false, literal("Program Manager"), literal("explorer.exe")),
            (true, literal("Ustawienia – Zażółć"), literal("app.exe")),
        ],
        literal(""),
    ));
    // The second sample refers back: title 3 and program 4 of the first.
    file.extend(sample(&[(true, vec![3], vec![4])], vec![5]));
    let (records, covered) = frame(&file);
    assert_eq!(records.len(), 2);
    assert_eq!(covered, file.len());
    let (a, b) = records[0];
    let (out, carry) = ArbttParser
        .parse(&&file[a..b], &vec![], &crate::RecordContext::none())
        .unwrap();
    let s = &out[0];
    assert_eq!(
        s.sampled_at.unix_ns(),
        1_657_614_993_125_473_800,
        "2022-07-12T08:36:33.1254738Z"
    );
    assert_eq!((s.rate_ms, s.idle_ms), (60_000, 2_390));
    assert_eq!(s.active_title.as_deref(), Some("Ustawienia – Zażółć"));
    assert_eq!(s.desktop, None);
    let (a, b) = records[1];
    let (out, _) = ArbttParser
        .parse(&&file[a..b], &carry, &crate::RecordContext::none())
        .unwrap();
    assert_eq!(out[0].active_program.as_deref(), Some("app.exe"));
    assert_eq!(out[0].windows.len(), 1);
}

#[devkit::test]
fn a_cut_sample_waits_and_a_bad_reference_fails() {
    let mut file = MAGIC.to_vec();
    file.extend(sample(&[(true, literal("t"), literal("p"))], literal("")));
    let cut = &file[..file.len() - 3];
    let (records, covered) = frame(cut);
    assert!(records.is_empty());
    assert_eq!(covered, MAGIC.len());
    let one = sample(&[(true, vec![9], literal("p"))], literal(""));
    assert!(
        ArbttParser
            .parse(&one.as_slice(), &vec![], &crate::RecordContext::none())
            .is_err()
    );
}

use super::*;
#[devkit::test]
fn cue_ranges_cover_non_ascii_crlf_and_true_eof() {
    let bytes = "WEBVTT\r\n\r\nid\r\n00:00.000 --> 00:01.500\r\nŻółw\r\nline two\r\n\r\n00:02.000 --> 00:03.000\r\nlast".as_bytes();
    let (ranges, end) = frame(bytes, true);
    assert_eq!(end, bytes.len());
    assert_eq!(ranges.len(), 3);
    assert!(
        std::str::from_utf8(&bytes[ranges[1].0..ranges[1].1])
            .unwrap()
            .contains("Żółw")
    );
    for cut in 0..bytes.len() {
        let (prefix, through) = frame(&bytes[..cut], false);
        assert!(prefix.iter().all(|r| ranges.contains(r)));
        let (rest, _) = frame(&bytes[through..], true);
        let joined: Vec<_> = prefix
            .into_iter()
            .chain(rest.into_iter().map(|(s, e)| (s + through, e + through)))
            .collect();
        assert_eq!(joined, ranges, "split at {cut}");
    }
}
#[devkit::test]
fn oversized_cue_stays_whole() {
    let text = format!("00:00.000 --> 00:01.000\n{}", "a".repeat(2 << 20));
    assert_eq!(frame(text.as_bytes(), false), (vec![], 0));
    assert_eq!(
        frame(text.as_bytes(), true),
        (vec![(0, text.len())], text.len())
    );
}

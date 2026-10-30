use super::*;
#[devkit::test]
fn signal_frame_replay_preserves_whole_original_json_and_host_line_anchors() {
    let native=b"  {\n \"kind\": \"signals\",\n \"frame\": \"synthetic\",\n \"signals\": [],\n \"receipts\": [],\n \"emergency\": [],\n \"class\": \"open\"\n}\n  ";
    assert_eq!(frame(native, true), (vec![(0, native.len())], native.len()));
    for split in 0..native.len() {
        assert_eq!(
            frame(&native[..split], false),
            (vec![], 0),
            "transport frame prefix {split} is not a different source record"
        );
    }
    let one = b"{\"record\":\"synthetic\",\"value\":{\"text\":\"line one\"}}\n";
    let two = b"{\"record\":\"synthetic\",\"value\":{\"text\":\"line two\"}}\n";
    let mut host = one.to_vec();
    host.extend_from_slice(two);
    let expected = vec![(0, one.len()), (one.len(), host.len())];
    assert_eq!(frame(&host, true), (expected.clone(), host.len()));
    for split in 0..host.len() {
        let (rows, end) = frame(&host[..split], false);
        assert_eq!(
            rows,
            expected
                .iter()
                .copied()
                .filter(|(_, end)| *end <= split)
                .collect::<Vec<_>>()
        );
        assert_eq!(end, rows.last().map_or(0, |(_, end)| *end));
    }
}

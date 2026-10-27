use super::*;

#[devkit::test]
fn every_buffer_split_keeps_complete_blocks_and_real_utf8_ranges() {
    let input = "; example\n\n2030-01-01 * Café|entrée\n  Assets:Example\n  Expenses:Example  1.00 TEST\n\ninclude other.journal\n2030-01-02 Second\n  Assets:Example  -2 TEST\n  Expenses:Example";
    let bytes = input.as_bytes();
    let whole = frame(bytes, true).0;
    assert_eq!(whole.len(), 4);
    for cut in 0..=bytes.len() {
        let (mut first, through) = frame(&bytes[..cut], false);
        first.extend(
            frame(&bytes[through..], true)
                .0
                .into_iter()
                .map(|(a, b)| (through + a, through + b)),
        );
        assert_eq!(first, whole, "cut {cut}");
    }
    assert_eq!(&bytes[whole[2].0..whole[2].1], b"include other.journal\n");
    assert_eq!(whole[3].1, bytes.len());
}

#[devkit::test]
fn a_buffer_end_is_not_a_transaction_end() {
    let bytes = b"2030-01-01 Example\n  Assets:Example\n";
    assert_eq!(frame(bytes, false), (vec![], 0));
    assert_eq!(frame(bytes, true), (vec![(0, bytes.len())], bytes.len()));
    let large = format!(
        "2030-01-01 Example\n  Assets:Example\n  Expenses:Example  1 TEST ; {}",
        "a".repeat(200_000)
    );
    assert!(frame(large.as_bytes(), false).0.is_empty());
    assert_eq!(complete(large.as_bytes()).0, [(0, large.len())]);
}

#[devkit::test]
fn multiline_comment_bodies_do_not_turn_into_directives_at_buffer_boundaries() {
    let input = b"comment\nfree text\n\n2030-01-01 is still comment text\nend comment\n2030-01-02 Entry\n  Assets:Example\n";
    let expected = complete(input).0;
    assert_eq!(expected.len(), 2);
    for cut in 0..=input.len() {
        let (mut records, through) = frame(&input[..cut], false);
        records.extend(
            frame(&input[through..], true)
                .0
                .into_iter()
                .map(|(a, b)| (through + a, through + b)),
        );
        assert_eq!(records, expected, "cut {cut}");
    }
}

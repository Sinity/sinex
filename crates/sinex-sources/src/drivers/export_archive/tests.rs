use super::*;

#[test]
fn delimited_records_start_at_each_delimiter_and_hold_back_the_last() {
    let bytes = b"<head>css</head><c>one</c><c>two</c><c>thr";
    let (records, covered) = Framing::Delimited(b"<c>").frame(bytes, false);
    assert_eq!(records, vec![(16, 26), (26, 36)]);
    assert_eq!(covered, 36, "the last cell may be cut, so it waits");
    let (records, covered) = Framing::Delimited(b"<c>").frame(bytes, true);
    assert_eq!(records.len(), 3);
    assert_eq!(covered, bytes.len());
    let (records, covered) = Framing::Delimited(b"<c>").frame(b"<head>no cells", false);
    assert!(records.is_empty());
    assert_eq!(
        covered, 11,
        "a delimiter could still start in the last bytes"
    );
}

#[test]
fn json_array_inside_an_object() {
    let doc = br#"{"Browser History": [ {"a": 1}, {"b": [2, 3]} ], "x": 0}"#;
    let (r, _) = Framing::JsonArray("/Browser History").frame(doc, true);
    let items: Vec<&[u8]> = r.iter().map(|(s, e)| &doc[*s..*e]).collect();
    assert_eq!(items, vec![&br#"{"a": 1}"#[..], br#"{"b": [2, 3]}"#]);
}

#[test]
fn csv_keeps_quoted_newlines_in_one_row_and_holds_back_a_cut_row() {
    let data = b"id,note\n1,\"two\nlines\"\n2,x\n3,cut";
    let (r, consumed) = Framing::Csv.frame(data, false);
    assert_eq!(r.len(), 3, "header, row 1, row 2; row 3 may be cut");
    assert_eq!(&data[r[1].0..r[1].1], b"1,\"two\nlines\"\n");
    assert_eq!(consumed, r[2].1);
    assert_eq!(Framing::Csv.frame(data, true).0.len(), 4);
}

#[test]
fn mbox_splits_at_from_lines_only() {
    let data =
        b"From a@b Mon\nSubject: one\n\n>From quoted\nbody\nFrom c@d Tue\nSubject: two\n\nx\n";
    let (r, consumed) = Framing::Mbox.frame(data, false);
    assert_eq!(r.len(), 1);
    assert_eq!(consumed, r[0].1);
    let (r, _) = Framing::Mbox.frame(data, true);
    assert_eq!(r.len(), 2);
    assert!(data[r[1].0..].starts_with(b"From c@d"));
}

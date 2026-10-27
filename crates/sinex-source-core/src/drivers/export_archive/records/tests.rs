use super::*;

#[devkit::test]
fn json_records_keep_their_exact_original_spelling_and_order() {
    let input = br#"{"ignored":"prefix", "a/b":{"~key":[12, -2.00e+4, true, null, "\\u0061", {"x": 1}, [2, 3]]}, "tail":[0]}"#;
    let mut got = Vec::new();
    visit(
        &mut input.as_slice(),
        Framing::JsonArray("/a~1b/~0key"),
        8,
        &mut |record| {
            got.push(record);
            Ok(())
        },
    )
    .unwrap();
    let expected: &[&[u8]] = &[
        b"12",
        b"-2.00e+4",
        b"true",
        b"null",
        br#""\\u0061""#,
        br#"{"x": 1}"#,
        b"[2, 3]",
    ];
    assert_eq!(got, expected);
}

#[devkit::test]
fn streaming_frames_equal_whole_frames_across_small_budgets() {
    for (framing, input) in [
        (Framing::Lines, &b"a\nb\nc"[..]),
        (Framing::Csv, &b"h,v\n1,\"two\nlines\"\n2,z\n"[..]),
        (
            Framing::Mbox,
            &b"From a\nSubject: x\n\ny\nFrom b\n\nz\n"[..],
        ),
        (
            Framing::Delimited(b"<cell>"),
            &b"header<cell>a<cell>bbb<cell>c"[..],
        ),
        (Framing::Whole, &b"a complete record"[..]),
    ] {
        let expected: Vec<Vec<u8>> = framing
            .frame(input, true)
            .0
            .iter()
            .map(|&(s, e)| input[s..e].to_vec())
            .collect();
        for budget in 1..32 {
            let mut got = Vec::new();
            let mut reader = input;
            visit(&mut reader, framing, budget, &mut |record| {
                got.push(record);
                Ok(())
            })
            .unwrap();
            assert_eq!(got, expected, "{framing:?}, budget{budget}");
        }
    }
}

#[devkit::test]
fn malformed_json_and_missing_arrays_are_reported_and_consumer_cancellation_survives() {
    for input in [b"[1, bad]".as_slice(), b"{\"other\": []}".as_slice()] {
        let mut reader = input;
        assert!(
            visit(
                &mut reader,
                Framing::JsonArray("/items"),
                8,
                &mut |_| Ok(())
            )
            .is_err()
        );
    }
    let error = visit(
        &mut b"[1,2,3]".as_slice(),
        Framing::JsonArray(""),
        8,
        &mut |_| Err(DriverError::Cancelled),
    )
    .unwrap_err();
    assert!(matches!(error, DriverError::Cancelled));
}

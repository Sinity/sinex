use super::*;

#[test]
fn double_encoded_values() {
    let (_, header) = SmsParser
        .parse(
            &&b"\xef\xbb\xbfITEM_DATA,FILE_LIST,TIMESTAMP,SIZE,VALUE,CREATETIME,KEY\n"[..],
            &vec![],
            &crate::RecordContext::none(),
        )
        .unwrap();
    let row = br#""",,1659525740863,723,"""{\""_id\"":\""639\"",\""thread_id\"":\""59\"",\""address\"":\""4860\"",\""date\"":\""1659525740863\"",\""type\"":\""1\"",\""body\"":\""Hi\\n\""}""",1,k
"#;
    let (out, _) = SmsParser
        .parse(&&row[..], &header, &crate::RecordContext::none())
        .unwrap();
    let m = &out[0];
    assert_eq!(m.message_id.as_deref(), Some("639"));
    assert_eq!(m.sender, "4860");
    assert_eq!(m.kind.as_deref(), Some("received"));
    assert_eq!(m.text.as_deref(), Some("Hi\n"));
    assert_eq!(m.sent_at, fields::from_unix_ms(1_659_525_740_863).unwrap());
}

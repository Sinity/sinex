use super::*;

fn cp1250(s: &str) -> Vec<u8> {
    encoding_rs::WINDOWS_1250.encode(s).0.into_owned()
}

#[devkit::test]
fn csv_rows_are_windows_1250_with_polish_headers() {
    let header = cp1250(
        "\"Temat\",\"Treść\",\"Od: (imię/nazwisko)\",\"Od: (adres)\",\"Od: (rodzaj)\",\"Do: (imię/nazwisko)\",\"Do: (adres)\",\"Do: (rodzaj)\",\"DW: (imię/nazwisko)\",\"DW: (adres)\",\"DW: (rodzaj)\",\"Kategorie\"\n",
    );
    let (_, h) = CsvParser
        .parse(&header.as_slice(), &vec![], &crate::RecordContext::none())
        .unwrap();
    let row = cp1250(
        "\"Studia, umowa\",\"Zażółć\nlinia\",\"Jan Kowalski\",\"jan@x.pl\",\"SMTP\",\"Anna\",\"anna@y.pl\",\"SMTP\",\"\",\"\",\"\",\"Praca\"\n",
    );
    let (out, _) = CsvParser
        .parse(&row.as_slice(), &h, &crate::RecordContext::none())
        .unwrap();
    let m = &out[0];
    assert_eq!(m.subject.as_deref(), Some("Studia, umowa"));
    assert_eq!(m.text.as_deref(), Some("Zażółć\nlinia"));
    assert_eq!(m.from, vec!["Jan Kowalski <jan@x.pl>"]);
    assert_eq!(m.to, vec!["Anna <anna@y.pl>"]);
    assert_eq!(m.labels, vec!["Praca"]);
    assert_eq!(m.date, None);
    assert!(m.message_id.starts_with("blake3:"));
    assert!(
        CsvParser
            .parse(&row.as_slice(), &vec![], &crate::RecordContext::none())
            .is_err()
    );
}

#[devkit::test]
fn blind_copies_are_distinct_from_ordinary_copies() {
    let header = cp1250("Temat,DW: (adres),UDW: (adres)\n");
    let (_, header) = CsvParser
        .parse(&header.as_slice(), &vec![], &crate::RecordContext::none())
        .unwrap();
    let (messages, _) = CsvParser
        .parse(
            &b"Subject,cc@example.org,bcc@example.org\n".as_slice(),
            &header,
            &crate::RecordContext::none(),
        )
        .unwrap();
    assert_eq!(messages[0].cc, ["cc@example.org"]);
    assert_eq!(messages[0].bcc, ["bcc@example.org"]);
}

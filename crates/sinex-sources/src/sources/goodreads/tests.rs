use super::*;

const HEADER: &[u8] = b"Book Id,Title,Author,ISBN,ISBN13,My Rating,Average Rating,Date Read,Date Added,Bookshelves,Exclusive Shelf\n";

#[devkit::test]
fn shelved_and_read_books() {
    let (none, header) = GoodreadsParser
        .parse(&HEADER, &vec![], &crate::RecordContext::none())
        .unwrap();
    assert!(none.is_empty());
    let row = b"184,\"Equoid (Laundry, #2.9)\",C. S.,\"=\"\"1466855479\"\"\",\"=\"\"\"\"\",0,3.96,,2017/08/12,to-read,to-read\n";
    let (out, _) = GoodreadsParser
        .parse(&&row[..], &header, &crate::RecordContext::none())
        .unwrap();
    assert_eq!(out.len(), 1, "unread: shelved only");
    assert_eq!(out[0].isbn.as_deref(), Some("1466855479"));
    assert_eq!(out[0].isbn13, None);
    assert_eq!(out[0].my_rating, None, "0 means unrated");
    assert_eq!(out[0].at, fields::day("2017-08-12").unwrap());
    let read = b"185,T,A,,,4,4.1,2020/01/02,2019/12/01,\"x, y\",read\n";
    let (out, _) = GoodreadsParser
        .parse(&&read[..], &header, &crate::RecordContext::none())
        .unwrap();
    assert_eq!(out.len(), 2);
    assert_eq!(out[1].action, "read");
    assert_eq!(out[1].my_rating, Some(4));
    assert_eq!(out[0].shelves, vec!["x", "y"]);
}

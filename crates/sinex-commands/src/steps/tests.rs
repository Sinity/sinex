use super::*;
use serde_json::json;
use sinex_protocol::rows::{ReadView, Step};

#[test]
fn read_arguments_have_one_strict_typed_owner() {
    let words = ["--view", "trace", "--page-size", "2"].map(str::to_owned);
    let parsed = steps().parse_step("read", &words).unwrap();
    let Step::Read(read) = serde_json::from_value::<Step>(parsed).unwrap();
    assert_eq!(read.view, ReadView::Trace);
    assert_eq!(read.page_size, Some(2));
    assert_eq!(read.cursor, None);
    assert!(serde_json::from_value::<Step>(json!({"verb":"read", "veiw":"trace"})).is_err());
    assert!(steps().parse_step("read", &["unexpected".into()]).is_err());
}

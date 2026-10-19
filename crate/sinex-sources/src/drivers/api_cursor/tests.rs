use super::*;
use std::cell::RefCell;

/// An API over a list: the cursor is how many items were read.
struct List(RefCell<Vec<&'static str>>);

impl Api for List {
    fn input(&self) -> InputRef {
        InputRef {
            kind: "api".into(),
            locator: "test".into(),
            host: "h".into(),
        }
    }
    fn fetch(&self, cursor: &Value) -> Result<Page, DriverError> {
        let from = cursor.as_u64().unwrap_or(0) as usize;
        let items = self.0.borrow();
        Ok(Page {
            records: items[from..]
                .iter()
                .map(|s| json!({"v": s}).as_object().unwrap().clone())
                .collect(),
            next: json!(items.len()),
        })
    }
}

#[test]
fn each_answer_is_one_material_and_the_cursor_moves_only_at_its_end() {
    let d = ApiCursor {
        api: List(RefCell::new(vec!["a", "b"])),
    };
    let first = d.poll(&Value::Null).unwrap();
    let m = first.material.as_ref().unwrap();
    assert_eq!(m.records.len(), 2);
    let middle = (m.cursor_at)(m.records[0].1);
    assert_eq!(middle["api"], Value::Null, "half stored: ask again");
    let end = (m.cursor_at)(m.bytes.len());
    assert_eq!(end["api"], json!(2));

    assert!(d.poll(&end).unwrap().material.is_none());
    d.api.0.borrow_mut().push("c");
    let next = d.poll(&end).unwrap();
    let n = next.material.as_ref().unwrap();
    assert_eq!((n.part_index, n.previous_part), (1, Some(m.id)));
    assert_eq!(n.records.len(), 1);
    assert!((n.reading_order)(0) > (m.reading_order)(m.records[1].0));
}

use super::*;
use serde_json::json;

fn read(path: &str, csv: &str) -> Result<NoteObserved, RecordError> {
    let manifest = sinex_model::Manifest::from_json(json!({ "path": path }));
    let context = RecordContext {
        manifest: &manifest,
        member: None,
    };
    AssistantParser
        .parse(&csv.as_bytes(), &(), &context)
        .map(|(mut out, ())| out.remove(0))
}

#[devkit::test]
fn a_note_is_its_text_at_the_time_its_name_gives() {
    let n = read(
        "Takeout/Assistant Notes and Lists/2019-02-10T20_35_06.574.csv",
        "Note title,Note content,Note state\n,buy stamps,Active\n",
    )
    .unwrap();
    assert_eq!(n.service, "google-assistant");
    assert_eq!(
        n.created_at,
        Timestamp::from_unix_ns(1_549_830_906_574_000_000).unwrap()
    );
    assert_eq!(n.text.as_deref(), Some("buy stamps"));
    assert_eq!(n.title, None);
    assert_eq!(n.extra["state"], "Active");
}

#[devkit::test]
fn a_list_has_its_items_after_the_item_header() {
    let l = read(
        "Takeout/Assistant Notes and Lists/Shopping list2019-02-10T20_35_18.476.csv",
        "List title,List state\nShopping list,Active\nItem content,Item state\nmilk,Active\nbread,Completed\n",
    )
    .unwrap();
    assert_eq!(l.title.as_deref(), Some("Shopping list"));
    assert_eq!(
        l.items,
        vec![
            NoteItem {
                text: "milk".into(),
                checked: false
            },
            NoteItem {
                text: "bread".into(),
                checked: true
            },
        ]
    );
    assert_eq!(l.extra["item_states"], json!(["Active", "Completed"]));
    let empty = read(
        "Takeout/Assistant Notes and Lists/Shopping list2019-02-10T20_35_18.476.csv",
        "List title,List state\nShopping list,Active\nItem content,Item state\n",
    )
    .unwrap();
    assert!(empty.items.is_empty());
}

#[devkit::test]
fn a_file_without_a_time_or_header_fails() {
    assert!(
        read(
            "Takeout/Assistant Notes and Lists/notes.csv",
            "Note title\n"
        )
        .is_err()
    );
    assert!(
        read(
            "Takeout/Assistant Notes and Lists/2019-02-10T20_35_06.574.csv",
            "Something,else\n"
        )
        .is_err()
    );
    assert!(select(
        "Takeout/Assistant Notes and Lists/2019-02-10T20_35_06.574.csv"
    ));
    assert!(!select("Takeout/My Activity/Assistant/MyActivity.html"));
}

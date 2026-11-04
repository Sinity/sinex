use super::*;

#[devkit::test]
fn gmail_labels_and_rfc_identity_survive_imap_and_mbox_overlap() {
    let mut gmail = message("communication.gmail", "rfc@example.test", H + 5.0);
    gmail.payload["labels"] = json!(["Sent", "Draft", "Work / Research"]);
    gmail.payload["thread_id"] = json!("gmail-thread");
    let mut mbox = message("communication.mbox", "rfc@example.test", H + 5.0);
    let mut imap = message("communication.imap", "rfc@example.test", H + 5.0);
    for event in [&mut gmail, &mut mbox, &mut imap] {
        event.payload["account"] = json!("owner@example.test");
    }
    let output = fuse(mail(), &[mbox, imap, gmail]);
    assert_eq!(output.len(), 1);
    assert_eq!(output[0]["message_id"], "rfc@example.test");
    assert_eq!(output[0]["thread_id"], "gmail-thread");
    assert_eq!(
        output[0]["labels"],
        json!(["Sent", "Draft", "Work / Research"])
    );
    assert_eq!(
        sources(&output[0]),
        [
            "communication.gmail",
            "communication.imap",
            "communication.mbox"
        ]
    );
}

#[devkit::test]
fn the_same_message_id_in_different_accounts_is_not_joined() {
    let mut first = message("communication.gmail", "rfc@example.test", H + 5.0);
    first.payload["account"] = json!("one@example.test");
    let mut second = message("communication.imap", "rfc@example.test", H + 5.0);
    second.payload["account"] = json!("two@example.test");
    let mut unknown = message("communication.mbox", "rfc@example.test", H + 5.0);
    unknown.payload["account"] = Value::Null;
    let inputs = [first.clone(), second, unknown];
    assert!(fuse(mail(), &inputs).is_empty());
    assert_eq!(occurrences(mail(), &inputs).len(), 3);
    let mut same = message("communication.imap", "rfc@example.test", H + 5.0);
    same.payload["account"] = json!("one@example.test");
    let joined = fuse(mail(), &[first, same]);
    assert_eq!(joined.len(), 1);
    assert_eq!(joined[0]["account"], "one@example.test");
}

#[devkit::test]
fn messages_from_unknown_accounts_in_different_sources_stay_separate() {
    let mut first = message("communication.imap", "rfc@example.test", H + 5.0);
    let mut second = message("communication.mbox", "rfc@example.test", H + 5.0);
    first.payload["account"] = Value::Null;
    second.payload["account"] = Value::Null;
    let inputs = [first, second];
    assert!(fuse(mail(), &inputs).is_empty());
    let read = occurrences(mail(), &inputs);
    assert_eq!(read.len(), 2);
    assert!(read.iter().all(|message| message["account"].is_null()));
}

use super::*;

#[devkit::test]
fn gmail_labels_and_rfc_identity_survive_imap_and_mbox_overlap() {
    let mut gmail = mail("communication.gmail", "rfc@example.test", H + 5.0);
    gmail.payload["labels"] = json!(["Sent", "Draft", "Work / Research"]);
    gmail.payload["thread_id"] = json!("gmail-thread");
    let mut mbox = mail("communication.mbox", "rfc@example.test", H + 5.0);
    let mut imap = mail("communication.imap", "rfc@example.test", H + 5.0);
    for event in [&mut gmail, &mut mbox, &mut imap] {
        event.payload["account"] = json!("owner@example.test");
    }
    let output = fuse(&MAIL, &[mbox, imap, gmail]);
    assert_eq!(output.len(), 1);
    assert_eq!(output[0]["message_id"], "rfc@example.test");
    assert_eq!(output[0]["thread_id"], "gmail-thread");
    assert_eq!(
        output[0]["labels"],
        json!(["Sent", "Draft", "Work / Research"])
    );
    assert_eq!(
        observed_by(&output[0]),
        [
            "communication.gmail",
            "communication.imap",
            "communication.mbox"
        ]
    );
}

#[devkit::test]
fn the_same_message_id_in_different_accounts_is_not_joined() {
    let mut first = mail("communication.gmail", "rfc@example.test", H + 5.0);
    first.payload["account"] = json!("one@example.test");
    let mut second = mail("communication.imap", "rfc@example.test", H + 5.0);
    second.payload["account"] = json!("two@example.test");
    let mut unknown = mail("communication.mbox", "rfc@example.test", H + 5.0);
    unknown.payload["account"] = Value::Null;
    assert_eq!(fuse(&MAIL, &[first.clone(), second, unknown]).len(), 3);
    let mut same = mail("communication.imap", "rfc@example.test", H + 5.0);
    same.payload["account"] = json!("one@example.test");
    let joined = fuse(&MAIL, &[first, same]);
    assert_eq!(joined.len(), 1);
    assert_eq!(joined[0]["account"], "one@example.test");
}

#[devkit::test]
fn messages_from_unknown_accounts_in_different_sources_stay_separate() {
    let mut first = mail("communication.imap", "rfc@example.test", H + 5.0);
    let mut second = mail("communication.mbox", "rfc@example.test", H + 5.0);
    first.payload["account"] = Value::Null;
    second.payload["account"] = Value::Null;
    let output = fuse(&MAIL, &[first, second]);
    assert_eq!(output.len(), 2);
    assert!(output.iter().all(|message| message["account"].is_null()));
}

use super::*;

#[devkit::test]
fn gmail_labels_and_rfc_identity_survive_imap_and_mbox_overlap() {
    let mut gmail = mail("communication.gmail", "rfc@example.test", H + 5.0);
    gmail.payload["labels"] = json!(["Sent", "Draft", "Work / Research"]);
    gmail.payload["thread_id"] = json!("gmail-thread");
    let output = fuse(
        &MAIL,
        &[
            mail("communication.mbox", "rfc@example.test", H + 5.0),
            mail("communication.imap", "rfc@example.test", H + 5.0),
            gmail,
        ],
    );
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
    let unknown = mail("communication.mbox", "rfc@example.test", H + 5.0);
    assert_eq!(fuse(&MAIL, &[first.clone(), second, unknown]).len(), 3);
    let mut same = mail("communication.imap", "rfc@example.test", H + 5.0);
    same.payload["account"] = json!("one@example.test");
    let joined = fuse(&MAIL, &[first, same]);
    assert_eq!(joined.len(), 1);
    assert_eq!(joined[0]["account"], "one@example.test");
}

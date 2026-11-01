use super::*;
use serde_json::json;

#[devkit::test]
fn a_report_names_each_file_its_products_and_what_no_parser_reads() {
    let report = json!({
        "operation": "01900000-0000-7000-8000-000000000001",
        "result": "stopped_early",
        "inventory": {"files": [
            {
                "path": "/synthetic/takeout-001.zip",
                "reading": {"as": "parsed", "formats": ["takeout", "takeout-chrome"]},
                "members": [
                    {"name": "Takeout/Keep/a.json", "disposition": "read", "sources": ["google.keep"]},
                    {"name": "Takeout/Travel/t.json", "disposition": "stored_not_interpreted", "candidates": []}
                ],
                "products": {
                    "google.keep": {"state": "read", "accounting": {
                        "framed": 3, "removed": 0, "read": 2, "headers": 0, "failures": 1,
                        "readings": 2, "new": 1, "same": 1, "revised": 0, "older": 0, "superseded": 0,
                        "first_failure": "missing title"}},
                    "browser.takeout-chrome-history": {"state": "failed", "error": "synthetic read error",
                        "accounting": {"framed": 0}}
                }
            },
            {
                "path": "/synthetic/takeout-003.zip",
                "reading": {"as": "unparsed", "candidates": []},
                "members": [{"name": "Takeout/Drive/d.pdf", "disposition": "stored_not_interpreted", "candidates": []}],
                "products": {}
            }
        ]}
    });
    let text = render(&report);
    let lines: Vec<&str> = text.lines().collect();
    assert_eq!(
        lines[0],
        "stage 01900000-0000-7000-8000-000000000001  stopped_early"
    );
    assert_eq!(
        lines[1],
        "/synthetic/takeout-001.zip  2 member(s)  read by takeout, takeout-chrome"
    );
    assert!(
        text.contains("google.keep  read  records 3 (read 2, headers 0, failures 1)  readings 2: new 1, same 1, revised 0, older 0, superseded 0; first unreadable: missing title"),
        "{text}"
    );
    assert!(text.contains("synthetic read error"), "{text}");
    assert!(
        text.contains("stored, not interpreted: 1 member(s): Takeout/Travel/t.json"),
        "{text}"
    );
    assert!(
        text.contains(
            "/synthetic/takeout-003.zip  1 member(s)  stored, not interpreted (no parser)"
        ),
        "{text}"
    );
}

#[devkit::test]
fn a_stage_that_has_not_ended_says_so() {
    let text = render(&json!({"operation": "op", "result": null, "inventory": {"files": []}}));
    assert_eq!(text, "stage op  not ended\n");
}

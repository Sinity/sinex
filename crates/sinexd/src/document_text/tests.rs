use super::extract::{Format, format, markup_text, text};
use super::*;
use std::io::Write;

fn zip_of(members: &[(&str, &str)]) -> Vec<u8> {
    let mut bytes = Vec::new();
    {
        let mut zip = zip::ZipWriter::new(std::io::Cursor::new(&mut bytes));
        for (name, content) in members {
            zip.start_file(*name, zip::write::SimpleFileOptions::default())
                .unwrap();
            zip.write_all(content.as_bytes()).unwrap();
        }
        zip.finish().unwrap();
    }
    bytes
}

#[devkit::test]
fn formats_come_from_the_bytes_and_text_from_the_extension() {
    assert_eq!(
        format("application/pdf", "/a/b.bin", b"%PDF-1.7"),
        Some(Format::Pdf)
    );
    assert_eq!(
        format("text/plain", "/a/page.HTML", b"<p>x"),
        Some(Format::Html)
    );
    assert_eq!(
        format("text/plain", "/a/notes.md", b"# x"),
        Some(Format::Markdown)
    );
    assert_eq!(format("text/plain", "/a/readme", b"x"), Some(Format::Text));
    assert_eq!(format("image/png", "/a/b.png", b"\x89PNG"), None);
    let docx = zip_of(&[("word/document.xml", "<w:p/>")]);
    assert_eq!(format("application/zip", "/a/b", &docx), Some(Format::Docx));
    let odt = zip_of(&[
        ("mimetype", "application/vnd.oasis.opendocument.text"),
        ("content.xml", "<text:p/>"),
    ]);
    assert_eq!(format("application/zip", "/a/b", &odt), Some(Format::Odt));
    let other = zip_of(&[("x.txt", "x")]);
    assert_eq!(format("application/zip", "/a/b.zip", &other), None);
}

#[devkit::test]
fn markup_becomes_plain_text() {
    let html = r#"<html><head><title>A &amp; B</title><style>p{}</style></head>
        <body><h1>Heading</h1><p>One &lt;two&gt;&#x20;three&nbsp;four</p>
        <script>var x = "<p>";</script><ul><li>first</li><li>second &#8212; ok</li></ul></body></html>"#;
    let (title, read) = text(Format::Html, html.as_bytes()).unwrap();
    assert_eq!(title.as_deref(), Some("A & B"));
    assert_eq!(
        read,
        "Heading\nOne <two> three four\nfirst\nsecond \u{2014} ok"
    );
    let word = r#"<w:document><w:body><w:p><w:r><w:t>Zażółć</w:t></w:r><w:r><w:t xml:space="preserve"> gęślą</w:t></w:r></w:p><w:p><w:r><w:t>jaźń &amp; co</w:t></w:r></w:p></w:body></w:document>"#;
    assert_eq!(markup_text(word, &["w:p"], &[]), "Zażółć gęślą\njaźń & co");
    let (title, md) = text(Format::Markdown, "# Plan\n\nSteps.\n".as_bytes()).unwrap();
    assert_eq!(
        (title.as_deref(), md.as_str()),
        (Some("Plan"), "# Plan\n\nSteps.")
    );
    assert!(text(Format::Text, b"\xff\xfe").is_err());
}

#[devkit::test]
fn a_stored_document_requires_its_bytes_and_a_referenced_copy_gives_no_text() {
    let key = sinex_model::event_types::library::item_key("h", "/lib", "notes/page.html");
    let copy = sinex_model::event_types::library::FileCopy {
        scan: "scan".into(),
        part_index: 0,
        item: sinex_model::event_types::library::item_id(&key),
        item_key: key,
        scanned_at: sinex_model::Timestamp::from_unix_secs(1_800_000_000).unwrap(),
        host: "h".into(),
        root: "/lib".into(),
        path: "/lib/notes/page.html".into(),
        content: "blake3:00".into(),
        size: 10,
        sha256: String::new(),
        md5: String::new(),
        media_type: "text/plain".into(),
        width: None,
        height: None,
        duration_ms: None,
        stored: true,
        device: None,
        inode: None,
        modified_at: Some(sinex_model::Timestamp::from_unix_secs(1_800_000_000).unwrap()),
        moved_from: None,
        move_candidates: vec![],
        previous_content: None,
        vanished_at: None,
    };
    let event = Event::build(
        &copy,
        "library.scan",
        Origin::Record {
            location: sinex_model::RecordLocation {
                material: sinex_model::MaterialId::new(),
                anchor: 0,
            },
            output_index: 0,
        },
        ReadingOrder(0),
        [0; 32],
        sinex_model::Class::Open,
    )
    .unwrap();
    let d = DocumentText { pdf_reader: None };
    let mut video = event.clone();
    video.payload["media_type"] = serde_json::json!("video/mp4");
    assert_eq!(d.reads_for(&video), Reads::Nothing);
    let mut referenced = event.clone();
    referenced.payload["stored"] = serde_json::json!(false);
    assert_eq!(d.reads_for(&referenced), Reads::Nothing);
    assert!(
        d.derive(
            Input {
                event: &referenced,
                bytes: None
            },
            &Env::at(
                sinex_model::Timestamp::now(),
                sinex_model::time_settings::TimeSettings::new("Europe/Warsaw", 4, 0)
                    .expect("fixture day setting")
            )
        )
        .unwrap()
        .is_empty()
    );

    let env = Env::at(
        sinex_model::Timestamp::now(),
        sinex_model::time_settings::TimeSettings::new("Europe/Warsaw", 4, 0)
            .expect("fixture day setting"),
    );
    let out = d
        .derive(
            Input {
                event: &event,
                bytes: Some(b"<title>T</title><p>Body</p>"),
            },
            &env,
        )
        .unwrap();
    assert_eq!(out.len(), 1);
    let t: DocumentTextPayload = out[0].payload_as().unwrap();
    assert_eq!(
        (t.format.as_str(), t.part, t.text.as_str()),
        ("html", 0, "T\nBody")
    );
    assert_eq!(t.title.as_deref(), Some("T"));
    assert_eq!(out[0].ts_orig.at, copy.modified_at);
    let mut other_copy = copy.clone();
    other_copy.path = "/lib/other/page.html".into();
    other_copy.item_key =
        sinex_model::event_types::library::item_key("h", "/lib", "other/page.html");
    other_copy.item = sinex_model::event_types::library::item_id(&other_copy.item_key);
    let other = Event::build(
        &other_copy,
        "library.scan",
        Origin::Record {
            location: sinex_model::RecordLocation {
                material: sinex_model::MaterialId::new(),
                anchor: 0,
            },
            output_index: 0,
        },
        ReadingOrder(0),
        [1; 32],
        sinex_model::Class::Guarded,
    )
    .unwrap();
    let other_text = d
        .derive(
            Input {
                event: &other,
                bytes: Some(b"<title>T</title><p>Body</p>"),
            },
            &env,
        )
        .unwrap();
    assert_ne!(
        out[0].equivalence_key, other_text[0].equivalence_key,
        "identical bytes in different copies keep independent parents"
    );
    assert_eq!(other_text[0].class, sinex_model::Class::Guarded);
    let mut replayed = event.clone();
    replayed.id = sinex_model::EventId::new();
    let replayed_text = d
        .derive(
            Input {
                event: &replayed,
                bytes: Some(b"<title>T</title><p>Body</p>"),
            },
            &env,
        )
        .unwrap();
    assert_eq!(out[0].equivalence_key, replayed_text[0].equivalence_key);
    assert_eq!(
        replayed_text[0].origin,
        Origin::Derived {
            parents: vec![replayed.id]
        }
    );

    assert!(
        d.derive(
            Input {
                event: &event,
                bytes: None
            },
            &env
        )
        .is_err()
    );
}

#[devkit::test]
fn markup_keeps_quoted_angle_brackets_and_ignores_comments() {
    assert_eq!(
        markup_text(
            "<p title='x > y'>One</p><!-- hidden <p> --> <p>Two &#0000000000000000000065;</p>",
            &["p"],
            &[]
        ),
        "One\nTwo A"
    );
    assert!(text(Format::Html, b"\xff").is_err());
}

#[devkit::test]
fn installing_or_updating_the_pdf_reader_reprocesses_earlier_inputs() {
    let missing = DocumentText { pdf_reader: None };
    let installed = DocumentText {
        pdf_reader: Some("reader 1".into()),
    };
    let updated = DocumentText {
        pdf_reader: Some("reader 2".into()),
    };
    assert_ne!(PerEvent::version(&missing), PerEvent::version(&installed));
    assert_ne!(PerEvent::version(&installed), PerEvent::version(&updated));
}

fn pdf_document() -> Vec<u8> {
    let contents = "BT /F1 12 Tf 50 700 Td (Synthetic PDF) Tj ET\n";
    let objects = [
        "<< /Type /Catalog /Pages 2 0 R >>".to_owned(),
        "<< /Type /Pages /Kids [3 0 R] /Count 1 >>".to_owned(),
        "<< /Type /Page /Parent 2 0 R /MediaBox [0 0 612 792] /Resources << /Font << /F1 4 0 R >> >> /Contents 5 0 R >>".to_owned(),
        "<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica >>".to_owned(),
        format!("<< /Length {} >>\nstream\n{contents}endstream", contents.len()),
    ];
    let mut pdf = b"%PDF-1.4\n".to_vec();
    let mut offsets = Vec::new();
    for (index, object) in objects.iter().enumerate() {
        offsets.push(pdf.len());
        pdf.extend_from_slice(format!("{} 0 obj\n{object}\nendobj\n", index + 1).as_bytes());
    }
    let xref = pdf.len();
    pdf.extend_from_slice(b"xref\n0 6\n0000000000 65535 f \n");
    for offset in offsets {
        pdf.extend_from_slice(format!("{offset:010} 00000 n \n").as_bytes());
    }
    pdf.extend_from_slice(
        format!("trailer\n<< /Size 6 /Root 1 0 R >>\nstartxref\n{xref}\n%%EOF\n").as_bytes(),
    );
    pdf
}

#[devkit::test]
fn the_local_pdf_reader_extracts_the_stored_document() {
    assert!(
        pdf_reader().is_ok(),
        "the development shell provides pdftotext"
    );
    assert_eq!(pdf_pages(&pdf_document()).unwrap(), ["Synthetic PDF"]);
}

#[devkit::test]
fn html_titles_use_the_title_element_and_respect_quoted_attributes() {
    let (title, _) = text(
        Format::Html,
        b"<title-extra>Wrong</title-extra><title data-note='x > y'>Right &amp; complete</title>",
    )
    .unwrap();
    assert_eq!(title.as_deref(), Some("Right & complete"));
    assert_eq!(
        text(Format::Html, b"<title-extra>Wrong</title-extra>")
            .unwrap()
            .0,
        None
    );
}

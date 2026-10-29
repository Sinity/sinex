//! Documents enter through the public library command and become searchable
//! derived readings whose selectors and parents still name the stored bytes.
#[allow(dead_code)]
#[path = "support/daemon.rs"]
mod daemon;

use serde_json::{Value, json};
use std::io::{Read, Write};
use std::os::unix::net::UnixStream;
use std::path::Path;
use std::time::{Duration, Instant};

const TOKEN: &str = "synthetic-document-operator";

fn call(socket: &Path, request: Value) -> Value {
    let body = serde_json::to_vec(&request).unwrap();
    let mut stream = UnixStream::connect(socket).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(30)))
        .unwrap();
    write!(stream, "POST /v1 HTTP/1.1\r\nHost: localhost\r\nAuthorization: Bearer {TOKEN}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n", body.len()).unwrap();
    stream.write_all(&body).unwrap();
    let mut response = Vec::new();
    stream.read_to_end(&mut response).unwrap();
    let split = response
        .windows(4)
        .position(|bytes| bytes == b"\r\n\r\n")
        .unwrap();
    let headers = std::str::from_utf8(&response[..split]).unwrap();
    assert!(headers.starts_with("HTTP/1.1 200"), "{headers}");
    let reply: sinex_protocol::Reply = serde_json::from_slice(&response[split + 4..]).unwrap();
    match reply {
        sinex_protocol::Reply::Ok { result } => result,
        sinex_protocol::Reply::Refused { reason, .. } => panic!("{request}: {reason}"),
    }
}

async fn wait_rows(socket: &Path, words: &str, count: usize) -> Vec<Value> {
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let answer = call(
            socket,
            json!({"method":"search","words":format!("event_type = document.text and {words}"),"page":100}),
        );
        let rows = answer["rows"].as_array().unwrap();
        if rows.len() == count {
            return rows.clone();
        }
        assert!(
            Instant::now() < deadline,
            "search {words}: expected {count} rows: {answer}"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

fn zip_of(members: &[(&str, &str)]) -> Vec<u8> {
    let mut zip = zip::ZipWriter::new(std::io::Cursor::new(Vec::new()));
    for (name, content) in members {
        zip.start_file(*name, zip::write::SimpleFileOptions::default())
            .unwrap();
        zip.write_all(content.as_bytes()).unwrap();
    }
    zip.finish().unwrap().into_inner()
}

fn pdf() -> Vec<u8> {
    let page = |contents: &str| {
        format!(
            "<< /Length {} >>\nstream\n{contents}endstream",
            contents.len()
        )
    };
    let objects = [
        "<< /Type /Catalog /Pages 2 0 R >>".to_owned(),
        "<< /Type /Pages /Kids [3 0 R 6 0 R] /Count 2 >>".to_owned(),
        "<< /Type /Page /Parent 2 0 R /MediaBox [0 0 612 792] /Resources << /Font << /F1 4 0 R >> >> /Contents 5 0 R >>".to_owned(),
        "<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica >>".to_owned(),
        page("BT /F1 12 Tf 50 700 Td (amberpdfpageone) Tj ET\n"),
        "<< /Type /Page /Parent 2 0 R /MediaBox [0 0 612 792] /Resources << /Font << /F1 4 0 R >> >> /Contents 7 0 R >>".to_owned(),
        page("BT /F1 12 Tf 50 700 Td (cobaltpdfpagetwo) Tj ET\n"),
    ];
    let mut bytes = b"%PDF-1.4\n".to_vec();
    let mut offsets = Vec::new();
    for (index, object) in objects.iter().enumerate() {
        offsets.push(bytes.len());
        bytes.extend_from_slice(format!("{} 0 obj\n{object}\nendobj\n", index + 1).as_bytes());
    }
    let xref = bytes.len();
    bytes.extend_from_slice(b"xref\n0 8\n0000000000 65535 f \n");
    for offset in offsets {
        bytes.extend_from_slice(format!("{offset:010} 00000 n \n").as_bytes());
    }
    bytes.extend_from_slice(
        format!("trailer\n<< /Size 8 /Root 1 0 R >>\nstartxref\n{xref}\n%%EOF\n").as_bytes(),
    );
    bytes
}

struct Fixture {
    name: &'static str,
    format: &'static str,
    bytes: Vec<u8>,
    parts: Vec<(u64, &'static str)>,
}

fn fixtures() -> Vec<Fixture> {
    vec![
        Fixture { name:"pages.pdf",format:"pdf",bytes:pdf(),parts:vec![(1,"amberpdfpageone"),(2,"cobaltpdfpagetwo")] },
        Fixture { name:"word.docx",format:"docx",bytes:zip_of(&[
            ("[Content_Types].xml",r#"<Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types"><Default Extension="rels" ContentType="application/vnd.openxmlformats-package.relationships+xml"/><Override PartName="/word/document.xml" ContentType="application/vnd.openxmlformats-officedocument.wordprocessingml.document.main+xml"/></Types>"#),
            ("_rels/.rels",r#"<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships"><Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument" Target="word/document.xml"/></Relationships>"#),
            ("word/document.xml",r#"<w:document xmlns:w="http://schemas.openxmlformats.org/wordprocessingml/2006/main"><w:body><w:p><w:r><w:t>orchiddocx &amp; meadow</w:t></w:r></w:p></w:body></w:document>"#),
        ]),parts:vec![(0,"orchiddocx & meadow")] },
        Fixture { name:"open.odt",format:"odt",bytes:zip_of(&[
            ("mimetype","application/vnd.oasis.opendocument.text"),
            ("META-INF/manifest.xml",r#"<manifest:manifest xmlns:manifest="urn:oasis:names:tc:opendocument:xmlns:manifest:1.0"><manifest:file-entry manifest:full-path="/" manifest:media-type="application/vnd.oasis.opendocument.text"/><manifest:file-entry manifest:full-path="content.xml" manifest:media-type="text/xml"/></manifest:manifest>"#),
            ("content.xml",r#"<office:document-content xmlns:office="urn:oasis:names:tc:opendocument:xmlns:office:1.0" xmlns:text="urn:oasis:names:tc:opendocument:xmlns:text:1.0"><office:body><office:text><text:p>violetodt &amp; river</text:p></office:text></office:body></office:document-content>"#),
        ]),parts:vec![(0,"violetodt & river")] },
        Fixture { name:"page.html",format:"html",bytes:b"<!doctype html><html><head><title>Neutral title</title></head><body><p>silverhtml &amp; tree</p><script>hiddenhtmlscript</script></body></html>".to_vec(),parts:vec![(0,"silverhtml & tree")] },
        Fixture { name:"notes.md",format:"markdown",bytes:b"# Neutral notes\n\nbronzenotes first revision\n".to_vec(),parts:vec![(0,"# Neutral notes\n\nbronzenotes first revision")] },
    ]
}

fn assert_provenance(
    socket: &Path,
    store: &Path,
    row: &Value,
    fixture: &Fixture,
    part: u64,
    text: &str,
) -> String {
    let payload = &row["payload"];
    assert_eq!(payload["format"], fixture.format);
    assert_eq!(payload["part"], part);
    assert_eq!(payload["text"], text);
    let content = format!("blake3:{}", blake3::hash(&fixture.bytes));
    assert_eq!(payload["content"], content);
    let trace = call(
        socket,
        json!({"method":"trace","reference":format!("sinex:ev/{}",row["id"].as_str().unwrap()),"page":100}),
    );
    let lineage = trace["rows"].as_array().unwrap();
    let copy = lineage
        .iter()
        .find(|event| event["event_type"] == "library.copy")
        .unwrap();
    assert_eq!(copy["payload"]["content"], content);
    assert_eq!(copy["payload"]["size"], fixture.bytes.len() as u64);
    assert!(
        copy["payload"]["path"]
            .as_str()
            .unwrap()
            .ends_with(fixture.name)
    );
    let child = lineage
        .iter()
        .find(|event| event["id"] == row["id"])
        .unwrap();
    assert_eq!(child["parents"], json!([copy["id"].clone()]));
    let blobs = sinex_store::Blobs::open(store).unwrap();
    assert_eq!(
        blobs.read(&content).unwrap(),
        fixture.bytes,
        "whole content range remains byte-identical"
    );
    let material_bytes = blobs.read(copy["bytes"].as_str().unwrap()).unwrap();
    let offset = copy["offset"].as_u64().unwrap() as usize;
    let end = material_bytes[offset..]
        .iter()
        .position(|byte| *byte == b'\n')
        .map_or(material_bytes.len(), |end| offset + end);
    let record: Value = serde_json::from_slice(&material_bytes[offset..end]).unwrap();
    assert_eq!(
        record["content"], content,
        "trace reaches the exact retained source record"
    );
    copy["material"].as_str().unwrap().to_owned()
}

#[devkit::test]
async fn documents_are_searchable_with_exact_ranges_across_replay_and_revision() {
    let db = devkit::pg::test_database().expect("run through xtask test");
    let nats = devkit::nats::test_nats().expect("run through xtask test");
    let dir = tempfile::tempdir().unwrap();
    let root = std::fs::canonicalize(dir.path()).unwrap();
    let store = root.join("store");
    let library = root.join("documents");
    std::fs::create_dir(&library).unwrap();
    let fixtures = fixtures();
    for fixture in &fixtures {
        std::fs::write(library.join(fixture.name), &fixture.bytes).unwrap();
    }
    let tokens = root.join("tokens");
    std::fs::create_dir(&tokens).unwrap();
    std::fs::write(tokens.join("operator.token"), TOKEN).unwrap();
    let sources = root.join("sources.json");
    daemon::sources(&sources, &[]);
    let socket = root.join("api.sock");
    let mut process = daemon::Process::start(
        daemon::run(
            Path::new(env!("CARGO_BIN_EXE_sinexd")),
            &db.url(None),
            &store,
            &[
                "run",
                "--nats",
                nats.url(),
                "--subject-root",
                nats.prefix(),
                "--host",
                "documents",
                "--zone",
                "UTC",
                "--day-start",
                "04:00",
            ],
        )
        .arg("--tokens")
        .arg(&tokens)
        .arg("--sources")
        .arg(&sources)
        .arg("--socket")
        .arg(&socket),
        root.join("daemon.log"),
    );
    let deadline = Instant::now() + Duration::from_secs(30);
    while UnixStream::connect(&socket).is_err() {
        process.running();
        assert!(Instant::now() < deadline, "{}", process.log());
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    call(&socket, json!({"method":"library-add","path":library}));
    let mut originals = Vec::new();
    let mut materials = std::collections::BTreeSet::new();
    for fixture in &fixtures {
        for (part, text) in &fixture.parts {
            let word = text.split_whitespace().find(|word| word.len() > 8).unwrap();
            let rows = wait_rows(&socket, word, 1).await;
            materials.insert(assert_provenance(
                &socket, &store, &rows[0], fixture, *part, text,
            ));
            originals.push((fixture.name, *part, word.to_owned(), rows[0].clone()));
        }
    }
    assert!(
        call(
            &socket,
            json!({"method":"search","words":"hiddenhtmlscript"})
        )["rows"]
            .as_array()
            .unwrap()
            .is_empty()
    );
    std::fs::remove_dir_all(&library).unwrap();
    call(
        &socket,
        json!({"method":"replay","material":materials.into_iter().collect::<Vec<_>>() }),
    );
    for (name, part, word, old) in &originals {
        let fixture = fixtures
            .iter()
            .find(|fixture| fixture.name == *name)
            .unwrap();
        let deadline = Instant::now() + Duration::from_secs(30);
        loop {
            let rows = wait_rows(&socket, word, 1).await;
            if rows[0]["id"] != old["id"] {
                let text = fixture.parts.iter().find(|(p, _)| p == part).unwrap().1;
                assert_provenance(&socket, &store, &rows[0], fixture, *part, text);
                break;
            }
            assert!(
                Instant::now() < deadline,
                "replay must replace the derived reading"
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }
    std::fs::create_dir(&library).unwrap();
    let revised = Fixture {
        name: "notes.md",
        format: "markdown",
        bytes: b"# Neutral notes\n\ngoldennotes second revision\n".to_vec(),
        parts: vec![(0, "# Neutral notes\n\ngoldennotes second revision")],
    };
    std::fs::write(library.join(revised.name), &revised.bytes).unwrap();
    call(&socket, json!({"method":"library-add","path":library}));
    let rows = wait_rows(&socket, "goldennotes", 1).await;
    assert_provenance(&socket, &store, &rows[0], &revised, 0, revised.parts[0].1);
    let original = fixtures
        .iter()
        .find(|fixture| fixture.name == "notes.md")
        .unwrap();
    assert_ne!(
        rows[0]["payload"]["content"],
        format!("blake3:{}", blake3::hash(&original.bytes))
    );
    let blobs = sinex_store::Blobs::open(&store).unwrap();
    assert_eq!(
        blobs
            .read(&format!("blake3:{}", blake3::hash(&original.bytes)))
            .unwrap(),
        original.bytes
    );
    process.stop().await;
}

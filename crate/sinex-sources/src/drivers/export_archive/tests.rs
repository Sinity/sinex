use super::*;
use std::io::Write;

#[test]
fn delimited_records_start_at_each_delimiter_and_hold_back_the_last() {
    let bytes = b"<head>css</head><c>one</c><c>two</c><c>thr";
    let (records, covered) = Framing::Delimited(b"<c>").frame(bytes, false);
    assert_eq!(records, vec![(16, 26), (26, 36)]);
    assert_eq!(covered, 36, "the last cell may be cut, so it waits");
    let (records, covered) = Framing::Delimited(b"<c>").frame(bytes, true);
    assert_eq!(records.len(), 3);
    assert_eq!(covered, bytes.len());
    let (records, covered) = Framing::Delimited(b"<c>").frame(b"<head>no cells", false);
    assert!(records.is_empty());
    assert_eq!(
        covered, 11,
        "a delimiter could still start in the last bytes"
    );
}

fn dir() -> PathBuf {
    let d = std::env::temp_dir().join(format!("sinex-export-{}", uuid::Uuid::now_v7()));
    std::fs::create_dir_all(&d).unwrap();
    d
}

fn records(a: &Acquired) -> Vec<String> {
    let m = a.material.as_ref().unwrap();
    m.records
        .iter()
        .map(|(s, e)| String::from_utf8_lossy(&m.bytes[*s..*e]).into_owned())
        .collect()
}

fn driver(path: PathBuf, framing: Framing, part_bytes: usize) -> ExportArchive {
    ExportArchive {
        path,
        host: "h".into(),
        select: |p| p.ends_with(".json"),
        framing,
        part_bytes,
    }
}

#[test]
fn json_array_inside_an_object() {
    let doc = br#"{"Browser History": [ {"a": 1}, {"b": [2, 3]} ], "x": 0}"#;
    let (r, _) = Framing::JsonArray("/Browser History").frame(doc, true);
    let items: Vec<&[u8]> = r.iter().map(|(s, e)| &doc[*s..*e]).collect();
    assert_eq!(items, vec![&br#"{"a": 1}"#[..], br#"{"b": [2, 3]}"#]);
}

#[test]
fn csv_keeps_quoted_newlines_in_one_row_and_holds_back_a_cut_row() {
    let data = b"id,note\n1,\"two\nlines\"\n2,x\n3,cut";
    let (r, consumed) = Framing::Csv.frame(data, false);
    assert_eq!(r.len(), 3, "header, row 1, row 2; row 3 may be cut");
    assert_eq!(&data[r[1].0..r[1].1], b"1,\"two\nlines\"\n");
    assert_eq!(consumed, r[2].1);
    assert_eq!(Framing::Csv.frame(data, true).0.len(), 4);
}

#[test]
fn mbox_splits_at_from_lines_only() {
    let data =
        b"From a@b Mon\nSubject: one\n\n>From quoted\nbody\nFrom c@d Tue\nSubject: two\n\nx\n";
    let (r, consumed) = Framing::Mbox.frame(data, false);
    assert_eq!(r.len(), 1);
    assert_eq!(consumed, r[0].1);
    let (r, _) = Framing::Mbox.frame(data, true);
    assert_eq!(r.len(), 2);
    assert!(data[r[1].0..].starts_with(b"From c@d"));
}

#[test]
fn plain_file_is_read_in_parts_and_resumes() {
    let d = dir();
    let path = d.join("export.csv");
    std::fs::write(&path, "id,t\n1,a\n2,b\n3,c\n4,d\n").unwrap();
    let x = driver(path, Framing::Csv, 9);
    let mut cursor = Value::Null;
    let mut all = Vec::new();
    for _ in 0..10 {
        let a = x.poll(&cursor).unwrap();
        let Some(m) = a.material.as_ref() else {
            cursor = a.cursor_unchanged.clone();
            break;
        };
        all.extend(records(&a));
        cursor = (m.cursor_at)(m.bytes.len());
    }
    assert_eq!(all, vec!["id,t\n", "1,a\n", "2,b\n", "3,c\n", "4,d\n"]);
    assert_eq!(cursor["done"], json!(true));
    // Unchanged afterwards.
    assert!(x.poll(&cursor).unwrap().material.is_none());
}

#[test]
fn a_touch_is_not_a_new_export() {
    let d = dir();
    let path = d.join("a.json");
    std::fs::write(&path, "[1, 2]").unwrap();
    let x = driver(path.clone(), Framing::JsonArray(""), PART_BYTES);
    let a = x.poll(&Value::Null).unwrap();
    let m = a.material.as_ref().unwrap();
    let cursor = (m.cursor_at)(m.bytes.len());
    let f = std::fs::File::options().append(true).open(&path).unwrap();
    f.set_modified(std::time::SystemTime::now() + std::time::Duration::from_secs(5))
        .unwrap();
    let b = x.poll(&cursor).unwrap();
    assert!(b.material.is_none());
    std::fs::write(&path, "[1, 2, 3]").unwrap();
    let c = x.poll(&b.cursor_unchanged).unwrap();
    assert_eq!(records(&c), vec!["1", "2", "3"]);
    assert_eq!(c.revision, RevisionChange::Replaced);
}

#[test]
fn zip_members_form_one_stream_with_an_index() {
    let d = dir();
    let path = d.join("takeout.zip");
    {
        let mut z = zip::ZipWriter::new(std::fs::File::create(&path).unwrap());
        let o = zip::write::SimpleFileOptions::default();
        z.start_file("Takeout/a.json", o).unwrap();
        z.write_all(b"[1, 2]").unwrap();
        z.start_file("Takeout/skip.txt", o).unwrap();
        z.write_all(b"nope").unwrap();
        z.start_file("Takeout/b.json", o).unwrap();
        z.write_all(b"[3]").unwrap();
        z.finish().unwrap();
    }
    let a = driver(path.clone(), Framing::JsonArray(""), PART_BYTES)
        .poll(&Value::Null)
        .unwrap();
    assert_eq!(records(&a), vec!["1", "2", "3"]);
    let m = a.material.as_ref().unwrap();
    assert_eq!(m.stored_file.as_deref(), Some(path.as_path()));
    assert_eq!(m.manifest["container"], json!("zip"));
    assert_eq!(
        m.manifest["members"],
        json!([
            {"path": "Takeout/a.json", "stream_offset": 0, "length": 6},
            {"path": "Takeout/b.json", "stream_offset": 6, "length": 3}
        ])
    );
    let member = crate::drain::member_at(&m.manifest, m.records[2].0).unwrap();
    assert_eq!(member["path"], json!("Takeout/b.json"));
    assert_eq!((m.cursor_at)(m.bytes.len())["done"], json!(true));
}

#[test]
fn gzipped_tar_is_recognized_and_decoded() {
    let d = dir();
    let path = d.join("export.tgz");
    {
        let gz = flate2::write::GzEncoder::new(
            std::fs::File::create(&path).unwrap(),
            flate2::Compression::fast(),
        );
        let mut t = tar::Builder::new(gz);
        let data = b"[\"x\"]";
        let mut h = tar::Header::new_gnu();
        h.set_size(data.len() as u64);
        h.set_mode(0o644);
        h.set_cksum();
        t.append_data(&mut h, "Takeout/x.json", &data[..]).unwrap();
        t.into_inner().unwrap().finish().unwrap();
    }
    assert_eq!(sniff(&path).unwrap(), Container::TarGzip);
    let a = driver(path, Framing::JsonArray(""), PART_BYTES)
        .poll(&Value::Null)
        .unwrap();
    assert_eq!(records(&a), vec!["\"x\""]);
}

#[test]
fn a_record_longer_than_a_part_is_read_whole() {
    let d = dir();
    let path = d.join("long.txt");
    std::fs::write(&path, format!("{}\nb\n", "a".repeat(50))).unwrap();
    let a = driver(path, Framing::Lines, 8).poll(&Value::Null).unwrap();
    assert_eq!(records(&a)[0].len(), 51);
}

#[test]
fn a_zip_is_read_in_parts_of_whole_members_covering_every_record() {
    let d = dir();
    let path = d.join("takeout.zip");
    {
        let mut z = zip::ZipWriter::new(std::fs::File::create(&path).unwrap());
        let o = zip::write::SimpleFileOptions::default();
        for i in 0..20 {
            z.start_file(format!("Takeout/{i:02}.json"), o).unwrap();
            z.write_all(format!("[{}, {}]", i * 2, i * 2 + 1).as_bytes())
                .unwrap();
        }
        z.finish().unwrap();
    }
    // A part of 20 bytes: two or three members each, never a split one.
    let x = driver(path.clone(), Framing::JsonArray(""), 20);
    let mut cursor = Value::Null;
    let mut read = Vec::new();
    let mut parts = 0;
    loop {
        let a = x.poll(&cursor).unwrap();
        let Some(m) = &a.material else { break };
        parts += 1;
        assert!(
            m.bytes.len() <= 20 + 8,
            "a part holds whole members up to the size"
        );
        // Only the first part stores the archive itself; later parts
        // store their decoded stream, read as plain.
        if parts == 1 {
            assert_eq!(m.stored_file.as_deref(), Some(path.as_path()));
        } else {
            assert!(m.stored_file.is_none());
            assert_eq!(m.manifest["container"], json!("plain"));
        }
        read.extend(records(&a));
        cursor = (m.cursor_at)(m.bytes.len());
    }
    assert!(parts > 5, "{parts} parts");
    let expected: Vec<String> = (0..40).map(|i| i.to_string()).collect();
    assert_eq!(read, expected);
    assert_eq!(cursor["done"], json!(true));
    assert!(x.poll(&cursor).unwrap().material.is_none());
}

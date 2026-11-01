use super::*;

fn extended_history(line: &[u8]) -> bool {
    line.starts_with(b": ") && line.contains(&b';')
}

#[devkit::test]
fn a_first_line_structure_reads_only_the_first_line() {
    let dir = tempfile::tempdir().unwrap();
    let matching = dir.path().join("matching");
    std::fs::write(&matching, ": 1600000000:0;synthetic\r\nplain second line\n").unwrap();
    let later = dir.path().join("later");
    std::fs::write(&later, "plain first line\n: 1600000000:0;synthetic\n").unwrap();
    let unterminated = dir.path().join("unterminated");
    std::fs::write(&unterminated, ": 1600000000:0;no newline").unwrap();
    let empty = dir.path().join("empty");
    std::fs::write(&empty, "").unwrap();

    let structure = Structure::FirstLine(extended_history);
    assert!(structure.matches_file(&matching).unwrap());
    assert!(!structure.matches_file(&later).unwrap());
    assert!(structure.matches_file(&unterminated).unwrap());
    assert!(!structure.matches_file(&empty).unwrap());
}

#[devkit::test]
fn a_first_line_longer_than_the_detection_window_matches_nothing() {
    let dir = tempfile::tempdir().unwrap();
    let long = dir.path().join("long");
    let mut bytes = b": 1600000000:0;".to_vec();
    bytes.extend(std::iter::repeat_n(b'x', FIRST_LINE_BYTES));
    bytes.push(b'\n');
    std::fs::write(&long, bytes).unwrap();
    assert!(
        !Structure::FirstLine(extended_history)
            .matches_file(&long)
            .unwrap()
    );
}

#[devkit::test]
fn an_archive_layout_matches_members_and_never_a_whole_file() {
    let dir = tempfile::tempdir().unwrap();
    let file = dir.path().join("Takeout");
    std::fs::write(&file, "Takeout/\n").unwrap();
    let layout = Structure::ArchiveLayout(|member| member.starts_with("Takeout/"));
    assert!(layout.is_archive_layout());
    assert!(!layout.matches_file(&file).unwrap());
    assert!(layout.matches_member("Takeout/Keep/note.json"));
    assert!(!layout.matches_member("Other/Keep/note.json"));
    assert!(!Structure::FirstLine(extended_history).matches_member("Takeout/x"));
}

#[devkit::test]
fn a_missing_file_is_an_error_not_a_mismatch() {
    let dir = tempfile::tempdir().unwrap();
    let error = Structure::FirstLine(extended_history)
        .matches_file(&dir.path().join("absent"))
        .unwrap_err();
    assert!(error.contains("absent"), "{error}");
}

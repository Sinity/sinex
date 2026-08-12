use super::*;

#[test]
#[ignore = "sinex-ouyv open: validate_file_path only rejects a directory indicator via an explicit \
            trailing slash/backslash -- a bare directory path with no trailing separator (e.g. \
            /tmp) passes as if it were a valid file path, violating the function's stated intent \
            to validate FILE (not directory) paths"]
fn validate_file_path_rejects_a_known_directory_with_no_trailing_separator() {
    let result = validate_file_path("/tmp");
    assert!(
        result.is_err(),
        "'/tmp' is a directory with no trailing separator and should be rejected by \
         validate_file_path, but it was accepted: {result:?}"
    );
}

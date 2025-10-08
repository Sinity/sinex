use camino::Utf8PathBuf as PathBuf;
use serde_json::{json, Value};
use sinex_core::types::validation::core::{
    contains_shell_metacharacters, deserialize_json_with_validation, normalize_unicode,
    sanitize_filename_component, validate_json, validate_json_value, validate_path, Result,
};
use sinex_test_utils::sinex_test;

#[sinex_test]
fn path_validation_blocks_traversal() -> Result<()> {
    assert!(validate_path("normal/path.txt").is_ok());
    assert!(validate_path("/absolute/path.txt").is_ok());
    assert!(validate_path("/etc/passwd\0.txt").is_err());
    assert!(validate_path("../../../etc/passwd").is_err());
    assert!(validate_path(&"a".repeat(5000)).is_err());

    let cleaned = validate_path("./some/../path/./file.txt").unwrap();
    assert_eq!(cleaned, PathBuf::from("path/file.txt"));
    Ok(())
}

#[sinex_test]
fn filename_sanitization_removes_disallowed_characters() -> Result<()> {
    assert_eq!(
        sanitize_filename_component("normal.txt").unwrap(),
        "normal.txt"
    );

    let sanitized = sanitize_filename_component("file<>:\"|?*.txt")?;
    assert!(!sanitized.contains('<'));
    assert!(!sanitized.contains('>'));
    assert!(!sanitized.contains(':'));

    assert!(sanitize_filename_component("").is_err());
    Ok(())
}

#[sinex_test]
fn json_validation_enforces_limits() -> Result<()> {
    assert!(validate_json(r#"{"key": "value", "number": 42}"#).is_ok());

    let large = format!(r#"{{"data": "{}"}}"#, "x".repeat(11_000_000));
    assert!(validate_json(&large).is_err());

    let mut deep = String::from("{");
    for _ in 0..40 {
        deep.push_str(r#""a":{"#);
    }
    deep.push('1');
    for _ in 0..40 {
        deep.push('}');
    }
    deep.push('}');
    assert!(validate_json(&deep).is_err());
    Ok(())
}

#[sinex_test]
fn json_value_validation_checks_structure() -> Result<()> {
    let valid = json!({"key": "value", "number": 42});
    assert!(validate_json_value(&valid).is_ok());

    let mut large_obj = serde_json::Map::new();
    for i in 0..1100 {
        large_obj.insert(format!("key{}", i), json!("value"));
    }
    let large_value = Value::Object(large_obj);
    assert!(validate_json_value(&large_value).is_err());

    let deep_json = r#"{"a":{"b":{"c":{"d":{"e":{"f":{"g":{"h":{"i":{"j":{"k":{"l":{"m":{"n":{"o":{"p":{"q":{"r":{"s":{"t":{"u":{"v":{"w":{"x":{"y":{"z":{"aa":{"bb":{"cc":{"dd":{"ee":{"ff":{"gg":{"hh":{"ii":{"jj":{"kk":{"ll":{"mm":{"nn": 1}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}"#;
    let deep_value: Value = serde_json::from_str(deep_json).unwrap();
    assert!(validate_json_value(&deep_value).is_err());
    Ok(())
}

#[sinex_test]
fn deserialize_json_with_validation_enforces_schema() -> Result<()> {
    #[derive(Debug, serde::Deserialize, PartialEq)]
    struct TestStruct {
        name: String,
        age: u32,
    }

    let value: TestStruct = deserialize_json_with_validation(r#"{"name": "Alice", "age": 30}"#)?;
    assert_eq!(value.name, "Alice");
    assert_eq!(value.age, 30);

    assert!(deserialize_json_with_validation::<TestStruct>(r#"{"name": "Bob"}"#).is_err());

    let large_json = format!(r#"{{"name": "{}", "age": 25}}"#, "x".repeat(11_000_000));
    assert!(deserialize_json_with_validation::<TestStruct>(&large_json).is_err());
    Ok(())
}

#[sinex_test]
fn unicode_normalization_rejects_control_sequences() -> Result<()> {
    assert_eq!(normalize_unicode("hello")?, "hello");
    assert!(normalize_unicode("hello\u{200B}world").is_err());
    assert!(normalize_unicode("file\u{202E}txt.exe").is_err());
    Ok(())
}

#[sinex_test]
fn shell_metacharacter_detection_identifies_risky_strings() -> Result<()> {
    assert!(!contains_shell_metacharacters("normal command"));
    assert!(!contains_shell_metacharacters("rm -rf /"));
    assert!(contains_shell_metacharacters("echo $(whoami)"));
    assert!(contains_shell_metacharacters("cat /etc/passwd | grep root"));
    assert!(contains_shell_metacharacters("ls; rm file"));
    assert!(contains_shell_metacharacters("echo 'test'"));
    assert!(contains_shell_metacharacters("file*"));
    Ok(())
}

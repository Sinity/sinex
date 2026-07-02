use super::*;
use serde_json::json;
use xtask::sandbox::sinex_test;

#[sinex_test]
async fn test_url_extraction() -> TestResult<()> {
    let text = "Check out https://github.com/Sinity/sinex for more info.";
    let result = find_first_entity(text);
    assert!(result.is_some());
    let entity = result.unwrap();
    assert_eq!(entity.entity_type, EntityTypeName::new("url"));
    assert!(entity.raw_name.contains("github.com"));
    Ok(())
}

#[sinex_test]
async fn test_email_extraction() -> TestResult<()> {
    let text = "Contact user@example.com for support.";
    let result = find_first_entity(text);
    assert!(result.is_some());
    let entity = result.unwrap();
    assert_eq!(entity.entity_type, EntityTypeName::new("person"));
    assert_eq!(entity.raw_name, "user@example.com");
    Ok(())
}

#[sinex_test]
async fn test_file_path_extraction() -> TestResult<()> {
    let text = "Reading from /home/user/.config/nix/nix.conf.";
    let result = find_first_entity(text);
    assert!(result.is_some());
    let entity = result.unwrap();
    assert_eq!(entity.entity_type, EntityTypeName::new("file"));
    Ok(())
}

#[sinex_test]
async fn test_command_extraction() -> TestResult<()> {
    let text = "Run nix build to compile the project.";
    let result = find_first_entity(text);
    assert!(result.is_some());
    let entity = result.unwrap();
    assert_eq!(entity.entity_type, EntityTypeName::new("tool"));
    assert_eq!(entity.raw_name, "nix");
    Ok(())
}

#[sinex_test]
async fn test_url_priority_over_file_path() -> TestResult<()> {
    let text = "See https://example.com/foo/bar for details.";
    let result = find_first_entity(text);
    assert!(result.is_some());
    let entity = result.unwrap();
    // URL should match first, not file path
    assert_eq!(entity.entity_type, EntityTypeName::new("url"));
    Ok(())
}

#[sinex_test]
async fn test_empty_text() -> TestResult<()> {
    let result = find_first_entity("");
    assert!(result.is_none());
    Ok(())
}

#[sinex_test]
async fn test_no_entity() -> TestResult<()> {
    let result = find_first_entity("This is a simple sentence with nothing extractable.");
    assert!(result.is_none());
    Ok(())
}

#[sinex_test]
async fn test_extract_text_fields() -> TestResult<()> {
    let input = json!({
        "text": "Hello https://example.com world",
        "id": "should-be-skipped",
        "byte_offset": 42,
        "nested": {"body": "another text"}
    });
    let text = extract_text_fields(&input);
    assert!(text.contains("Hello"));
    assert!(text.contains("https://example.com"));
    assert!(text.contains("another text"));
    assert!(!text.contains("should-be-skipped"));
    Ok(())
}

use camino::Utf8PathBuf;
use sinex_core::types::utils::directory_manager::{DirectoryConfig, DirectoryManager};
use sinex_test_utils::sinex_test;
use tempfile::TempDir;

#[sinex_test]
async fn directory_manager_creates_and_ensures_directories() -> TestResult<()> {
    let temp_dir = TempDir::new().unwrap();
    let config = DirectoryConfig {
        base_path: Utf8PathBuf::from_path_buf(temp_dir.path().to_path_buf()).unwrap(),
        ..Default::default()
    };

    let manager = DirectoryManager::new(config);

    manager.create_directory("test_dir").await?;
    assert!(manager.directory_exists("test_dir").await?);

    manager.ensure_directory("test_dir").await?;
    manager.ensure_directory("new_dir").await?;
    assert!(manager.directory_exists("new_dir").await?);
    Ok(())
}

#[sinex_test]
async fn directory_manager_lists_contents() -> TestResult<()> {
    let temp_dir = TempDir::new().unwrap();
    let config = DirectoryConfig {
        base_path: Utf8PathBuf::from_path_buf(temp_dir.path().to_path_buf()).unwrap(),
        ..Default::default()
    };

    let manager = DirectoryManager::new(config);
    manager.create_directory("dir1").await?;
    manager.create_directory("dir2").await?;

    let entries = manager.list_directory(".").await?;
    let dir_names: Vec<String> = entries
        .iter()
        .filter_map(|p| p.file_name())
        .map(|s| s.to_string())
        .collect();

    assert!(dir_names.contains(&"dir1".to_string()));
    assert!(dir_names.contains(&"dir2".to_string()));
    Ok(())
}

#[sinex_test]
async fn directory_manager_removes_directories() -> TestResult<()> {
    let temp_dir = TempDir::new().unwrap();
    let config = DirectoryConfig {
        base_path: Utf8PathBuf::from_path_buf(temp_dir.path().to_path_buf()).unwrap(),
        ..Default::default()
    };

    let manager = DirectoryManager::new(config);
    manager.create_directory("temp_dir").await?;
    assert!(manager.directory_exists("temp_dir").await?);

    manager.remove_directory("temp_dir").await?;
    assert!(!manager.directory_exists("temp_dir").await?);
    Ok(())
}

use sinex_terminal_node::shell_detection::{
    detect_capabilities, detect_shell_type, ShellType,
};
use sinex_test_utils::{sinex_test, TestResult};

#[sinex_test]
fn detects_common_shell_types() -> TestResult<()> {
    assert_eq!(detect_shell_type("/bin/bash"), ShellType::Bash);
    assert_eq!(detect_shell_type("/usr/bin/zsh"), ShellType::Zsh);
    assert_eq!(detect_shell_type("fish"), ShellType::Fish);
    assert_eq!(detect_shell_type("/usr/local/bin/nu"), ShellType::Nushell);
    assert_eq!(
        detect_shell_type("unknown"),
        ShellType::Unknown("unknown".to_string())
    );
    Ok(())
}

#[sinex_test]
fn capabilities_follow_shell_conventions() -> TestResult<()> {
    let bash_caps = detect_capabilities(&ShellType::Bash);
    assert!(bash_caps.supports_hooks);
    assert!(bash_caps.supports_functions);
    assert!(bash_caps.supports_aliases);

    let nushell_caps = detect_capabilities(&ShellType::Nushell);
    assert!(!nushell_caps.supports_hooks);
    assert!(nushell_caps.supports_functions);
    assert!(!nushell_caps.supports_aliases);
    Ok(())
}

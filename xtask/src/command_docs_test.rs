use super::*;
use crate::command_catalog::collect_command_catalog;
use crate::sandbox::sinex_test;

#[sinex_test]
async fn help_category_paths_exist() -> ::xtask::sandbox::TestResult<()> {
    let commands = collect_command_catalog();
    for category in HELP_CATEGORIES {
        for path in category.command_paths {
            assert!(
                find_command(&commands, path).is_some(),
                "missing help path: {path}"
            );
        }
    }
    Ok(())
}

#[sinex_test]
async fn guide_paths_exist() -> ::xtask::sandbox::TestResult<()> {
    let commands = collect_command_catalog();
    for section in GUIDE_SECTIONS {
        for entry in section.entries {
            assert!(
                find_command(&commands, entry.path).is_some(),
                "missing guide path: {}",
                entry.path
            );
        }
    }
    Ok(())
}

#[sinex_test]
async fn reference_renders_global_flags() -> ::xtask::sandbox::TestResult<()> {
    let rendered = render_command_reference(&[CommandInfo {
        name: "check".to_string(),
        about: Some("Compile verification".to_string()),
        args: vec![ArgInfo {
            name: "package".to_string(),
            short: Some('p'),
            long: Some("package".to_string()),
            help: Some("Check specific package(s) only".to_string()),
            required: false,
            global: false,
            possible_values: vec![],
            takes_value: true,
        }],
        subcommands: vec![],
    }]);

    assert!(rendered.contains("# xtask Command Reference"));
    assert!(rendered.contains("## Global Flags"));
    assert!(rendered.contains("## `xtask check`"));
    assert!(
        rendered.contains("| `-p, --package` | yes | no | Check specific package(s) only |")
    );
    Ok(())
}

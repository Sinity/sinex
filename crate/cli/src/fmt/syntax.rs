use color_eyre::Result;
use syntect::easy::HighlightLines;
use syntect::highlighting::{Style, ThemeSet};
use syntect::parsing::SyntaxSet;
use syntect::util::{LinesWithEndings, as_24_bit_terminal_escaped};

/// Syntax highlight JSON content
pub fn highlight_json(json: &str) -> Result<String> {
    highlight_code(json, "json")
}

/// Syntax highlight YAML content
pub fn highlight_yaml(yaml: &str) -> Result<String> {
    highlight_code(yaml, "yaml")
}

/// Syntax highlight code with the given extension
fn highlight_code(code: &str, extension: &str) -> Result<String> {
    let ps = SyntaxSet::load_defaults_newlines();
    let ts = ThemeSet::load_defaults();

    let syntax = ps
        .find_syntax_by_extension(extension)
        .ok_or_else(|| color_eyre::eyre::eyre!("No syntax found for {}", extension))?;

    // Use a theme that works well in terminals
    let theme = &ts.themes["base16-ocean.dark"];
    let mut h = HighlightLines::new(syntax, theme);

    let mut output = String::new();
    for line in LinesWithEndings::from(code) {
        let ranges: Vec<(Style, &str)> = h.highlight_line(line, &ps)?;
        let escaped = as_24_bit_terminal_escaped(&ranges[..], false);
        output.push_str(&escaped);
    }

    // Reset terminal colors at the end
    output.push_str("\x1b[0m");

    Ok(output)
}

/// Check if terminal supports color (for disabling syntax highlighting if needed)
pub fn terminal_supports_color() -> bool {
    // Check if stdout is a terminal
    atty::is(atty::Stream::Stdout)
}

#[cfg(test)]
mod tests {
    use super::*;
    use xtask::sandbox::prelude::*;

    #[sinex_test]
    fn test_highlight_json() -> TestResult<()> {
        let json = r#"{"name": "test", "count": 42}"#;
        let result = highlight_json(json);
        assert!(result.is_ok());
        // Output will have ANSI color codes
        let highlighted = result.unwrap();
        assert!(!highlighted.is_empty());
        Ok(())
    }

    #[sinex_test]
    fn test_highlight_yaml() -> TestResult<()> {
        let yaml = "name: test\ncount: 42\n";
        let result = highlight_yaml(yaml);
        assert!(result.is_ok());
        let highlighted = result.unwrap();
        assert!(!highlighted.is_empty());
        Ok(())
    }

    #[sinex_test]
    fn test_invalid_extension() -> TestResult<()> {
        let result = highlight_code("test", "invalid_ext");
        // syntect returns error for unknown extensions
        assert!(result.is_err());
        Ok(())
    }

    #[sinex_test]
    fn test_empty_input() -> TestResult<()> {
        let result = highlight_json("");
        assert!(result.is_ok());
        Ok(())
    }
}

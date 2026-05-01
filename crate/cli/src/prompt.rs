use std::io::{self, Write};

use color_eyre::eyre::{Result, eyre};

pub fn select(prompt: &str, options: &[&str], default_index: usize) -> Result<String> {
    if options.is_empty() {
        return Err(eyre!("prompt `{prompt}` has no options"));
    }
    let default = default_index.min(options.len() - 1);

    println!("{prompt}");
    for (idx, option) in options.iter().enumerate() {
        let marker = if idx == default { "*" } else { " " };
        println!("  {marker} {}. {option}", idx + 1);
    }

    loop {
        let answer = read_line(&format!("Choice [{}]: ", default + 1))?;
        if answer.is_empty() {
            return Ok(options[default].to_string());
        }
        if let Ok(choice) = answer.parse::<usize>()
            && (1..=options.len()).contains(&choice)
        {
            return Ok(options[choice - 1].to_string());
        }
        eprintln!("Enter a number from 1 to {}.", options.len());
    }
}

pub fn text(prompt: &str, default: Option<&str>, help: Option<&str>) -> Result<String> {
    if let Some(help) = help {
        println!("{help}");
    }
    let suffix = default.map_or(String::new(), |value| format!(" [{value}]"));
    let answer = read_line(&format!("{prompt}{suffix}: "))?;
    if answer.is_empty() {
        Ok(default.unwrap_or_default().to_string())
    } else {
        Ok(answer)
    }
}

pub fn optional_text(prompt: &str, help: Option<&str>) -> Result<Option<String>> {
    let answer = text(prompt, None, help)?;
    Ok((!answer.is_empty()).then_some(answer))
}

pub fn confirm(prompt: &str, default: bool) -> Result<bool> {
    let default_label = if default { "Y/n" } else { "y/N" };
    loop {
        let answer = read_line(&format!("{prompt} [{default_label}]: "))?;
        match answer.to_ascii_lowercase().as_str() {
            "" => return Ok(default),
            "y" | "yes" => return Ok(true),
            "n" | "no" => return Ok(false),
            _ => eprintln!("Enter yes or no."),
        }
    }
}

fn read_line(prompt: &str) -> Result<String> {
    print!("{prompt}");
    io::stdout().flush()?;

    let mut input = String::new();
    io::stdin().read_line(&mut input)?;
    Ok(input.trim().to_string())
}

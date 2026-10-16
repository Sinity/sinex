mod boundaries;
use std::process::{Command, ExitCode};
fn main() -> ExitCode {
    let mut args = std::env::args().skip(1);
    let cmd = args.next().unwrap_or_else(|| "help".into());
    let tail: Vec<_> = args.collect();
    let features = if matches!(cmd.as_str(), "build" | "test" | "check") {
        match boundaries::routing_features(&tail) {
            Ok(features) => features,
            Err(error) => {
                eprintln!("{error}");
                return ExitCode::from(2);
            }
        }
    } else {
        Vec::new()
    };
    if matches!(cmd.as_str(), "boundaries" | "build" | "test" | "check") {
        if cmd == "boundaries" && tail.len() > 1 {
            eprintln!("xtask boundaries [workspace]");
            return ExitCode::from(2);
        }
        let root = if cmd == "boundaries" {
            tail.first().map(String::as_str).unwrap_or(".")
        } else {
            "."
        };
        if let Err(error) = boundaries::check(std::path::Path::new(root), &features) {
            eprintln!("{error}");
            return ExitCode::FAILURE;
        }
        if cmd == "boundaries" {
            println!("workspace dependency boundaries passed");
            return ExitCode::SUCCESS;
        }
    }
    if cmd == "demo" {
        if tail.len() < 2 {
            eprintln!(
                "xtask demo <workspace> <spec-repository> [--controls] [--boundaries] [--provider-loss]"
            );
            return ExitCode::from(2);
        }
        let script = std::path::Path::new(&tail[0]).join("tests/demo.py");
        if !script.is_file() {
            eprintln!("workspace does not contain tests/demo.py: {}", tail[0]);
            return ExitCode::from(2);
        }
        return match Command::new("python3")
            .arg(script)
            .args(&tail[1..])
            .status()
        {
            Ok(status) => ExitCode::from(status.code().unwrap_or(1) as u8),
            Err(error) => {
                eprintln!("could not execute the demonstration driver: {error}");
                ExitCode::FAILURE
            }
        };
    }
    let cargo_args: Vec<&str> = match cmd.as_str() {
        "build" => vec!["build", "--workspace"],
        "test" => vec!["test", "--workspace"],
        "check" => vec!["check", "--workspace"],
        "fmt" => vec![
            "fmt",
            "--package",
            "sinex-model",
            "--package",
            "sinex-protocol",
            "--package",
            "sinex-store",
            "--package",
            "sinexd",
            "--package",
            "sinexctl",
            "--package",
            "xtask",
            "--",
            "--check",
        ],
        "help" => {
            println!(
                "xtask build|test|check|fmt [cargo arguments]; xtask boundaries [workspace]; xtask demo <workspace> <spec-repository> [--controls] [--boundaries] [--provider-loss]; runtime builds require an isolated development schema"
            );
            return ExitCode::SUCCESS;
        }
        _ => {
            eprintln!("unsupported xtask operation: {cmd}");
            return ExitCode::from(2);
        }
    };
    match Command::new("cargo").args(cargo_args).args(tail).status() {
        Ok(s) => ExitCode::from(s.code().unwrap_or(1) as u8),
        Err(e) => {
            eprintln!("could not execute cargo: {e}");
            ExitCode::FAILURE
        }
    }
}

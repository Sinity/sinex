use std::process::{Command, ExitCode};
fn main() -> ExitCode {
    let mut args = std::env::args().skip(1);
    let cmd = args.next().unwrap_or_else(|| "help".into());
    let tail: Vec<_> = args.collect();
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
                "xtask build|test|check|fmt [cargo arguments]; requires the isolated development schema for runtime builds"
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

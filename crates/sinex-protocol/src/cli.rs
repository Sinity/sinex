//! Command-line parsers built from the command table, so `sinexctl` (through
//! the daemon) and `sinexd admin` (locally, without one) offer the same
//! commands with the same arguments.
use crate::{ArgKind, COMMANDS, Command};
use clap::{Arg, ArgAction, ArgMatches};
use serde_json::{Map, Value};

/// One table command as a clap subcommand.
pub fn subcommand(c: &Command) -> clap::Command {
    let mut sub = clap::Command::new(c.name).about(c.summary);
    for a in c.args {
        let mut arg = Arg::new(a.name).help(a.help).required(a.required);
        arg = match (a.kind, a.positional) {
            (ArgKind::Rest, _) => arg
                .num_args(1..)
                .trailing_var_arg(true)
                .allow_hyphen_values(true),
            (ArgKind::Bool, _) => arg.long(a.name).action(ArgAction::SetTrue).required(false),
            (ArgKind::List, _) => arg.long(a.name).action(ArgAction::Append),
            (ArgKind::Integer, false) => arg.long(a.name).value_parser(clap::value_parser!(u64)),
            (_, false) => arg.long(a.name),
            (_, true) => arg,
        };
        sub = sub.arg(arg);
    }
    sub
}

/// Every table command as a clap subcommand.
pub fn subcommands() -> impl Iterator<Item = clap::Command> {
    COMMANDS.iter().map(subcommand)
}

/// A command's arguments as the JSON object its request is built from.
pub fn arguments(spec: &Command, m: &ArgMatches) -> Map<String, Value> {
    let mut out = Map::new();
    for a in spec.args {
        let value = match a.kind {
            ArgKind::Rest => m.get_many::<String>(a.name).map(|words| {
                let words: Vec<String> = words
                    .map(|w| {
                        // For a search, one argument with spaces is a phrase.
                        if spec.name == "search" && w.contains(char::is_whitespace) {
                            serde_json::to_string(w).expect("strings serialize")
                        } else {
                            w.clone()
                        }
                    })
                    .collect();
                Value::String(words.join(" "))
            }),
            ArgKind::Bool => m.get_flag(a.name).then_some(Value::Bool(true)),
            ArgKind::Integer => m.get_one::<u64>(a.name).map(|n| Value::from(*n)),
            ArgKind::List => m
                .get_many::<String>(a.name)
                .map(|values| Value::Array(values.cloned().map(Value::String).collect())),
            ArgKind::Text | ArgKind::Reference => m
                .get_one::<String>(a.name)
                .map(|s| Value::String(s.clone())),
        };
        if let Some(v) = value {
            out.insert(a.name.to_string(), v);
        }
    }
    out
}

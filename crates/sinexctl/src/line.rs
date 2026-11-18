//! Selection lines use the declared steps; query and reference interpretation
//! remains with the daemon. Existing commands retain their owning handlers.
use command_surface::{
    Failure, FailureKind, Format, RunnableRequest, TokenBudget,
    line::{FirstWord, SelectionRules, SplitError},
};
use serde_json::Value;
use sinex_protocol::rows::{Rows, Step};
use std::convert::Infallible;

#[derive(Default)]
struct Options {
    endpoint: Option<String>,
    token_file: Option<String>,
    json: bool,
    has_request_options: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Stdin {
    Unused,
    References,
    Request,
}

pub(crate) struct Line {
    request: RunnableRequest,
    options: Options,
    stdin: Stdin,
    whole_sql: bool,
}

struct Rules;
impl SelectionRules for Rules {
    type Additional = Infallible;
    fn is_command(&self, word: &str) -> bool {
        sinex_protocol::command(word).is_some()
            || matches!(
                word,
                "mcp" | "doctor" | "store" | "dev" | "help" | "--help" | "-h" | "--version" | "-V"
            )
    }
    // These distinctions do not change a rows request. The daemon owns its
    // saved-query expansion, catalog, query parser and reference resolver.
    fn saved_name(&self, _: &str) -> Option<String> {
        None
    }
    fn is_unit(&self, _: &str) -> bool {
        false
    }
    fn is_row_numbers(&self, _: &str) -> bool {
        false
    }
    fn is_reference(&self, _: &str) -> bool {
        false
    }
    fn additional(&self, _: &str) -> Option<Infallible> {
        None
    }
    fn is_query(&self, _: &str) -> bool {
        true
    }
}

fn bad(message: impl Into<String>) -> Failure {
    Failure::new(FailureKind::BadRequest, message)
}

fn value(args: &[String], index: &mut usize) -> Result<String, Failure> {
    *index += 1;
    args.get(*index)
        .cloned()
        .ok_or_else(|| bad(format!("{} needs a value", args[*index - 1])))
}

fn option(
    args: &[String],
    index: &mut usize,
    request: &mut RunnableRequest,
    options: &mut Options,
) -> Result<bool, Failure> {
    let (name, inline) = args[*index]
        .split_once('=')
        .map_or((args[*index].as_str(), None), |(name, value)| {
            (name, Some(value.to_owned()))
        });
    let mut argument = || inline.clone().map(Ok).unwrap_or_else(|| value(args, index));
    match name {
        "--endpoint" => options.endpoint = Some(argument()?),
        "--token-file" => options.token_file = Some(argument()?),
        "--json" if inline.is_none() => {
            options.json = true;
            request.format = Format::Json;
        }
        "--json" => return Err(bad("--json takes no value")),
        "--request-id" => {
            request.request_id = argument()?;
            options.has_request_options = true;
        }
        "--deadline" => {
            request.deadline = sinex_protocol::deadline(&argument()?)?;
            options.has_request_options = true;
        }
        "--budget" => {
            let word = argument()?;
            request.budget_tokens = if word == "none" {
                TokenBudget::Unlimited
            } else {
                TokenBudget::Tokens(
                    word.parse()
                        .map_err(|_| bad("budget must be a number of tokens or none"))?,
                )
            };
            options.has_request_options = true;
        }
        _ => return Ok(false),
    }
    Ok(true)
}

fn listing(
    args: &[String],
    index: &mut usize,
    request: &mut RunnableRequest,
    rows: &mut Rows,
) -> Result<bool, Failure> {
    let (name, inline) = args[*index]
        .split_once('=')
        .map_or((args[*index].as_str(), None), |(name, value)| {
            (name, Some(value.to_owned()))
        });
    let mut argument = || inline.clone().map(Ok).unwrap_or_else(|| value(args, index));
    match name {
        "--page-size" => {
            let size = argument()?
                .parse::<u64>()
                .map_err(|_| bad("page-size needs a positive integer"))?;
            if size == 0 {
                return Err(bad("page-size needs a positive integer"));
            }
            rows.page_size = Some(size);
        }
        "--cursor" => request.cursor = Some(argument()?),
        _ => return Ok(false),
    }
    Ok(true)
}

/// None selects the existing command parser, with the original argv intact.
/// Parsing never opens stdin, a token file or a connection.
pub(crate) fn parse(args: &[String]) -> Result<Option<Line>, (Failure, bool)> {
    let mut options = Options::default();
    parse_input(args, &mut options).map_err(|error| (error, options.json))
}

fn command_start(args: &[String]) -> (usize, bool) {
    let mut index = 0;
    let mut json = false;
    while index < args.len() {
        let (name, inline) = args[index]
            .split_once('=')
            .map_or((args[index].as_str(), false), |(name, _)| (name, true));
        match name {
            "--endpoint" | "--token-file" | "--deadline" | "--request-id" | "--budget"
            | "--cursor" => index += if inline { 1 } else { 2 },
            "--json" => {
                json = true;
                index += 1;
            }
            "--all" => index += 1,
            _ => break,
        }
    }
    (index, json)
}

pub(crate) fn help(args: &[String]) -> Option<String> {
    let (index, _) = command_start(args);
    let words: Vec<_> = args.get(index..)?.iter().map(String::as_str).collect();
    matches!(
        words.as_slice(),
        ["dev", "sql", "--help" | "-h"] | ["help", "dev", "sql"] | ["dev", "--help" | "-h"]
    )
    .then(sql_help)
}

pub(crate) fn sql_help() -> String {
    sinex_commands::declarations()
        .help_for("sql")
        .expect("SQL has declared help")
        .replacen("sinex ", "sinexctl ", 1)
}

fn parse_input(args: &[String], options: &mut Options) -> Result<Option<Line>, Failure> {
    // The original command parser still owns these commands and their flags.
    // In particular its --all means pagination, unlike selection-line --all.
    let (command, json) = command_start(args);
    if args.get(command).is_some_and(|word| word == "dev") {
        options.json = json;
        let parsed = sinex_commands::declarations()
            .parse_cli(args, sinex_model::OperationId::new().to_string())?;
        options.json = parsed.json;
        options.endpoint = parsed
            .calling_process
            .get("endpoint")
            .and_then(Value::as_str)
            .map(str::to_owned);
        options.token_file = parsed
            .calling_process
            .get("token_file")
            .and_then(Value::as_str)
            .map(str::to_owned);
        return Ok(Some(Line {
            request: parsed.request,
            options: std::mem::take(options),
            stdin: Stdin::Unused,
            whole_sql: true,
        }));
    }
    if command >= args.len()
        || command_surface::line::classify_first(
            args.get(command).map(String::as_str),
            &Rules,
            sinex_commands::steps(),
        ) == FirstWord::Command
    {
        return Ok(None);
    }
    let mut request = RunnableRequest::new(
        "rows",
        Value::Null,
        sinex_model::OperationId::new().to_string(),
    );
    let mut rows = Rows::default();
    let mut index = 0;
    while index < args.len() {
        if !option(args, &mut index, &mut request, options)? {
            if !listing(args, &mut index, &mut request, &mut rows)? {
                break;
            }
            options.has_request_options = true;
        }
        index += 1;
    }
    if index == args.len() {
        return Ok(None);
    }
    if args[index] == "--request" || args[index].starts_with("--request=") {
        if options.has_request_options {
            return Err(bad(
                "put request metadata inside the complete --request value",
            ));
        }
        let json = match args[index].strip_prefix("--request=") {
            Some(value) => value.to_owned(),
            None => value(args, &mut index)?,
        };
        index += 1;
        if index != args.len() {
            return Err(bad("--request takes one complete typed request"));
        }
        let stdin_request = json == "-";
        if !stdin_request {
            request =
                serde_json::from_str(&json).map_err(|error| bad(format!("request: {error}")))?;
        }
        return Ok(Some(Line {
            request,
            options: std::mem::take(options),
            whole_sql: false,
            stdin: if stdin_request {
                Stdin::Request
            } else {
                Stdin::Unused
            },
        }));
    }
    let literal = args[index] == "--";
    if literal {
        index += 1;
        if index == args.len() {
            return Err(bad("-- needs a selection"));
        }
    }
    if !literal && args[index].starts_with('-') && args[index] != "-" {
        return Err(bad(format!("unknown selection option {}", args[index])));
    }
    let first = if literal {
        FirstWord::QueryText
    } else {
        command_surface::line::classify_first(
            args.get(index).map(String::as_str),
            &Rules,
            sinex_commands::steps(),
        )
    };
    if first == FirstWord::Command {
        return Ok(None);
    }
    let stdin = if first == FirstWord::Stdin {
        Stdin::References
    } else {
        Stdin::Unused
    };
    if first != FirstWord::Verb {
        if stdin != Stdin::References {
            rows.selection = Some(args[index].clone());
        }
        index += 1;
    }
    while index < args.len() {
        if option(args, &mut index, &mut request, options)? {
            index += 1;
            continue;
        }
        if rows.steps.is_empty() && listing(args, &mut index, &mut request, &mut rows)? {
            index += 1;
            continue;
        }
        let split = sinex_commands::steps()
            .split_step(args, index, |_, args, index| {
                option(args, index, &mut request, options)
            })
            .map_err(|error| match error {
                SplitError::Arguments(error) | SplitError::Local(error) => error,
                SplitError::UnknownVerb { index } => bad(format!(
                    "{:?} is not a verb; a query is one shell argument",
                    args[index]
                )),
                SplitError::UnknownOption { index } => {
                    bad(format!("unknown step option {}", args[index]))
                }
                SplitError::MissingValue { index } => bad(format!("{} needs a value", args[index])),
                SplitError::MissingArgument { name, .. } => bad(format!("missing {name}")),
            })?;
        index = split.end;
        rows.steps.push(
            serde_json::from_value::<Step>(split.step).map_err(|error| bad(error.to_string()))?,
        );
    }
    if stdin == Stdin::References {
        rows.references = Some(Vec::new());
    }
    request.arguments = serde_json::to_value(rows).expect("rows serialize");
    Ok(Some(Line {
        request,
        options: std::mem::take(options),
        stdin,
        whole_sql: false,
    }))
}

/// Structured follow-ups retain every request option and never repeat writes.
fn validate(request: &RunnableRequest) -> Result<(), Failure> {
    if request.request_id.is_empty() {
        return Err(bad("request_id must not be empty"));
    }
    if request.operation == "sql" {
        serde_json::from_value::<sinex_protocol::sql::Sql>(request.arguments.clone())
            .map_err(|error| bad(error.to_string()))?;
        return Ok(());
    }
    if request.operation != "rows" {
        return Err(bad("this request form takes the rows or sql operation"));
    }
    let rows: Rows = serde_json::from_value(request.arguments.clone())
        .map_err(|error| bad(error.to_string()))?;
    rows.validate()
}

pub(crate) async fn run(mut line: Line) -> std::process::ExitCode {
    use tokio::io::AsyncReadExt;
    let prepared: Result<(), Failure> = async {
        if line.stdin != Stdin::Unused {
            let mut input = String::new();
            tokio::io::stdin()
                .read_to_string(&mut input)
                .await
                .map_err(|error| Failure::new(FailureKind::Io, error.to_string()))?;
            if line.stdin == Stdin::Request {
                line.request = serde_json::from_str(&input)
                    .map_err(|error| bad(format!("request: {error}")))?;
            } else {
                let references: Vec<_> = input
                    .lines()
                    .map(str::trim)
                    .filter(|line| !line.is_empty())
                    .collect();
                line.request.arguments["references"] =
                    serde_json::to_value(references).expect("references serialize");
            }
        }
        validate(&line.request)
    }
    .await;
    line.options.json |= line.request.format == Format::Json;
    if let Err(error) = prepared {
        return failure(&error, line.options.json);
    }
    let client = match super::client_options(
        line.options.endpoint.clone(),
        line.options.token_file.clone(),
        line.request.deadline,
    ) {
        Ok(client) => client,
        Err(error) => return failure(&error, line.options.json),
    };
    if line.request.operation == "sql" {
        if line.whole_sql {
            return super::sql::whole(&client, line.request, line.options.json).await;
        }
        let answer = match client
            .surface::<sinex_protocol::sql::SqlReply>(&line.request)
            .await
        {
            Ok(answer) => answer,
            Err(error) => command_surface::Answer::error(error.failure("sql")),
        };
        let exit = answer.exit_code();
        return print_answer(&line, &answer, exit);
    }
    let answer = match client
        .surface::<sinex_protocol::rows::RowsReply>(&line.request)
        .await
    {
        Ok(answer) => answer,
        Err(error) => command_surface::Answer::error(error.failure("rows")),
    };
    let exit = answer
        .value()
        .and_then(|rows| rows.failure())
        .map_or_else(|| answer.exit_code(), |error| error.kind.exit_code());
    print_answer(&line, &answer, exit)
}

fn print_answer<T: serde::Serialize>(
    line: &Line,
    answer: &command_surface::Answer<T>,
    exit: u8,
) -> std::process::ExitCode {
    // The complete typed envelope keeps receipts, partial results and ready
    // requests available to scripts as well as a person reading the JSON.
    let value = serde_json::to_value(&answer).expect("answer serializes");
    super::print_json(&value);
    if !line.options.json {
        let mut program = "sinexctl".to_string();
        for (flag, value) in [
            ("--endpoint", line.options.endpoint.as_ref()),
            ("--token-file", line.options.token_file.as_ref()),
        ] {
            if let Some(value) = value {
                program.push_str(&format!(
                    " {flag} {}",
                    command_surface::quote_argument(value)
                ));
            }
        }
        for next in &answer.envelope.next {
            let request = serde_json::to_string(&next.request).expect("request serializes");
            let label = serde_json::to_string(&next.label).expect("label serializes");
            super::output::emit(&format!(
                "next: {label}: {program} --request {}\n",
                command_surface::quote_argument(&request)
            ));
        }
    }
    std::process::ExitCode::from(exit)
}

/// Selection lines report the same typed failure envelope as their HTTP owner.
pub(crate) fn failure(error: &Failure, json: bool) -> std::process::ExitCode {
    if json {
        super::print_json(
            &serde_json::to_value(command_surface::Answer::<Value>::error(error.clone()))
                .expect("failure serializes"),
        );
        std::process::ExitCode::from(error.kind.exit_code())
    } else {
        super::fail(error, false)
    }
}

#[cfg(test)]
mod tests;

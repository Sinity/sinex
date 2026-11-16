use super::*;
use sinex_protocol::{Request, cli};

fn words(line: &str) -> Vec<String> {
    let mut words = Vec::new();
    let mut word = String::new();
    let mut quote = None;
    let mut escaped = false;
    let mut started = false;
    for character in line.chars() {
        if escaped {
            word.push(character);
            escaped = false;
        } else if quote == Some('\'') {
            if character == '\'' {
                quote = None;
            } else {
                word.push(character);
            }
        } else if character == '\\' {
            escaped = true;
            started = true;
        } else if let Some(delimiter) = quote {
            if character == delimiter {
                quote = None;
            } else {
                word.push(character);
            }
        } else if matches!(character, '\'' | '"') {
            quote = Some(character);
            started = true;
        } else if character.is_whitespace() {
            if started {
                words.push(std::mem::take(&mut word));
                started = false;
            }
        } else {
            word.push(character);
            started = true;
        }
    }
    assert!(quote.is_none() && !escaped, "unfinished quoting: {line}");
    if started {
        words.push(word);
    }
    words
}

#[devkit::test]
async fn help_examples_run_on_fixture(db: devkit::pg::TestDatabase) {
    use age::secrecy::ExposeSecret;
    let directory = tempfile::tempdir().unwrap();
    let root = std::fs::canonicalize(directory.path()).unwrap();
    let image = root.join("example.png");
    let history = root.join("history.zsh");
    let sidecar = root.join("example.png.json");
    let identity_file = root.join("identity.age");
    let png: &[u8] = &[
        0x89, 0x50, 0x4e, 0x47, 0x0d, 0x0a, 0x1a, 0x0a, 0, 0, 0, 0x0d, 0x49, 0x48, 0x44, 0x52, 0,
        0, 0, 1, 0, 0, 0, 1, 8, 2, 0, 0, 0, 0x90, 0x77, 0x53, 0xde, 0, 0, 0, 0x0c, 0x49, 0x44,
        0x41, 0x54, 0x78, 0x9c, 0x63, 0xf8, 0xcf, 0xc0, 0, 0, 3, 1, 1, 0, 0xc9, 0xfe, 0x92, 0xef,
        0, 0, 0, 0, 0x49, 0x45, 0x4e, 0x44, 0xae, 0x42, 0x60, 0x82,
    ];
    std::fs::write(&image, png).unwrap();
    std::fs::write(&history, ": 1600000000:0;synthetic staged command\n").unwrap();
    std::fs::write(
        &sidecar,
        json!({"category":"danbooru","id":7,"tags":["red","square"]}).to_string(),
    )
    .unwrap();
    let identity = sinex_sealing::Identity::generate();
    std::fs::write(&identity_file, identity.to_string().expose_secret()).unwrap();
    let world = world(db).await;
    let (_, added) = call_unbounded(
        &world,
        Some(OPERATOR),
        json!({"method":"library-add","path":image}),
    )
    .await;
    assert_eq!(added["outcome"], "ok", "{added}");
    let copies = sinex_library::reads::copies_at(
        &mut world.reads.acquire().await.unwrap(),
        &[format!("h:{}", image.display())],
    )
    .await
    .unwrap();
    let item = copies[0].item.clone();
    admit(&world, &["synthetic example history"], 0).await;
    let (_, archived) = call_unbounded(
        &world,
        Some(OPERATOR),
        json!({"method":"archive","source":"terminal.atuin"}),
    )
    .await;
    assert_eq!(archived["outcome"], "ok", "{archived}");
    let operation = archived["result"]["operation"].as_str().unwrap();
    let mut failures = Vec::new();
    for command in sinex_protocol::COMMANDS {
        for example in command.examples {
            let mut words = words(example);
            assert_eq!(words[0], "sinexctl", "{example}");
            assert_eq!(words[1], command.name, "{example}");
            for word in &mut words {
                *word = match word.as_str() {
                    "/synthetic/example.png" => image.display().to_string(),
                    "/synthetic/history.zsh" => history.display().to_string(),
                    "/synthetic/library-sidecar.json" => sidecar.display().to_string(),
                    "/synthetic/identity.age" => identity_file.display().to_string(),
                    "library:example" => item.clone(),
                    "00000000-0000-7000-8000-000000000001" if command.name == "undo" => {
                        operation.to_owned()
                    }
                    _ => word.clone(),
                };
            }
            let matches = match cli::subcommand(command).try_get_matches_from(&words[1..]) {
                Ok(matches) => matches,
                Err(error) => {
                    failures.push(format!("{example}: {error}"));
                    continue;
                }
            };
            let mut request =
                match Request::from_args(command.name, cli::arguments(command, &matches)) {
                    Ok(request) => request,
                    Err(error) => {
                        failures.push(format!("{example}: {error}"));
                        continue;
                    }
                };
            // The CLI reads the explicitly named identity file before sending the request.
            if let Request::Unlock { identity, .. } = &mut request {
                *identity = std::fs::read_to_string(&*identity).unwrap();
            }
            if let Request::Follow { query } = request {
                let query = percent_encoding::utf8_percent_encode(
                    &query,
                    percent_encoding::NON_ALPHANUMERIC,
                );
                let request = HttpRequest::get(format!("/v1/follow?deadline=none&query={query}"))
                    .header("authorization", format!("Bearer {OPERATOR}"))
                    .body(Body::empty())
                    .unwrap();
                let response = world.app.clone().oneshot(request).await.unwrap();
                assert_eq!(response.status(), StatusCode::OK, "{example}");
                let mut body = response.into_body();
                let mut seen = String::new();
                read_until(&mut body, &mut seen, "event: notice").await;
            } else {
                let (_, body) = call_unbounded(
                    &world,
                    Some(OPERATOR),
                    serde_json::to_value(request).unwrap(),
                )
                .await;
                let reply: sinex_protocol::Reply = serde_json::from_value(body.clone()).unwrap();
                if reply
                    .failure()
                    .is_some_and(|failure| failure.kind.http_status() == Some(400))
                {
                    failures.push(format!("{example}: {body}"));
                }
            }
        }
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

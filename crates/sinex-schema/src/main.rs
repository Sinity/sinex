//! `sinex-schema apply <database-url>`: create the schema in an empty database
//! (or confirm it is current). Development tooling runs this to provision the
//! database the store's compile-time SQL checks against.
#[tokio::main(flavor = "current_thread")]
async fn main() -> std::process::ExitCode {
    let mut args: Vec<String> = std::env::args().skip(1).collect();
    let mut owners = sinex_schema::kinds::Owners::default();
    while let Some(at) = args.iter().position(|a| a == "--kind-owner") {
        if at + 1 >= args.len() {
            eprintln!("--kind-owner needs PREFIX=PRODUCER");
            return std::process::ExitCode::from(2);
        }
        let mapping = args.remove(at + 1);
        args.remove(at);
        let Some((prefix, producer)) = mapping.split_once('=') else {
            eprintln!("--kind-owner needs PREFIX=PRODUCER");
            return std::process::ExitCode::from(2);
        };
        if prefix != "polylogue"
            || producer.is_empty()
            || !producer
                .chars()
                .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_' || c == '-')
        {
            eprintln!("invalid kind owner");
            return std::process::ExitCode::from(2);
        }
        owners.prefixes.insert(prefix.into(), producer.into());
    }
    let url = match args.as_slice() {
        [cmd, url] if cmd == "apply" => url.clone(),
        [cmd] if cmd == "apply" => match std::env::var("DATABASE_URL") {
            Ok(url) => url,
            Err(_) => {
                eprintln!("sinex-schema apply: give a database URL or set DATABASE_URL");
                return std::process::ExitCode::from(2);
            }
        },
        [cmd] if cmd == "hash" => {
            println!("{}", sinex_schema::declaration_hash());
            return std::process::ExitCode::SUCCESS;
        }
        _ => {
            eprintln!("usage: sinex-schema apply [DATABASE_URL] | sinex-schema hash");
            return std::process::ExitCode::from(2);
        }
    };
    use sqlx::Connection;
    let result = async {
        let mut connection = sqlx::PgConnection::connect(&url).await?;
        sinex_schema::apply_with_owners(&mut connection, &owners).await
    }
    .await;
    match result {
        Ok(outcome) => {
            println!("{outcome}");
            std::process::ExitCode::SUCCESS
        }
        Err(e) => {
            eprintln!("sinex-schema: {e}");
            std::process::ExitCode::FAILURE
        }
    }
}

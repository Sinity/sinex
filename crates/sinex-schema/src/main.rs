//! `sinex-schema apply <database-url>`: create the schema in an empty database
//! (or confirm it is current). Development tooling runs this to provision the
//! database the store's compile-time SQL checks against.
#[tokio::main(flavor = "current_thread")]
async fn main() -> std::process::ExitCode {
    let args: Vec<String> = std::env::args().skip(1).collect();
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
    match sinex_schema::apply_url(&url).await {
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

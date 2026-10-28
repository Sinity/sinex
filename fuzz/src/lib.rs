//! Properties shared by fuzz experiments and deterministic regressions.
pub fn manifest_roundtrip(bytes: &[u8]) {
    if let Ok(manifest) = serde_json::from_slice::<sinex_model::Manifest>(bytes) {
        let encoded = serde_json::to_vec(&manifest).expect("manifest serializes");
        let decoded: sinex_model::Manifest =
            serde_json::from_slice(&encoded).expect("manifest decodes");
        assert_eq!(manifest, decoded);
    }
}
pub fn producer_roundtrip(bytes: &[u8]) {
    if let Ok(frame) = serde_json::from_slice::<sinex_protocol::nats::Ingress>(bytes) {
        let encoded = serde_json::to_vec(&frame).expect("producer frame serializes");
        let decoded: sinex_protocol::nats::Ingress =
            serde_json::from_slice(&encoded).expect("producer frame decodes");
        assert_eq!(frame, decoded);
    }
}
pub fn query(bytes: &[u8]) {
    if let Ok(text) = std::str::from_utf8(bytes) {
        let context = sinex_model::query::Context::utc(jiff::Timestamp::UNIX_EPOCH);
        if let Ok(bound) = sinex_model::query::bind(text, &context) {
            let sentence = sinex_model::query::read_as(&bound);
            assert!(!sentence.is_empty());
        }
        let _ = sinex_model::query::parse_reference(text);
    }
}
/// Compare the admission encoder with PostgreSQL's COPY reader, including
/// PostgreSQL's refusal of NUL in text values.
pub fn copy_escape(bytes: &[u8]) {
    use sqlx::{Connection, Executor};
    use std::sync::{Mutex, OnceLock};
    static DATABASE: OnceLock<Mutex<(tokio::runtime::Runtime, sqlx::PgConnection)>> =
        OnceLock::new();
    let Ok(text) = std::str::from_utf8(bytes) else {
        return;
    };
    let database = DATABASE.get_or_init(|| {
        let runtime = tokio::runtime::Runtime::new().unwrap();
        let connection = runtime.block_on(async {
            let url =
                std::env::var("DATABASE_URL").expect("COPY fuzzing requires the xtask database");
            let mut connection = sqlx::PgConnection::connect(&url).await.unwrap();
            connection
                .execute("CREATE TEMP TABLE copy_input (value text)")
                .await
                .unwrap();
            connection
        });
        Mutex::new((runtime, connection))
    });
    let mut database = database.lock().unwrap();
    let (runtime, connection) = &mut *database;
    runtime.block_on(copy_escape_in(connection, text));
}

async fn copy_escape_in(connection: &mut sqlx::PgConnection, text: &str) {
    use sqlx::Executor;
    connection.execute("TRUNCATE copy_input").await.unwrap();
    let mut encoded = String::new();
    sinex_store::copy_text::escape(&mut encoded, text);
    encoded.push('\n');
    let mut copy = connection
        .copy_in_raw("COPY copy_input FROM STDIN")
        .await
        .unwrap();
    copy.send(encoded.into_bytes()).await.unwrap();
    let result = copy.finish().await;
    if text.contains('\0') {
        let error = result.expect_err("PostgreSQL text refuses NUL");
        assert_eq!(
            error.as_database_error().and_then(|e| e.code()).as_deref(),
            Some("22021")
        );
    } else {
        assert_eq!(result.unwrap(), 1);
        let values: Vec<String> = sqlx::query_scalar("SELECT value FROM copy_input")
            .fetch_all(connection)
            .await
            .unwrap();
        assert_eq!(values, [text]);
    }
}

#[cfg(test)]
mod tests;

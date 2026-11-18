//! The SQL command prints its complete result; exact ready requests read one page.
use crate::client::Client;
use command_surface::RunnableRequest;
use sinex_protocol::sql::SqlReply;
use std::process::ExitCode;

pub(crate) async fn whole(client: &Client, mut request: RunnableRequest, json: bool) -> ExitCode {
    let mut rows = Vec::new();
    loop {
        let answer = match client.surface::<SqlReply>(&request).await {
            Ok(answer) => answer,
            Err(error) => return crate::fail(&error.failure("sql"), json),
        };
        if let Some(failure) = answer.failure() {
            return crate::fail(failure, json);
        }
        let page = answer.value().expect("successful SQL answer has its page");
        rows.extend(page.rows.iter().cloned());
        if let Some(cursor) = &page.page.cursor {
            let Some(next) = answer.envelope.next.first().filter(|next| {
                next.request.operation == "sql"
                    && next.request.arguments == request.arguments
                    && next.request.cursor.as_ref() == Some(cursor)
            }) else {
                return crate::fail(
                    &crate::client::protocol_mismatch(
                        "sql",
                        "SQL page has no matching read continuation",
                    ),
                    json,
                );
            };
            request = next.request.clone();
            continue;
        }
        if json {
            crate::print_json(&serde_json::json!({"count":rows.len(),"rows":rows}));
        } else {
            let columns: Vec<String> = rows
                .first()
                .and_then(serde_json::Value::as_object)
                .map(|row| row.keys().cloned().collect())
                .unwrap_or_default();
            crate::output::emit(&crate::output::table(
                &columns,
                &rows,
                &crate::output::Local::new(None),
            ));
            crate::output::emit(&format!("{} rows; times in UTC\n", rows.len()));
        }
        return crate::answered(rows.is_empty());
    }
}

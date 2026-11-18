//! Shared operation handlers. Product authentication supplies the caller;
//! the common table owns request metadata, schema validation and dispatch.
use super::*;
use command_surface::{Answer as SurfaceAnswer, Operation, OperationFuture, RequestContext};
use sinex_protocol::surface::Status;
use std::sync::Arc;

pub(crate) fn table(api: Api) -> command_surface::OperationTable {
    let mut table =
        command_surface::OperationTable::new(Arc::clone(sinex_protocol::surface::declarations()));
    table
        .bind("status", StatusOperation(api))
        .expect("status binds its declared request and reply");
    table
}

struct StatusOperation(Api);

impl Operation for StatusOperation {
    type Request = Status;
    type Response = Value;

    fn execute(
        &self,
        _: Status,
        context: RequestContext,
    ) -> OperationFuture<'_, SurfaceAnswer<Value>> {
        Box::pin(async move {
            let Some(caller) = context.caller().identity::<Caller>() else {
                return SurfaceAnswer::error(command_surface::Failure::new(
                    command_surface::FailureKind::Refused,
                    "status needs an authenticated Sinex caller",
                ));
            };
            let api = self.0.clone();
            let caller = caller.clone();
            let remaining = context
                .read_deadline()
                .map(|deadline| deadline.saturating_duration_since(tokio::time::Instant::now()));
            // Database acquisition and statement cancellation use the same
            // accepted deadline as the common operation context.
            let reply =
                crate::request_deadline::run(remaining, async move { api.status(&caller).await })
                    .await;
            match reply {
                Ok(Reply::Ok { result }) => SurfaceAnswer::ok(result),
                Ok(Reply::Failed { failure } | Reply::StoppedEarly { failure, .. }) => {
                    SurfaceAnswer::error(failure)
                }
                Err(error) => SurfaceAnswer::error(error.failure()),
            }
        })
    }
}

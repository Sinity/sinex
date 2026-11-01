//! Open the decision snapshot after a fresh peer preflight.
use super::*;
use sqlx::{Postgres, Transaction};

pub(super) async fn open(
    pool: &sqlx::PgPool,
    attempt: &AttemptContext,
    diagnostic: &mut Diagnostic<'_>,
    peers: Option<&PeerConditions<'_>>,
) -> Result<(Transaction<'static, Postgres>, Conditions), PolicyFailure> {
    attempt.check().map_err(PolicyFailure::Interrupted)?;
    // The peer preflight is a read under the original lifetime and basis,
    // outside the SQL snapshot.
    let conditions = match peers {
        Some(peers) => peers.read(attempt, diagnostic).await?,
        None => BTreeMap::new(),
    };
    let connection = attempt
        .run(async {
            let mut connection = pool.acquire().await.map_err(|error| {
                diagnostic.error(Operation::SnapshotConnection, ErrorClass::Pool, error)
            })?;
            sinex_store::query::initialize_calendar(&mut connection)
                .await
                .map_err(|error| {
                    diagnostic.store_error(Operation::CalendarInitialization, error)
                })?;
            Ok::<_, PolicyFailure>(connection)
        })
        .await
        .map_err(PolicyFailure::Interrupted)??;
    attempt.check().map_err(PolicyFailure::Interrupted)?;
    // Once BEGIN is submitted, observe its acknowledgement. If the attempt
    // expired meanwhile, the only remaining work is an acknowledged rollback.
    let mut tx: Transaction<'static, Postgres> = Transaction::begin(connection, None)
        .await
        .map_err(|error| diagnostic.error(Operation::SnapshotBegin, ErrorClass::Database, error))?;
    let isolated = attempt
        .run(sinex_store::signals::effect_snapshot(&mut tx))
        .await;
    let result = match isolated {
        Ok(Ok(())) => return Ok((tx, conditions)),
        Ok(Err(error)) => {
            diagnostic.store_error(Operation::SnapshotIsolation, error);
            Err(PolicyFailure::Unavailable)
        }
        Err(interrupted) => Err(PolicyFailure::Interrupted(interrupted)),
    };
    tx.rollback().await.map_err(|error| {
        diagnostic.error(Operation::SnapshotRollback, ErrorClass::Database, error)
    })?;
    result
}

#[cfg(test)]
mod tests;

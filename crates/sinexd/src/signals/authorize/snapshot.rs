//! Retry only the first policy lock, before decision-snapshot reads.
use super::*;
use sqlx::{Postgres, Transaction};

fn retryable(error: &sinex_store::Error) -> bool {
    matches!(error, sinex_store::Error::Database(error)
        if matches!(error.as_database_error().and_then(|error| error.code()).as_deref(), Some("40001" | "40P01")))
}

pub(super) async fn open(
    pool: &sqlx::PgPool,
    attempt: &AttemptContext,
    expected_epoch: i64,
    diagnostic: &mut Diagnostic<'_>,
    peers: Option<&PeerConditions<'_>>,
) -> Result<
    (
        Transaction<'static, Postgres>,
        sinex_store::privacy::projection::Epoch,
        Conditions,
    ),
    PolicyFailure,
> {
    loop {
        attempt.check().map_err(PolicyFailure::Interrupted)?;
        // A conflict starts a fresh peer preflight too. It is a read under the
        // original lifetime and basis, outside every SQL policy fence.
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
            .map_err(|error| {
                diagnostic.error(Operation::SnapshotBegin, ErrorClass::Database, error)
            })?;
        let locked = attempt
            .run(async {
                sinex_store::signals::effect_snapshot(&mut tx)
                    .await
                    .map_err(|error| (false, error))?;
                sinex_store::privacy::projection::lock_epoch(&mut tx)
                    .await
                    .map_err(|error| (true, error))
            })
            .await;
        let result = match locked {
            Ok(Ok(epoch)) if epoch.ready() && epoch.active == expected_epoch => {
                return Ok((tx, epoch, conditions));
            }
            Ok(Ok(epoch)) => {
                diagnostic.record(
                    None,
                    if epoch.ready() {
                        Refusal::AuthorityPolicyEpochChanged
                    } else {
                        Refusal::PolicyNotReady
                    },
                );
                Err(PolicyFailure::Unavailable)
            }
            Ok(Err((first_lock, error))) => {
                let retry = first_lock && retryable(&error);
                if first_lock && let sinex_store::Error::Database(database) = &error {
                    let code = database.as_database_error().and_then(|error| error.code());
                    // PostgreSQL's typed code is metadata; its message and SQL are private.
                    eprintln!(
                        "signal_authorization operation=PolicyLock sqlstate={code:?} retry={retry}"
                    );
                }
                diagnostic.store_error(
                    if first_lock {
                        Operation::PolicyLock
                    } else {
                        Operation::SnapshotIsolation
                    },
                    error,
                );
                tx.rollback().await.map_err(|error| {
                    diagnostic.error(Operation::SnapshotRollback, ErrorClass::Database, error)
                })?;
                attempt.check().map_err(PolicyFailure::Interrupted)?;
                if retry {
                    continue;
                }
                return Err(PolicyFailure::Unavailable);
            }
            Err(interrupted) => Err(PolicyFailure::Interrupted(interrupted)),
        };
        tx.rollback().await.map_err(|error| {
            diagnostic.error(Operation::SnapshotRollback, ErrorClass::Database, error)
        })?;
        return result;
    }
}

#[cfg(test)]
mod tests;

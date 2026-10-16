//! Explicit, one-shot qualification rendezvous at real writer instructions. No ambient controls.
use crate::{Error, Result};
use serde_json::{Value, json};
use std::io::{Read, Write};
use std::path::PathBuf;

#[derive(Clone)]
pub struct QualificationBoundary {
    point: String,
    socket: PathBuf,
}
impl QualificationBoundary {
    pub fn new(point: String, socket: PathBuf) -> Result<Self> {
        let replay = point
            .strip_prefix("replay-staged-")
            .or_else(|| point.strip_prefix("replay-committed-"))
            .or_else(|| point.strip_prefix("projection-staged-"))
            .or_else(|| point.strip_prefix("projection-committed-"))
            .is_some_and(|step| step.parse::<usize>().is_ok_and(|n| n < 64));
        if (!replay
            && !matches!(
                point.as_str(),
                "retained-fsynced"
                    | "transaction-staged"
                    | "transaction-committed"
                    | "delete-staged"
                    | "delete-committed"
                    | "fusion-replacement-committed"
                    | "session-replacement-committed"
            ))
            || !socket.is_absolute()
        {
            return Err(Error::Invalid(
                "qualification requires a named writer boundary and absolute Unix socket",
            ));
        }
        Ok(Self { point, socket })
    }
    pub(crate) fn observes_atuin_queue(&self) -> bool {
        self.point == "fusion-replacement-committed"
    }
    pub(crate) async fn reach(
        control: &mut Option<Self>,
        point: &str,
        evidence: Value,
    ) -> Result<()> {
        if control.as_ref().is_none_or(|c| c.point != point) {
            return Ok(());
        }
        let control = control.take().expect("matching control exists");
        let notice = json!({"component":"sinex-store","qualification_boundary":point,"pid":std::process::id(),"evidence":evidence});
        tokio::task::spawn_blocking(move || -> Result<()> {
            let mut socket = std::os::unix::net::UnixStream::connect(control.socket)?;
            socket.set_read_timeout(Some(std::time::Duration::from_secs(120)))?;
            socket.set_write_timeout(Some(std::time::Duration::from_secs(10)))?;
            serde_json::to_writer(&mut socket, &notice)
                .map_err(|_| Error::Invalid("cannot encode boundary evidence"))?;
            socket.write_all(b"\n")?;
            socket.flush()?;
            eprintln!("{notice}");
            // The real writer cannot cross this instruction until the controller releases
            // it with 'c' or kills its recorded process. EOF/timeouts fail the admission.
            let mut release = [0];
            socket.read_exact(&mut release)?;
            if release != [b'c'] {
                return Err(Error::Invalid("qualification boundary release refused"));
            }
            Ok(())
        })
        .await??;
        Ok(())
    }
}

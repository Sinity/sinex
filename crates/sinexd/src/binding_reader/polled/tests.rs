use super::*;

struct SlowReader {
    entered: Option<tokio::sync::oneshot::Sender<()>>,
    release: std::sync::mpsc::Receiver<()>,
}

impl Reader for SlowReader {
    fn poll(&mut self, _: &Value) -> Result<drivers::Acquired, drivers::DriverError> {
        self.entered.take().unwrap().send(()).unwrap();
        self.release
            .recv_timeout(std::time::Duration::from_secs(5))
            .map_err(|_| {
                drivers::DriverError::Io("runtime did not progress during the poll".into())
            })?;
        Err(drivers::DriverError::Io("synthetic poll finished".into()))
    }
}

#[tokio::test(flavor = "current_thread")]
async fn synchronous_poll_does_not_stop_other_runtime_tasks() {
    let (entered, waiting) = tokio::sync::oneshot::channel();
    let (release, resume) = std::sync::mpsc::channel();
    let reader = PolledReader::new(Box::new(SlowReader {
        entered: Some(entered),
        release: resume,
    }));
    let unrelated = async {
        waiting.await.unwrap();
        release.send(()).unwrap();
    };
    let (result, ()) = tokio::join!(reader.acquire(Value::Null), unrelated);
    assert!(result.err().unwrap().contains("synthetic poll finished"));
}

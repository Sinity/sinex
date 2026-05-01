/// Progress reporter for known-duration operations.
pub struct ProgressReporter {
    total: u64,
    pos: std::sync::atomic::AtomicU64,
    message: String,
}

impl ProgressReporter {
    /// Create a new progress reporter.
    #[must_use]
    pub fn new(total: u64, message: &str) -> Self {
        eprintln!("{message}: 0/{total}");
        Self {
            total,
            pos: std::sync::atomic::AtomicU64::new(0),
            message: message.to_string(),
        }
    }

    /// Update progress.
    pub fn set_position(&self, pos: u64) {
        self.pos
            .store(pos.min(self.total), std::sync::atomic::Ordering::Relaxed);
    }

    /// Increment progress.
    pub fn inc(&self, delta: u64) {
        let current = self
            .pos
            .fetch_add(delta, std::sync::atomic::Ordering::Relaxed)
            .saturating_add(delta)
            .min(self.total);
        if self.total > 0 && (current == self.total || current % 100 == 0) {
            eprintln!("{}: {current}/{}", self.message, self.total);
        }
    }

    /// Finish progress with message.
    pub fn finish_with_message(&self, message: &str) {
        eprintln!("✓ {message}");
    }

    /// Abandon progress for errors.
    pub fn abandon_with_message(&self, message: &str) {
        eprintln!("✗ {message}");
    }
}

/// Spinner for unknown-duration operations.
pub struct Spinner {
    message: String,
}

impl Spinner {
    /// Create a new spinner with a message.
    #[must_use]
    pub fn new(message: &str) -> Self {
        eprintln!("{message}");
        Self {
            message: message.to_string(),
        }
    }

    /// Update the spinner message.
    pub fn set_message(&mut self, message: &str) {
        self.message = message.to_string();
        eprintln!("{message}");
    }

    /// Finish the spinner with a success message.
    pub fn finish_with_message(self, message: &str) {
        eprintln!("✓ {message}");
    }

    /// Finish the spinner, clearing it.
    pub fn finish_and_clear(self) {}

    /// Abandon the spinner with an error message.
    pub fn abandon_with_message(self, message: &str) {
        eprintln!("✗ {message}");
    }
}

/// Execute an async operation with a spinner.
pub async fn with_spinner<T, F>(message: &str, f: F) -> T
where
    F: AsyncFnOnce() -> T,
{
    let spinner = Spinner::new(message);
    let result = f().await;
    spinner.finish_and_clear();
    result
}

/// Execute an async Result-returning operation with a spinner that handles success/failure.
pub async fn with_spinner_result<T, E, F>(
    message: impl Into<String>,
    success_msg: impl Into<String>,
    future: F,
) -> Result<T, E>
where
    F: std::future::Future<Output = Result<T, E>>,
    E: std::fmt::Display,
{
    let spinner = Spinner::new(&message.into());
    let success_msg = success_msg.into();

    match future.await {
        Ok(value) => {
            spinner.finish_with_message(&success_msg);
            Ok(value)
        }
        Err(error) => {
            spinner.abandon_with_message(&format!("Failed: {error}"));
            Err(error)
        }
    }
}

/// RAII guard for spinners that automatically cleans up on drop.
pub struct SpinnerGuard {
    spinner: Option<Spinner>,
    default_error_msg: String,
}

impl SpinnerGuard {
    /// Create a new spinner guard with a message.
    pub fn new(message: impl Into<String>) -> Self {
        let msg = message.into();
        Self {
            spinner: Some(Spinner::new(&msg)),
            default_error_msg: "Operation failed".to_string(),
        }
    }

    /// Create a new spinner guard with a custom error message.
    pub fn with_error_msg(message: impl Into<String>, error_msg: impl Into<String>) -> Self {
        let msg = message.into();
        Self {
            spinner: Some(Spinner::new(&msg)),
            default_error_msg: error_msg.into(),
        }
    }

    /// Update the spinner message during operation.
    pub fn update_message(&mut self, message: &str) {
        if let Some(ref mut spinner) = self.spinner {
            spinner.set_message(message);
        }
    }

    /// Finish the spinner successfully with a message and consume the guard.
    pub fn finish(mut self, message: &str) {
        if let Some(spinner) = self.spinner.take() {
            spinner.finish_with_message(message);
        }
    }

    /// Abandon the spinner with an error message and consume the guard.
    pub fn abandon(mut self, message: &str) {
        if let Some(spinner) = self.spinner.take() {
            spinner.abandon_with_message(message);
        }
    }
}

impl Drop for SpinnerGuard {
    fn drop(&mut self) {
        if let Some(spinner) = self.spinner.take() {
            spinner.abandon_with_message(&self.default_error_msg);
        }
    }
}

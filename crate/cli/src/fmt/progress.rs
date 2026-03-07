use indicatif::{ProgressBar, ProgressStyle};

/// Progress reporter for known-duration operations (progress bar)
pub struct ProgressReporter {
    bar: ProgressBar,
}

impl ProgressReporter {
    /// Create a new progress reporter with a progress bar
    #[allow(clippy::unwrap_used)]
    #[must_use]
    pub fn new(total: u64, message: &str) -> Self {
        let bar = ProgressBar::new(total);
        bar.set_style(
            ProgressStyle::default_bar()
                .template("{msg}\n{spinner:.green} [{elapsed_precise}] [{wide_bar:.cyan/blue}] {pos}/{len} ({eta})")
                .unwrap()
                .progress_chars("#>-"),
        );
        bar.set_message(message.to_string());

        Self { bar }
    }

    /// Update progress
    pub fn set_position(&self, pos: u64) {
        self.bar.set_position(pos);
    }

    /// Increment progress
    pub fn inc(&self, delta: u64) {
        self.bar.inc(delta);
    }

    /// Finish progress with message
    pub fn finish_with_message(&self, message: &str) {
        self.bar.finish_with_message(message.to_string());
    }

    /// Abandon progress (for errors)
    pub fn abandon_with_message(&self, message: &str) {
        self.bar.abandon_with_message(message.to_string());
    }
}

/// Spinner for unknown-duration operations
pub struct Spinner {
    bar: ProgressBar,
}

impl Spinner {
    /// Create a new spinner with a message
    #[allow(clippy::unwrap_used)]
    #[must_use]
    pub fn new(message: &str) -> Self {
        let bar = ProgressBar::new_spinner();
        bar.set_style(
            ProgressStyle::default_spinner()
                .template("{spinner:.cyan} {msg}")
                .unwrap()
                .tick_chars("⠋⠙⠹⠸⠼⠴⠦⠧⠇⠏"),
        );
        bar.set_message(message.to_string());
        bar.enable_steady_tick(std::time::Duration::from_millis(80));

        Self { bar }
    }

    /// Update the spinner message
    pub fn set_message(&self, message: &str) {
        self.bar.set_message(message.to_string());
    }

    /// Finish the spinner with a success message
    pub fn finish_with_message(&self, message: &str) {
        self.bar.finish_with_message(format!("✓ {message}"));
    }

    /// Finish the spinner, clearing it
    pub fn finish_and_clear(&self) {
        self.bar.finish_and_clear();
    }

    /// Abandon the spinner with an error message
    pub fn abandon_with_message(&self, message: &str) {
        self.bar.abandon_with_message(format!("✗ {message}"));
    }
}

/// Execute an async operation with a spinner
/// Returns the result of the operation
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
///
/// The spinner automatically shows success on Ok, or abandons with error message on Err.
/// This provides RAII-style cleanup - the spinner is properly cleaned up even on panic.
///
/// # Examples
///
/// ```ignore
/// let result = with_spinner_result(
///     "Draining node...",
///     "Node drained successfully",
///     client.drain_node(node_id)
/// ).await?;
/// ```
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
///
/// This ensures the spinner is properly abandoned/finished even if the
/// operation panics or returns early.
///
/// # Examples
///
/// ```ignore
/// async fn complex_operation() -> Result<()> {
///     let mut spinner = SpinnerGuard::new("Starting operation...");
///
///     // Step 1
///     some_work().await?;
///     spinner.update_message("Processing step 2...");
///
///     // Step 2
///     more_work().await?;
///
///     // On success, mark as complete
///     spinner.finish("Operation completed");
///     Ok(())
///     // If there's an error, Drop abandons the spinner automatically
/// }
/// ```
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
    pub fn update_message(&self, message: &str) {
        if let Some(ref spinner) = self.spinner {
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
        // If spinner wasn't explicitly finished/abandoned, abandon it with default error
        if let Some(spinner) = self.spinner.take() {
            spinner.abandon_with_message(&self.default_error_msg);
        }
    }
}

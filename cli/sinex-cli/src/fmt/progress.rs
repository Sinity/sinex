use indicatif::{ProgressBar, ProgressStyle};

/// Progress reporter for known-duration operations (progress bar)
pub struct ProgressReporter {
    bar: ProgressBar,
}

impl ProgressReporter {
    /// Create a new progress reporter with a progress bar
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
        self.bar.finish_with_message(format!("✓ {}", message));
    }

    /// Finish the spinner, clearing it
    pub fn finish_and_clear(&self) {
        self.bar.finish_and_clear();
    }

    /// Abandon the spinner with an error message
    pub fn abandon_with_message(&self, message: &str) {
        self.bar.abandon_with_message(format!("✗ {}", message));
    }
}

/// Execute an async operation with a spinner
/// Returns the result of the operation
pub async fn with_spinner<T, F, Fut>(message: &str, f: F) -> T
where
    F: FnOnce() -> Fut,
    Fut: std::future::Future<Output = T>,
{
    let spinner = Spinner::new(message);
    let result = f().await;
    spinner.finish_and_clear();
    result
}

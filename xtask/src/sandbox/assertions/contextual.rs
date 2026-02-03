use crate::sandbox::prelude::TestResult;

/// Rich assertion helper with contextual error messages
pub struct ContextualAssert {
    context: String,
}

impl ContextualAssert {
    #[must_use]
    pub fn new(context: &str) -> Self {
        Self {
            context: context.to_string(),
        }
    }

    /// Assert two values are equal
    pub fn eq<T>(self, left: &T, right: &T) -> TestResult<Self>
    where
        T: std::fmt::Debug + PartialEq,
    {
        if left != right {
            color_eyre::eyre::bail!(
                "{}: values are not equal\n  Left: {:?}\n  Right: {:?}",
                self.context,
                left,
                right
            );
        }
        Ok(self)
    }

    /// Assert a condition is true
    pub fn that(self, condition: bool, message: &str) -> TestResult<Self> {
        if !condition {
            color_eyre::eyre::bail!("{}: {}", self.context, message);
        }
        Ok(self)
    }

    /// Assert collection is not empty
    pub fn not_empty<T>(self, collection: &[T]) -> TestResult<Self> {
        if collection.is_empty() {
            color_eyre::eyre::bail!("{}: collection should not be empty", self.context);
        }
        Ok(self)
    }

    /// Assert collection has specific size
    pub fn has_size<T>(self, collection: &[T], expected_size: usize) -> TestResult<Self> {
        if collection.len() != expected_size {
            color_eyre::eyre::bail!(
                "{}: collection size mismatch. Expected: {}, Actual: {}",
                self.context,
                expected_size,
                collection.len()
            );
        }
        Ok(self)
    }

    /// Assert option is Some
    pub fn some<T>(self, option: &Option<T>) -> TestResult<Self> {
        if option.is_none() {
            color_eyre::eyre::bail!("{}: option should be Some, but was None", self.context);
        }
        Ok(self)
    }

    /// Assert option is None
    pub fn none<T>(self, option: &Option<T>) -> TestResult<Self> {
        if option.is_some() {
            color_eyre::eyre::bail!("{}: option should be None, but was Some", self.context);
        }
        Ok(self)
    }

    /// Assert result contains error with specific message
    pub fn error_contains<T, E>(self, result: &Result<T, E>, expected_msg: &str) -> TestResult<Self>
    where
        T: std::fmt::Debug,
        E: std::fmt::Display,
    {
        match result {
            Ok(val) => {
                color_eyre::eyre::bail!(
                    "{}: expected error containing '{}', but got Ok({:?})",
                    self.context,
                    expected_msg,
                    val
                );
            }
            Err(e) => {
                let msg = e.to_string();
                if !msg.contains(expected_msg) {
                    color_eyre::eyre::bail!(
                        "{}: error message '{}' does not contain '{}'",
                        self.context,
                        msg,
                        expected_msg
                    );
                }
            }
        }
        Ok(self)
    }
}

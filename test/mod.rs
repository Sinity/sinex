#[macro_use]
mod common;

// Test categories organized by scope and resource requirements
#[cfg(test)]
mod unit;

#[cfg(test)]
mod integration;

#[cfg(test)]
mod system;

#[cfg(test)]
mod property;

#[cfg(test)]
mod adversarial;

#[cfg(test)]
mod validation;
//! Non-empty collection types for enforcing invariants at the type level

use serde::{Deserialize, Serialize};
use std::fmt;
use std::ops::Deref;

/// A vector that is guaranteed to contain at least one element.
/// This prevents representing invalid states where a collection must be non-empty.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize)]
#[serde(transparent)]
pub struct NonEmptyVec<T> {
    inner: Vec<T>,
}

// Manual Deserialize implementation to handle the non-empty constraint
impl<'de, T> Deserialize<'de> for NonEmptyVec<T>
where
    T: Deserialize<'de>,
{
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let vec = Vec::<T>::deserialize(deserializer)?;
        if vec.is_empty() {
            Err(serde::de::Error::custom("NonEmptyVec cannot be empty"))
        } else {
            Ok(NonEmptyVec { inner: vec })
        }
    }
}

impl<T> NonEmptyVec<T> {
    /// Create a new NonEmptyVec with a single element
    pub fn single(value: T) -> Self {
        NonEmptyVec { inner: vec![value] }
    }

    /// Create a new NonEmptyVec from a vector, returning None if empty
    pub fn from_vec(vec: Vec<T>) -> Option<Self> {
        if vec.is_empty() {
            None
        } else {
            Some(NonEmptyVec { inner: vec })
        }
    }

    /// Create a new NonEmptyVec from a head element and tail vector
    pub fn from_head_tail(head: T, tail: Vec<T>) -> Self {
        let mut inner = vec![head];
        inner.extend(tail);
        NonEmptyVec { inner }
    }

    /// Get the first element (always exists)
    pub fn first(&self) -> &T {
        &self.inner[0] // Safe indexing - will panic with clear message if invariant violated
    }

    /// Get the last element (always exists)
    pub fn last(&self) -> &T {
        &self.inner[self.inner.len() - 1] // Safe indexing - will panic with clear message if invariant violated
    }

    /// Get the number of elements
    pub fn len(&self) -> usize {
        self.inner.len()
    }

    /// NonEmptyVec can never be empty by construction
    pub fn is_empty(&self) -> bool {
        false
    }

    /// Push an element to the end
    pub fn push(&mut self, value: T) {
        self.inner.push(value);
    }

    /// Iterate over the elements
    pub fn iter(&self) -> std::slice::Iter<'_, T> {
        self.inner.iter()
    }

    /// Convert into the underlying vector
    pub fn into_vec(self) -> Vec<T> {
        self.inner
    }

    /// Get a reference to the underlying vector
    pub fn as_vec(&self) -> &Vec<T> {
        &self.inner
    }

    /// Map each element to a new type
    pub fn map<U, F>(self, f: F) -> NonEmptyVec<U>
    where
        F: FnMut(T) -> U,
    {
        NonEmptyVec {
            inner: self.inner.into_iter().map(f).collect(),
        }
    }
}

impl<T> Deref for NonEmptyVec<T> {
    type Target = Vec<T>;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl<T> IntoIterator for NonEmptyVec<T> {
    type Item = T;
    type IntoIter = std::vec::IntoIter<T>;

    fn into_iter(self) -> Self::IntoIter {
        self.inner.into_iter()
    }
}

impl<'a, T> IntoIterator for &'a NonEmptyVec<T> {
    type Item = &'a T;
    type IntoIter = std::slice::Iter<'a, T>;

    fn into_iter(self) -> Self::IntoIter {
        self.inner.iter()
    }
}

impl<T: fmt::Display> fmt::Display for NonEmptyVec<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "[")?;
        for (i, item) in self.inner.iter().enumerate() {
            if i > 0 {
                write!(f, ", ")?;
            }
            write!(f, "{item}")?;
        }
        write!(f, "]")
    }
}

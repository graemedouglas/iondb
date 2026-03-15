//! # Structural Tests
//!
//! Tests that validate the shape of the IonDB codebase, not its behavior.
//! These run as part of `cargo test` and enforce architectural constraints.
//!
//! ## What's validated
//!
//! - **Dependency direction**: No implementation crate depends on another
//!   implementation crate (parsed from Cargo.toml files).
//! - **File size limits**: No source file exceeds 500 lines.
//! - **Naming conventions**: Module, trait, and error variant names follow
//!   project conventions.
//! - **No forbidden patterns**: No `unwrap()` in library code, no `Box<dyn Error>`,
//!   etc.

#[cfg(test)]
mod dependency_tests;

#[cfg(test)]
mod file_size_tests;

#[cfg(test)]
mod naming_tests;

#[cfg(test)]
mod pattern_tests;

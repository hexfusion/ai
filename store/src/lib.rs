// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Praxis Contributors

//! Internal, unstable persisted-state contracts for praxis-ai.
//!
//! SQL-free and transport-free: the persistence traits, record types, the owner
//! identity, a unified backend registry, and an in-memory backend. This crate
//! is a first-party workspace implementation detail with no external API and no
//! semver promise.

mod owner;
mod registry;
mod traits;
mod types;

#[cfg(feature = "test-support")]
pub mod memory;

#[cfg(feature = "test-support")]
pub mod contract_tests;

pub use owner::{StateOwner, StateOwnerError, validate_component};
pub use registry::{OwnerScopedStore, StoreRegistry};
pub use traits::{ConversationItemStore, PersistedStateBackend, ResponseStore};
pub use types::{ConversationItemRecord, ConversationRecord, PendingApprovalRecord, ResponseRecord, StoreError};

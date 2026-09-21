// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Praxis Contributors

//! Internal, unstable persisted-state contracts for praxis-ai.
//!
//! SQL-free and transport-free: the persistence traits, record types, and the
//! owner identity. This crate is a first-party workspace implementation detail
//! with no external API and no semver promise.

mod owner;
mod traits;
mod types;

pub use owner::{StateOwner, StateOwnerError};
pub use traits::{ConversationItemStore, PersistedStateBackend, ResponseStore};
pub use types::{ConversationItemRecord, ConversationRecord, PendingApprovalRecord, ResponseRecord, StoreError};

// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Praxis Contributors

//! Unified, transport-free registry of persisted-state backends.
//!
//! The registry holds a combined [`PersistedStateBackend`] handle, so a resolved
//! backend provably implements both the response and conversation-item halves.
//! Request-driven consumers take an [`OwnerScopedStore`] bound to one validated
//! owner rather than the raw backend, so a later operation cannot substitute an
//! arbitrary tenant or principal.

use std::sync::Arc;

use dashmap::{DashMap, mapref::entry::Entry};

use crate::{
    owner::StateOwner,
    traits::{PersistedStateBackend, ResponseStore},
    types::{ConversationRecord, PendingApprovalRecord, ResponseRecord, StoreError},
};

/// Thread-safe registry of named persisted-state backends.
///
/// Each listener owns a registry populated at startup. The registry carries no
/// transport dependency; the pipeline binding lives in the transport layer.
#[derive(Clone)]
pub struct StoreRegistry {
    /// Named combined backends.
    #[expect(clippy::type_complexity, reason = "DashMap of trait objects is inherently verbose")]
    stores: Arc<DashMap<Arc<str>, Arc<dyn PersistedStateBackend>>>,
}

/// Backend handle permanently bound to one validated owner.
///
/// Request-driven consumers obtain this facade from [`StoreRegistry::get_scoped`]
/// instead of the raw backend, so a later operation cannot substitute an
/// arbitrary owner. Conversation-item scoped access is added with the
/// conversations service extraction; today the facade exposes the response
/// surface its callers use.
#[derive(Clone)]
pub struct OwnerScopedStore {
    /// Shared combined backend hidden behind the owner-bound facade.
    store: Arc<dyn PersistedStateBackend>,
    /// Immutable scope applied to every operation.
    owner: StateOwner,
}

impl OwnerScopedStore {
    /// Retrieve a response visible to this owner.
    ///
    /// # Errors
    ///
    /// Returns a store error when the backend query fails.
    pub async fn get_response(&self, id: &str) -> Result<Option<ResponseRecord>, StoreError> {
        self.store.get_response(&self.owner, id).await
    }

    /// Delete a response visible to this owner.
    ///
    /// # Errors
    ///
    /// Returns a store error when the backend mutation fails.
    pub async fn delete_response(&self, id: &str) -> Result<bool, StoreError> {
        self.store.delete_response(&self.owner, id).await
    }

    /// Retrieve a conversation visible to this owner.
    ///
    /// # Errors
    ///
    /// Returns a store error when the backend query fails.
    pub async fn get_conversation(&self, id: &str) -> Result<Option<ConversationRecord>, StoreError> {
        // Both trait halves expose get_conversation; the facade uses the
        // ResponseStore read-only view. Upcast to disambiguate.
        let store: &dyn ResponseStore = self.store.as_ref();
        store.get_conversation(&self.owner, id).await
    }

    /// Persist a response only when its immutable owner matches this handle.
    ///
    /// # Errors
    ///
    /// Returns an invalid-input error for an owner mismatch or the backend
    /// error from persistence.
    pub async fn upsert_response(&self, record: &ResponseRecord) -> Result<(), StoreError> {
        self.require_matching_owner(&record.owner)?;
        self.store.upsert_response(record).await
    }

    /// Retrieve pending approvals issued to this owner.
    ///
    /// # Errors
    ///
    /// Returns a store error when the backend query fails.
    pub async fn get_pending_approvals(
        &self,
        response_id: &str,
        approval_ids: &[&str],
    ) -> Result<Vec<PendingApprovalRecord>, StoreError> {
        self.store
            .get_pending_approvals(&self.owner, response_id, approval_ids)
            .await
    }

    /// Atomically consume approvals issued to this owner.
    ///
    /// # Errors
    ///
    /// Returns a store error when the backend mutation fails.
    pub async fn consume_approvals(
        &self,
        response_id: &str,
        approval_ids: &[&str],
        consumed_at: i64,
    ) -> Result<Option<usize>, StoreError> {
        self.store
            .consume_approvals(&self.owner, response_id, approval_ids, consumed_at)
            .await
    }

    /// Reject records built under a different owner scope.
    fn require_matching_owner(&self, owner: &StateOwner) -> Result<(), StoreError> {
        if owner == &self.owner {
            Ok(())
        } else {
            Err(StoreError::InvalidInput(
                "record owner does not match owner-scoped store".to_owned(),
            ))
        }
    }
}

impl StoreRegistry {
    /// Create an empty registry.
    #[must_use]
    pub fn new() -> Self {
        Self {
            stores: Arc::new(DashMap::new()),
        }
    }

    /// Register a named combined backend.
    ///
    /// # Errors
    ///
    /// Returns `StoreError::Unavailable` if a store with the same name is
    /// already registered.
    pub fn register(&self, name: &Arc<str>, store: Arc<dyn PersistedStateBackend>) -> Result<(), StoreError> {
        match self.stores.entry(Arc::clone(name)) {
            Entry::Vacant(entry) => {
                entry.insert(store);
                Ok(())
            },
            Entry::Occupied(_) => Err(StoreError::Unavailable(format!(
                "persisted-state store '{name}' is already registered"
            ))),
        }
    }

    /// Look up a backend by name and bind all request-driven access to `owner`.
    #[must_use]
    pub fn get_scoped(&self, name: &str, owner: &StateOwner) -> Option<OwnerScopedStore> {
        self.get_backend(name).map(|store| OwnerScopedStore {
            store,
            owner: owner.clone(),
        })
    }

    /// Return whether a named backend is already registered.
    #[must_use]
    pub fn contains(&self, name: &str) -> bool {
        self.stores.contains_key(name)
    }

    /// Internal raw lookup used only to construct a constrained facade.
    fn get_backend(&self, name: &str) -> Option<Arc<dyn PersistedStateBackend>> {
        self.stores.get(name).map(|r| Arc::clone(r.value()))
    }

    /// Return whether two registry handles share the same backing storage.
    #[must_use]
    pub fn shares_storage_with(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.stores, &other.stores)
    }
}

impl Default for StoreRegistry {
    fn default() -> Self {
        Self::new()
    }
}

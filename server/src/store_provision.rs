// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Praxis Contributors

//! Serving-runtime provisioning of response-store backends.
//!
//! The store filter reads an owner-scoped handle from a per-listener registry it
//! never populates. A Pingora background service provisions the configured
//! backends and registers them into that registry on the serving runtime. sqlx
//! pools bind to the runtime that opens them, so provisioning must run there, not
//! on the config-watcher runtime a reload uses.

#![cfg(any(feature = "store-postgres", feature = "store-sqlite"))]

use std::{collections::HashMap, sync::Arc};

use async_trait::async_trait;
use pingora_core::{server::ShutdownWatch, services::background::BackgroundService};
use praxis_ai_apis::store::{
    DEFAULT_STORE_NAME, RESPONSE_STORE_FILTER_NAME, ResponseStoreRegistry, store_backend_factories,
};
use praxis_ai_store::StoreRegistry;
use praxis_ai_store_lifecycle::{BackendCache, BackendLease, ProvisionError, StoreRef};
use praxis_core::config::Config;
use tracing::{error, info};

/// A listener's shared store registry and the references provisioned into it.
///
/// The registry handle is installed into the listener's pipeline (as a
/// [`ResponseStoreRegistry`]) before serving starts, and the background service
/// registers the provisioned backends into the same backing map.
struct ListenerStorePlan {
    /// Listener the plan belongs to.
    listener: String,
    /// Shared registry backing both the pipeline handle and provisioning.
    registry: StoreRegistry,
    /// Store references to provision (one default store, so at most one).
    refs: Vec<StoreRef>,
}

/// Build one store reference from a response-store filter's config.
///
/// The `backend` field selects the factory. The rest is the inline config the
/// factory parses. Returns `None` for a config missing a string `backend` or one
/// that cannot map to JSON.
fn store_ref_from_config(filter_config: &serde_yaml::Value) -> Option<StoreRef> {
    let backend = filter_config.get("backend").and_then(serde_yaml::Value::as_str)?;
    let mut config = serde_json::to_value(filter_config).ok()?;
    config.as_object_mut()?.remove("backend");
    Some(StoreRef {
        name: Arc::from(DEFAULT_STORE_NAME),
        backend_id: Arc::from(backend),
        config,
    })
}

/// Build a per-listener store plan for every listener whose chains configure a
/// response store. The store is instance-scoped to one default name, so the
/// first store filter in a listener's chains wins.
fn build_listener_store_plans(config: &Config) -> Vec<ListenerStorePlan> {
    let chains: HashMap<&str, &[_]> = config
        .filter_chains
        .iter()
        .map(|c| (c.name.as_str(), c.filters.as_slice()))
        .collect();

    let mut plans = Vec::new();
    for listener in &config.listeners {
        let store_ref = listener
            .filter_chains
            .iter()
            .filter_map(|name| chains.get(name.as_str()))
            .flat_map(|filters| filters.iter())
            .find(|entry| entry.filter_type == RESPONSE_STORE_FILTER_NAME)
            .and_then(|entry| store_ref_from_config(&entry.config));
        if let Some(store_ref) = store_ref {
            plans.push(ListenerStorePlan {
                listener: listener.name.clone(),
                registry: StoreRegistry::new(),
                refs: vec![store_ref],
            });
        }
    }
    plans
}

/// Map each plan to the [`ResponseStoreRegistry`] installed into its pipeline,
/// sharing the plan's backing storage so provisioning is observed there.
fn registries_map(plans: &[ListenerStorePlan]) -> HashMap<String, ResponseStoreRegistry> {
    plans
        .iter()
        .map(|p| (p.listener.clone(), ResponseStoreRegistry::from(p.registry.clone())))
        .collect()
}

/// The per-listener registries to install into pipelines, plus the provisioner
/// when any store is configured.
pub(crate) type StoreWiring = (HashMap<String, ResponseStoreRegistry>, Option<StoreProvisionService>);

/// Build the per-listener store registries to install into pipelines and, when
/// any store is configured, the serving-runtime provisioner.
///
/// Store configuration is validated eagerly so a malformed or unknown-backend
/// config fails startup rather than at first traffic. Connectivity surfaces
/// later, when the background service opens the pools on the serving runtime.
///
/// # Errors
///
/// Returns [`ProvisionError`] when a configured store names an unknown backend
/// or its configuration is rejected.
pub(crate) fn build_store_wiring(config: &Config) -> Result<StoreWiring, ProvisionError> {
    let plans = build_listener_store_plans(config);
    let registries = registries_map(&plans);
    if plans.is_empty() {
        return Ok((registries, None));
    }
    let cache = Arc::new(BackendCache::new(store_backend_factories()));
    let refs: Vec<StoreRef> = plans.iter().flat_map(|p| p.refs.iter().cloned()).collect();
    cache.validate(&refs)?;
    Ok((registries, Some(StoreProvisionService { cache, plans })))
}

/// Provisions response-store backends on the serving runtime and holds their
/// leases for the process lifetime.
pub(crate) struct StoreProvisionService {
    /// Process-wide backend cache built from the compiled-in factories.
    cache: Arc<BackendCache>,
    /// Per-listener registries and references to provision.
    plans: Vec<ListenerStorePlan>,
}

#[async_trait]
impl BackgroundService for StoreProvisionService {
    async fn start(&self, mut shutdown: ShutdownWatch) {
        let mut leases: Vec<BackendLease> = Vec::with_capacity(self.plans.len());
        for plan in &self.plans {
            // Await to completion: provision_into is not cancellation-safe, so it
            // must not run under a select! that could drop the future and leak a
            // half-opened pool.
            match self.cache.provision_into(&plan.refs, &plan.registry).await {
                Ok(lease) => {
                    info!(listener = %plan.listener, "response store provisioned");
                    leases.push(lease);
                },
                Err(e) => error!(listener = %plan.listener, error = %e, "response store provisioning failed"),
            }
        }
        // Hold the leases so the backends outlive pipeline swaps, then release
        // on shutdown so the pools close cleanly. A dropped sender resolves the
        // same way, and either resolution proceeds to release.
        let _changed = shutdown.changed().await;
        for lease in leases {
            lease.release().await;
        }
    }
}

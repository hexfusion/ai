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

use std::{collections::HashMap, sync::Arc, time::Duration};

use async_trait::async_trait;
use pingora_core::{server::ShutdownWatch, services::background::BackgroundService};
use praxis_ai_apis::store::{
    DEFAULT_STORE_NAME, RESPONSE_STORE_FILTER_NAME, ResponseStoreRegistry, store_backend_factories,
};
use praxis_ai_store::StoreRegistry;
use praxis_ai_store_lifecycle::{BackendCache, BackendLease, ProvisionError, StoreRef};
use praxis_core::config::Config;
use tokio::sync::watch;
use tracing::{error, info};

/// Initial backoff before retrying a failed provisioning attempt.
const PROVISION_RETRY_INITIAL: Duration = Duration::from_millis(50);

/// Ceiling on the backoff between provisioning retries.
const PROVISION_RETRY_MAX: Duration = Duration::from_secs(5);

/// Readiness of the configured response-store backends.
///
/// One aggregate state across every configured listener: `Ready` only once all
/// hold a provisioned backend. Observers read it through a [`StoreReadinessHandle`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StoreReadiness {
    /// Provisioning has not yet completed for every listener.
    Pending,
    /// The most recent attempt failed. A retry is scheduled with backoff.
    Failed,
    /// Every configured listener holds a provisioned backend.
    Ready,
}

/// A cloneable, read-only handle to observe store-provisioning readiness.
///
/// The provisioning background service owns the sender. Consumers (the readiness
/// endpoint, the integration harness, sibling workstreams) clone this to observe
/// or await readiness. One owned state, many observers.
#[derive(Clone)]
pub struct StoreReadinessHandle {
    /// Receives readiness transitions from the provisioning service.
    rx: watch::Receiver<StoreReadiness>,
}

impl StoreReadinessHandle {
    /// A handle already at `Ready`, for when no store is configured. The sender
    /// is dropped, so the value never changes and `wait_ready` returns at once.
    #[must_use]
    pub fn ready() -> Self {
        let (_tx, rx) = watch::channel(StoreReadiness::Ready);
        Self { rx }
    }

    /// The current readiness snapshot.
    #[must_use]
    pub fn current(&self) -> StoreReadiness {
        *self.rx.borrow()
    }

    /// Whether every configured store is provisioned.
    #[must_use]
    pub fn is_ready(&self) -> bool {
        self.current() == StoreReadiness::Ready
    }

    /// Await until every configured store is provisioned. Returns at once when
    /// already ready, and returns if the provisioner stops without reaching
    /// `Ready` so a caller cannot block forever.
    pub async fn wait_ready(&mut self) {
        // Err only if the sender dropped before reaching Ready. Either way, stop
        // waiting. The borrowed guard is released at once.
        let _ready = self.rx.wait_for(|s| *s == StoreReadiness::Ready).await;
    }
}

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

/// The per-listener registries to install into pipelines, the provisioner when
/// any store is configured, and a readiness handle observers await.
pub type StoreWiring = (
    HashMap<String, ResponseStoreRegistry>,
    Option<StoreProvisionService>,
    StoreReadinessHandle,
);

/// Build the per-listener store registries to install into pipelines and, when
/// any store is configured, the serving-runtime provisioner and its readiness
/// handle.
///
/// Store configuration is validated eagerly so a malformed or unknown-backend
/// config fails startup rather than at first traffic. Connectivity surfaces
/// later, when the background service opens the pools on the serving runtime.
/// The readiness handle reaches `Ready` once every pool is open.
///
/// # Errors
///
/// Returns [`ProvisionError`] when a configured store names an unknown backend
/// or its configuration is rejected.
pub fn build_store_wiring(config: &Config) -> Result<StoreWiring, ProvisionError> {
    let plans = build_listener_store_plans(config);
    let registries = registries_map(&plans);
    if plans.is_empty() {
        return Ok((registries, None, StoreReadinessHandle::ready()));
    }
    let cache = Arc::new(BackendCache::new(store_backend_factories()));
    let refs: Vec<StoreRef> = plans.iter().flat_map(|p| p.refs.iter().cloned()).collect();
    cache.validate(&refs)?;
    let (readiness, rx) = watch::channel(StoreReadiness::Pending);
    Ok((
        registries,
        Some(StoreProvisionService {
            cache,
            plans,
            readiness,
        }),
        StoreReadinessHandle { rx },
    ))
}

/// Provisions response-store backends on the serving runtime and holds their
/// leases for the process lifetime.
pub struct StoreProvisionService {
    /// Process-wide backend cache built from the compiled-in factories.
    cache: Arc<BackendCache>,
    /// Per-listener registries and references to provision.
    plans: Vec<ListenerStorePlan>,
    /// Publishes readiness transitions to observers.
    readiness: watch::Sender<StoreReadiness>,
}

impl StoreProvisionService {
    /// Provision every listener, retrying a failed attempt with bounded backoff
    /// so a transient database or TLS failure self-heals. Returns `false` when
    /// shutdown interrupts a retry. Acquired leases are pushed onto `leases` so
    /// the caller releases them regardless of outcome.
    async fn provision_all(&self, leases: &mut Vec<BackendLease>, shutdown: &mut ShutdownWatch) -> bool {
        for plan in &self.plans {
            let mut backoff = PROVISION_RETRY_INITIAL;
            loop {
                // Await to completion: provision_into is not cancellation-safe, so
                // it must not run under a select! that could drop the future and
                // leak a half-opened pool. Only the backoff waits under select!.
                match self.cache.provision_into(&plan.refs, &plan.registry).await {
                    Ok(lease) => {
                        info!(listener = %plan.listener, "response store provisioned");
                        leases.push(lease);
                        break;
                    },
                    Err(e) => {
                        // Non-Ready until a later attempt succeeds.
                        let _sent = self.readiness.send(StoreReadiness::Failed);
                        error!(
                            listener = %plan.listener,
                            error = %e,
                            backoff_ms = backoff.as_millis(),
                            "response store provisioning failed; retrying",
                        );
                        tokio::select! {
                            () = tokio::time::sleep(backoff) => {},
                            _ = shutdown.changed() => return false,
                        }
                        backoff = backoff.saturating_mul(2).min(PROVISION_RETRY_MAX);
                    },
                }
            }
        }
        true
    }
}

#[async_trait]
impl BackgroundService for StoreProvisionService {
    async fn start(&self, mut shutdown: ShutdownWatch) {
        let mut leases: Vec<BackendLease> = Vec::with_capacity(self.plans.len());
        // Signal Ready only once every listener holds a lease, so observers never
        // see a partially provisioned instance as ready.
        if self.provision_all(&mut leases, &mut shutdown).await {
            let _sent = self.readiness.send(StoreReadiness::Ready);
            info!("all response stores provisioned");
            let _changed = shutdown.changed().await;
        }
        // Hold the leases so the backends outlive pipeline swaps, then release on
        // shutdown so the pools close cleanly.
        for lease in leases {
            lease.release().await;
        }
    }
}

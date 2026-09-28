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

use std::{
    collections::HashMap,
    sync::{Arc, Weak, mpsc as std_mpsc},
    time::Duration,
};

use async_trait::async_trait;
use futures::{StreamExt as _, stream::FuturesUnordered};
use pingora_core::{server::ShutdownWatch, services::background::BackgroundService};
#[cfg(feature = "openai-conversations")]
use praxis_ai_apis::store::{
    CONVERSATIONS_STORE_FILTER_NAME, CONVERSATIONS_STORE_NAME, conversations_store_ref_config,
};
use praxis_ai_apis::store::{
    DEFAULT_STORE_NAME, RESPONSE_STORE_FILTER_NAME, ResponseStoreRegistry, store_backend_factories,
};
use praxis_ai_store::{BackendError, EffectiveConfigKey, StoreBackendFactory, StoreRegistry};
use praxis_ai_store_lifecycle::{BackendCache, BackendLease, ProvisionError, StoreRef};
use praxis_core::config::{Config, FilterEntry};
use praxis_filter::FilterPipeline;
use tokio::sync::{Mutex as AsyncMutex, mpsc, watch};
use tracing::{error, info};

use crate::store_config::find_listener_store_configs;

/// Poll interval while an old pipeline generation drains in-flight requests.
const PIPELINE_DRAIN_POLL: Duration = Duration::from_millis(10);

/// Readiness of the configured response-store backends.
///
/// One aggregate state across every configured listener: `Ready` only once all
/// hold a provisioned backend. Observers read it through a [`StoreReadinessHandle`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StoreReadiness {
    /// Provisioning has not yet completed for every listener.
    Pending,
    /// The initial generation failed terminally. A later valid reload can
    /// provision a replacement generation without restarting the process.
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

    /// Await until provisioning either succeeds or fails terminally. Returns at
    /// once when already settled, and also returns if the provisioner stops so
    /// a caller cannot block forever. Inspect [`Self::current`] for the result.
    pub async fn wait_ready(&mut self) {
        // Err only if the sender drops while still pending. Either way, stop
        // waiting. The borrowed guard is released at once.
        let _settled = self.rx.wait_for(|s| *s != StoreReadiness::Pending).await;
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

/// Build the response-store reference from a response-store filter's config.
///
/// The `backend` field selects the factory. The rest is the inline config the
/// factory parses. Returns `None` for a config missing a string `backend` or one
/// that cannot map to JSON.
fn response_store_ref(filter_config: &serde_yaml::Value) -> Option<StoreRef> {
    let backend = filter_config.get("backend").and_then(serde_yaml::Value::as_str)?;
    let mut config = serde_json::to_value(filter_config).ok()?;
    config.as_object_mut()?.remove("backend");
    normalize_sqlite_factory_config(backend, &mut config);
    Some(StoreRef {
        name: Arc::from(DEFAULT_STORE_NAME),
        backend_id: Arc::from(backend),
        config,
    })
}

/// Build the conversations-store reference from a conversations filter's config.
///
/// The apis layer owns the table defaults and the generated (unused)
/// responses-table name the combined backend requires. The store is registered
/// under its own name; the lifecycle cache still shares one backend with the
/// response store when their effective configs match.
#[cfg(feature = "openai-conversations")]
fn conversations_store_ref(filter_config: &serde_yaml::Value) -> Option<StoreRef> {
    match conversations_store_ref_config(filter_config) {
        Ok((backend_id, mut config)) => {
            normalize_sqlite_factory_config(&backend_id, &mut config);
            Some(StoreRef {
                name: Arc::from(CONVERSATIONS_STORE_NAME),
                backend_id: Arc::from(backend_id.as_str()),
                config,
            })
        },
        Err(e) => {
            error!(error = %e, "conversations store config could not be prepared for provisioning");
            None
        },
    }
}

/// Remove accepted no-op `PostgreSQL` fields before dispatching to `SQLite`.
///
/// The public filter configs accept explicit false/null defaults for backward
/// compatibility, while the backend factory intentionally rejects fields that
/// are not part of its SQLite schema. Preserve that strict factory boundary by
/// removing only values that filter validation has already established as
/// no-ops. Non-default values remain and still fail closed.
fn normalize_sqlite_factory_config(backend_id: &str, config: &mut serde_json::Value) {
    if backend_id != "sqlite" {
        return;
    }
    let Some(config) = config.as_object_mut() else {
        return;
    };
    for field in ["ssl_mode", "ssl_root_cert", "ssl_client_cert", "ssl_client_key"] {
        if config.get(field).is_some_and(serde_json::Value::is_null) {
            config.remove(field);
        }
    }
    for field in ["require_certificate_authentication", "allow_private_database_url"] {
        if config.get(field).and_then(serde_json::Value::as_bool) == Some(false) {
            config.remove(field);
        }
    }
}

/// Build a per-listener store plan for every listener whose chains configure a
/// response or conversations store. All reachable filters are retained here so
/// their effective configurations can be checked before duplicate registry
/// names are collapsed.
fn build_listener_store_plans(config: &Config) -> Vec<ListenerStorePlan> {
    let chains: HashMap<&str, &[FilterEntry]> = config
        .filter_chains
        .iter()
        .map(|c| (c.name.as_str(), c.filters.as_slice()))
        .collect();

    let mut plans = Vec::new();
    for listener in &config.listeners {
        let mut refs = Vec::new();
        refs.extend(
            find_listener_store_configs(listener, &chains, RESPONSE_STORE_FILTER_NAME)
                .into_iter()
                .filter_map(|filter_config| response_store_ref(&filter_config)),
        );
        #[cfg(feature = "openai-conversations")]
        refs.extend(
            find_listener_store_configs(listener, &chains, CONVERSATIONS_STORE_FILTER_NAME)
                .into_iter()
                .filter_map(|filter_config| conversations_store_ref(&filter_config)),
        );
        if !refs.is_empty() {
            plans.push(ListenerStorePlan {
                listener: listener.name.clone(),
                registry: StoreRegistry::new(),
                refs,
            });
        }
    }
    plans
}

/// Whether any listener reaches a persisted-state store filter.
pub(crate) fn config_uses_store(config: &Config) -> bool {
    !build_listener_store_plans(config).is_empty()
}

/// Listener names whose live pipelines can hold references to a store
/// generation built from `config`.
pub(crate) fn store_listener_names(config: &Config) -> Vec<String> {
    build_listener_store_plans(config)
        .into_iter()
        .map(|plan| plan.listener)
        .collect()
}

/// Reject one listener binding a registry name to different effective backend
/// configurations, then collapse repeated references to the same backend.
#[expect(
    clippy::too_many_lines,
    reason = "factory resolution, effective-key comparison, and deduplication are one validation pass"
)]
fn validate_and_deduplicate_refs(
    plans: &mut [ListenerStorePlan],
    factories: &[Arc<dyn StoreBackendFactory>],
) -> Result<(), ProvisionError> {
    let factories: HashMap<&str, &dyn StoreBackendFactory> = factories
        .iter()
        .map(|factory| (factory.backend_id(), factory.as_ref()))
        .collect();

    for plan in plans {
        let mut bound: HashMap<Arc<str>, (Arc<str>, EffectiveConfigKey)> = HashMap::new();
        let mut unique = Vec::with_capacity(plan.refs.len());
        for store_ref in plan.refs.drain(..) {
            let factory =
                factories
                    .get(store_ref.backend_id.as_ref())
                    .ok_or_else(|| ProvisionError::UnknownBackend {
                        name: Arc::clone(&store_ref.name),
                        backend_id: Arc::clone(&store_ref.backend_id),
                    })?;
            let effective = factory
                .effective_key(&store_ref.config)
                .map_err(|source| ProvisionError::Backend {
                    name: Arc::clone(&store_ref.name),
                    source,
                })?;
            if let Some((backend_id, existing)) = bound.get(&store_ref.name) {
                if backend_id != &store_ref.backend_id || existing != &effective {
                    return Err(ProvisionError::Backend {
                        name: Arc::clone(&store_ref.name),
                        source: BackendError::Config(format!(
                            "listener '{}' configures conflicting stores for registry name '{}'",
                            plan.listener, store_ref.name
                        )),
                    });
                }
                continue;
            }
            bound.insert(
                Arc::clone(&store_ref.name),
                (Arc::clone(&store_ref.backend_id), effective),
            );
            unique.push(store_ref);
        }
        plan.refs = unique;
    }
    Ok(())
}

/// Map each plan to the [`ResponseStoreRegistry`] installed into its pipeline,
/// sharing the plan's backing storage so provisioning is observed there.
fn registries_map(plans: &[ListenerStorePlan]) -> HashMap<String, ResponseStoreRegistry> {
    plans
        .iter()
        .map(|p| (p.listener.clone(), ResponseStoreRegistry::from(p.registry.clone())))
        .collect()
}

/// The per-listener registries to install into pipelines, the serving-runtime
/// provisioner, its reload handle, and a readiness handle observers await.
pub type StoreWiring = (
    HashMap<String, ResponseStoreRegistry>,
    StoreProvisionService,
    StoreReloadHandle,
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
    let mut plans = build_listener_store_plans(config);
    let factories = store_backend_factories();
    validate_and_deduplicate_refs(&mut plans, &factories)?;
    let registries = registries_map(&plans);
    let cache = Arc::new(BackendCache::new(factories.clone()));
    let refs: Vec<StoreRef> = plans.iter().flat_map(|p| p.refs.iter().cloned()).collect();
    cache.validate(&refs)?;
    let initial_readiness = if plans.is_empty() {
        StoreReadiness::Ready
    } else {
        StoreReadiness::Pending
    };
    let (readiness, rx) = watch::channel(initial_readiness);
    let (commands, command_rx) = mpsc::unbounded_channel();
    Ok((
        registries,
        StoreProvisionService {
            cache,
            factories,
            plans,
            readiness,
            commands: AsyncMutex::new(Some(command_rx)),
        },
        StoreReloadHandle { commands },
        StoreReadinessHandle { rx },
    ))
}

/// A store generation provisioned on the serving runtime but not yet installed
/// into the live pipelines.
pub(crate) struct PreparedStoreReload {
    /// Monotonic generation identifier owned by the provisioner.
    generation: u64,
    /// Ready registries to attach while building the replacement pipelines.
    pub(crate) registries: crate::StoreRegistries,
}

/// Cloneable command handle used by the config-watcher thread.
///
/// Commands cross onto the serving runtime before touching SQL pools. The
/// watcher waits synchronously because it must not swap a pipeline until its
/// replacement generation is fully provisioned.
#[derive(Clone)]
pub struct StoreReloadHandle {
    /// Commands consumed by [`StoreProvisionService`].
    commands: mpsc::UnboundedSender<StoreCommand>,
}

impl Default for StoreReloadHandle {
    fn default() -> Self {
        let (commands, _receiver) = mpsc::unbounded_channel();
        Self { commands }
    }
}

impl StoreReloadHandle {
    /// Provision and validate a replacement store generation.
    pub(crate) fn prepare(&self, config: &Config) -> Result<PreparedStoreReload, String> {
        let (reply, response) = std_mpsc::sync_channel(1);
        self.commands
            .send(StoreCommand::Prepare {
                config: Box::new(config.clone()),
                reply,
            })
            .map_err(|_send_error| "store provisioner stopped before reload preparation".to_owned())?;
        response
            .recv()
            .map_err(|_recv_error| "store provisioner dropped the reload preparation response".to_owned())?
    }

    /// Promote a prepared generation immediately before the replacement
    /// pipelines are published.
    pub(crate) fn commit(
        &self,
        prepared: PreparedStoreReload,
        old_pipelines: Vec<Weak<FilterPipeline>>,
    ) -> Result<(), String> {
        let PreparedStoreReload { generation, registries } = prepared;
        drop(registries);
        let (reply, response) = std_mpsc::sync_channel(1);
        self.commands
            .send(StoreCommand::Commit {
                generation,
                old_pipelines,
                reply,
            })
            .map_err(|_send_error| "store provisioner stopped before reload commit".to_owned())?;
        response
            .recv()
            .map_err(|_recv_error| "store provisioner dropped the reload commit response".to_owned())?
    }

    /// Release a prepared generation whose pipeline build failed.
    pub(crate) fn abort(&self, prepared: PreparedStoreReload) -> Result<(), String> {
        let PreparedStoreReload { generation, registries } = prepared;
        drop(registries);
        let (reply, response) = std_mpsc::sync_channel(1);
        self.commands
            .send(StoreCommand::Abort { generation, reply })
            .map_err(|_send_error| "store provisioner stopped before reload abort".to_owned())?;
        response
            .recv()
            .map_err(|_recv_error| "store provisioner dropped the reload abort response".to_owned())?
    }
}

/// Commands sent from the watcher runtime to the serving runtime.
enum StoreCommand {
    /// Build a candidate generation without disturbing the active one.
    Prepare {
        /// Reloaded proxy configuration.
        config: Box<Config>,
        /// Completion sent after provisioning succeeds or fails.
        reply: std_mpsc::SyncSender<Result<PreparedStoreReload, String>>,
    },
    /// Make a prepared generation active after pipeline swap.
    Commit {
        /// Prepared generation identifier.
        generation: u64,
        /// Weak observers of previous pipelines whose request-held `Arc`s must
        /// drain. Weak references cannot keep each other alive across reloads.
        old_pipelines: Vec<Weak<FilterPipeline>>,
        /// Completion acknowledgement.
        reply: std_mpsc::SyncSender<Result<(), String>>,
    },
    /// Discard a prepared generation after pipeline construction failed.
    Abort {
        /// Prepared generation identifier.
        generation: u64,
        /// Completion acknowledgement.
        reply: std_mpsc::SyncSender<Result<(), String>>,
    },
}

/// Provisions response-store backends on the serving runtime and owns every
/// active or pending generation lease.
pub struct StoreProvisionService {
    /// Process-wide backend cache built from the compiled-in factories.
    cache: Arc<BackendCache>,
    /// Factories used to validate and de-duplicate reload plans.
    factories: Vec<Arc<dyn StoreBackendFactory>>,
    /// Per-listener registries and references to provision.
    plans: Vec<ListenerStorePlan>,
    /// Publishes readiness transitions to observers.
    readiness: watch::Sender<StoreReadiness>,
    /// Single receiver taken when the background service starts.
    commands: AsyncMutex<Option<mpsc::UnboundedReceiver<StoreCommand>>>,
}

impl StoreProvisionService {
    /// Validate one config and construct its fresh per-listener registries.
    fn prepare_plans(&self, config: &Config) -> Result<Vec<ListenerStorePlan>, ProvisionError> {
        let mut plans = build_listener_store_plans(config);
        validate_and_deduplicate_refs(&mut plans, &self.factories)?;
        let refs: Vec<StoreRef> = plans.iter().flat_map(|p| p.refs.iter().cloned()).collect();
        self.cache.validate(&refs)?;
        Ok(plans)
    }

    /// Provision all listeners concurrently as one atomic generation.
    ///
    /// [`BackendCache`] already applies the bounded transient retry budget. Any
    /// error returned here, including [`BackendError::Unavailable`], is terminal
    /// for this generation and must not be retried by the server.
    #[expect(
        clippy::too_many_lines,
        reason = "concurrent provisioning, rollback, and atomic publication form one operation"
    )]
    async fn provision_all(&self, plans: &[ListenerStorePlan]) -> Result<Vec<BackendLease>, ProvisionError> {
        let mut workers = plans
            .iter()
            .map(|plan| async move {
                let result = self.cache.provision_into(&plan.refs, &plan.registry).await;
                (plan, result)
            })
            .collect::<FuturesUnordered<_>>();
        let mut leases = Vec::with_capacity(plans.len());
        let mut failure = None;

        while let Some((plan, result)) = workers.next().await {
            match result {
                Ok(lease) => {
                    info!(listener = %plan.listener, "persisted-state stores provisioned");
                    leases.push(lease);
                },
                Err(e) => {
                    error!(
                        listener = %plan.listener,
                        error = %e,
                        "response store provisioning failed permanently; not retrying",
                    );
                    if failure.is_none() {
                        failure = Some(e);
                    }
                },
            }
        }

        if let Some(error) = failure {
            release_leases(leases).await;
            return Err(error);
        }

        for plan in plans {
            plan.registry.mark_ready();
        }
        Ok(leases)
    }
}

#[async_trait]
impl BackgroundService for StoreProvisionService {
    #[expect(
        clippy::too_many_lines,
        reason = "startup and reload commands share one serving-runtime lease owner"
    )]
    async fn start(&self, mut shutdown: ShutdownWatch) {
        let Some(mut commands) = self.commands.lock().await.take() else {
            error!("store provisioner command receiver was already taken");
            return;
        };
        let initial_provisioning = self.provision_all(&self.plans);
        tokio::pin!(initial_provisioning);
        let initial_result = tokio::select! {
            result = &mut initial_provisioning => result,
            _ = shutdown.changed() => return,
        };
        let mut active_leases = if let Ok(leases) = initial_result {
            let _sent = self.readiness.send(StoreReadiness::Ready);
            if !self.plans.is_empty() {
                info!("all persisted-state stores provisioned");
            }
            leases
        } else {
            let _sent = self.readiness.send(StoreReadiness::Failed);
            Vec::new()
        };
        let mut pending: HashMap<u64, Vec<BackendLease>> = HashMap::new();
        let mut next_generation = 1_u64;

        loop {
            tokio::select! {
                _ = shutdown.changed() => break,
                command = commands.recv() => {
                    let Some(command) = command else { break };
                    match command {
                        StoreCommand::Prepare { config, reply } => {
                            let result = match self.prepare_plans(&config) {
                                Ok(plans) => {
                                    let provisioning = self.provision_all(&plans);
                                    tokio::pin!(provisioning);
                                    let provisioned = tokio::select! {
                                        result = &mut provisioning => result,
                                        _ = shutdown.changed() => {
                                            let _sent = reply.send(Err(
                                                "store provisioner stopped during reload preparation".to_owned(),
                                            ));
                                            break;
                                        },
                                    };
                                    match provisioned {
                                        Ok(leases) => {
                                            let generation = next_generation;
                                            next_generation = next_generation.saturating_add(1);
                                            let registries = registries_map(&plans);
                                            pending.insert(generation, leases);
                                            Ok(PreparedStoreReload { generation, registries })
                                        },
                                        Err(e) => Err(e.to_string()),
                                    }
                                },
                                Err(e) => Err(e.to_string()),
                            };
                            let _sent = reply.send(result);
                        },
                        StoreCommand::Commit { generation, old_pipelines, reply } => {
                            let result = if let Some(new_leases) = pending.remove(&generation) {
                                let old_leases = std::mem::replace(&mut active_leases, new_leases);
                                tokio::spawn(release_after_drain(old_pipelines, old_leases));
                                let _sent = self.readiness.send(StoreReadiness::Ready);
                                Ok(())
                            } else {
                                Err(format!("unknown prepared store generation {generation}"))
                            };
                            let _sent = reply.send(result);
                        },
                        StoreCommand::Abort { generation, reply } => {
                            let result = if let Some(leases) = pending.remove(&generation) {
                                release_leases(leases).await;
                                Ok(())
                            } else {
                                Err(format!("unknown prepared store generation {generation}"))
                            };
                            let _sent = reply.send(result);
                        },
                    }
                },
            }
        }

        // Pending generations were never attached to request paths and can be
        // retired immediately. Active leases intentionally remain held through
        // Pingora's shutdown drain and disappear with the serving runtime.
        for (_, leases) in pending {
            release_leases(leases).await;
        }
        drop(active_leases);
    }
}

/// Release every lease in a generation on the serving runtime.
async fn release_leases(leases: Vec<BackendLease>) {
    for lease in leases {
        lease.release().await;
    }
}

/// Retain the old backend generation until every pipeline owner and request-held
/// `Arc` has drained, then retire backends no newer generation references.
async fn release_after_drain(old_pipelines: Vec<Weak<FilterPipeline>>, leases: Vec<BackendLease>) {
    while old_pipelines.iter().any(|pipeline| pipeline.strong_count() > 0) {
        tokio::time::sleep(PIPELINE_DRAIN_POLL).await;
    }
    release_leases(leases).await;
}

#[cfg(test)]
#[expect(clippy::allow_attributes, reason = "blanket test suppressions")]
#[allow(clippy::expect_used, clippy::too_many_lines, reason = "tests")]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use praxis_ai_store::{
        BackendError, EffectiveConfigKey, ProvisionedBackend, RetireBackend, StoreBackendFactory, memory::InMemoryStore,
    };
    use praxis_filter::FilterRegistry;
    use serde_json::json;

    use super::*;

    struct CountingRetire(Arc<AtomicUsize>);

    #[async_trait]
    impl RetireBackend for CountingRetire {
        async fn retire(&self) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }

    struct FakeFactory {
        retires: Arc<AtomicUsize>,
    }

    #[async_trait]
    impl StoreBackendFactory for FakeFactory {
        fn backend_id(&self) -> &str {
            "fake"
        }

        fn effective_key(&self, _config: &serde_json::Value) -> Result<EffectiveConfigKey, BackendError> {
            Ok(EffectiveConfigKey::new("fake"))
        }

        async fn build(&self, _config: &serde_json::Value) -> Result<ProvisionedBackend, BackendError> {
            Ok(ProvisionedBackend {
                backend: Arc::new(InMemoryStore::new()),
                retire: Arc::new(CountingRetire(Arc::clone(&self.retires))),
            })
        }
    }

    struct UnavailableFactory {
        builds: Arc<AtomicUsize>,
    }

    #[async_trait]
    impl StoreBackendFactory for UnavailableFactory {
        fn backend_id(&self) -> &str {
            "unavailable"
        }

        fn effective_key(&self, _config: &serde_json::Value) -> Result<EffectiveConfigKey, BackendError> {
            Ok(EffectiveConfigKey::new("unavailable"))
        }

        async fn build(&self, _config: &serde_json::Value) -> Result<ProvisionedBackend, BackendError> {
            self.builds.fetch_add(1, Ordering::SeqCst);
            Err(BackendError::Unavailable("test backend is down".to_owned()))
        }
    }

    struct RecoveringFactory {
        builds: Arc<AtomicUsize>,
        retires: Arc<AtomicUsize>,
    }

    struct HangingFactory {
        entered: Arc<tokio::sync::Notify>,
    }

    struct ReloadFactory {
        builds: Arc<AtomicUsize>,
        retires: Arc<AtomicUsize>,
    }

    #[async_trait]
    impl StoreBackendFactory for HangingFactory {
        fn backend_id(&self) -> &str {
            "hanging"
        }

        fn effective_key(&self, _config: &serde_json::Value) -> Result<EffectiveConfigKey, BackendError> {
            Ok(EffectiveConfigKey::new("hanging"))
        }

        async fn build(&self, _config: &serde_json::Value) -> Result<ProvisionedBackend, BackendError> {
            self.entered.notify_one();
            std::future::pending().await
        }
    }

    #[async_trait]
    impl StoreBackendFactory for ReloadFactory {
        fn backend_id(&self) -> &str {
            "reload"
        }

        fn effective_key(&self, config: &serde_json::Value) -> Result<EffectiveConfigKey, BackendError> {
            let url = config
                .get("database_url")
                .and_then(serde_json::Value::as_str)
                .ok_or_else(|| BackendError::Config("missing database_url".to_owned()))?;
            Ok(EffectiveConfigKey::new(url))
        }

        async fn build(&self, _config: &serde_json::Value) -> Result<ProvisionedBackend, BackendError> {
            self.builds.fetch_add(1, Ordering::SeqCst);
            Ok(ProvisionedBackend {
                backend: Arc::new(InMemoryStore::new()),
                retire: Arc::new(CountingRetire(Arc::clone(&self.retires))),
            })
        }
    }

    #[async_trait]
    impl StoreBackendFactory for RecoveringFactory {
        fn backend_id(&self) -> &str {
            "recovering"
        }

        fn effective_key(&self, _config: &serde_json::Value) -> Result<EffectiveConfigKey, BackendError> {
            Ok(EffectiveConfigKey::new("recovering"))
        }

        async fn build(&self, _config: &serde_json::Value) -> Result<ProvisionedBackend, BackendError> {
            let attempt = self.builds.fetch_add(1, Ordering::SeqCst);
            if attempt < 2 {
                return Err(BackendError::Transient("test backend has not recovered yet".to_owned()));
            }
            Ok(ProvisionedBackend {
                backend: Arc::new(InMemoryStore::new()),
                retire: Arc::new(CountingRetire(Arc::clone(&self.retires))),
            })
        }
    }

    fn conditional_store_config(backend: &str, first_url: &str, second_url: &str) -> Config {
        Config::from_yaml(&format!(
            r#"
listeners:
  - name: web
    address: "127.0.0.1:8080"
    filter_chains: [main]
filter_chains:
  - name: main
    filters:
      - filter: router
        branch_chains:
          - name: first
            chains:
              - name: first-store
                filters:
                  - filter: openai_response_store
                    backend: {backend}
                    database_url: "{first_url}"
                    responses_table: responses
                    conversations_table: conversations
          - name: second
            chains:
              - name: second-store
                filters:
                  - filter: openai_response_store
                    backend: {backend}
                    database_url: "{second_url}"
                    responses_table: responses
                    conversations_table: conversations
"#
        ))
        .expect("conditional store config")
    }

    fn single_store_config(backend: &str, database_url: &str) -> Config {
        Config::from_yaml(&format!(
            r#"
listeners:
  - name: web
    address: "127.0.0.1:8080"
    filter_chains: [main]
filter_chains:
  - name: main
    filters:
      - filter: openai_response_store
        backend: {backend}
        database_url: "{database_url}"
        responses_table: responses
        conversations_table: conversations
"#
        ))
        .expect("single store config")
    }

    #[test]
    fn store_listener_names_exclude_stateless_listeners() {
        let config = Config::from_yaml(
            r#"
listeners:
  - name: stateful
    address: "127.0.0.1:8080"
    filter_chains: [store]
  - name: stateless
    address: "127.0.0.1:8081"
    filter_chains: [plain]
filter_chains:
  - name: store
    filters:
      - filter: openai_response_store
        backend: sqlite
        database_url: "sqlite::memory:"
        responses_table: responses
        conversations_table: conversations
  - name: plain
    filters: []
"#,
        )
        .expect("mixed listener config");

        assert_eq!(store_listener_names(&config), ["stateful"]);
    }

    #[tokio::test]
    async fn wait_ready_returns_after_terminal_failure() {
        let (readiness, rx) = watch::channel(StoreReadiness::Pending);
        let mut handle = StoreReadinessHandle { rx };

        readiness.send(StoreReadiness::Failed).expect("readiness observer");
        tokio::time::timeout(Duration::from_millis(100), handle.wait_ready())
            .await
            .expect("terminal provisioning failure must unblock readiness waiters");
        assert_eq!(handle.current(), StoreReadiness::Failed);
    }

    #[test]
    fn conflicting_conditional_store_configs_are_rejected() {
        #[cfg(feature = "store-sqlite")]
        let config = conditional_store_config("sqlite", "sqlite:///first.db", "sqlite:///second.db");
        #[cfg(all(not(feature = "store-sqlite"), feature = "store-postgres"))]
        let config = conditional_store_config(
            "postgres",
            "postgresql://user:password@8.8.8.8/store",
            "postgresql://user:password@1.1.1.1/store",
        );
        let result = build_store_wiring(&config);

        assert!(
            matches!(
                result,
                Err(ProvisionError::Backend {
                    source: BackendError::Config(message),
                    ..
                }) if message.contains("conflicting stores")
            ),
            "one registry name must not silently select the first branch's backend"
        );
    }

    #[test]
    fn identical_conditional_store_configs_share_one_reference() {
        #[cfg(feature = "store-sqlite")]
        let config = conditional_store_config("sqlite", "sqlite:///shared.db", "sqlite:///shared.db");
        #[cfg(all(not(feature = "store-sqlite"), feature = "store-postgres"))]
        let config = conditional_store_config(
            "postgres",
            "postgresql://user:password@8.8.8.8/store",
            "postgresql://user:password@8.8.8.8/store",
        );
        let (registries, provisioner, _reload, _readiness) = build_store_wiring(&config).expect("identical stores");

        assert_eq!(registries.len(), 1);
        assert_eq!(provisioner.plans.len(), 1);
        assert_eq!(provisioner.plans.first().expect("listener plan").refs.len(), 1);
    }

    #[test]
    fn sqlite_normalization_removes_only_accepted_noop_fields() {
        let mut config = json!({
            "ssl_mode": null,
            "ssl_root_cert": null,
            "ssl_client_cert": null,
            "ssl_client_key": null,
            "require_certificate_authentication": false,
            "allow_private_database_url": false,
            "database_url": "sqlite::memory:",
        });

        normalize_sqlite_factory_config("sqlite", &mut config);

        let config = config.as_object().expect("object");
        assert_eq!(config.len(), 1);
        assert_eq!(config.get("database_url"), Some(&json!("sqlite::memory:")));
    }

    #[test]
    fn sqlite_normalization_preserves_rejected_nondefault_fields() {
        let mut config = json!({
            "ssl_mode": "verify_full",
            "require_certificate_authentication": true,
            "allow_private_database_url": true,
        });

        normalize_sqlite_factory_config("sqlite", &mut config);

        let config = config.as_object().expect("object");
        assert_eq!(config.get("ssl_mode"), Some(&json!("verify_full")));
        assert_eq!(config.get("require_certificate_authentication"), Some(&json!(true)));
        assert_eq!(config.get("allow_private_database_url"), Some(&json!(true)));
    }

    #[tokio::test]
    async fn ready_backend_is_not_retired_on_initial_shutdown_signal() {
        let retires = Arc::new(AtomicUsize::new(0));
        let factory: Arc<dyn StoreBackendFactory> = Arc::new(FakeFactory {
            retires: Arc::clone(&retires),
        });
        let cache = Arc::new(BackendCache::new(vec![factory]));
        let registry = StoreRegistry::new();
        let (readiness, mut readiness_rx) = watch::channel(StoreReadiness::Pending);
        let (_commands, command_rx) = mpsc::unbounded_channel();
        let service = Arc::new(StoreProvisionService {
            cache,
            factories: Vec::new(),
            plans: vec![ListenerStorePlan {
                listener: "web".to_owned(),
                registry,
                refs: vec![StoreRef {
                    name: Arc::from("default"),
                    backend_id: Arc::from("fake"),
                    config: json!({}),
                }],
            }],
            readiness,
            commands: AsyncMutex::new(Some(command_rx)),
        });
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let running = tokio::spawn({
            let service = Arc::clone(&service);
            async move { service.start(shutdown_rx).await }
        });

        readiness_rx
            .wait_for(|state| *state == StoreReadiness::Ready)
            .await
            .expect("service should become ready");
        shutdown_tx.send(true).expect("service should still receive shutdown");
        running.await.expect("provisioner task should stop");

        assert_eq!(
            retires.load(Ordering::SeqCst),
            0,
            "the initial shutdown signal precedes request draining"
        );
    }

    #[tokio::test]
    async fn permanent_failure_is_not_retried_or_partially_published() {
        let builds = Arc::new(AtomicUsize::new(0));
        let retires = Arc::new(AtomicUsize::new(0));
        let factories: Vec<Arc<dyn StoreBackendFactory>> = vec![
            Arc::new(UnavailableFactory {
                builds: Arc::clone(&builds),
            }),
            Arc::new(FakeFactory {
                retires: Arc::clone(&retires),
            }),
        ];
        let failed_registry = StoreRegistry::new();
        let healthy_registry = StoreRegistry::new();
        let (readiness, mut readiness_rx) = watch::channel(StoreReadiness::Pending);
        let (_commands, command_rx) = mpsc::unbounded_channel();
        let service = Arc::new(StoreProvisionService {
            cache: Arc::new(BackendCache::new(factories)),
            factories: Vec::new(),
            plans: vec![
                ListenerStorePlan {
                    listener: "failed".to_owned(),
                    registry: failed_registry.clone(),
                    refs: vec![StoreRef {
                        name: Arc::from("default"),
                        backend_id: Arc::from("unavailable"),
                        config: json!({}),
                    }],
                },
                ListenerStorePlan {
                    listener: "healthy".to_owned(),
                    registry: healthy_registry.clone(),
                    refs: vec![StoreRef {
                        name: Arc::from("default"),
                        backend_id: Arc::from("fake"),
                        config: json!({}),
                    }],
                },
            ],
            readiness,
            commands: AsyncMutex::new(Some(command_rx)),
        });
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let running = tokio::spawn({
            let service = Arc::clone(&service);
            async move { service.start(shutdown_rx).await }
        });

        tokio::time::timeout(
            Duration::from_secs(1),
            readiness_rx.wait_for(|state| *state == StoreReadiness::Failed),
        )
        .await
        .expect("permanent failure should be published")
        .expect("provisioner should remain alive for a correcting reload");
        assert_eq!(
            builds.load(Ordering::SeqCst),
            1,
            "terminal unavailable failures must not be retried by the server"
        );
        assert!(!failed_registry.is_ready());
        assert!(
            !healthy_registry.is_ready(),
            "a failed generation must not publish partially"
        );

        shutdown_tx.send(true).expect("service should still receive shutdown");
        running.await.expect("provisioner task should stop");
        assert_eq!(
            retires.load(Ordering::SeqCst),
            1,
            "a successfully provisioned sibling must be retired when the generation fails"
        );
    }

    #[tokio::test]
    async fn transient_failure_recovers_within_cache_retry_budget() {
        let builds = Arc::new(AtomicUsize::new(0));
        let retires = Arc::new(AtomicUsize::new(0));
        let factories: Vec<Arc<dyn StoreBackendFactory>> = vec![Arc::new(RecoveringFactory {
            builds: Arc::clone(&builds),
            retires: Arc::clone(&retires),
        })];
        let recovered_registry = StoreRegistry::new();
        let (readiness, _readiness_rx) = watch::channel(StoreReadiness::Pending);
        let (_commands, command_rx) = mpsc::unbounded_channel();
        let service = Arc::new(StoreProvisionService {
            cache: Arc::new(BackendCache::new(factories)),
            factories: Vec::new(),
            plans: vec![ListenerStorePlan {
                listener: "recovering".to_owned(),
                registry: recovered_registry.clone(),
                refs: vec![StoreRef {
                    name: Arc::from("default"),
                    backend_id: Arc::from("recovering"),
                    config: json!({}),
                }],
            }],
            readiness,
            commands: AsyncMutex::new(Some(command_rx)),
        });
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let running = tokio::spawn({
            let service = Arc::clone(&service);
            async move { service.start(shutdown_rx).await }
        });

        tokio::time::timeout(Duration::from_millis(500), async {
            while !recovered_registry.is_ready() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("transient failure should recover within the cache retry budget");
        assert_eq!(builds.load(Ordering::SeqCst), 3);

        shutdown_tx.send(true).expect("service should still receive shutdown");
        running.await.expect("provisioner task should stop");
        assert_eq!(retires.load(Ordering::SeqCst), 0);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn shutdown_cancels_reload_preparation_and_unblocks_watcher() {
        let entered = Arc::new(tokio::sync::Notify::new());
        let factory: Arc<dyn StoreBackendFactory> = Arc::new(HangingFactory {
            entered: Arc::clone(&entered),
        });
        let (readiness, _readiness_rx) = watch::channel(StoreReadiness::Ready);
        let (commands, command_rx) = mpsc::unbounded_channel();
        let handle = StoreReloadHandle { commands };
        let service = Arc::new(StoreProvisionService {
            cache: Arc::new(BackendCache::new(vec![Arc::clone(&factory)])),
            factories: vec![factory],
            plans: Vec::new(),
            readiness,
            commands: AsyncMutex::new(Some(command_rx)),
        });
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let running = tokio::spawn({
            let service = Arc::clone(&service);
            async move { service.start(shutdown_rx).await }
        });
        let config = single_store_config("hanging", "unused");
        let preparing = tokio::task::spawn_blocking(move || handle.prepare(&config));

        tokio::time::timeout(Duration::from_secs(1), entered.notified())
            .await
            .expect("replacement provisioning should start");
        shutdown_tx.send(true).expect("service should receive shutdown");
        let result = tokio::time::timeout(Duration::from_secs(1), preparing)
            .await
            .expect("watcher must not remain blocked by backend initialization")
            .expect("prepare task");
        assert!(result.is_err(), "shutdown must reject the pending reload");
        let error = result.err().expect("error checked above");
        assert!(error.contains("stopped during reload preparation"));
        tokio::time::timeout(Duration::from_secs(1), running)
            .await
            .expect("provisioner should stop promptly")
            .expect("provisioner task");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn reload_builds_swaps_drains_and_retires_store_generation() {
        let builds = Arc::new(AtomicUsize::new(0));
        let retires = Arc::new(AtomicUsize::new(0));
        let factory: Arc<dyn StoreBackendFactory> = Arc::new(ReloadFactory {
            builds: Arc::clone(&builds),
            retires: Arc::clone(&retires),
        });
        let factories = vec![Arc::clone(&factory)];
        let initial_config = single_store_config("reload", "initial");
        let mut plans = build_listener_store_plans(&initial_config);
        validate_and_deduplicate_refs(&mut plans, &factories).expect("initial plans");
        let cache = Arc::new(BackendCache::new(factories.clone()));
        let (readiness, mut readiness_rx) = watch::channel(StoreReadiness::Pending);
        let (commands, command_rx) = mpsc::unbounded_channel();
        let handle = StoreReloadHandle { commands };
        let service = Arc::new(StoreProvisionService {
            cache,
            factories,
            plans,
            readiness,
            commands: AsyncMutex::new(Some(command_rx)),
        });
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let running = tokio::spawn({
            let service = Arc::clone(&service);
            async move { service.start(shutdown_rx).await }
        });

        readiness_rx
            .wait_for(|state| *state == StoreReadiness::Ready)
            .await
            .expect("initial generation should become ready");
        assert_eq!(builds.load(Ordering::SeqCst), 1);

        let next_config = single_store_config("reload", "replacement");
        let prepared = tokio::task::spawn_blocking({
            let handle = handle.clone();
            move || handle.prepare(&next_config)
        })
        .await
        .expect("prepare task")
        .expect("replacement generation should provision");
        assert_eq!(builds.load(Ordering::SeqCst), 2);
        assert!(
            prepared
                .registries
                .get("web")
                .is_some_and(ResponseStoreRegistry::is_ready),
            "replacement registry must be ready before pipeline swap"
        );

        let registry = FilterRegistry::with_builtins();
        let mut entries = [];
        let old_pipeline = Arc::new(FilterPipeline::build(&mut entries, &registry).expect("old pipeline"));
        let in_flight = Arc::clone(&old_pipeline);
        let old_pipeline_observer = Arc::downgrade(&old_pipeline);
        tokio::task::spawn_blocking({
            let handle = handle.clone();
            move || handle.commit(prepared, vec![old_pipeline_observer])
        })
        .await
        .expect("commit task")
        .expect("replacement generation should commit");
        drop(old_pipeline);

        tokio::time::sleep(Duration::from_millis(30)).await;
        assert_eq!(
            retires.load(Ordering::SeqCst),
            0,
            "old backend must remain live while an in-flight request holds its pipeline"
        );
        drop(in_flight);
        tokio::time::timeout(Duration::from_secs(1), async {
            while retires.load(Ordering::SeqCst) != 1 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("old backend should retire after request drain");

        shutdown_tx.send(true).expect("service should still receive shutdown");
        running.await.expect("provisioner task should stop");
        assert_eq!(
            retires.load(Ordering::SeqCst),
            1,
            "active generation retires after runtime drain"
        );
    }

    #[tokio::test]
    async fn concurrent_drain_observers_do_not_keep_a_pipeline_alive() {
        let builds = Arc::new(AtomicUsize::new(0));
        let retires = Arc::new(AtomicUsize::new(0));
        let factory: Arc<dyn StoreBackendFactory> = Arc::new(ReloadFactory {
            builds,
            retires: Arc::clone(&retires),
        });
        let cache = BackendCache::new(vec![factory]);
        let first = cache
            .provision(&[StoreRef {
                name: Arc::from("default"),
                backend_id: Arc::from("reload"),
                config: json!({"database_url": "first"}),
            }])
            .await
            .expect("first generation");
        let second = cache
            .provision(&[StoreRef {
                name: Arc::from("default"),
                backend_id: Arc::from("reload"),
                config: json!({"database_url": "second"}),
            }])
            .await
            .expect("second generation");

        let registry = FilterRegistry::with_builtins();
        let pipeline = Arc::new(FilterPipeline::build(&mut [], &registry).expect("pipeline"));
        let observer = Arc::downgrade(&pipeline);
        let first_release = tokio::spawn(release_after_drain(vec![Weak::clone(&observer)], vec![first.lease]));
        let second_release = tokio::spawn(release_after_drain(vec![observer], vec![second.lease]));

        drop(pipeline);
        tokio::time::timeout(Duration::from_secs(1), async {
            first_release.await.expect("first release task");
            second_release.await.expect("second release task");
        })
        .await
        .expect("weak observers must not block one another");
        assert_eq!(retires.load(Ordering::SeqCst), 2);
    }
}

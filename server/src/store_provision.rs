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
use tokio::sync::watch;
use tracing::{error, info};

use crate::store_config::find_listener_store_configs;

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
    let mut plans = build_listener_store_plans(config);
    let factories = store_backend_factories();
    validate_and_deduplicate_refs(&mut plans, &factories)?;
    let registries = registries_map(&plans);
    if plans.is_empty() {
        return Ok((registries, None, StoreReadinessHandle::ready()));
    }
    let cache = Arc::new(BackendCache::new(factories));
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

/// Terminal result from one listener's independent provisioning worker.
enum ListenerProvisionOutcome {
    /// The listener owns a live backend lease.
    Provisioned(BackendLease),
    /// The listener configuration cannot succeed without a restart.
    PermanentFailure,
    /// Shutdown arrived between attempts.
    Shutdown,
}

impl StoreProvisionService {
    /// Provision one listener, with a retry clock independent of every sibling.
    #[expect(
        clippy::cognitive_complexity,
        clippy::too_many_lines,
        reason = "retry classification, logging, and cancellation form one listener lifecycle"
    )]
    async fn provision_listener(
        &self,
        plan: &ListenerStorePlan,
        mut shutdown: ShutdownWatch,
    ) -> ListenerProvisionOutcome {
        let mut backoff = PROVISION_RETRY_INITIAL;
        loop {
            match self.cache.provision_into(&plan.refs, &plan.registry).await {
                Ok(lease) => {
                    info!(listener = %plan.listener, "persisted-state stores provisioned");
                    plan.registry.mark_ready();
                    return ListenerProvisionOutcome::Provisioned(lease);
                },
                Err(e) => {
                    let _sent = self.readiness.send(StoreReadiness::Failed);
                    if matches!(
                        e,
                        ProvisionError::UnknownBackend { .. }
                            | ProvisionError::Backend {
                                source: BackendError::Config(_),
                                ..
                            }
                    ) {
                        error!(
                            listener = %plan.listener,
                            error = %e,
                            "response store provisioning failed permanently; not retrying",
                        );
                        return ListenerProvisionOutcome::PermanentFailure;
                    }
                    error!(
                        listener = %plan.listener,
                        error = %e,
                        backoff_ms = backoff.as_millis(),
                        "response store provisioning failed; retrying",
                    );
                },
            }
            tokio::select! {
                () = tokio::time::sleep(backoff) => {},
                _ = shutdown.changed() => return ListenerProvisionOutcome::Shutdown,
            }
            backoff = backoff.saturating_mul(2).min(PROVISION_RETRY_MAX);
        }
    }

    /// Run one independent worker per listener and retain every successful lease.
    async fn provision_all(&self, leases: &mut Vec<BackendLease>, shutdown: &mut ShutdownWatch) -> bool {
        let mut workers = self
            .plans
            .iter()
            .map(|plan| {
                let listener_shutdown = shutdown.clone();
                async move { self.provision_listener(plan, listener_shutdown).await }
            })
            .collect::<FuturesUnordered<_>>();
        let mut permanent_failure = false;

        while let Some(outcome) = workers.next().await {
            match outcome {
                ListenerProvisionOutcome::Provisioned(lease) => leases.push(lease),
                ListenerProvisionOutcome::PermanentFailure => permanent_failure = true,
                ListenerProvisionOutcome::Shutdown => return false,
            }
        }

        if permanent_failure {
            // Healthy listeners remain usable even though aggregate readiness
            // cannot become Ready. Keep their leases until request draining.
            let _changed = shutdown.changed().await;
            return false;
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
        if !self.provision_all(&mut leases, &mut shutdown).await {
            // Aggregate readiness does not prevent an independently healthy
            // listener from serving. As on the Ready path, leave its backend
            // alive until the serving runtime has drained requests and drops.
            return;
        }

        let _sent = self.readiness.send(StoreReadiness::Ready);
        info!("all persisted-state stores provisioned");
        let _changed = shutdown.changed().await;

        // Pingora broadcasts shutdown before its grace period and in-flight
        // request drain. Do not call BackendLease::release here: retiring a SQL
        // backend closes its pool and would break requests still persisting
        // state. Dropping these lease handles leaves their cache refcounts
        // intact; the service-owned cache, backends, and pools then drop with
        // the serving runtime after request draining completes. The lease
        // handles now fall out of scope without invoking async retirement.
    }
}

#[cfg(test)]
#[expect(clippy::allow_attributes, reason = "blanket test suppressions")]
#[allow(clippy::expect_used, clippy::too_many_lines, reason = "tests")]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use praxis_ai_store::{
        BackendError, EffectiveConfigKey, ProvisionedBackend, RetireBackend, StoreBackendFactory, memory::InMemoryStore,
    };
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

    struct SlowUnavailableFactory;

    #[async_trait]
    impl StoreBackendFactory for SlowUnavailableFactory {
        fn backend_id(&self) -> &str {
            "slow-unavailable"
        }

        fn effective_key(&self, _config: &serde_json::Value) -> Result<EffectiveConfigKey, BackendError> {
            Ok(EffectiveConfigKey::new("slow-unavailable"))
        }

        async fn build(&self, _config: &serde_json::Value) -> Result<ProvisionedBackend, BackendError> {
            tokio::time::sleep(Duration::from_millis(250)).await;
            Err(BackendError::Unavailable("test backend is slow and down".to_owned()))
        }
    }

    struct RecoveringFactory {
        builds: Arc<AtomicUsize>,
        retires: Arc<AtomicUsize>,
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
            if attempt < 3 {
                return Err(BackendError::Unavailable(
                    "test backend has not recovered yet".to_owned(),
                ));
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
        let (registries, provisioner, _readiness) = build_store_wiring(&config).expect("identical stores");

        assert_eq!(registries.len(), 1);
        let provisioner = provisioner.expect("store provisioner");
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
        let service = Arc::new(StoreProvisionService {
            cache,
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
    async fn failed_listener_does_not_starve_a_later_healthy_listener() {
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
        let (readiness, readiness_rx) = watch::channel(StoreReadiness::Pending);
        let service = Arc::new(StoreProvisionService {
            cache: Arc::new(BackendCache::new(factories)),
            plans: vec![
                ListenerStorePlan {
                    listener: "failed".to_owned(),
                    registry: failed_registry,
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
        });
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let running = tokio::spawn({
            let service = Arc::clone(&service);
            async move { service.start(shutdown_rx).await }
        });

        tokio::time::timeout(Duration::from_secs(1), async {
            while !healthy_registry.is_ready() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("healthy listener should be provisioned despite the earlier failure");
        assert!(builds.load(Ordering::SeqCst) > 0);
        assert_eq!(*readiness_rx.borrow(), StoreReadiness::Failed);

        shutdown_tx.send(true).expect("service should still receive shutdown");
        running.await.expect("provisioner task should stop");
        assert_eq!(retires.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn slow_listener_does_not_delay_a_later_healthy_listener() {
        let retires = Arc::new(AtomicUsize::new(0));
        let factories: Vec<Arc<dyn StoreBackendFactory>> = vec![
            Arc::new(SlowUnavailableFactory),
            Arc::new(FakeFactory {
                retires: Arc::clone(&retires),
            }),
        ];
        let healthy_registry = StoreRegistry::new();
        let (readiness, _readiness_rx) = watch::channel(StoreReadiness::Pending);
        let service = Arc::new(StoreProvisionService {
            cache: Arc::new(BackendCache::new(factories)),
            plans: vec![
                ListenerStorePlan {
                    listener: "slow".to_owned(),
                    registry: StoreRegistry::new(),
                    refs: vec![StoreRef {
                        name: Arc::from("default"),
                        backend_id: Arc::from("slow-unavailable"),
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
        });
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let running = tokio::spawn({
            let service = Arc::clone(&service);
            async move { service.start(shutdown_rx).await }
        });

        tokio::time::timeout(Duration::from_millis(200), async {
            while !healthy_registry.is_ready() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("healthy listener should not wait for the slow listener's factory retries");

        shutdown_tx.send(true).expect("service should still receive shutdown");
        tokio::time::timeout(Duration::from_secs(2), running)
            .await
            .expect("slow attempt should finish within its bounded test delay")
            .expect("provisioner task should stop");
        assert_eq!(retires.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn recovered_listener_retries_while_a_sibling_attempt_is_still_pending() {
        let builds = Arc::new(AtomicUsize::new(0));
        let retires = Arc::new(AtomicUsize::new(0));
        let factories: Vec<Arc<dyn StoreBackendFactory>> = vec![
            Arc::new(SlowUnavailableFactory),
            Arc::new(RecoveringFactory {
                builds: Arc::clone(&builds),
                retires: Arc::clone(&retires),
            }),
        ];
        let recovered_registry = StoreRegistry::new();
        let (readiness, _readiness_rx) = watch::channel(StoreReadiness::Pending);
        let service = Arc::new(StoreProvisionService {
            cache: Arc::new(BackendCache::new(factories)),
            plans: vec![
                ListenerStorePlan {
                    listener: "slow".to_owned(),
                    registry: StoreRegistry::new(),
                    refs: vec![StoreRef {
                        name: Arc::from("default"),
                        backend_id: Arc::from("slow-unavailable"),
                        config: json!({}),
                    }],
                },
                ListenerStorePlan {
                    listener: "recovering".to_owned(),
                    registry: recovered_registry.clone(),
                    refs: vec![StoreRef {
                        name: Arc::from("default"),
                        backend_id: Arc::from("recovering"),
                        config: json!({}),
                    }],
                },
            ],
            readiness,
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
        .expect("recovered listener should honor its retry clock while its sibling is pending");
        assert!(builds.load(Ordering::SeqCst) >= 4);

        shutdown_tx.send(true).expect("service should still receive shutdown");
        tokio::time::timeout(Duration::from_secs(2), running)
            .await
            .expect("slow attempt should finish within its bounded test delay")
            .expect("provisioner task should stop");
        assert_eq!(retires.load(Ordering::SeqCst), 0);
    }
}

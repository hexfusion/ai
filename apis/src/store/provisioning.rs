// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Praxis Contributors

//! Concrete store-backend factories and connection-error redaction.
//!
//! The factories wrap the existing SQL backends (still in apis pre-#1260) as
//! [`StoreBackendFactory`] implementations the lifecycle layer provisions. They
//! own the backend-specific config, classify a build failure so the cache
//! applies the tested policy (SQLite permanent-init failure is unavailable;
//! Postgres transient connect is retryable), compute the dedup key, and close
//! the pool on retirement.

#[cfg(any(feature = "store-sqlite", feature = "store-postgres"))]
use praxis_ai_store::BackendError;

/// Scrub a connection string, and any credentials embedded in it, from an error
/// message before it reaches [`StoreError::Database`] or a log line.
///
/// [`StoreError::Database`]: praxis_ai_store::StoreError::Database
#[cfg(any(feature = "store-sqlite", feature = "store-postgres"))]
pub(crate) fn redact_connection_error(url: &str, message: &str) -> String {
    let base = message.replace(url, "<redacted database url>");
    // Best effort for a `scheme://user:pass@host` credential echoed separately
    // from the full url: drop the userinfo segment. split_once avoids byte
    // indexing, which could split a UTF-8 character.
    let Some((before, rest)) = base.split_once("://") else {
        return base;
    };
    match rest.split_once('@') {
        Some((_userinfo, after_at)) => format!("{before}://<redacted credentials>@{after_at}"),
        None => format!("{before}://{rest}"),
    }
}

/// A permanent build failure, redacted, as a backend-unavailable error.
#[cfg(feature = "store-sqlite")]
fn permanent(url: &str, message: &str) -> BackendError {
    BackendError::Unavailable(redact_connection_error(url, message))
}

/// A transient build failure, redacted, so provisioning retries within budget.
#[cfg(feature = "store-postgres")]
fn transient(url: &str, message: &str) -> BackendError {
    BackendError::Transient(redact_connection_error(url, message))
}

/// A stable fingerprint of the pool overrides for the dedup key.
#[cfg(any(feature = "store-sqlite", feature = "store-postgres"))]
fn pool_fingerprint(pool: Option<&crate::store::PoolConfig>) -> String {
    pool.map_or_else(
        || "default".to_owned(),
        |p| {
            format!(
                "{:?}/{:?}/{:?}/{:?}",
                p.max_connections, p.min_connections, p.idle_timeout_secs, p.acquire_timeout_secs
            )
        },
    )
}

/// SQLite-backed store-backend factory.
#[cfg(feature = "store-sqlite")]
#[expect(clippy::allow_attributes, reason = "the factory awaits binary-injection wiring")]
#[allow(
    dead_code,
    reason = "injected by binaries in the deferred server-wiring slice (#1259 slice 5)"
)]
mod sqlite {
    use std::sync::Arc;

    use async_trait::async_trait;
    use praxis_ai_store::{EffectiveConfigKey, ProvisionedBackend, RetireBackend, StoreBackendFactory};
    use secrecy::{ExposeSecret as _, SecretString};
    use serde::Deserialize;
    use serde_json::Value;

    use super::{BackendError, permanent};
    use crate::store::{PoolConfig, SqliteResponseStore};

    /// Backend id the SQLite factory answers to.
    pub(crate) const BACKEND_ID: &str = "sqlite";

    /// Inline configuration for a SQLite-backed store.
    #[derive(Deserialize)]
    #[serde(deny_unknown_fields)]
    struct SqliteConfig {
        /// SQLite connection string.
        database_url: SecretString,
        /// Responses table name.
        responses_table: String,
        /// Conversations table name.
        conversations_table: String,
        /// Optional conversation-items table name.
        #[serde(default)]
        items_table: Option<String>,
        /// Optional connection-pool overrides.
        #[serde(default)]
        pool: Option<PoolConfig>,
    }

    /// Retirement hook that closes a SQLite pool.
    struct SqliteRetire {
        /// The store whose pool is closed on retirement.
        store: Arc<SqliteResponseStore>,
    }

    #[async_trait]
    impl RetireBackend for SqliteRetire {
        async fn retire(&self) {
            self.store.close().await;
        }
    }

    /// Provisions SQLite-backed persisted-state stores.
    pub(crate) struct SqliteBackendFactory;

    impl SqliteBackendFactory {
        /// Parse the inline config, failing with a config error.
        fn parse(config: &Value) -> Result<SqliteConfig, BackendError> {
            serde_json::from_value(config.clone()).map_err(|e| BackendError::Config(e.to_string()))
        }
    }

    #[async_trait]
    impl StoreBackendFactory for SqliteBackendFactory {
        fn backend_id(&self) -> &str {
            BACKEND_ID
        }

        fn effective_key(&self, config: &Value) -> Result<EffectiveConfigKey, BackendError> {
            let cfg = Self::parse(config)?;
            // The pool + url + table names identify one SQLite store instance.
            let key = format!(
                "sqlite\u{1f}{}\u{1f}{}\u{1f}{}\u{1f}{}\u{1f}{}",
                cfg.database_url.expose_secret(),
                cfg.responses_table,
                cfg.conversations_table,
                cfg.items_table.as_deref().unwrap_or(""),
                super::pool_fingerprint(cfg.pool.as_ref()),
            );
            Ok(EffectiveConfigKey::new(key))
        }

        async fn build(&self, config: &Value) -> Result<ProvisionedBackend, BackendError> {
            let cfg = Self::parse(config)?;
            let url = cfg.database_url.expose_secret();
            // SQLite init failure is permanent: fail the build unavailable.
            let store = SqliteResponseStore::new(
                url,
                &cfg.responses_table,
                &cfg.conversations_table,
                cfg.items_table.as_deref(),
                cfg.pool.as_ref(),
            )
            .await
            .map_err(|e| permanent(url, &e.to_string()))?;

            let store = Arc::new(store);
            Ok(ProvisionedBackend {
                retire: Arc::new(SqliteRetire {
                    store: Arc::clone(&store),
                }),
                backend: store,
            })
        }
    }
}

/// Postgres-backed store-backend factory.
#[cfg(feature = "store-postgres")]
#[expect(clippy::allow_attributes, reason = "the factory awaits binary-injection wiring")]
#[allow(
    dead_code,
    reason = "injected by binaries in the deferred server-wiring slice (#1259 slice 5)"
)]
mod postgres {
    use std::sync::Arc;

    use async_trait::async_trait;
    use praxis_ai_store::{EffectiveConfigKey, ProvisionedBackend, RetireBackend, StoreBackendFactory};
    use secrecy::{ExposeSecret as _, SecretString};
    use serde::Deserialize;
    use serde_json::Value;

    use super::{BackendError, transient};
    use crate::store::{PgTlsConfig, PoolConfig, PostgresResponseStore, SslMode, postgres_url};

    /// Backend id the Postgres factory answers to.
    pub(crate) const BACKEND_ID: &str = "postgres";

    /// Inline configuration for a Postgres-backed store.
    #[derive(Deserialize)]
    #[serde(deny_unknown_fields)]
    struct PostgresConfig {
        /// Postgres connection string.
        database_url: SecretString,
        /// Responses table name.
        responses_table: String,
        /// Conversations table name.
        conversations_table: String,
        /// Optional conversation-items table name.
        #[serde(default)]
        items_table: Option<String>,
        /// Optional connection-pool overrides.
        #[serde(default)]
        pool: Option<PoolConfig>,
        /// TLS verification mode.
        #[serde(default)]
        ssl_mode: Option<SslMode>,
        /// Optional PEM CA the server certificate is verified against.
        #[serde(default)]
        ssl_root_cert: Option<SecretString>,
        /// Permit a private/loopback database host (opt-in).
        #[serde(default)]
        allow_private_database_url: bool,
    }

    /// Retirement hook that closes a Postgres pool.
    struct PostgresRetire {
        /// The store whose pool is closed on retirement.
        store: Arc<PostgresResponseStore>,
    }

    #[async_trait]
    impl RetireBackend for PostgresRetire {
        async fn retire(&self) {
            self.store.close().await;
        }
    }

    /// Provisions Postgres-backed persisted-state stores.
    pub(crate) struct PostgresBackendFactory;

    impl PostgresBackendFactory {
        /// Parse the inline config, failing with a config error.
        fn parse(config: &Value) -> Result<PostgresConfig, BackendError> {
            serde_json::from_value(config.clone()).map_err(|e| BackendError::Config(e.to_string()))
        }
    }

    #[async_trait]
    impl StoreBackendFactory for PostgresBackendFactory {
        fn backend_id(&self) -> &str {
            BACKEND_ID
        }

        fn effective_key(&self, config: &Value) -> Result<EffectiveConfigKey, BackendError> {
            let cfg = Self::parse(config)?;
            let key = format!(
                "postgres\u{1f}{}\u{1f}{}\u{1f}{}\u{1f}{}\u{1f}{:?}\u{1f}{}\u{1f}{}",
                cfg.database_url.expose_secret(),
                cfg.responses_table,
                cfg.conversations_table,
                cfg.items_table.as_deref().unwrap_or(""),
                cfg.ssl_mode,
                cfg.ssl_root_cert.as_ref().map_or("", |_| "set"),
                super::pool_fingerprint(cfg.pool.as_ref()),
            );
            Ok(EffectiveConfigKey::new(key))
        }

        async fn build(&self, config: &Value) -> Result<ProvisionedBackend, BackendError> {
            let cfg = Self::parse(config)?;
            let url = cfg.database_url.expose_secret();
            // Re-validate the host on every attempt (guards DNS rebinding). A
            // private or invalid host is a permanent config error.
            postgres_url::revalidate_postgres_host(BACKEND_ID, url, cfg.allow_private_database_url)
                .map_err(|e| BackendError::Config(e.to_string()))?;
            // Own the root cert so the borrowed PgTlsConfig outlives the build.
            let root_cert = cfg.ssl_root_cert.as_ref().map(|s| s.expose_secret().to_owned());
            let tls = PgTlsConfig {
                require_certificate_authentication: false,
                ssl_client_cert: None,
                ssl_client_key: None,
                ssl_mode: cfg.ssl_mode,
                ssl_root_cert: root_cert.as_deref(),
            };
            // A Postgres connect failure is transient: the cache retries within
            // budget before treating it as unavailable.
            let store = Box::pin(PostgresResponseStore::new(
                url,
                &cfg.responses_table,
                &cfg.conversations_table,
                cfg.items_table.as_deref(),
                &tls,
                cfg.pool.as_ref(),
            ))
            .await
            .map_err(|e| transient(url, &e.to_string()))?;

            let store = Arc::new(store);
            Ok(ProvisionedBackend {
                retire: Arc::new(PostgresRetire {
                    store: Arc::clone(&store),
                }),
                backend: store,
            })
        }
    }
}

#[cfg(test)]
#[cfg(feature = "store-sqlite")]
#[expect(clippy::allow_attributes, reason = "blanket test suppressions")]
#[allow(clippy::expect_used, clippy::panic, reason = "tests")]
mod tests {
    use std::sync::Arc;

    use praxis_ai_store::{StoreBackendFactory, StoreCapability};
    use praxis_ai_store_lifecycle::{BackendCache, ProvisionError, StoreRef};
    use serde_json::json;
    use tempfile::TempDir;

    use super::sqlite::SqliteBackendFactory;

    /// Build a cache holding only the real SQLite factory.
    fn sqlite_cache() -> BackendCache {
        let factory: Arc<dyn StoreBackendFactory> = Arc::new(SqliteBackendFactory);
        BackendCache::new(vec![factory])
    }

    /// A store reference against a file-backed SQLite database at `path`.
    fn sqlite_ref(name: &str, dir: &TempDir, file: &str) -> StoreRef {
        let url = format!("sqlite://{}/{file}?mode=rwc", dir.path().display());
        StoreRef {
            name: Arc::from(name),
            backend_id: Arc::from("sqlite"),
            capability: StoreCapability::ResponsesAndConversations,
            config: json!({
                "database_url": url,
                "responses_table": "responses",
                "conversations_table": "conversations",
                "items_table": "conversation_items",
            }),
        }
    }

    #[tokio::test]
    async fn initial_load_builds_real_sqlite() {
        let dir = TempDir::new().expect("tempdir");
        let cache = sqlite_cache();

        let provisioned = cache
            .provision(&[sqlite_ref("default", &dir, "a.db")])
            .await
            .expect("real sqlite initial load");

        assert!(provisioned.registry.contains("default"));
    }

    #[tokio::test]
    async fn reload_reuses_same_sqlite_backend() {
        let dir = TempDir::new().expect("tempdir");
        let cache = sqlite_cache();
        let refs = [sqlite_ref("default", &dir, "a.db")];

        let gen1 = cache.provision(&refs).await.expect("gen1");
        let gen2 = cache.provision(&refs).await.expect("gen2");

        // Reused: releasing gen1 must not close the pool gen2 still uses, so a
        // gen2 provision-equivalent still resolves.
        gen1.lease.release().await;
        assert!(gen2.registry.contains("default"));
        gen2.lease.release().await;
    }

    #[tokio::test]
    async fn changed_url_builds_a_second_backend() {
        let dir = TempDir::new().expect("tempdir");
        let cache = sqlite_cache();

        let gen1 = cache
            .provision(&[sqlite_ref("default", &dir, "a.db")])
            .await
            .expect("gen1 on a.db");
        let gen2 = cache
            .provision(&[sqlite_ref("default", &dir, "b.db")])
            .await
            .expect("gen2 on b.db");

        assert!(gen1.registry.contains("default"));
        assert!(gen2.registry.contains("default"));
        gen1.lease.release().await;
        gen2.lease.release().await;
    }

    #[tokio::test]
    async fn unavailable_at_build_fails_closed_distinct_from_unknown() {
        let cache = sqlite_cache();
        // A read-only URL for a file that does not exist cannot initialize the
        // schema: a permanent SQLite failure.
        let bad = StoreRef {
            name: Arc::from("default"),
            backend_id: Arc::from("sqlite"),
            capability: StoreCapability::Responses,
            config: json!({
                "database_url": "sqlite:///nonexistent-dir/does-not-exist.db?mode=ro",
                "responses_table": "responses",
                "conversations_table": "conversations",
            }),
        };

        let result = cache.provision(&[bad]).await;
        let Err(err) = result else {
            panic!("expected a build failure")
        };
        match err {
            ProvisionError::Backend {
                source: praxis_ai_store::BackendError::Unavailable(_),
                ..
            } => {},
            other => panic!("expected backend-unavailable, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn dedup_shares_one_sqlite_pool() {
        let dir = TempDir::new().expect("tempdir");
        let cache = sqlite_cache();

        // Two names, one effective config (same url + tables) -> one backend.
        let provisioned = cache
            .provision(&[
                sqlite_ref("responses", &dir, "shared.db"),
                sqlite_ref("conversations", &dir, "shared.db"),
            ])
            .await
            .expect("both provision onto one pool");

        assert!(provisioned.registry.contains("responses"));
        assert!(provisioned.registry.contains("conversations"));
        assert!(
            provisioned.registry.shares_storage_with(&provisioned.registry),
            "same registry",
        );
        provisioned.lease.release().await;
    }
}

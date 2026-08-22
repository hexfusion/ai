// SPDX-License-Identifier: MIT
// Copyright (c) 2026 Praxis Contributors

//! Live load signals collected from the local grid operator.
//!
//! The operator scrapes every provider it knows about and republishes what it
//! saw on a federation endpoint, labelled with the site and provider the series
//! came from. This module polls that endpoint and keeps a bounded window per
//! series so routing can compare candidates on what they are doing now rather
//! than on an order rendered at reconcile time.
//!
//! Samples carry the operator's observation time, not the time they arrived. A
//! value the operator is republishing from its own cache is therefore not
//! mistaken for a new one: a series only advances when its timestamp does.

use std::{
    collections::HashMap,
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use dashmap::DashMap;
use praxis_filter::FilterError;
use serde::Deserialize;
use tokio_util::sync::CancellationToken;

/// Default poll interval against the operator.
const DEFAULT_INTERVAL_MS: u64 = 2_000;

/// Default retention for each series.
const DEFAULT_WINDOW_SECS: u64 = 300;

/// Default age past which a sample no longer describes the present.
const DEFAULT_MAX_AGE_MS: i64 = 30_000;

/// Default request timeout, kept under the poll interval.
const DEFAULT_TIMEOUT_MS: u64 = 1_500;

/// Cap on series retained, so a misconfigured endpoint cannot grow the store
/// without bound.
const MAX_SERIES: usize = 4_096;

/// Collector configuration.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct LoadConfig {
    /// Signals endpoint on the local operator, e.g.
    /// `http://grid-operator:9091/metrics`.
    ///
    /// Unqualified, this carries the local site and every peer the operator has
    /// collected. Adding `?target=<site>` narrows it to one site, which is what
    /// peer operators ask for, so pointing here at that instead would leave
    /// every remote candidate unscored.
    pub endpoint: String,

    /// Metric name that carries queue depth.
    ///
    /// Named here rather than assumed, because the operator republishes what a
    /// provider exposes and providers do not agree on what to call it.
    pub queue_metric: String,

    /// Metric names sent as `collect[]`, narrowing what the operator returns.
    ///
    /// These are bare names, not selectors. The operator filters by metric name
    /// only, so a label matcher here would match nothing and silently drop the
    /// series it was meant to narrow.
    #[serde(default)]
    pub collect: Vec<String>,

    /// Poll interval in milliseconds.
    #[serde(default = "default_interval_ms")]
    pub interval_ms: u64,

    /// Retention per series in seconds.
    #[serde(default = "default_window_secs")]
    pub window_secs: u64,

    /// Age past which a sample is ignored for routing, in milliseconds.
    ///
    /// A liveness bound rather than a freshness score: it stops a dead operator
    /// from pinning routing to values that stopped describing anything, and it
    /// does not otherwise rank one candidate above another.
    #[serde(default = "default_max_age_ms")]
    pub max_age_ms: i64,

    /// Request timeout in milliseconds.
    #[serde(default = "default_timeout_ms")]
    pub timeout_ms: u64,
}

/// Default poll interval.
const fn default_interval_ms() -> u64 {
    DEFAULT_INTERVAL_MS
}

/// Default retention per series.
const fn default_window_secs() -> u64 {
    DEFAULT_WINDOW_SECS
}

/// Default liveness bound on a sample.
const fn default_max_age_ms() -> i64 {
    DEFAULT_MAX_AGE_MS
}

/// Default request timeout.
const fn default_timeout_ms() -> u64 {
    DEFAULT_TIMEOUT_MS
}

/// One observation of a series.
#[derive(Clone, Copy, Debug, PartialEq)]
pub(crate) struct Sample {
    /// Operator observation time, in milliseconds since the epoch.
    pub at_ms: i64,
    /// Value as the provider reported it.
    pub value: f64,
}

/// A bounded window of one series, oldest first.
#[derive(Debug, Default)]
struct Series {
    /// Samples in timestamp order.
    samples: Vec<Sample>,
}

impl Series {
    /// Append `sample` if it is newer than what is held, then evict past `window`.
    fn push(&mut self, sample: Sample, window: Duration) {
        if self.samples.last().is_some_and(|last| sample.at_ms <= last.at_ms) {
            return;
        }
        self.samples.push(sample);
        let Ok(window_ms) = i64::try_from(window.as_millis()) else {
            return;
        };
        let cutoff = sample.at_ms.saturating_sub(window_ms);
        let keep_from = self.samples.partition_point(|s| s.at_ms < cutoff);
        if keep_from > 0 {
            self.samples.drain(..keep_from);
        }
    }
}

/// Series held for one provider, keyed by metric name.
#[derive(Debug, Default)]
struct Provider {
    /// Metric name to its window.
    metrics: HashMap<Box<str>, Series>,
}

/// Windowed signals for every provider the operator reports.
///
/// Keyed by `"site/cluster"`, which is what a route candidate names, so a
/// lookup on the request path is a hash of borrowed strings and allocates
/// nothing.
#[derive(Debug)]
pub(crate) struct LoadStore {
    /// Provider key to its series.
    providers: DashMap<Box<str>, Provider>,
    /// Retention per series.
    window: Duration,
}

impl LoadStore {
    /// Create an empty store retaining `window` of history per series.
    pub fn new(window: Duration) -> Self {
        Self {
            providers: DashMap::new(),
            window,
        }
    }

    /// The key under which a candidate's series are held.
    pub fn key(site: &str, cluster: &str) -> Box<str> {
        format!("{site}/{cluster}").into_boxed_str()
    }

    /// Most recent sample of `metric` for `key`, if any.
    pub fn latest(&self, key: &str, metric: &str) -> Option<Sample> {
        let provider = self.providers.get(key)?;
        provider.metrics.get(metric)?.samples.last().copied()
    }

    /// Most recent sample of `metric` for `key`, if it is younger than `max_age_ms`.
    pub fn fresh(&self, key: &str, metric: &str, now_ms: i64, max_age_ms: i64) -> Option<Sample> {
        // The range starts at zero deliberately. A sample stamped in the future
        // means the publishing site's clock is ahead of this one, and a negative
        // age would otherwise compare as fresh forever, letting a site that has
        // stopped reporting keep winning on its last value. An unusable reading
        // is withheld, which costs a fallback to the rendered order.
        self.latest(key, metric)
            .filter(|s| (0..=max_age_ms).contains(&now_ms.saturating_sub(s.at_ms)))
    }

    /// Number of providers held.
    #[cfg(test)]
    pub fn len(&self) -> usize {
        self.providers.len()
    }

    /// Absorb an exposition response.
    ///
    /// Lines that cannot be read are skipped. A response is a copy of somebody
    /// else's scrape, and one bad line should not cost the rest.
    pub fn ingest(&self, text: &str) {
        for line in text.lines() {
            let Some(observation) = parse_line(line) else {
                continue;
            };
            let key = Self::key(observation.site, observation.cluster);
            if !self.providers.contains_key(&key) && self.providers.len() >= MAX_SERIES {
                continue;
            }
            let sample = Sample {
                at_ms: observation.at_ms,
                value: observation.value,
            };
            self.providers
                .entry(key)
                .or_default()
                .metrics
                .entry(observation.metric.into())
                .or_default()
                .push(sample, self.window);
        }
    }
}

/// One parsed sample line.
struct Observation<'a> {
    /// Metric name.
    metric: &'a str,
    /// Owning site, from the `grid_site` label.
    site: &'a str,
    /// Owning provider, from the `grid_provider` label.
    cluster: &'a str,
    /// Reported value.
    value: f64,
    /// Operator observation time.
    at_ms: i64,
}

/// Parse `name{labels} value timestamp`, which is what the operator serves.
///
/// A line without a timestamp is skipped: without one there is no way to tell a
/// new observation from a republished one, and appending it would corrupt the
/// window it lands in.
fn parse_line(line: &str) -> Option<Observation<'_>> {
    let line = line.trim();
    if line.is_empty() || line.starts_with('#') {
        return None;
    }
    let (head, timestamp) = line.rsplit_once(' ')?;
    let (head, value) = head.rsplit_once(' ')?;
    let at_ms = timestamp.parse().ok()?;
    let value = value.parse().ok()?;
    let (metric, labels) = head.split_once('{')?;
    let labels = labels.strip_suffix('}')?;
    let mut site = None;
    let mut cluster = None;
    for pair in labels.split(',') {
        match pair.trim().split_once('=') {
            Some(("grid_site", v)) => site = Some(v.trim_matches('"')),
            Some(("grid_provider", v)) => cluster = Some(v.trim_matches('"')),
            _ => {},
        }
    }
    Some(Observation {
        metric,
        site: site?,
        cluster: cluster?,
        value,
        at_ms,
    })
}

/// Milliseconds since the epoch.
pub(crate) fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| i64::try_from(d.as_millis()).unwrap_or(i64::MAX))
}

/// A running collector, stopped on drop.
#[derive(Debug)]
pub(crate) struct LoadCollector {
    /// Signals the poll loop to exit.
    cancel: CancellationToken,
}

impl Drop for LoadCollector {
    fn drop(&mut self) {
        self.cancel.cancel();
    }
}

/// Start polling `config.endpoint` into a new store.
///
/// Runs on its own thread with its own current-thread runtime, so the collector
/// does not depend on a runtime being current when the filter is built. The
/// overlay watcher owns a thread for the same reason.
///
/// A poll that fails is logged and retried on the next tick. The store keeps
/// what it had, and `max_age_ms` is what stops that from being used
/// indefinitely.
pub(crate) fn spawn(config: &LoadConfig) -> Result<(Arc<LoadStore>, LoadCollector), FilterError> {
    let store = Arc::new(LoadStore::new(Duration::from_secs(config.window_secs)));
    let url = build_url(&config.endpoint, &config.collect);
    let interval = Duration::from_millis(config.interval_ms);
    let timeout = Duration::from_millis(config.timeout_ms);
    let cancel = CancellationToken::new();

    let polling = Arc::clone(&store);
    let stopping = cancel.clone();
    std::thread::Builder::new()
        .name("load-collector".to_owned())
        .spawn(move || {
            let runtime = match tokio::runtime::Builder::new_current_thread().enable_all().build() {
                Ok(rt) => rt,
                Err(error) => {
                    tracing::error!(%error, "load collector runtime unavailable; routing falls back to overlay order");
                    return;
                },
            };
            runtime.block_on(poll_loop(polling, stopping, url, interval, timeout));
        })
        .map_err(|e| -> FilterError { format!("intelligent_route: load collector thread: {e}").into() })?;

    Ok((store, LoadCollector { cancel }))
}

/// Poll until cancelled, feeding every response into `store`.
async fn poll_loop(
    store: Arc<LoadStore>,
    cancel: CancellationToken,
    url: String,
    interval: Duration,
    timeout: Duration,
) {
    let client = match reqwest::Client::builder().timeout(timeout).build() {
        Ok(c) => c,
        Err(error) => {
            tracing::error!(%error, "load collector client unavailable; routing falls back to overlay order");
            return;
        },
    };
    let mut ticker = tokio::time::interval(interval);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        tokio::select! {
            () = cancel.cancelled() => return,
            _ = ticker.tick() => {},
        }
        poll_once(&client, &url, &store).await;
    }
}

/// Fetch once and absorb the response, logging rather than propagating failure.
async fn poll_once(client: &reqwest::Client, url: &str, store: &LoadStore) {
    match client.get(url).send().await {
        Ok(response) => match response.text().await {
            Ok(body) => store.ingest(&body),
            Err(error) => tracing::debug!(%error, "load endpoint body unreadable"),
        },
        Err(error) => tracing::debug!(%error, "load endpoint poll failed"),
    }
}

/// Append `collect[]` parameters for each metric name.
fn build_url(endpoint: &str, collect: &[String]) -> String {
    if collect.is_empty() {
        return endpoint.to_owned();
    }
    let query = collect
        .iter()
        .map(|s| format!("collect[]={}", encode(s)))
        .collect::<Vec<_>>()
        .join("&");
    let separator = if endpoint.contains('?') { '&' } else { '?' };
    format!("{endpoint}{separator}{query}")
}

/// Percent-encode a query value, leaving the unreserved set alone.
fn encode(value: &str) -> String {
    value
        .bytes()
        .map(|b| match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => (b as char).to_string(),
            _ => format!("%{b:02X}"),
        })
        .collect()
}

// -----------------------------------------------------------------------------
// Tests
// -----------------------------------------------------------------------------

#[cfg(test)]
#[expect(clippy::allow_attributes, reason = "blanket test suppressions")]
#[allow(
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::significant_drop_tightening,
    clippy::unwrap_used,
    reason = "tests"
)]
mod tests {
    use super::*;

    const QUEUE: &str = "inference_pool_average_queue_size";

    fn line(site: &str, cluster: &str, value: f64, at_ms: i64) -> String {
        format!(r#"{QUEUE}{{grid_site="{site}",grid_provider="{cluster}"}} {value} {at_ms}"#)
    }

    fn store() -> LoadStore {
        LoadStore::new(Duration::from_secs(300))
    }

    #[test]
    fn ingests_a_labelled_sample() {
        let store = store();
        store.ingest(&line("east", "pool-a", 3.0, 1_000));
        let sample = store.latest(&LoadStore::key("east", "pool-a"), QUEUE).expect("sample");
        assert_eq!(
            sample,
            Sample {
                at_ms: 1_000,
                value: 3.0
            },
            "value and time as reported"
        );
    }

    #[test]
    fn a_republished_sample_does_not_advance_the_series() {
        let store = store();
        let repeated = line("east", "pool-a", 3.0, 1_000);
        store.ingest(&repeated);
        store.ingest(&repeated);
        store.ingest(&repeated);
        let key = LoadStore::key("east", "pool-a");
        let provider = store.providers.get(&key).expect("provider");
        let held = provider.metrics.get(QUEUE).expect("series").samples.len();
        assert_eq!(held, 1, "the operator's cached republish is not a new observation");
    }

    #[test]
    fn a_newer_sample_advances_the_series() {
        let store = store();
        store.ingest(&line("east", "pool-a", 3.0, 1_000));
        store.ingest(&line("east", "pool-a", 5.0, 2_000));
        let sample = store.latest(&LoadStore::key("east", "pool-a"), QUEUE).expect("sample");
        assert_eq!(sample.value, 5.0, "the newer value wins");
    }

    #[test]
    fn samples_older_than_the_window_are_evicted() {
        let store = LoadStore::new(Duration::from_secs(10));
        for at_ms in [1_000, 5_000, 20_000] {
            store.ingest(&line("east", "pool-a", 1.0, at_ms));
        }
        let key = LoadStore::key("east", "pool-a");
        let provider = store.providers.get(&key).expect("provider");
        let samples = &provider.metrics.get(QUEUE).expect("series").samples;
        assert_eq!(samples.len(), 1, "only what falls inside the window: {samples:?}");
        assert_eq!(samples.first().map(|s| s.at_ms), Some(20_000), "the newest survives");
    }

    #[test]
    fn sites_do_not_collide_on_a_shared_cluster_name() {
        let store = store();
        store.ingest(&line("east", "pool-a", 1.0, 1_000));
        store.ingest(&line("west", "pool-a", 9.0, 1_000));
        assert_eq!(store.len(), 2, "the site is part of the key");
        let west = store.latest(&LoadStore::key("west", "pool-a"), QUEUE).expect("west");
        assert_eq!(west.value, 9.0, "each site keeps its own value");
    }

    #[test]
    fn a_line_without_a_timestamp_is_skipped() {
        let store = store();
        store.ingest(&format!(r#"{QUEUE}{{grid_site="east",grid_provider="pool-a"}} 3"#));
        assert_eq!(
            store.len(),
            0,
            "without a timestamp there is no way to order the sample"
        );
    }

    #[test]
    fn unlabelled_and_malformed_lines_are_skipped_without_losing_the_rest() {
        let store = store();
        let text = format!(
            "# HELP something\n{QUEUE} 3 1000\nnot a metric\n{}",
            line("east", "pool-a", 3.0, 1_000)
        );
        store.ingest(&text);
        assert_eq!(store.len(), 1, "the one usable line still lands");
    }

    #[test]
    fn a_stale_sample_is_withheld_from_routing() {
        let store = store();
        store.ingest(&line("east", "pool-a", 3.0, 1_000));
        let key = LoadStore::key("east", "pool-a");
        assert!(store.fresh(&key, QUEUE, 10_000, 30_000).is_some(), "inside the bound");
        assert!(store.fresh(&key, QUEUE, 60_000, 30_000).is_none(), "past the bound");
    }

    #[test]
    fn a_sample_from_a_clock_ahead_of_ours_is_withheld() {
        let store = store();
        store.ingest(&line("east", "pool-a", 3.0, 60_000));
        let key = LoadStore::key("east", "pool-a");
        assert!(
            store.fresh(&key, QUEUE, 10_000, 30_000).is_none(),
            "a future timestamp must not read as fresh, or a dead site keeps winning"
        );
    }

    #[test]
    fn metric_names_become_collect_parameters() {
        let url = build_url("http://operator:9091/metrics", &[QUEUE.to_owned()]);
        assert_eq!(
            url,
            format!("http://operator:9091/metrics?collect[]={QUEUE}"),
            "one parameter carrying a bare metric name"
        );
    }

    #[test]
    fn an_endpoint_without_metric_names_is_left_alone() {
        let url = build_url("http://operator:9091/metrics", &[]);
        assert_eq!(url, "http://operator:9091/metrics", "nothing appended");
    }
}

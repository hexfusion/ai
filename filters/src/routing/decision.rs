// SPDX-License-Identifier: MIT
// Copyright (c) 2026 Praxis Contributors

//! The routing decision record: what the routing stage chose, and the
//! scoreboard it chose from.
//!
//! Built once per routed request and consumed two ways: a compact summary rides
//! back on the response as debugging evidence, and (with the `opentelemetry`
//! feature) the full record is attached to the routing span for a trace sink to
//! render.
//!
//! The record is self-describing. Each candidate carries whatever signals the
//! scorers reported, each with the value, weight, and freshness the decision was
//! made on. Extending the scoring logic (a new [`Scorer`], a new signal)
//! extends this record and the tools that read it with no change here.
//!
//! [`Scorer`]: super::scoring::Scorer

use std::sync::Arc;

use super::{
    descriptor::{CapabilityKind, RouteCandidate},
    scoring::SignalReading,
};

/// Schema tag carried in the record so a consumer can pin what it parses.
const SCHEMA: &str = "praxis.routing.decision/v1";

/// The longest summary the compact response header carries. The filter metadata
/// channel bounds a value at 256 bytes; a longer summary is truncated to fit.
const SUMMARY_MAX: usize = 256;

/// One candidate's line in the decision: identity, the combined score it earned,
/// and the per-signal readings behind that score.
struct Row {
    /// Owning site.
    site: Box<str>,
    /// Selected upstream cluster.
    cluster: Box<str>,
    /// Stable candidate identity.
    stable_id: Box<str>,
    /// Overlay-declared freshness, distinct from the live signal freshness.
    overlay_fresh: bool,
    /// Combined score, or `None` when no signal rated this candidate.
    score: Option<f64>,
    /// Whether this candidate was the pick.
    winner: bool,
    /// Per-signal readings, one per active scorer.
    signals: Vec<SignalReading>,
}

/// A completed routing decision, ready to summarise or serialise.
pub(crate) struct RoutingDecision {
    /// Requested capability name (the model).
    model: Box<str>,
    /// Routed capability kind.
    kind: &'static str,
    /// Site that handled the inbound request.
    local_site: Box<str>,
    /// Why-class the routing stage recorded.
    basis: &'static str,
    /// Whether the pick fell back off live load (a candidate went unscored).
    fallback: bool,
    /// The picked candidate's cluster: the concrete routing target and the
    /// value of the `x-grid-route` header. In a topology where several clusters
    /// are bridged under one site, this is what distinguishes the pick.
    route: Box<str>,
    /// One line per candidate that was in the running.
    rows: Vec<Row>,
}

impl RoutingDecision {
    /// Build the record from the admitted candidates, their combined scores, and
    /// their per-signal readings.
    ///
    /// `scores` and `readings` are aligned to `admitted`; `scores` may be empty
    /// when no load source is configured, and `readings` empty when no scorer
    /// ran.
    #[expect(
        clippy::too_many_arguments,
        reason = "the record captures the full selection context"
    )]
    pub(crate) fn build(
        admitted: &[&RouteCandidate],
        scores: &[Option<f64>],
        readings: &[Vec<SignalReading>],
        chosen: &RouteCandidate,
        basis: &'static str,
        fallback: bool,
        local_site: &Arc<str>,
        kind: CapabilityKind,
        name: &str,
    ) -> Self {
        let rows = admitted
            .iter()
            .enumerate()
            .map(|(index, candidate)| Row {
                site: Box::from(&*candidate.site),
                cluster: Box::from(&*candidate.cluster),
                stable_id: Box::from(&*candidate.stable_id),
                overlay_fresh: candidate.fresh,
                score: scores.get(index).copied().flatten(),
                winner: candidate.stable_id == chosen.stable_id,
                signals: readings.get(index).cloned().unwrap_or_default(),
            })
            .collect();
        Self {
            model: Box::from(name),
            kind: kind.as_str(),
            local_site: Box::from(&**local_site),
            basis,
            fallback,
            route: Box::from(&*chosen.cluster),
            rows,
        }
    }

    /// The picked cluster, for the `x-grid-route` header.
    pub(crate) fn route(&self) -> &str {
        &self.route
    }

    /// A compact, human-readable one-liner for the `x-grid-decision` header.
    ///
    /// Bounded to [`SUMMARY_MAX`] bytes on a character boundary, so it always
    /// fits the filter metadata channel. Shows the pick, the why-class, and each
    /// candidate's score with the freshness that decided whether it counted.
    pub(crate) fn summary(&self) -> String {
        let mut parts = vec![format!(
            "pick={} basis={} fallback={}",
            self.route, self.basis, self.fallback
        )];
        for row in &self.rows {
            let mark = if row.winner { "*" } else { "" };
            let score = row.score.map_or_else(|| "-".to_owned(), |s| format!("{s:.2}"));
            parts.push(format!(
                "{}/{}{}:{}{}",
                row.site,
                row.cluster,
                mark,
                score,
                freshness_digest(&row.signals)
            ));
        }
        let mut out = parts.join(" ");
        truncate_on_char_boundary(&mut out, SUMMARY_MAX);
        out
    }

    /// The recorded why-class.
    #[cfg(feature = "opentelemetry")]
    pub(crate) fn basis(&self) -> &'static str {
        self.basis
    }

    /// Whether the pick fell back off live load.
    #[cfg(feature = "opentelemetry")]
    pub(crate) fn fallback(&self) -> bool {
        self.fallback
    }

    /// The full record as a JSON document for a trace attribute.
    ///
    /// Bounded to routing state only: site, cluster, stable id, scores, and
    /// signal readings. No prompt, body, credential, or client identity.
    #[cfg_attr(
        not(feature = "opentelemetry"),
        expect(dead_code, reason = "only the routing trace consumes the full document")
    )]
    #[expect(clippy::too_many_lines, reason = "serialises every candidate and its signals")]
    pub(crate) fn to_json(&self) -> String {
        let candidates: Vec<serde_json::Value> = self
            .rows
            .iter()
            .map(|row| {
                let signals: Vec<serde_json::Value> = row
                    .signals
                    .iter()
                    .map(|s| {
                        serde_json::json!({
                            "metric": &*s.metric,
                            "weight": s.weight,
                            "value": s.reading.value,
                            "age_ms": s.reading.age_ms,
                            "fresh": s.reading.fresh,
                        })
                    })
                    .collect();
                serde_json::json!({
                    "site": &*row.site,
                    "cluster": &*row.cluster,
                    "stable_id": &*row.stable_id,
                    "overlay_fresh": row.overlay_fresh,
                    "score": row.score,
                    "winner": row.winner,
                    "signals": signals,
                })
            })
            .collect();
        let document = serde_json::json!({
            "schema": SCHEMA,
            "model": &*self.model,
            "kind": self.kind,
            "local_site": &*self.local_site,
            "basis": self.basis,
            "fallback": self.fallback,
            "pick": &*self.route,
            "candidates": candidates,
        });
        document.to_string()
    }
}

/// Format a millisecond age as seconds for a compact display.
#[expect(clippy::cast_precision_loss, reason = "age is small and for display only")]
fn fmt_age(ms: i64) -> String {
    format!("{:.1}s", ms as f64 / 1000.0)
}

/// A compact freshness note for a candidate's signals, kept short so every
/// candidate fits the bounded summary rather than one being truncated away.
///
/// `(missing)` when no signal had a value at all — the case that makes a
/// candidate unscored and forces the fallback; otherwise the oldest present
/// sample's age, flagged `stale` when any sample was too old to count.
fn freshness_digest(signals: &[SignalReading]) -> String {
    if signals.is_empty() {
        return String::new();
    }
    let present: Vec<&SignalReading> = signals.iter().filter(|s| s.reading.value.is_some()).collect();
    if present.is_empty() {
        return "(missing)".to_owned();
    }
    let oldest = present.iter().filter_map(|s| s.reading.age_ms).max();
    let any_stale = present.iter().any(|s| !s.reading.fresh);
    let age = oldest.map_or_else(|| "?".to_owned(), fmt_age);
    if any_stale {
        format!("({age} stale)")
    } else {
        format!("({age})")
    }
}

/// Truncate a string to at most `max` bytes without splitting a character.
fn truncate_on_char_boundary(value: &mut String, max: usize) {
    if value.len() <= max {
        return;
    }
    let mut boundary = max;
    while boundary > 0 && !value.is_char_boundary(boundary) {
        boundary -= 1;
    }
    value.truncate(boundary);
}

#[cfg(test)]
#[expect(clippy::allow_attributes, reason = "blanket test suppressions")]
#[allow(clippy::unwrap_used, clippy::indexing_slicing, reason = "tests")]
mod tests {
    use super::{
        super::{
            descriptor::{self, AdmissionState, CapabilityKind, RouteCandidate},
            scoring::{Reading, SignalReading},
        },
        *,
    };

    fn candidate(site: &str, cluster: &str) -> RouteCandidate {
        RouteCandidate {
            admission_state: AdmissionState::NewAndExisting,
            cluster: Arc::from(cluster),
            credential: None,
            fresh: true,
            kind: CapabilityKind::InferenceModel,
            name: Arc::from("llama"),
            rank: None,
            selection_tier: None,
            site: Arc::from(site),
            stable_id: descriptor::default_stable_id(CapabilityKind::InferenceModel, "llama", site, cluster),
        }
    }

    fn reading(metric: &str, value: Option<f64>, age_ms: Option<i64>, fresh: bool) -> SignalReading {
        SignalReading {
            metric: Box::from(metric),
            weight: 1.0,
            reading: Reading { value, age_ms, fresh },
        }
    }

    #[test]
    fn summary_names_pick_and_reports_freshness() {
        let a = candidate("site-a", "inf-a");
        let b = candidate("site-b", "inf-b");
        let admitted = vec![&a, &b];
        let scores = vec![Some(0.10), Some(0.88)];
        let readings = vec![
            vec![reading("kv_cache", Some(0.90), Some(1200), true)],
            vec![reading("kv_cache", Some(0.12), Some(300), true)],
        ];
        let local_site = Arc::from("hub");
        let decision = RoutingDecision::build(
            &admitted,
            &scores,
            &readings,
            &b,
            "live_load",
            false,
            &local_site,
            CapabilityKind::InferenceModel,
            "llama",
        );
        let summary = decision.summary();
        assert!(
            summary.contains("pick=inf-b"),
            "summary names the picked cluster: {summary}"
        );
        assert!(
            summary.contains("site-b/inf-b*:0.88"),
            "winner marked and scored: {summary}"
        );
        assert!(summary.len() <= SUMMARY_MAX, "summary within bound: {}", summary.len());
        assert_eq!(decision.route(), "inf-b", "picked cluster reported");
    }

    #[cfg(feature = "opentelemetry")]
    #[test]
    fn json_carries_signals_and_pick() {
        let a = candidate("site-a", "inf-a");
        let admitted = vec![&a];
        let scores = vec![Some(0.5)];
        let readings = vec![vec![reading("kv_cache", Some(0.4), Some(500), true)]];
        let local_site = Arc::from("hub");
        let decision = RoutingDecision::build(
            &admitted,
            &scores,
            &readings,
            &a,
            "live_load",
            false,
            &local_site,
            CapabilityKind::InferenceModel,
            "llama",
        );
        let parsed: serde_json::Value = serde_json::from_str(&decision.to_json()).unwrap();
        assert_eq!(parsed["pick"], "inf-a", "picked cluster in json");
        assert_eq!(parsed["schema"], SCHEMA, "schema tag present");
        assert_eq!(
            parsed["candidates"][0]["signals"][0]["metric"], "kv_cache",
            "signal name in json"
        );
        assert_eq!(
            parsed["candidates"][0]["signals"][0]["age_ms"], 500,
            "signal age in json"
        );
    }
}

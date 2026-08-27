// SPDX-License-Identifier: MIT
// Copyright (c) 2026 Praxis Contributors

//! Candidate scoring, shaped after the endpoint picker's scheduling framework.
//!
//! A [`Scorer`] rates every candidate on one signal, normalised so that higher
//! is better and comparable across signals. Scores are combined by weight and a
//! picker takes the best. Adding a signal is a new [`Scorer`], not a change to
//! the selection path.
//!
//! One difference from the endpoint picker is deliberate. It drops endpoints
//! that report no metrics, because an endpoint in a pool is expected to report.
//! A route candidate is not: a third-party API is routable and exposes nothing.
//! Dropping those would make the set of reachable providers depend on which
//! ones happen to be observable, so instead an incompletely covered set is left
//! in the order the overlay rendered.

use std::sync::Arc;

use super::{descriptor::RouteCandidate, load::LoadStore};

/// Rates candidates on one signal.
///
/// Returns `None` for a candidate the signal says nothing about, which is not
/// the same as rating it badly.
pub(crate) trait Scorer: Send + Sync {
    /// Raw values per candidate, in the order given.
    ///
    /// Normalisation is applied by [`score_all`], so an implementation reports
    /// the quantity it measures rather than a rating.
    fn measure(&self, candidates: &[&RouteCandidate], now_ms: i64) -> Vec<Option<f64>>;

    /// Whether a lower measurement is better.
    fn lower_is_better(&self) -> bool;

    /// Whether the measurement is already a 0.0 to 1.0 rating.
    ///
    /// Min-max scaling makes a signal relative to the candidates in hand,
    /// which is right for an unbounded quantity like queue depth and wrong for
    /// a ratio: two pools at 0.10 and 0.12 utilisation would score 1.0 and 0.0
    /// and the gateway would treat a trivial difference as a decisive one.
    fn already_normalised(&self) -> bool {
        false
    }

    /// Relative weight in the combined score.
    fn weight(&self) -> f64 {
        1.0
    }
}

/// Rate a measurement against the other candidates, so the best reading in
/// hand becomes 1.0 and the worst 0.0.
///
/// This is what the endpoint picker's queue scorer does:
/// `(max - v) / (max - min)`, and 1.0 for everyone when they are all equal.
fn normalise(raw: &[Option<f64>], lower_is_better: bool) -> Vec<Option<f64>> {
    let present: Vec<f64> = raw.iter().flatten().copied().collect();
    let (Some(min), Some(max)) = (
        present.iter().copied().reduce(f64::min),
        present.iter().copied().reduce(f64::max),
    ) else {
        return vec![None; raw.len()];
    };
    let span = max - min;
    raw.iter()
        .map(|value| {
            value.map(|v| {
                if span <= f64::EPSILON {
                    return 1.0;
                }
                let scaled = (v - min) / span;
                if lower_is_better { 1.0 - scaled } else { scaled }
            })
        })
        .collect()
}

/// Take a measurement that is already a rating, inverting it when lower is
/// better and clamping what a misreporting provider might send.
fn rate_directly(raw: &[Option<f64>], lower_is_better: bool) -> Vec<Option<f64>> {
    raw.iter()
        .map(|value| {
            value.map(|v| {
                let clamped = v.clamp(0.0, 1.0);
                if lower_is_better { 1.0 - clamped } else { clamped }
            })
        })
        .collect()
}

/// One metric, read from the live load store.
///
/// The signal it rates is named by configuration, so a grid that publishes a
/// signal this code has never heard of scores on it without a change here.
pub(crate) struct MetricScorer {
    /// Windowed signals keyed by `"site/cluster"`.
    pub store: Arc<LoadStore>,
    /// Metric that carries this signal.
    pub metric: Box<str>,
    /// Age past which a sample is ignored.
    pub max_age_ms: i64,
    /// Relative weight in the combined score.
    pub weight: f64,
    /// Whether a lower reading is the better one.
    pub lower_is_better: bool,
    /// Whether the reading is already a 0.0 to 1.0 rating.
    pub already_normalised: bool,
}

impl Scorer for MetricScorer {
    fn measure(&self, candidates: &[&RouteCandidate], now_ms: i64) -> Vec<Option<f64>> {
        candidates
            .iter()
            .map(|c| {
                let key = LoadStore::key(&c.site, &c.cluster);
                self.store
                    .fresh(&key, &self.metric, now_ms, self.max_age_ms)
                    .map(|s| s.value)
            })
            .collect()
    }

    fn lower_is_better(&self) -> bool {
        self.lower_is_better
    }

    fn already_normalised(&self) -> bool {
        self.already_normalised
    }

    fn weight(&self) -> f64 {
        self.weight
    }
}

/// Combined score per candidate, or `None` where no scorer had anything to say.
pub(crate) fn score_all(scorers: &[Box<dyn Scorer>], candidates: &[&RouteCandidate], now_ms: i64) -> Vec<Option<f64>> {
    let mut totals = vec![0.0; candidates.len()];
    let mut weights = vec![0.0; candidates.len()];
    for scorer in scorers {
        let weight = scorer.weight();
        let measured = scorer.measure(candidates, now_ms);
        let rated = if scorer.already_normalised() {
            rate_directly(&measured, scorer.lower_is_better())
        } else {
            normalise(&measured, scorer.lower_is_better())
        };
        for (index, score) in rated.into_iter().enumerate() {
            if let (Some(score), Some(total), Some(sum)) = (score, totals.get_mut(index), weights.get_mut(index)) {
                *total += score * weight;
                *sum += weight;
            }
        }
    }
    totals
        .into_iter()
        .zip(weights)
        .map(|(total, sum)| (sum > 0.0).then(|| total / sum))
        .collect()
}

/// Pick the highest-scoring candidate, or `None` to keep the overlay's order.
///
/// A set where any candidate is unscored is returned unpicked. Preferring the
/// observable ones would let a provider that exposes no metrics lose every
/// request for a reason unrelated to how loaded it is.
pub(crate) fn pick<'a>(candidates: &[&'a RouteCandidate], scores: &[Option<f64>]) -> Option<&'a RouteCandidate> {
    if candidates.is_empty() || scores.len() != candidates.len() || scores.iter().any(Option::is_none) {
        return None;
    }
    let mut best: Option<(&RouteCandidate, f64)> = None;
    for (candidate, score) in candidates.iter().zip(scores.iter().flatten()) {
        // Strictly greater, so an earlier candidate wins a tie and the overlay's
        // order still decides where the signal does not.
        if best.is_none_or(|(_, high)| *score > high) {
            best = Some((candidate, *score));
        }
    }
    best.map(|(c, _)| c)
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
    use super::{
        super::descriptor::{AdmissionState, CapabilityKind},
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
            stable_id: Arc::from(format!("inference_model/llama/{site}/{cluster}")),
        }
    }

    /// Reports the values it was built with, standing in for a live signal.
    struct Fixed {
        values: Vec<Option<f64>>,
        lower_is_better: bool,
        weight: f64,
    }

    impl Scorer for Fixed {
        fn measure(&self, _candidates: &[&RouteCandidate], _now_ms: i64) -> Vec<Option<f64>> {
            self.values.clone()
        }

        fn lower_is_better(&self) -> bool {
            self.lower_is_better
        }

        fn weight(&self) -> f64 {
            self.weight
        }
    }

    fn scorers(values: Vec<Option<f64>>, lower_is_better: bool) -> Vec<Box<dyn Scorer>> {
        vec![Box::new(Fixed {
            values,
            lower_is_better,
            weight: 1.0,
        })]
    }

    #[test]
    fn the_lightest_candidate_wins() {
        let (a, b) = (candidate("east", "a"), candidate("west", "b"));
        let set = [&a, &b];
        let scores = score_all(&scorers(vec![Some(9.0), Some(1.0)], true), &set, 0);
        assert_eq!(
            pick(&set, &scores).map(|c| &*c.cluster),
            Some("b"),
            "lower queue is better"
        );
    }

    #[test]
    fn direction_is_respected() {
        let (a, b) = (candidate("east", "a"), candidate("west", "b"));
        let set = [&a, &b];
        let scores = score_all(&scorers(vec![Some(9.0), Some(1.0)], false), &set, 0);
        assert_eq!(
            pick(&set, &scores).map(|c| &*c.cluster),
            Some("a"),
            "higher is better here"
        );
    }

    #[test]
    fn an_unscored_candidate_leaves_the_overlay_order_alone() {
        let (a, b) = (candidate("east", "a"), candidate("west", "b"));
        let set = [&a, &b];
        let scores = score_all(&scorers(vec![None, Some(1.0)], true), &set, 0);
        assert!(
            pick(&set, &scores).is_none(),
            "a provider that reports nothing must not lose for that reason"
        );
    }

    #[test]
    fn nothing_observed_leaves_the_overlay_order_alone() {
        let (a, b) = (candidate("east", "a"), candidate("west", "b"));
        let set = [&a, &b];
        let scores = score_all(&scorers(vec![None, None], true), &set, 0);
        assert!(pick(&set, &scores).is_none(), "no signal, no change");
    }

    #[test]
    fn equal_measurements_keep_the_first_candidate() {
        let (a, b) = (candidate("east", "a"), candidate("west", "b"));
        let set = [&a, &b];
        let scores = score_all(&scorers(vec![Some(4.0), Some(4.0)], true), &set, 0);
        assert_eq!(
            pick(&set, &scores).map(|c| &*c.cluster),
            Some("a"),
            "a tie is decided by the overlay, not arbitrarily"
        );
    }

    #[test]
    fn weights_decide_between_disagreeing_signals() {
        let (a, b) = (candidate("east", "a"), candidate("west", "b"));
        let set = [&a, &b];
        let scorers: Vec<Box<dyn Scorer>> = vec![
            Box::new(Fixed {
                values: vec![Some(1.0), Some(9.0)],
                lower_is_better: true,
                weight: 3.0,
            }),
            Box::new(Fixed {
                values: vec![Some(9.0), Some(1.0)],
                lower_is_better: true,
                weight: 1.0,
            }),
        ];
        let scores = score_all(&scorers, &set, 0);
        assert_eq!(
            pick(&set, &scores).map(|c| &*c.cluster),
            Some("a"),
            "the heavier signal decides"
        );
    }

    #[test]
    fn a_score_set_of_the_wrong_length_is_refused() {
        let a = candidate("east", "a");
        let set = [&a];
        assert!(
            pick(&set, &[Some(1.0), Some(2.0)]).is_none(),
            "mismatched lengths cannot be trusted"
        );
    }
}

#[cfg(test)]
mod upstream_parity_tests {
    use super::{normalise, rate_directly};

    // The endpoint picker's multicluster queue scorer computes
    // (maxQ - q) / (maxQ - minQ), and 1.0 when every candidate is equal.
    // Ours has to agree, or a grid routes differently from a pool.
    #[test]
    fn queue_depth_matches_the_endpoint_pickers_min_max_rating() {
        let measured = [Some(2.0), Some(6.0), Some(10.0)];
        let rated = normalise(&measured, true);
        let (min_q, max_q) = (2.0_f64, 10.0_f64);
        for (i, raw) in measured.iter().enumerate() {
            let expected = raw.map(|q| (max_q - q) / (max_q - min_q));
            assert_eq!(rated.get(i).copied().flatten(), expected, "candidate {i}");
        }
    }

    #[test]
    fn an_equal_queue_across_candidates_rates_every_one_at_the_top() {
        let rated = normalise(&[Some(4.0), Some(4.0)], true);
        assert_eq!(rated, vec![Some(1.0), Some(1.0)]);
    }

    // Their KV scorer rates 1 - utilisation, absolute. Min-max scaling would
    // turn a trivial spread into a decisive one.
    #[test]
    fn kv_cache_rates_one_minus_utilisation_rather_than_a_spread() {
        let rated = rate_directly(&[Some(0.10), Some(0.12)], true);
        assert_eq!(rated, vec![Some(0.90), Some(0.88)]);

        let stretched = normalise(&[Some(0.10), Some(0.12)], true);
        assert_eq!(
            stretched,
            vec![Some(1.0), Some(0.0)],
            "min-max would have made two similar pools look opposite"
        );
    }

    #[test]
    fn a_utilisation_outside_the_ratio_is_clamped_not_trusted() {
        assert_eq!(
            rate_directly(&[Some(1.4), Some(-0.2)], true),
            vec![Some(0.0), Some(1.0)]
        );
    }

    #[test]
    fn a_candidate_the_signal_says_nothing_about_stays_unrated() {
        assert_eq!(rate_directly(&[None, Some(0.25)], true), vec![None, Some(0.75)]);
    }
}

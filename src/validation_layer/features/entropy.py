"""Entropy and information-density diagnostics for candidate features."""

from __future__ import annotations

from ..config.schemas import ValidationConfig
from ..data.contracts import FeatureSeries
from ..data.normalization import normalize_feature_series
from ..data.sessioning import build_regime_labels, build_session_labels
from ..reporting.models import ValidationModuleResult
from ..utils.stats import bucket_counts, clamp01, normalized_entropy, quantile_buckets, safe_mean, safe_stdev


def run_entropy_module(feature: FeatureSeries, config: ValidationConfig) -> ValidationModuleResult:
    normalized = normalize_feature_series(feature)
    values = normalized.values
    bucket_ids = quantile_buckets(values, config.entropy_bins)
    states = tuple(f"state_{bucket}" for bucket in bucket_ids)
    occupancy = bucket_counts(states)
    entropy_score_raw = normalized_entropy(occupancy.values())
    top_state_share = max(occupancy.values()) / len(states) if states else 1.0

    transitions = list(zip(states[:-1], states[1:]))
    transition_labels = [f"{left}->{right}" for left, right in transitions]
    transition_counts = bucket_counts(transition_labels)
    transition_concentration = max(transition_counts.values()) / len(transitions) if transitions else 0.0
    churn_measure = len(set(transition_labels)) / len(transitions) if transitions else 0.0

    sessions = build_session_labels(normalized.timestamps, config)
    regimes = build_regime_labels(len(values), config)
    information_density_by_regime: dict[str, float] = {}
    for label_source, label_values in (("session", sessions), ("regime", regimes)):
        grouped: dict[str, list[str]] = {}
        for label, state in zip(label_values, states):
            grouped.setdefault(label, []).append(state)
        for label, group in grouped.items():
            information_density_by_regime[f"{label_source}:{label}"] = normalized_entropy(bucket_counts(group).values())

    session_information_consistency = clamp01(
        1.0 - safe_stdev(list(information_density_by_regime.values())) / (safe_mean(list(information_density_by_regime.values())) + 1e-9)
    )
    entropy_score = clamp01(1.0 - abs(entropy_score_raw - 0.65) / 0.65)
    state_concentration_score = clamp01(1.0 - max(0.0, top_state_share - 0.45) / 0.55)

    status = "pass"
    if entropy_score < config.thresholds.entropy_min:
        status = "warn"
    summary = (
        f"Entropy score {entropy_score:.2f}, top-state share {top_state_share:.2f}, "
        f"session consistency {session_information_consistency:.2f}."
    )
    return ValidationModuleResult(
        module_name="entropy",
        status=status,
        summary=summary,
        metrics={
            "entropy_score": round(entropy_score, config.report_precision),
            "state_concentration_score": round(state_concentration_score, config.report_precision),
            "session_information_consistency": round(session_information_consistency, config.report_precision),
            "transition_concentration": round(transition_concentration, config.report_precision),
            "churn_measure": round(churn_measure, config.report_precision),
        },
        diagnostics={
            "entropy_like_dispersion_metric": entropy_score_raw,
            "state_occupancy_distribution": dict(occupancy),
            "transition_concentration_or_churn": {
                "transition_concentration": transition_concentration,
                "churn_measure": churn_measure,
            },
            "information_density_summary_by_regime_session": information_density_by_regime,
        },
        artifacts={},
        recommendations=["Use this as a noise filter, not an alpha claim."],
    )

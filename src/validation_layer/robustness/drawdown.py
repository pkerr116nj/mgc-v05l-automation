"""Drawdown geometry analysis."""

from __future__ import annotations

from dataclasses import dataclass

from ..config.schemas import ValidationConfig
from ..data.contracts import StrategyBacktest
from ..data.normalization import normalize_strategy_backtest
from ..data.sessioning import build_regime_labels
from ..reporting.models import ValidationModuleResult
from ..utils.stats import clamp01, percentile, safe_divide, safe_mean


@dataclass(frozen=True)
class DrawdownProfile:
    max_drawdown: float
    average_drawdown: float
    drawdown_duration: int
    recovery_duration: int
    tail_severity: float
    clustering_of_pain: float
    regime_linked_drawdown_concentration: float
    ulcer_like_pain: float
    drawdowns: tuple[float, ...]

    def to_dict(self) -> dict[str, float | int | list[float]]:
        return {
            "max_drawdown": self.max_drawdown,
            "average_drawdown": self.average_drawdown,
            "drawdown_duration": self.drawdown_duration,
            "recovery_duration": self.recovery_duration,
            "tail_severity": self.tail_severity,
            "clustering_of_pain": self.clustering_of_pain,
            "regime_linked_drawdown_concentration": self.regime_linked_drawdown_concentration,
            "ulcer_like_pain": self.ulcer_like_pain,
            "drawdowns": list(self.drawdowns),
        }


def calculate_drawdown_profile(strategy: StrategyBacktest, config: ValidationConfig) -> DrawdownProfile:
    normalized = normalize_strategy_backtest(strategy)
    points = normalized.equity_curve
    running_peak = points[0].equity
    drawdowns: list[float] = []
    current_duration = 0
    max_duration = 0
    episode_lengths: list[int] = []
    episode_drawdowns: list[float] = []
    current_episode_max = 0.0
    current_episode_length = 0

    regimes = build_regime_labels(len(points), config)
    regime_pain: dict[str, float] = {}

    for index, point in enumerate(points):
        running_peak = max(running_peak, point.equity)
        drawdown = max(0.0, running_peak - point.equity)
        drawdowns.append(drawdown)
        if drawdown > 0:
            current_duration += 1
            current_episode_length += 1
            current_episode_max = max(current_episode_max, drawdown)
            regime = regimes[index]
            regime_pain[regime] = regime_pain.get(regime, 0.0) + drawdown
        else:
            if current_episode_length:
                episode_lengths.append(current_episode_length)
                episode_drawdowns.append(current_episode_max)
            current_duration = 0
            current_episode_length = 0
            current_episode_max = 0.0
        max_duration = max(max_duration, current_duration)

    if current_episode_length:
        episode_lengths.append(current_episode_length)
        episode_drawdowns.append(current_episode_max)

    non_zero_drawdowns = [value for value in drawdowns if value > 0]
    average_drawdown = safe_mean(non_zero_drawdowns)
    max_drawdown = max(drawdowns) if drawdowns else 0.0
    tail_severity = percentile(non_zero_drawdowns, 0.95) if non_zero_drawdowns else 0.0
    ulcer_like_pain = (safe_mean([value * value for value in non_zero_drawdowns]) ** 0.5) if non_zero_drawdowns else 0.0
    clustering = safe_divide(safe_mean(episode_lengths), len(drawdowns), default=0.0)
    pain_total = sum(regime_pain.values())
    regime_concentration = max(regime_pain.values()) / pain_total if pain_total else 0.0
    recovery_duration = max(episode_lengths) if episode_lengths else 0
    return DrawdownProfile(
        max_drawdown=max_drawdown,
        average_drawdown=average_drawdown,
        drawdown_duration=max_duration,
        recovery_duration=recovery_duration,
        tail_severity=tail_severity,
        clustering_of_pain=clustering,
        regime_linked_drawdown_concentration=regime_concentration,
        ulcer_like_pain=ulcer_like_pain,
        drawdowns=tuple(drawdowns),
    )


def run_drawdown_module(strategy: StrategyBacktest, config: ValidationConfig) -> ValidationModuleResult:
    profile = calculate_drawdown_profile(strategy, config)
    terminal_equity = strategy.equity_curve[-1].equity
    initial_equity = strategy.equity_curve[0].equity
    net_pnl = terminal_equity - initial_equity
    severity_ratio = safe_divide(profile.max_drawdown, abs(net_pnl) + 1.0)
    duration_ratio = safe_divide(profile.drawdown_duration, len(strategy.equity_curve), default=1.0)
    drawdown_score = clamp01((1.0 / (1.0 + severity_ratio) + (1.0 - duration_ratio)) / 2.0)
    pain_penalty = safe_mean(
        [
            safe_divide(profile.tail_severity, profile.max_drawdown or 1.0, default=0.0),
            profile.clustering_of_pain,
            profile.regime_linked_drawdown_concentration,
        ]
    )
    pain_profile_score = clamp01(1.0 - pain_penalty)
    status = "pass"
    if drawdown_score < config.thresholds.drawdown_min:
        status = "fail"
    elif pain_profile_score < config.thresholds.pain_profile_min:
        status = "warn"
    summary = (
        f"Max drawdown {profile.max_drawdown:.2f}, worst duration {profile.drawdown_duration} points, "
        f"pain concentration {profile.regime_linked_drawdown_concentration:.2f}."
    )
    recommendations: list[str] = []
    if status != "pass":
        recommendations.append("Reduce drawdown depth and recovery drag before promotion.")
    return ValidationModuleResult(
        module_name="drawdown",
        status=status,
        summary=summary,
        metrics={
            "max_drawdown": round(profile.max_drawdown, config.report_precision),
            "average_drawdown": round(profile.average_drawdown, config.report_precision),
            "drawdown_duration": profile.drawdown_duration,
            "recovery_duration": profile.recovery_duration,
            "tail_severity": round(profile.tail_severity, config.report_precision),
            "clustering_of_pain": round(profile.clustering_of_pain, config.report_precision),
            "regime_linked_drawdown_concentration": round(profile.regime_linked_drawdown_concentration, config.report_precision),
            "drawdown_score": round(drawdown_score, config.report_precision),
            "pain_profile_score": round(pain_profile_score, config.report_precision),
        },
        diagnostics=profile.to_dict(),
        artifacts={},
        recommendations=recommendations,
    )

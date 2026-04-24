"""Small statistical helpers built only from the standard library."""

from __future__ import annotations

import math
from collections import Counter
from statistics import mean, median, pstdev
from typing import Iterable, Sequence


def clamp01(value: float) -> float:
    return max(0.0, min(1.0, value))


def safe_divide(numerator: float, denominator: float, default: float = 0.0) -> float:
    if abs(denominator) < 1e-12:
        return default
    return numerator / denominator


def safe_mean(values: Sequence[float]) -> float:
    return mean(values) if values else 0.0


def safe_median(values: Sequence[float]) -> float:
    return median(values) if values else 0.0


def safe_stdev(values: Sequence[float]) -> float:
    return pstdev(values) if len(values) > 1 else 0.0


def percentile(values: Sequence[float], q: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    position = clamp01(q) * (len(ordered) - 1)
    low = int(math.floor(position))
    high = int(math.ceil(position))
    if low == high:
        return float(ordered[low])
    weight = position - low
    return float(ordered[low] * (1.0 - weight) + ordered[high] * weight)


def rolling_windows(values: Sequence[float], window: int) -> tuple[tuple[float, ...], ...]:
    if window <= 0 or len(values) < window:
        return ()
    return tuple(tuple(values[index : index + window]) for index in range(len(values) - window + 1))


def normalized_entropy(counts: Iterable[int]) -> float:
    observed = [count for count in counts if count > 0]
    if not observed:
        return 0.0
    total = sum(observed)
    if total <= 0:
        return 0.0
    probabilities = [count / total for count in observed]
    entropy = -sum(probability * math.log(probability) for probability in probabilities)
    max_entropy = math.log(len(observed)) if len(observed) > 1 else 1.0
    return clamp01(entropy / max_entropy if max_entropy else 0.0)


def linear_slope(values: Sequence[float]) -> float:
    if len(values) < 2:
        return 0.0
    xs = list(range(len(values)))
    x_mean = safe_mean(xs)
    y_mean = safe_mean(values)
    numerator = sum((x - x_mean) * (y - y_mean) for x, y in zip(xs, values))
    denominator = sum((x - x_mean) ** 2 for x in xs)
    return safe_divide(numerator, denominator)


def run_lengths(flags: Sequence[bool]) -> tuple[int, ...]:
    if not flags:
        return ()
    lengths: list[int] = []
    current = flags[0]
    count = 1
    for flag in flags[1:]:
        if flag == current:
            count += 1
            continue
        lengths.append(count)
        current = flag
        count = 1
    lengths.append(count)
    return tuple(lengths)


def bucket_counts(values: Sequence[str]) -> Counter[str]:
    return Counter(values)


def quantile_buckets(values: Sequence[float], bucket_count: int) -> tuple[int, ...]:
    if not values:
        return ()
    if bucket_count <= 1:
        return tuple(0 for _ in values)
    ordered = sorted(values)
    thresholds = [percentile(ordered, index / bucket_count) for index in range(1, bucket_count)]
    assignments: list[int] = []
    for value in values:
        bucket = 0
        while bucket < len(thresholds) and value > thresholds[bucket]:
            bucket += 1
        assignments.append(bucket)
    return tuple(assignments)

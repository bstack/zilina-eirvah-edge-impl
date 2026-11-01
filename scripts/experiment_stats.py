"""Statistics over observed scrape windows, never percentiles of counter samples."""

from __future__ import annotations

import json
import math
from collections import defaultdict
from itertools import pairwise
from typing import Any

Row = tuple[float, str, str, float]


def _increase(samples: list[tuple[float, float]]) -> tuple[float | None, str]:
    if len(samples) < 2 or samples[-1][0] <= samples[0][0]:
        return None, "insufficient distinct samples"
    if any(b[1] < a[1] for a, b in pairwise(samples)):
        return None, "counter reset observed; increase unknown"
    return samples[-1][1] - samples[0][
        1
    ], "observed window only; unseen resets/increments not recoverable"


def _quantile(buckets: list[tuple[float, float]], quantile: float) -> float | None:
    total = buckets[-1][1]
    if total <= 0:
        return None
    rank = total * quantile
    lower, previous = 0.0, 0.0
    for upper, count in buckets:
        if count >= rank:
            # An unbounded bucket cannot establish a finite latency percentile.
            if not math.isfinite(upper):
                return None
            if count == previous:
                return upper
            return lower + (upper - lower) * (rank - previous) / (count - previous)
        lower, previous = upper, count
    return None


def summarize(rows: list[Row]) -> list[dict[str, Any]]:
    series: dict[tuple[str, str], dict[float, float]] = defaultdict(dict)
    for timestamp, metric, labels, value in rows:
        series[metric, labels][timestamp] = value
    summary: list[dict[str, Any]] = []
    histograms: dict[tuple[str, str], dict[float, list[tuple[float, float]]]] = defaultdict(dict)
    for (metric, labels), values in sorted(series.items()):
        samples = sorted(values.items())
        duration = samples[-1][0] - samples[0][0]
        row: dict[str, Any] = {
            "metric": metric,
            "labels": labels,
            "samples": len(samples),
            "window_start": samples[0][0],
            "window_end": samples[-1][0],
            "elapsed_seconds": duration,
        }
        if metric.endswith("_bucket"):
            tags = json.loads(labels)
            bound = float(tags.pop("le"))
            key = metric.removesuffix("_bucket"), json.dumps(tags, sort_keys=True)
            histograms[key][bound] = samples
        if metric.endswith(("_total", "_sum", "_count", "_bucket")):
            increase, limitation = _increase(samples)
            row.update(
                kind="counter",
                increase=increase,
                rate_per_second=increase / duration if increase is not None else None,
                limitation=limitation,
            )
        else:
            observed = [value for _, value in samples]
            row.update(
                kind="gauge",
                sample_mean=sum(observed) / len(observed),
                minimum=min(observed),
                maximum=max(observed),
                limitation="sample statistics, not a time-weighted mean",
            )
        summary.append(row)

    for (metric, labels), bucket_series in sorted(histograms.items()):
        row = {
            "metric": metric,
            "labels": labels,
            "kind": "histogram",
            "observations": None,
            "mean_seconds": None,
            "p50_seconds": None,
            "p95_seconds": None,
            "p99_seconds": None,
        }
        count_series = series.get((metric + "_count", labels), {})
        sum_series = series.get((metric + "_sum", labels), {})
        members = [dict(samples) for samples in bucket_series.values()] + [count_series, sum_series]
        common = set.intersection(*(set(member) for member in members))
        if len(common) < 2 or math.inf not in bucket_series:
            row["limitation"] = "incomplete histogram or fewer than two aligned snapshots"
            summary.append(row)
            continue
        start, end = min(common), max(common)
        increases = []
        for member in members:
            delta, _ = _increase(sorted((t, v) for t, v in member.items() if start <= t <= end))
            increases.append(delta)
        if any(value is None for value in increases):
            row["limitation"] = "histogram reset or insufficient evidence; percentiles unknown"
            summary.append(row)
            continue
        # Use the same aligned window for every bucket, count and sum.
        buckets = sorted(
            (bound, dict(samples)[end] - dict(samples)[start])
            for bound, samples in bucket_series.items()
        )
        observations = count_series[end] - count_series[start]
        if (
            observations != buckets[-1][1]
            or any(count < 0 for _, count in buckets)
            or any(a[1] > b[1] for a, b in pairwise(buckets))
        ):
            row["limitation"] = "inconsistent histogram; percentiles unknown"
            summary.append(row)
            continue
        row.update(
            observations=observations,
            window_start=start,
            window_end=end,
            elapsed_seconds=end - start,
            mean_seconds=(sum_series[end] - sum_series[start]) / observations
            if observations
            else None,
            limitation="approximate bucket-interpolated per-label percentiles over observed window; "
            "no pooled replica percentile; blank tails exceed finite buckets or have no observations",
        )
        for quantile in (50, 95, 99):
            row[f"p{quantile}_seconds"] = _quantile(buckets, quantile / 100)
        summary.append(row)
    return summary

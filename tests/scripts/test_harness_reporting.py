"""Regression checks for trustworthy experiment evidence."""

from __future__ import annotations

import asyncio
import json
import sys
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock, patch

import harness
import httpx
import pytest
from experiment_stats import summarize


def test_counter_summary_uses_observed_difference_and_elapsed_time() -> None:
    report = summarize([(0, "completed_total", "{}", 10), (10, "completed_total", "{}", 30)])
    row = report[0]
    assert row["increase"] == 20
    assert row["rate_per_second"] == 2
    assert row.get("p95_seconds") is None


def test_histogram_percentiles_use_bucket_increases_not_sampled_sums() -> None:
    rows = []
    for bound, count in [("0.1", 50), ("0.5", 90), ("1", 100), ("+Inf", 100)]:
        labels = json.dumps({"path": "telemetry", "le": bound})
        rows.extend([(0, "latency_bucket", labels, 0), (10, "latency_bucket", labels, count)])
    rows.extend(
        [
            (0, "latency_count", '{"path": "telemetry"}', 0),
            (10, "latency_count", '{"path": "telemetry"}', 100),
            (0, "latency_sum", '{"path": "telemetry"}', 0),
            (10, "latency_sum", '{"path": "telemetry"}', 30),
        ]
    )
    histogram = next(row for row in summarize(rows) if row["kind"] == "histogram")
    assert histogram["observations"] == 100
    assert histogram["mean_seconds"] == pytest.approx(0.3)
    assert histogram["p95_seconds"] == pytest.approx(0.75)
    assert histogram["p99_seconds"] == pytest.approx(0.95)
    assert "approximate" in histogram["limitation"]


def test_counter_reset_and_single_sample_do_not_fabricate_throughput() -> None:
    reset = summarize([(0, "done_total", "{}", 10), (10, "done_total", "{}", 2)])[0]
    assert reset["increase"] is None
    assert "reset" in reset["limitation"]
    single = summarize([(0, "done_total", "{}", 10)])[0]
    assert single["rate_per_second"] is None


@pytest.mark.parametrize(
    "response",
    [
        {"status": "success", "data": {"resultType": "vector", "result": []}},
        {"status": "error", "error": "bad query"},
    ],
)
async def test_missing_and_failed_queries_are_retained(tmp_path: Path, response: dict) -> None:
    scraper = harness.Scraper("http://test", {"required_total": "required_total"})
    client = httpx.AsyncClient(
        transport=httpx.MockTransport(lambda req: httpx.Response(200, json=response))
    )
    async with client:
        await scraper._poll_once(client)
    scraper.flush(tmp_path)
    report = json.loads((tmp_path / "collection.json").read_text())
    assert report["complete"] is False
    assert report["issues"][0]["metric"] == "required_total"
    assert (tmp_path / "raw.parquet").exists()


async def test_optional_hpa_absence_is_visible_but_not_a_failed_baseline(tmp_path: Path) -> None:
    scraper = harness.Scraper(
        "http://test", {"hpa_replicas": "hpa"}, optional_queries={"hpa_replicas"}
    )
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(
            lambda req: httpx.Response(
                200, json={"status": "success", "data": {"resultType": "vector", "result": []}}
            )
        )
    ) as client:
        await scraper._poll_once(client)
    scraper.flush(tmp_path)
    report = json.loads((tmp_path / "collection.json").read_text())
    assert report["issues"] and report["complete"] is True


async def test_child_failure_retains_exit_code_and_stderr(tmp_path: Path) -> None:
    result = await harness._run_child(
        [sys.executable, "-c", "import sys; print('failed', file=sys.stderr); sys.exit(7)"],
        tmp_path,
        "generator",
        timeout=5,
    )
    assert result["exit_code"] == 7
    assert result["timed_out"] is False
    assert "failed" in (tmp_path / "generator.stderr.log").read_text()


async def test_child_timeout_is_not_success(tmp_path: Path) -> None:
    result = await harness._run_child(
        [sys.executable, "-c", "import time; time.sleep(30)"], tmp_path, "generator", timeout=0.1
    )
    assert result["timed_out"] is True


async def test_failed_run_persists_reason_and_returns_failed(tmp_path: Path) -> None:
    cfg = harness.HarnessConfig(experiment="c", output_dir=tmp_path)
    with patch.dict(
        harness._EXPERIMENTS, {"c": AsyncMock(side_effect=RuntimeError("inject failed"))}
    ):
        outcome = await harness._run(cfg)
    report = json.loads(next(tmp_path.rglob("run.json")).read_text())
    assert outcome == report["outcome"] == "failed"
    assert "inject failed" in report["details"]["error"]
    assert "namespace" in report["params"]


async def test_incomplete_measurements_cannot_become_success(tmp_path: Path) -> None:
    (tmp_path / "collection.json").write_text('{"complete": false, "issues": []}')
    result = harness._result(tmp_path, {"generator": {"exit_code": 0, "timed_out": False}})
    assert result["outcome"] == "incomplete"


async def test_dry_run_creates_no_evidence_files(tmp_path: Path) -> None:
    outcome = await harness._run(
        harness.HarnessConfig(experiment="c", output_dir=tmp_path, dry_run=True)
    )
    assert outcome == "dry_run"
    assert not list(tmp_path.iterdir())


async def test_generator_summary_is_machine_readable(tmp_path: Path) -> None:
    from types import SimpleNamespace

    from load_inject import run

    target = tmp_path / "generator.json"
    client = SimpleNamespace(
        request=AsyncMock(side_effect=Exception("rejected")), close=AsyncMock()
    )
    with patch("load_inject.nats.connect", AsyncMock(return_value=client)):
        stats = await run(rate=1, duration=0.01, nats_url="nats://test", summary_path=target)
    report = json.loads(target.read_text())
    assert report["sent"] == stats.sent
    assert report["errors"] == stats.errors
    assert report["send_rate_per_second"] > 0
    assert report["completion_rate_per_second"] == 0


def test_invalid_generator_accounting_fails_run(tmp_path: Path) -> None:
    (tmp_path / "collection.json").write_text('{"complete": true}')
    (tmp_path / "generator.json").write_text(
        '{"sent": 10, "succeeded": 5, "errors": 0, "unresolved": 0}'
    )
    result = harness._result(
        tmp_path, {"generator": {"exit_code": 0, "timed_out": False}}, require_generator=True
    )
    assert result["outcome"] == "failed"


@pytest.mark.parametrize(
    "exit_code,complete,expected", [(0, True, "ok"), (7, True, "failed"), (0, False, "incomplete")]
)
async def test_load_experiment_retains_generator_and_process_results(
    tmp_path: Path,
    exit_code: int,
    complete: bool,
    expected: str,
) -> None:
    @asynccontextmanager
    async def port(*args: Any, **kwargs: Any):
        yield

    @asynccontextmanager
    async def collect(scraper: Any, directory: Path):
        yield
        (directory / "collection.json").write_text(json.dumps({"complete": complete}))

    async def child(cmd: list[str], directory: Path, name: str, **kwargs: Any) -> dict:
        assert "--summary-json" in cmd
        summary = dict(
            sent=10,
            succeeded=10,
            errors=0,
            unresolved=0,
            send_rate_per_second=10,
            completion_rate_per_second=9,
        )
        (directory / "generator.json").write_text(json.dumps(summary))
        return dict(exit_code=exit_code, timed_out=False)

    with (
        patch("harness.port_forward", port),
        patch("harness._collect", collect),
        patch("harness._run_child", side_effect=child),
        patch("harness._sleep", AsyncMock()),
    ):
        outcome = await harness._run(harness.HarnessConfig(experiment="c", output_dir=tmp_path))
    report = json.loads(next(tmp_path.rglob("run.json")).read_text())
    assert outcome == report["outcome"] == expected
    assert report["details"]["generator"]["sent"] == 10
    assert report["details"]["processes"]["generator"]["exit_code"] == exit_code


async def test_interruption_retains_partial_raw_evidence_and_status(tmp_path: Path) -> None:
    async def experiment(cfg: Any, directory: Path) -> dict:
        scraper = harness.Scraper("http://test", {})
        scraper.rows = [(1, "done_total", "{}", 1)]
        with (
            patch.object(scraper, "check_connectivity", AsyncMock()),
            patch.object(scraper, "run", AsyncMock()),
        ):
            async with harness._collect(scraper, directory):
                raise asyncio.CancelledError

    with patch.dict(harness._EXPERIMENTS, {"c": experiment}), pytest.raises(asyncio.CancelledError):
        await harness._run(harness.HarnessConfig(experiment="c", output_dir=tmp_path))
    report = json.loads(next(tmp_path.rglob("run.json")).read_text())
    assert report["outcome"] == "interrupted"
    assert list(tmp_path.rglob("raw.parquet"))


def test_unbounded_histogram_tail_is_unknown() -> None:
    from experiment_stats import _quantile

    assert _quantile([(0.1, 1), (float("inf"), 100)], 0.99) is None


def test_repeated_flush_does_not_duplicate_raw_samples(tmp_path: Path) -> None:
    import pyarrow.parquet as pq

    scraper = harness.Scraper("http://test", {})
    scraper.rows = [(1, "done_total", "{}", 1)]
    scraper.flush(tmp_path)
    scraper.flush(tmp_path)
    assert pq.read_table(tmp_path / "raw.parquet").num_rows == 1


async def test_stale_samples_are_flagged(tmp_path: Path) -> None:
    scraper = harness.Scraper("http://test", {"required_total": "required_total"})
    response = {
        "status": "success",
        "data": {"resultType": "vector", "result": [{"metric": {}, "value": [1, "10"]}]},
    }
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(lambda req: httpx.Response(200, json=response))
    ) as client:
        await scraper._poll_once(client)
    scraper.flush(tmp_path)
    report = json.loads((tmp_path / "collection.json").read_text())
    assert report["complete"] is False
    assert "stale" in report["issues"][0]["reason"]

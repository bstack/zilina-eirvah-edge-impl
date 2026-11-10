"""Missing samples and failed trials must survive combined analysis."""

from study_report import paired_summary, summarize_resources


def test_missing_metrics_do_not_turn_into_zero_cpu():
    rows = [dict(backend="kubernetes", status="unknown", values=[], error="unavailable")]
    result = summarize_resources(rows)
    assert result["coverage"] == "unknown"
    assert result["cpu_peak_cores"] is None


def test_observed_metrics_have_explicit_sample_scope():
    rows = [
        dict(
            backend="kubernetes",
            status="ok",
            missing_pods=[],
            values=[dict(cpu_cores=0.1, memory_bytes=100)],
        ),
        dict(
            backend="kubernetes",
            status="ok",
            missing_pods=[],
            values=[dict(cpu_cores=0.3, memory_bytes=120)],
        ),
    ]
    result = summarize_resources(rows)
    assert result["cpu_peak_cores"] == 0.3
    assert result["memory_peak_bytes"] == 120
    assert result["coverage"] == "observed_samples"


def test_paired_analysis_does_not_pool_requests_across_repetitions():
    rows = [
        dict(repetition=1, mode="shared", flow="command", p95_s=0.2),
        dict(repetition=1, mode="separate", flow="command", p95_s=0.1),
        dict(repetition=2, mode="shared", flow="command", p95_s=0.4),
        dict(repetition=2, mode="separate", flow="command", p95_s=0.3),
    ]
    result = paired_summary(rows)
    assert result[0]["pairs"] == 2
    assert round(result[0]["median_separate_minus_shared_s"], 6) == -0.1


def test_long_collection_gap_remains_partial():
    rows = [
        dict(
            backend="kubernetes",
            status="ok",
            collected_begin=t,
            values=[dict(cpu_cores=0.1, memory_bytes=100)],
        )
        for t in ("2026-11-10T10:00:00+00:00", "2026-11-10T10:01:00+00:00")
    ]

    result = summarize_resources(rows, interval=5)

    assert result["coverage"] == "partial"
    assert result["largest_collection_gap_s"] == 60
    assert result["delayed_collection_intervals"] == 1


def test_missing_planned_trials_cannot_pass_combined_execution(tmp_path):
    import json

    from study_report import write_report

    suite = dict(
        status="pass",
        trials=[],
        plan=dict(
            scope="local_engineering",
            repetitions=2,
            stages=["probe"],
            data_rates=[60],
            budgets=None,
        ),
    )
    (tmp_path / "suite.json").write_text(json.dumps(suite))

    result = write_report(tmp_path)

    assert result["execution"] == "unknown"
    assert result["formal_study"] == "not_established"


def test_missing_pods_make_namespace_resources_partial():
    rows = [
        dict(
            backend="kubernetes",
            status="ok",
            missing_pods=["worker"],
            values=[dict(cpu_cores=0.1, memory_bytes=100)],
        )
    ]

    result = summarize_resources(rows)

    assert result["coverage"] == "partial"
    assert result["missing_attempts"] == 1


def test_achieved_rate_uses_observed_schedule_and_drain():
    from study_report import workload_rates

    rows = [
        dict(flow="command", sent=1, scheduled=0, latency_s=1, status="ok"),
        dict(flow="data", sent=4, scheduled=3, latency_s=1, status="ok"),
    ]

    result = workload_rates(rows, "command")

    assert result["observed_window_s"] == 5
    assert result["achieved_completions_per_s"] == 0.2
    assert result["submission_delay_p95_s"] == 1

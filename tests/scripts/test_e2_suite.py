"""Selected preparation cannot be confused with whole-study conformance."""

import pytest
from e2_suite import assess


@pytest.mark.parametrize(
    "case,status",
    [("native", "fail"), ("quality", "unknown"), ("commands", "fail"), ("all", "pass")],
)
def test_summary_preserves_failures_unknowns_and_scope(monkeypatch, case, status):
    import e2_suite

    for name, key in (
        ("native", "native_case"),
        ("quality", "quality_case"),
        ("commands", "command_case"),
    ):
        verdict = status if name == case else "pass"
        monkeypatch.setattr(
            e2_suite,
            {
                "native": "evaluate_native",
                "quality": "evaluate_quality",
                "commands": "evaluate_commands",
            }[name],
            lambda data, key=key, verdict=verdict: {key: verdict},
        )

    report = assess(dict(source_addition_requested=True), {}, {})

    assert report["selected_preparation"] == status
    assert report["whole_study"] == "unknown"
    assert report["C04"] == "unknown"


def test_native_default_profile_does_not_establish_source_addition(monkeypatch):
    import e2_suite

    monkeypatch.setattr(e2_suite, "evaluate_native", lambda data: dict(native_case="pass"))
    monkeypatch.setattr(e2_suite, "evaluate_quality", lambda data: dict(quality_case="pass"))
    monkeypatch.setattr(e2_suite, "evaluate_commands", lambda data: dict(command_case="pass"))

    report = assess({}, {}, {})

    assert report["selected_preparation"] == "unknown"

"""Collect the selected E2 preparation results without promoting whole-study conformance."""

from __future__ import annotations

import argparse
import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from e2_command_response import evaluate as evaluate_commands
from e2_native import evaluate as evaluate_native
from e2_semantics import QUALITY_SOURCES, capture_quality, evaluate_quality
from interoperability import ROOT, digest


def assess(
    native: dict[str, Any], quality: dict[str, Any], commands: dict[str, Any]
) -> dict[str, Any]:
    reports = dict(
        native=evaluate_native(native),
        quality=evaluate_quality(quality),
        commands=evaluate_commands(commands),
    )
    verdicts = [
        reports["native"]["native_case"],
        reports["quality"]["quality_case"],
        reports["commands"]["command_case"],
    ]
    # A passing native default profile does not include the gateway-addition task.
    if not native.get("source_addition_requested"):
        verdicts.append("unknown")
    status = "fail" if "fail" in verdicts else "unknown" if "unknown" in verdicts else "pass"
    return dict(
        selected_preparation=status,
        whole_study="unknown",
        C04="unknown",
        reports=reports,
        scope="selected native acquisition, gateway addition, in-process quality policies and paired operator command/process response",
        excluded=[
            "live automatic consumer-to-command lineage",
            "integration human effort",
            "controlled repeated comparative experiments",
            "scaling",
            "deployment flexibility",
        ],
    )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--native-run", type=Path, required=True)
    parser.add_argument("--command-run", type=Path, required=True)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    output = args.output or ROOT / "results/e2-preparation" / datetime.now(UTC).strftime(
        "%Y-%m-%dT%H-%M-%S-%fZ"
    )
    output.mkdir(parents=True, exist_ok=False)
    native_path, command_path = (
        args.native_run / "evidence.json",
        args.command_run / "evidence.json",
    )
    native, commands = (
        json.loads(p.read_text(encoding="utf-8")) for p in (native_path, command_path)
    )
    # Ensure this summary refers to the same acquisition evidence as the paired command case.
    if commands.get("native_sha256") != digest(native_path):
        parser.error("command rehearsal used different native acquisition evidence")
    quality = capture_quality()
    report = assess(native, quality, commands)
    report["inputs"] = {str(p.resolve()): digest(p) for p in (native_path, command_path)}
    report["summary_source_sha256"] = digest(Path(__file__))
    (output / "quality-evidence.json").write_text(
        json.dumps(quality, indent=2, allow_nan=False) + "\n", encoding="utf-8"
    )
    (output / "report.json").write_text(
        json.dumps(report, indent=2, allow_nan=False) + "\n", encoding="utf-8"
    )
    for source in [*QUALITY_SOURCES, Path(__file__)]:
        target = output / "sources" / source.relative_to(ROOT)
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(source.read_bytes())
    text = (
        "# Selected local E2 preparation\n\nSelected profile: **"
        + report["selected_preparation"]
        + "**. Whole study and C04: **unknown**.\n\n"
    )
    text += "| Case | Verdict |\n| --- | --- |\n"
    for name, key in (
        ("native", "native_case"),
        ("quality", "quality_case"),
        ("commands", "command_case"),
    ):
        text += "| " + name + " | " + report["reports"][name][key] + " |\n"
    text += "\nThe command case is a paired authenticated operator replay with native setpoint and simulated temperature readback; it does not exercise live automatic decision delivery.\n\nStill outside this result:\n\n"
    text += "\n".join("- " + item for item in report["excluded"]) + "\n"
    (output / "report.md").write_text(text, encoding="utf-8")
    print(
        json.dumps(
            dict(
                selected_preparation=report["selected_preparation"],
                report=str(output / "report.md"),
            ),
            indent=2,
        )
    )
    raise SystemExit(0 if report["selected_preparation"] == "pass" else 1)


if __name__ == "__main__":
    main()

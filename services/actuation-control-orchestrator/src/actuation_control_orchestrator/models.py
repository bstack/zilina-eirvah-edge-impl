"""Internal models for the actuation control orchestrator."""

from __future__ import annotations

from pathlib import Path

import yaml
from pydantic import BaseModel


class ActuationPipelineStage(BaseModel):
    name: str
    subject: str
    timeout_s: float = 2.0


class ActuationPipelineConfig(BaseModel):
    stages: list[ActuationPipelineStage]
    dlq_subject: str = "act.dlq.rejected"


def load_pipeline_config(path: Path) -> ActuationPipelineConfig:
    raw = yaml.safe_load(path.read_text())
    return ActuationPipelineConfig.model_validate(raw)

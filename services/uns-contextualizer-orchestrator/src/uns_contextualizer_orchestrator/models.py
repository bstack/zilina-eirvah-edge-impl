"""Internal models for the UNS contextualizer orchestrator."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

import yaml
from eirvah_contracts.pipeline import ContextualizeResult, PublishRequest
from eirvah_contracts.signals import NormalizedSignalEnvelope, RawSignalEnvelope
from pydantic import BaseModel


class PipelineStage(BaseModel):
    name: str
    subject: str
    timeout_s: float = 2.0


class PipelineConfig(BaseModel):
    stages: list[PipelineStage]
    dlq_subject: str


def load_pipeline_config(path: Path) -> PipelineConfig:
    raw = yaml.safe_load(path.read_text())
    return PipelineConfig.model_validate(raw)


@dataclass
class PipelineContext:
    correlation_id: str
    raw: RawSignalEnvelope
    ingress_at: datetime
    normalized: NormalizedSignalEnvelope | None = None
    contextualized: ContextualizeResult | None = None

    def build_publish_request(self) -> PublishRequest:
        assert self.normalized is not None, "normalized must be set"
        assert self.contextualized is not None, "contextualized must be set"
        return PublishRequest(
            uns_topic=self.contextualized.uns_topic,
            correlation_id=self.correlation_id,
            value=self.normalized.value,
            value_type=self.normalized.value_type,
            unit=self.normalized.unit,
            quality=self.normalized.quality,
            semantic_type=self.contextualized.semantic_type,
            uns_path=self.contextualized.uns_path,
            source_endpoint=self.raw.source_endpoint,
            source_node_id=self.raw.node_id,
            source_timestamp=self.raw.source_timestamp,
            edge_ingress=self.raw.received_at,
        )

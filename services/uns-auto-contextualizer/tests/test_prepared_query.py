"""Preparing a query must remove repeated parsing without caching node results."""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
from unittest.mock import patch

from eirvah_contracts.observation import load_bindings
from eirvah_contracts.signals import NormalizedSignalEnvelope
from rdflib import Literal, URIRef
from rdflib.plugins.sparql import processor
from uns_auto_contextualizer import service

ONTOLOGY = Path(__file__).resolve().parents[3] / "config/eirvah-line-a.jsonld"


def signal(node_id: str) -> NormalizedSignalEnvelope:
    now = datetime.now(UTC)
    binding = next(
        (b for b in load_bindings(ONTOLOGY).values() if b["node"] == node_id),
        {"unit": "dimensionless", "value_type": "double"},
    )
    return NormalizedSignalEnvelope(
        node_id=node_id,
        value=1.0,
        value_type=binding["value_type"],
        unit=binding["unit"],
        quality="good",
        source_timestamp=now,
        received_at=now,
    )


def test_repeated_requests_do_not_reparse_sparql() -> None:
    graph = service.load_ontology(ONTOLOGY)
    with patch.object(processor, "parseQuery", wraps=processor.parseQuery) as parse:
        for _ in range(3):
            assert service.contextualize(signal("Capper.TorqueSensor01"), graph) is not None
    assert parse.call_count == 0


def test_prepared_and_text_queries_produce_identical_results() -> None:
    from load_inject import NODE_IDS

    graph = service.load_ontology(ONTOLOGY)
    query = graph.query
    for node_id in [*NODE_IDS, "Unknown.Node"]:
        actual = service.contextualize(signal(node_id), graph)
        with patch.object(
            graph, "query", side_effect=lambda _, **kwargs: query(service._SPARQL, **kwargs)
        ):
            reference = service.contextualize(signal(node_id), graph)
        assert actual == reference, node_id
        assert (actual is None) == (node_id == "Unknown.Node")


def test_updated_graph_is_still_queried() -> None:
    graph = service.load_ontology(ONTOLOGY)
    normalized = signal("Capper.TorqueSensor01")
    before = service.contextualize(normalized, graph)
    assert before is not None
    predicate = URIRef("https://eirvah.uniza/ontology/semanticType")
    graph.set((URIRef(before.property_uri), predicate, Literal("updated.torque")))
    after = service.contextualize(normalized, graph)
    assert after is not None
    assert after.semantic_type == "updated.torque"
    assert after.uns_topic == before.uns_topic


def test_contextualizer_rejects_units_that_disagree_with_the_ontology():
    import pytest

    graph = service.load_ontology(ONTOLOGY)
    normalized = signal("Capper.TorqueSensor01").model_copy(update={"unit": "degC"})

    with pytest.raises(ValueError, match="ontology unit/type"):
        service.contextualize(normalized, graph)

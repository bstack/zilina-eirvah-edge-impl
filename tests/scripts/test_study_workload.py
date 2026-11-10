import asyncio

import e1_rehearsal
import pytest
import study_workload


@pytest.mark.asyncio
async def test_counter_reconciliation_waits_for_selected_pod_scrape(monkeypatch):
    responses = iter(
        [
            [],
            [{"metric": {"flow": "data"}, "value": [0, "9"]}],
            [{"metric": {"flow": "data"}, "value": [0, "10"]}],
        ]
    )
    queries = []
    sleeps = []

    async def query(client, expression):
        queries.append(expression)
        return next(responses)

    async def sleep(seconds):
        sleeps.append(seconds)

    monkeypatch.setattr(e1_rehearsal, "query", query)
    monkeypatch.setattr(asyncio, "sleep", sleep)

    rows = await study_workload.counters(None, "selected-worker", {"data": 10})

    assert rows[0]["value"][1] == "10"
    assert len(sleeps) == 2
    assert all('pod="selected-worker"' in value for value in queries)


@pytest.mark.asyncio
async def test_absent_counters_are_not_accepted_as_zero(monkeypatch):
    async def query(client, expression):
        return []

    async def sleep(seconds):
        pass

    monkeypatch.setattr(e1_rehearsal, "query", query)
    monkeypatch.setattr(asyncio, "sleep", sleep)

    with pytest.raises(RuntimeError, match="did not reconcile"):
        await study_workload.counters(None, "selected-worker", {"data": 0})

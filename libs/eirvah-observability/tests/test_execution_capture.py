"""Bounded capture must expose eviction and keep immutable sequence fences."""


def test_capture_checkpoint_reconciles_every_record_between_fences():
    from eirvah_observability.execution import EvidenceBuffer

    buffer = EvidenceBuffer(capacity=3)
    start = buffer.snapshot()
    buffer.append({"record_id": "a", "value": {"x": 1}})
    buffer.append({"record_id": "b"})

    end = buffer.snapshot(after=start["record_seq"])

    assert end["complete"] is True
    assert [r["record_seq"] for r in end["records"]] == [1, 2]
    end["records"][0]["value"]["x"] = 9
    assert buffer.snapshot()["records"][0]["value"]["x"] == 1


def test_buffer_eviction_and_cursor_ahead_cannot_claim_complete_capture():
    from eirvah_observability.execution import EvidenceBuffer

    buffer = EvidenceBuffer(capacity=2)
    for i in range(3):
        buffer.append({"record_id": str(i)})

    dropped = buffer.snapshot(after=0)
    ahead = buffer.snapshot(after=4)

    assert dropped["complete"] is False and dropped["first_available_seq"] == 2
    assert ahead["complete"] is False


def test_clock_sample_brackets_the_monotonic_read():
    from eirvah_observability.execution import clock_snapshot

    clock = clock_snapshot()

    assert clock["monotonic_before_ns"] <= clock["monotonic_ns"] <= clock["monotonic_after_ns"]
    assert clock["wall_time_ns"] > 0 and clock["timestamp_resolution_ns"] >= 1


def test_private_file_journal_has_atomic_checkpoint_and_detectable_capacity(tmp_path):
    import json

    from eirvah_observability.execution import EvidenceBuffer

    buffer = EvidenceBuffer(capacity=2, directory=tmp_path)
    buffer.append({"record_id": "one"})
    buffer.append({"record_id": "two"})
    buffer.append({"record_id": "three"})

    checkpoint = json.loads((tmp_path / "current.json").read_text())
    records = [
        json.loads(line)
        for line in (tmp_path / checkpoint["journal_file"]).read_text().splitlines()
    ]

    assert checkpoint["record_seq"] == 3 and checkpoint["overflow"] is True
    assert [r["record_seq"] for r in records] == [1, 2]

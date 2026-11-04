import gzip
import json
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.ingest.replay import replay_file, replay_record

NOW = datetime(2026, 11, 4, 16, 20, 0, tzinfo=timezone.utc)


class FakeProducer:
    def __init__(self):
        self.sent = []

    def send(self, topic, key=None, value=None):
        self.sent.append({"topic": topic, "key": key, "value": value})

    def flush(self):
        pass


def collector_record(**over):
    """Record đúng như collector.py ghi ra: có 'raw', không có 'clock_skew'."""
    rec = {
        "event_id": "abc-123",
        "wiki": "enwiki",
        "page_id": 987654,
        "page_title": "2026 Pacific typhoon season",
        "namespace": 0,
        "rev_id": 1234567890,
        "rev_parent_id": 1234567889,
        "user_hash": "abc123def4567890",
        "is_anon": True,
        "is_minor": False,
        "is_bot": False,
        "comment": "Updated casualty figures",
        "byte_old": 48210,
        "byte_new": 49102,
        "tags": [],
        "server_name": "en.wikipedia.org",
        "event_time": "2026-11-04T16:12:00+00:00",
        "ingest_time": "2026-11-04T16:12:03+00:00",
        "raw": {"wiki": "enwiki", "type": "edit"},
    }
    rec.update(over)
    return rec


def write_multimember_gzip(path: Path, members: list[list[dict]]) -> Path:
    for batch in members:
        blob = ("\n".join(json.dumps(r) for r in batch) + "\n").encode("utf-8")
        with gzip.GzipFile(path, "ab") as gz:
            gz.write(blob)
    return path


def test_replays_every_line_of_multimember_gzip(tmp_path):
    path = write_multimember_gzip(
        tmp_path / "d.jsonl.gz", [[collector_record()] * 10 for _ in range(3)]
    )
    p = FakeProducer()
    assert replay_file(str(path), p, "rc.raw", now=NOW) == 30
    assert len(p.sent) == 30


def test_user_hash_from_collector_is_not_rehashed(tmp_path):
    path = write_multimember_gzip(tmp_path / "d.jsonl.gz", [[collector_record()]])
    p = FakeProducer()
    replay_file(str(path), p, "rc.raw", now=NOW)
    assert json.loads(p.sent[0]["value"])["user_hash"] == "abc123def4567890"


def test_key_is_page_title(tmp_path):
    path = write_multimember_gzip(
        tmp_path / "d.jsonl.gz", [[collector_record(page_title="Bão_Yagi")]]
    )
    p = FakeProducer()
    replay_file(str(path), p, "rc.raw", now=NOW)
    assert p.sent[0]["key"] == "Bão_Yagi".encode("utf-8")


# --- ruling Task 2: replay là nơi kẹp clock skew --------------------------


def test_collector_record_gains_clock_skew_false(tmp_path):
    out = replay_record(collector_record(), NOW)
    assert out["clock_skew"] is False


def test_future_event_time_is_clamped_and_flagged():
    future = (NOW + timedelta(days=1)).isoformat()
    out = replay_record(collector_record(event_time=future), NOW)
    assert out["clock_skew"] is True
    assert datetime.fromisoformat(out["event_time"]) <= NOW + timedelta(minutes=1)


def test_event_time_slightly_ahead_is_left_alone():
    near = (NOW + timedelta(seconds=30)).isoformat()
    out = replay_record(collector_record(event_time=near), NOW)
    assert out["clock_skew"] is False
    assert out["event_time"] == near


def test_raw_is_renamed_to_raw_json_string():
    out = replay_record(collector_record(), NOW)
    assert "raw" not in out
    assert isinstance(out["raw_json"], str)
    assert json.loads(out["raw_json"])["wiki"] == "enwiki"


def test_replayed_record_keys_match_bronze_fields():
    from src.common.schema import BRONZE_FIELDS

    assert set(replay_record(collector_record(), NOW)) == set(BRONZE_FIELDS)


# --- độ bền ---------------------------------------------------------------


def test_truncated_final_line_is_skipped_not_fatal(tmp_path):
    path = tmp_path / "d.jsonl.gz"
    write_multimember_gzip(path, [[collector_record(), collector_record()]])
    with gzip.GzipFile(path, "ab") as gz:
        gz.write(b'{"event_id": "half-writ')
    p = FakeProducer()
    assert replay_file(str(path), p, "rc.raw", now=NOW) == 2


def test_record_missing_rev_id_is_skipped(tmp_path):
    bad = collector_record()
    del bad["rev_id"]
    path = write_multimember_gzip(tmp_path / "d.jsonl.gz", [[bad, collector_record()]])
    p = FakeProducer()
    assert replay_file(str(path), p, "rc.raw", now=NOW) == 1


def test_blank_lines_are_ignored(tmp_path):
    path = tmp_path / "d.jsonl.gz"
    with gzip.GzipFile(path, "ab") as gz:
        gz.write(b"\n\n" + json.dumps(collector_record()).encode() + b"\n\n")
    p = FakeProducer()
    assert replay_file(str(path), p, "rc.raw", now=NOW) == 1

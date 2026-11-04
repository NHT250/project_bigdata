import sys
from dataclasses import replace
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.common.config import Settings
from src.ingest.main import kafka_key, run

NOW = datetime(2026, 11, 4, 16, 20, 0, tzinfo=timezone.utc)
EVENT_TS = int(datetime(2026, 11, 4, 16, 12, 0, tzinfo=timezone.utc).timestamp())


def settings(tmp_path, **over):
    base = Settings(
        kafka_bootstrap="localhost:9092",
        kafka_topic="rc.raw",
        pg_dsn="postgresql://u:p@localhost/db",
        delta_root=str(tmp_path / "delta"),
        wiki_contact="nhom6@example.com",
        salt_path=str(tmp_path / "state" / "salt.txt"),
        state_path=str(tmp_path / "state"),
    )
    return replace(base, **over) if over else base


def make_event(**over):
    ev = {
        "meta": {"id": "abc-123", "dt": "2026-11-04T16:12:00Z"},
        "wiki": "enwiki",
        "type": "edit",
        "bot": False,
        "id": 987654,
        "title": "2026 Pacific typhoon season",
        "namespace": 0,
        "revision": {"new": 1234567890, "old": 1234567889},
        "user": "203.113.45.7",
        "user_is_anon": True,
        "minor": False,
        "comment": "Updated casualty figures",
        "length": {"old": 48210, "new": 49102},
        "tags": [],
        "server_name": "en.wikipedia.org",
        "timestamp": EVENT_TS,
    }
    ev.update(over)
    return ev


class FakeProducer:
    def __init__(self):
        self.sent = []
        self.flushed = 0

    def send(self, topic, key=None, value=None):
        self.sent.append({"topic": topic, "key": key, "value": value})

    def flush(self):
        self.flushed += 1


def test_only_enwiki_non_bot_edits_are_published(tmp_path):
    p = FakeProducer()
    events = [
        (1, make_event()),
        (2, make_event(wiki="viwiki")),
        (3, make_event(bot=True)),
        (4, make_event(type="log")),
    ]
    stats = run(settings(tmp_path), p, iter(events), now=lambda: NOW)
    assert stats.published == 1
    assert stats.skipped == 3
    assert len(p.sent) == 1


def test_published_message_goes_to_configured_topic(tmp_path):
    p = FakeProducer()
    run(settings(tmp_path), p, iter([(1, make_event())]), now=lambda: NOW)
    assert p.sent[0]["topic"] == "rc.raw"


def test_kafka_key_is_page_title_utf8_encoded(tmp_path):
    p = FakeProducer()
    run(settings(tmp_path), p, iter([(1, make_event(title="Bão_Yagi"))]), now=lambda: NOW)
    assert p.sent[0]["key"] == "Bão_Yagi".encode("utf-8")


def test_very_long_page_title_key_is_truncated_to_255_bytes():
    key = kafka_key("ă" * 500)
    assert len(key) <= 255
    assert key.decode("utf-8")  # không cắt giữa một ký tự multibyte


def test_key_truncation_never_splits_a_multibyte_character():
    for n in range(120, 140):
        key = kafka_key("日" * n)
        key.decode("utf-8")
        assert len(key) <= 255


def test_page_title_with_slash_and_quote_is_preserved_in_key():
    assert kafka_key("Bão/Yagi's_path") == "Bão/Yagi's_path".encode("utf-8")


def test_missing_page_title_gives_stable_fallback_key():
    assert kafka_key(None) == b"__unknown__"
    assert kafka_key("") == b"__unknown__"


def test_value_is_json_bytes_with_iso_timestamps(tmp_path):
    import json

    p = FakeProducer()
    run(settings(tmp_path), p, iter([(1, make_event())]), now=lambda: NOW)
    payload = json.loads(p.sent[0]["value"].decode("utf-8"))
    assert payload["event_time"] == "2026-11-04T16:12:00+00:00"
    assert payload["ingest_time"] == "2026-11-04T16:20:00+00:00"
    assert payload["rev_id"] == 1234567890


def test_value_carries_no_pii(tmp_path):
    p = FakeProducer()
    run(settings(tmp_path), p, iter([(1, make_event())]), now=lambda: NOW)
    assert b"203.113" not in p.sent[0]["value"]


def test_last_event_id_persisted_after_run(tmp_path):
    p = FakeProducer()
    run(settings(tmp_path), p, iter([("abc", make_event())]), now=lambda: NOW)
    assert (tmp_path / "state" / "last_event_id.txt").read_text() == "abc"


def test_resume_reads_previously_saved_event_id(tmp_path):
    from src.ingest.main import load_last_event_id, save_last_event_id

    save_last_event_id(str(tmp_path / "state"), "xyz")
    assert load_last_event_id(str(tmp_path / "state")) == "xyz"


def test_load_last_event_id_returns_none_when_absent(tmp_path):
    from src.ingest.main import load_last_event_id

    assert load_last_event_id(str(tmp_path / "nope")) is None


def test_malformed_events_are_counted_as_rejected(tmp_path):
    p = FakeProducer()
    bad = make_event()
    del bad["revision"]
    stats = run(settings(tmp_path), p, iter([(1, bad), (2, make_event())]), now=lambda: NOW)
    assert stats.published == 1
    assert stats.rejected == 1
    assert stats.reject_reasons["missing_revision"] == 1


def test_producer_is_flushed_at_end(tmp_path):
    p = FakeProducer()
    run(settings(tmp_path), p, iter([(1, make_event())]), now=lambda: NOW)
    assert p.flushed >= 1


def test_wikis_setting_is_respected(tmp_path):
    p = FakeProducer()
    s = settings(tmp_path, wikis=frozenset({"viwiki"}))
    stats = run(s, p, iter([(1, make_event(wiki="viwiki")), (2, make_event())]), now=lambda: NOW)
    assert stats.published == 1
    assert p.sent[0]["key"] == b"2026 Pacific typhoon season"

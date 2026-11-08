import json
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.common.schema import (
    BRONZE_FIELDS,
    REJECT_MISSING_LENGTH,
    REJECT_MISSING_REVISION,
    REJECT_NOT_EDIT,
    is_anonymous,
    to_record,
)

NOW = datetime(2026, 11, 4, 16, 20, 0, tzinfo=timezone.utc)
EVENT_TS = int(datetime(2026, 11, 4, 16, 12, 0, tzinfo=timezone.utc).timestamp())


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
        "tags": ["mobile edit"],
        "server_name": "en.wikipedia.org",
        "timestamp": EVENT_TS,
    }
    for key, value in over.items():
        if value is _DELETE:
            ev.pop(key, None)
        else:
            ev[key] = value
    return ev


class _Delete:
    pass


_DELETE = _Delete()


# --- trường hợp hợp lệ ---------------------------------------------------


def test_normal_edit_is_accepted_with_expected_values():
    r = to_record(make_event(), "s", NOW)
    assert r is not None
    assert r["wiki"] == "enwiki"
    assert r["rev_id"] == 1234567890
    assert r["rev_parent_id"] == 1234567889
    assert r["byte_old"] == 48210 and r["byte_new"] == 49102
    assert r["tags"] == ["mobile edit"]
    assert r["event_time"] == datetime(2026, 11, 4, 16, 12, tzinfo=timezone.utc)


def test_new_page_with_null_parent_is_kept():
    r = to_record(
        make_event(revision={"new": 5, "old": None}, length={"new": 100, "old": None}),
        "s",
        NOW,
    )
    assert r is not None
    assert r["rev_parent_id"] is None
    assert r["byte_old"] is None


# --- Review Focus #1 và #5: event thiếu trường ---------------------------


def test_missing_revision_is_rejected_not_crashed():
    assert to_record(make_event(revision=_DELETE), "s", NOW) is None


def test_missing_length_is_rejected_not_crashed():
    assert to_record(make_event(length=_DELETE), "s", NOW) is None


def test_revision_without_new_id_is_rejected():
    assert to_record(make_event(revision={"old": 1}), "s", NOW) is None


def test_non_edit_type_is_rejected():
    assert to_record(make_event(type="log"), "s", NOW) is None


def test_rejections_are_counted_by_reason():
    counter = {}
    to_record(make_event(revision=_DELETE), "s", NOW, rejects=counter)
    to_record(make_event(length=_DELETE), "s", NOW, rejects=counter)
    to_record(make_event(type="categorize"), "s", NOW, rejects=counter)
    assert counter == {
        REJECT_MISSING_REVISION: 1,
        REJECT_MISSING_LENGTH: 1,
        REJECT_NOT_EDIT: 1,
    }


# --- Review Focus #2: đồng hồ lệch ---------------------------------------


def test_event_time_far_in_future_is_clamped_to_ingest_time():
    r = to_record(make_event(timestamp=EVENT_TS + 86400), "s", NOW)
    assert r["event_time"] <= NOW + timedelta(minutes=1)
    assert r["clock_skew"] is True


def test_normal_event_time_is_not_marked_as_skewed():
    assert to_record(make_event(), "s", NOW)["clock_skew"] is False


def test_event_time_slightly_ahead_within_tolerance_is_not_clamped():
    ts = int((NOW + timedelta(seconds=30)).timestamp())
    r = to_record(make_event(timestamp=ts), "s", NOW)
    assert r["clock_skew"] is False
    assert r["event_time"] == NOW + timedelta(seconds=30)


def test_missing_timestamp_falls_back_to_meta_dt():
    r = to_record(make_event(timestamp=_DELETE), "s", NOW)
    assert r["event_time"] == datetime(2026, 11, 4, 16, 12, tzinfo=timezone.utc)


# --- PII ------------------------------------------------------------------


def test_raw_json_contains_no_user_field_and_no_ip():
    r = to_record(make_event(user="203.113.45.7"), "s", NOW)
    assert "203.113" not in r["raw_json"]
    assert '"user"' not in r["raw_json"]
    assert '"user_text"' not in r["raw_json"]


def test_registered_username_is_also_removed_from_raw_json():
    r = to_record(make_event(user="Jimbo_Wales", user_is_anon=False), "s", NOW)
    assert "Jimbo" not in json.dumps(r, default=str)


def test_user_hash_is_present_and_salted():
    a = to_record(make_event(), "salt-a", NOW)["user_hash"]
    b = to_record(make_event(), "salt-b", NOW)["user_hash"]
    assert len(a) == 16 and a != b


def test_anonymous_detection_covers_ipv4_ipv6_and_usernames():
    assert is_anonymous({"user": "203.113.45.7"}) is True
    assert is_anonymous({"user": "2001:db8::1"}) is True
    assert is_anonymous({"user": "Jimbo_Wales"}) is False
    assert is_anonymous({"user": "Jimbo", "user_is_anon": True}) is True


def test_username_containing_dots_is_not_mistaken_for_an_ip():
    assert is_anonymous({"user": "Mr.A.B.Smith.Jr"}) is False


# --- schema ---------------------------------------------------------------


def test_record_keys_match_bronze_fields_exactly():
    assert set(to_record(make_event(), "s", NOW)) == set(BRONZE_FIELDS)


def test_bronze_fields_have_no_duplicates():
    assert len(BRONZE_FIELDS) == len(set(BRONZE_FIELDS))


def test_bronze_fields_contain_no_pii_column():
    assert "user" not in BRONZE_FIELDS and "user_text" not in BRONZE_FIELDS


def test_record_is_json_serialisable():
    r = to_record(make_event(), "s", NOW)
    r["event_time"] = r["event_time"].isoformat()
    r["ingest_time"] = r["ingest_time"].isoformat()
    json.loads(json.dumps(r))


# --- chặn collector.py phân kỳ (ruling trong pre-flight) ------------------


def test_collector_record_keys_match_bronze_schema():
    """collector.py giữ bản sao riêng của to_record (ruling pre-flight).

    Test này là thứ chặn hai bản phân kỳ trong im lặng.
    """
    import importlib.util

    path = Path(__file__).resolve().parents[1] / "collector.py"
    spec = importlib.util.spec_from_file_location("collector_under_test", path)
    collector = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(collector)

    produced = collector.to_record(make_event(), "s")
    expected = set(BRONZE_FIELDS) - {"clock_skew"}
    assert set(produced) - {"raw"} | {"raw_json"} == expected


def test_collector_also_strips_pii():
    import importlib.util

    path = Path(__file__).resolve().parents[1] / "collector.py"
    spec = importlib.util.spec_from_file_location("collector_pii", path)
    collector = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(collector)

    produced = collector.to_record(make_event(user="203.113.45.7"), "s")
    assert "203.113" not in json.dumps(produced)


@pytest.mark.parametrize("user", ["~2026-12345-67", "~2025-1-2"])
def test_temporary_accounts_count_as_anonymous(user):
    """Wikipedia thay địa chỉ IP bằng tài khoản tạm dạng ~NĂM-..."""
    assert is_anonymous({"user": user}) is True


@pytest.mark.parametrize("user", ["Alice", "~Tilde fan", "~2026", "2026-1-2"])
def test_names_that_only_look_similar_are_not_anonymous(user):
    assert is_anonymous({"user": user}) is False

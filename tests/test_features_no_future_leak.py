"""Test quan trọng nhất của dự án (spec §6.1, §8.1).

Nếu feature ``*_prev_1h`` đếm cả edit tương lai, model nhìn thấy đáp án:
PR-AUC giả rất cao lúc train rồi sập khi chạy thật.
"""

import pytest

from tests.rows import at, bronze

pytest.importorskip("pyspark")

from src.common.schema import bronze_struct  # noqa: E402
from src.stream.features import add_window_features  # noqa: E402


def frame(spark, recs):
    return spark.createDataFrame(recs, schema=bronze_struct())


def counts(spark, recs, col="page_edits_prev_1h"):
    rows = add_window_features(frame(spark, recs)).orderBy("event_time", "rev_id").collect()
    return [getattr(r, col) for r in rows]


def test_window_counts_only_strictly_earlier_events(spark):
    recs = [bronze(event_time=at(m)) for m in (0, 30, 59)]
    assert counts(spark, recs) == [0, 1, 2]


def test_event_itself_is_never_counted(spark):
    assert counts(spark, [bronze(event_time=at(0))]) == [0]


def test_events_outside_one_hour_are_excluded(spark):
    recs = [bronze(event_time=at(0)), bronze(event_time=at(120))]
    assert counts(spark, recs) == [0, 0]


def test_other_pages_do_not_leak_into_count(spark):
    others = [bronze(page_title=f"Other {i}", event_time=at(5)) for i in range(5)]
    mine = bronze(page_title="Mine", event_time=at(10))
    rows = add_window_features(frame(spark, others + [mine])).collect()
    assert next(r for r in rows if r.page_title == "Mine").page_edits_prev_1h == 0


def test_adding_a_later_event_does_not_change_earlier_rows(spark):
    edits = [bronze(event_time=at(m)) for m in (0, 10, 20)]
    before = counts(spark, edits[:2])
    after = counts(spark, edits[:3])
    assert after[:2] == before


def test_user_window_counts_same_user_across_pages(spark):
    recs = [
        bronze(user_hash="u1", page_title="A", event_time=at(0)),
        bronze(user_hash="u1", page_title="B", event_time=at(5)),
        bronze(user_hash="u2", page_title="B", event_time=at(6)),
        bronze(user_hash="u1", page_title="C", event_time=at(10)),
    ]
    assert counts(spark, recs, "user_edits_prev_1h") == [0, 1, 0, 2]


def test_null_user_hash_counts_zero(spark):
    recs = [bronze(user_hash=None, event_time=at(m)) for m in (0, 1)]
    assert counts(spark, recs, "user_edits_prev_1h") == [0, 0]


def test_features_no_future_leak(spark):
    """Tên test bắt buộc theo spec §8.1: thêm edit tương lai không đổi feature quá khứ."""
    past = [bronze(user_hash="u1", event_time=at(m)) for m in (0, 15, 30)]
    future = [bronze(user_hash="u1", event_time=at(m)) for m in (31, 40, 45)]
    for col in ("page_edits_prev_1h", "user_edits_prev_1h"):
        assert counts(spark, past + future, col)[:3] == counts(spark, past, col) == [0, 1, 2]

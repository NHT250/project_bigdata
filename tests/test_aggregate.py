from datetime import timedelta

import pytest

from tests.rows import T0, bronze

pytest.importorskip("pyspark")

from src.batch.aggregate import page_hourly, to_postgres  # noqa: E402
from src.stream.main import silver_frame  # noqa: E402


def silver(spark, recs):
    for r in recs:
        r.setdefault("byte_delta", 10)
    return silver_frame(spark, recs)


def test_page_hourly_buckets_by_hour_and_counts_distinct_users(spark):
    recs = [bronze(user_hash=u, event_time=T0 + timedelta(minutes=m), is_anon=a, risk_score=s)
            for u, m, a, s in [("a", 1, True, 0.2), ("a", 20, False, 0.6), ("b", 59, False, None)]]
    row = page_hourly(silver(spark, recs)).head()
    assert row.edit_count == 3 and row.distinct_users == 2
    assert row.hour_bucket.replace(tzinfo=None) == T0.replace(tzinfo=None)
    assert row.anon_ratio == pytest.approx(1 / 3)
    assert row.max_risk_score == pytest.approx(0.6)
    assert row.mean_risk_score == pytest.approx(0.4)
    assert row.total_byte_delta == 30


def test_pages_below_min_edits_are_not_written_to_postgres(pg_conn, spark):
    recs = [bronze(event_time=T0 + timedelta(minutes=m)) for m in (1, 2)]
    assert to_postgres(pg_conn, page_hourly(silver(spark, recs)), min_edits_per_day=5) == 0


def test_min_edits_is_counted_per_day_across_hours(pg_conn, spark):
    # 5 edit trong một ngày nhưng rải 3 giờ khác nhau -> đủ ngưỡng, ghi cả 3 giờ
    recs = [bronze(event_time=T0 + timedelta(hours=h, minutes=m))
            for h, m in [(0, 1), (0, 2), (1, 1), (2, 1), (2, 5)]]
    assert to_postgres(pg_conn, page_hourly(silver(spark, recs)), min_edits_per_day=5) == 3
    assert pg_conn.execute("SELECT sum(edit_count) FROM page_hourly").fetchone()[0] == 5

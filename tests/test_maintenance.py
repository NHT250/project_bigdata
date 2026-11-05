from datetime import date, datetime, timedelta, timezone

import pytest

from tests.rows import bronze

pytest.importorskip("pyspark")

from src.batch.maintenance import (  # noqa: E402
    active_file_count,
    measure_late_reverts,
    optimize_tables,
)
from src.stream.main import merge_silver, silver_frame, write_bronze_batch  # noqa: E402

DAY = date(2026, 11, 4)
START = datetime(2026, 11, 4, 8, 0, tzinfo=timezone.utc)


def test_optimize_reduces_file_count(spark, tmp_path):
    for i in range(6):  # 6 lần append nhỏ = ít nhất 6 file nhỏ, như trigger 10 giây
        write_bronze_batch(spark, [bronze(event_time=START + timedelta(minutes=i))], str(tmp_path))
    before = active_file_count(spark, f"{tmp_path}/bronze/edits")
    result = optimize_tables(spark, str(tmp_path), DAY)
    after = active_file_count(spark, f"{tmp_path}/bronze/edits")
    assert after < before
    assert result["bronze/edits"]["files_before"] == before
    assert result["bronze/edits"]["files_after"] == after


def test_optimize_skips_missing_tables(spark, tmp_path):
    assert optimize_tables(spark, str(tmp_path), DAY) == {}


def test_measure_late_reverts_reports_missed_fraction(spark, tmp_path):
    # 10 edit: 2 bị revert sau 61–1440 phút -> nhãn 60 phút bỏ sót 2/10
    recs = []
    for i in range(10):
        t = START + timedelta(minutes=i * 10)
        recs.append(bronze(rev_id=100 + i, rev_parent_id=99 + i, page_title=f"P{i}",
                           user_hash=f"v{i}", event_time=t))
    for i, delay in ((0, 90), (1, 600)):
        recs.append(bronze(rev_id=900 + i, rev_parent_id=100 + i, page_title=f"P{i}",
                           user_hash="patrol", tags=["mw-rollback"],
                           event_time=recs[i]["event_time"] + timedelta(minutes=delay)))
    merge_silver(spark, silver_frame(spark, recs), str(tmp_path))

    r = measure_late_reverts(spark, str(tmp_path), DAY)
    # rollback 900/901 cũng là edit của ngày DAY và sống sót -> tổng 12 edit
    assert r["missed"] == 2 and r["total"] == 12
    assert r["missed_fraction"] == pytest.approx(2 / 12)
    assert r["positive_at_60m"] == 0 and r["positive_at_24h"] == 2
    # tỉ lệ revert thật bị nhãn 60 phút bỏ sót: 2/2
    assert r["missed_share_of_reverts"] == pytest.approx(1.0)

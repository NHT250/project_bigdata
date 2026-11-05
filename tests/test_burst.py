"""Phát hiện sự kiện nóng — thống kê, không phải AI (spec §6.2)."""

import pytest

from tests.rows import at, bronze

pytest.importorskip("pyspark")

from src.common.schema import bronze_struct  # noqa: E402
from src.stream.burst import detect  # noqa: E402


def frame(spark, recs):
    return spark.createDataFrame(recs, schema=bronze_struct())


def baseline_then_spike(n_users):
    # 3 giờ yên ắng: 1 edit mỗi 15 phút, rồi n_users mỗi người 3 edit trong 4 phút
    recs = [bronze(page_title="Quiet", user_hash=f"q{m % 2}", event_time=at(m))
            for m in range(0, 180, 15)]
    recs += [bronze(page_title="Quiet", user_hash=f"s{u}", event_time=at(200 + (u * 3 + k) * 0.15))
             for u in range(n_users) for k in range(3)]
    return recs


def edit_war(n_users):
    recs = baseline_then_spike(0)
    recs += [bronze(page_title="Quiet", user_hash=f"w{i % n_users}", event_time=at(200 + i * 0.2))
             for i in range(20)]
    return recs


def steady_traffic():
    # trang luôn đông: 2 edit/phút từ 6 user suốt 3 giờ
    return [bronze(page_title="Busy", user_hash=f"b{i % 6}", event_time=at(i * 0.5))
            for i in range(360)]


def single_new_page_burst():
    return [bronze(page_title="Brand new", user_hash=f"n{i}", event_time=at(i * 0.3))
            for i in range(6)]


def test_sudden_spike_from_many_users_is_hot(spark):
    out = detect(frame(spark, baseline_then_spike(n_users=8)))
    assert out.filter("is_hot").count() >= 1


def test_edit_war_between_two_users_is_not_hot(spark):
    # 20 edit trong 4 phút nhưng chỉ 2 user -> không phải sự kiện nóng
    assert detect(frame(spark, edit_war(n_users=2))).filter("is_hot").count() == 0


def test_steady_high_traffic_page_is_not_hot(spark):
    # trang luôn nhiều edit -> ewma cao -> z thấp
    assert detect(frame(spark, steady_traffic())).filter("is_hot").count() == 0


def test_brand_new_page_uses_global_baseline(spark):
    out = detect(frame(spark, single_new_page_burst())).orderBy("window_end").collect()
    assert out[0].ewma > 0  # không chia cho 0
    assert all(r.z_score is not None for r in out)


def test_output_columns(spark):
    cols = set(detect(frame(spark, single_new_page_burst())).columns)
    assert {"page_title", "wiki", "window_end", "edit_count", "distinct_users",
            "ewma", "z_score", "is_hot"} <= cols


def test_other_pages_history_does_not_raise_this_pages_baseline(spark):
    recs = steady_traffic() + baseline_then_spike(n_users=8)
    out = detect(frame(spark, recs)).filter("page_title = 'Quiet' AND is_hot")
    assert out.count() >= 1

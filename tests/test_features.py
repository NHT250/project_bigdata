import pytest

from tests.rows import bronze, with_diff

pytest.importorskip("pyspark")

from src.common.schema import enriched_input_struct  # noqa: E402
from src.stream.features import add_cheap_features  # noqa: E402


def df_with(spark, diff_added="", diff_removed="", **over):
    rec = with_diff(bronze(**over), added=diff_added, removed=diff_removed)
    return spark.createDataFrame([rec], schema=enriched_input_struct())


def row_for(spark, **kw):
    return add_cheap_features(df_with(spark, **kw)).collect()[0]


def test_byte_delta_is_null_safe_for_new_pages(spark):
    out = row_for(spark, byte_old=None, byte_new=100)
    assert out.byte_delta == 100 and out.abs_byte_delta == 100


def test_negative_byte_delta_has_positive_abs(spark):
    out = row_for(spark, byte_old=900, byte_new=100)
    assert out.byte_delta == -800 and out.abs_byte_delta == 800


def test_upper_ratio_counts_only_letters(spark):
    out = row_for(spark, diff_added="HELLO world 123!!!")
    assert out.upper_ratio == pytest.approx(5 / 10)


def test_upper_ratio_handles_unicode_letters(spark):
    out = row_for(spark, diff_added="ĐÂY là")
    assert out.upper_ratio == pytest.approx(3 / 5)


def test_repeat_char_detects_seven_or_more_repeats(spark):
    assert row_for(spark, diff_added="aaaaaaa").repeat_char is True
    assert row_for(spark, diff_added="aaa").repeat_char is False


def test_empty_diff_added_gives_zero_not_null(spark):
    out = row_for(spark, diff_added="")
    assert out.upper_ratio == 0.0 and out.words_added == 0
    assert out.repeat_char is False and out.has_url_added is False


def test_null_diff_from_failed_fetch_gives_zero_not_null(spark):
    out = row_for(spark, diff_added=None, diff_removed=None)
    assert out.words_added == 0 and out.words_removed == 0
    assert out.upper_ratio == 0.0


def test_word_counts_and_url_detection(spark):
    out = row_for(
        spark,
        diff_added="  see https://spam.example now  ",
        diff_removed="old text here",
    )
    assert out.words_added == 3 and out.words_removed == 3
    assert out.has_url_added is True


def test_comment_features(spark):
    assert row_for(spark, comment=None).comment_empty is True
    assert row_for(spark, comment="   ").comment_empty is True
    out = row_for(spark, comment="fix typo")
    assert out.comment_len == 8 and out.comment_empty is False


def test_time_features_use_event_time_utc(spark):
    from datetime import datetime, timezone

    # 2026-11-04 là thứ Tư -> dayofweek của Spark (CN=1) = 4
    out = row_for(spark, event_time=datetime(2026, 11, 4, 23, 30, tzinfo=timezone.utc))
    assert out.hour_of_day == 23 and out.day_of_week == 4

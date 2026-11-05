"""Nhãn tự sinh từ dấu hiệu revert (spec §6.1, §8.1 ``test_label_logic``)."""

from datetime import timedelta

import pytest

from tests.rows import T0, bronze

pytest.importorskip("pyspark")

from src.batch.labeling import label_edits  # noqa: E402
from src.stream.main import silver_frame  # noqa: E402

NOW = T0 + timedelta(hours=3)
OUR_REV = 500
OUR_PARENT = 499
OTHER_REV = 777


def ours(age_minutes=90, **over):
    return bronze(rev_id=OUR_REV, rev_parent_id=OUR_PARENT, user_hash="vandal",
                  event_time=NOW - timedelta(minutes=age_minutes), **over)


def later_edit(base, tags=(), comment="", parent=OUR_REV, after_minutes=2, rev_id=600):
    return bronze(rev_id=rev_id, rev_parent_id=parent, user_hash="patroller",
                  tags=list(tags), comment=comment,
                  event_time=base["event_time"] + timedelta(minutes=after_minutes))


def run(spark, recs, cutoff=60):
    return label_edits(spark, silver_frame(spark, recs), cutoff, NOW)


def label_for(spark, later_tags=(), later_comment="", undo_of=None, age_minutes=90,
              later="yes", after_minutes=2):
    o = ours(age_minutes)
    recs = [o]
    if later is not None:
        comment = later_comment
        if undo_of is not None:
            comment = f"Undid revision {undo_of} by [[Special:Contributions/1.2.3.4|1.2.3.4]]"
        recs.append(later_edit(o, later_tags, comment, after_minutes=after_minutes))
    row = run(spark, recs).filter(f"rev_id = {OUR_REV}").collect()[0]
    return row.label, row.label_source


def test_rollback_tag_on_later_edit_labels_target_as_reverted(spark):
    assert label_for(spark, later_tags=["mw-rollback"]) == (1, "tag_rollback")


def test_undo_tag_pointing_at_our_revision_labels_reverted(spark):
    assert label_for(spark, later_tags=["mw-undo"], undo_of=OUR_REV) == (1, "tag_undo")


def test_reverted_edits_by_comment_pattern_labels_reverted(spark):
    c = "Reverted edits by [[Special:Contributions/1.2.3.4|1.2.3.4]] (talk) to last version"
    assert label_for(spark, later_comment=c) == (1, "comment_match")


def test_edit_surviving_past_cutoff_labels_zero(spark):
    assert label_for(spark, age_minutes=61, later=None) == (0, "survived")


def test_edit_younger_than_cutoff_is_not_labeled_yet(spark):
    assert run(spark, [ours(age_minutes=30)]).filter(f"rev_id = {OUR_REV}").count() == 0


def test_undo_tag_pointing_at_a_different_revision_does_not_label_us(spark):
    assert label_for(spark, later_tags=["mw-undo"], undo_of=OTHER_REV) == (0, "survived")


def test_rollback_has_priority_over_comment_match(spark):
    c = "Reverted edits by [[Special:Contributions/1.2.3.4|1.2.3.4]]"
    assert label_for(spark, later_tags=["mw-rollback"], later_comment=c) == (1, "tag_rollback")


def test_revert_after_cutoff_does_not_count_at_60_minutes(spark):
    assert label_for(spark, later_tags=["mw-rollback"], after_minutes=75,
                     age_minutes=120) == (0, "survived")


def test_rollback_reverts_whole_run_of_same_user(spark):
    first = ours(age_minutes=90)
    second = bronze(rev_id=501, rev_parent_id=OUR_REV, user_hash="vandal",
                    event_time=first["event_time"] + timedelta(minutes=1))
    rb = later_edit(second, ["mw-rollback"], parent=501, after_minutes=1)
    rows = {r.rev_id: r.label for r in run(spark, [first, second, rb]).collect()}
    assert rows[OUR_REV] == 1 and rows[501] == 1 and rows[600] == 0


def test_rollback_on_other_page_does_not_label_us(spark):
    o = ours()
    other = later_edit(o, ["mw-rollback"])
    other["page_title"] = "Different page"
    assert {r.rev_id: r.label for r in run(spark, [o, other]).collect()}[OUR_REV] == 0


def test_labeled_rows_carry_features_and_labeled_at(spark):
    row = run(spark, [ours()]).collect()[0]
    assert row.labeled_at is not None and "byte_delta" in row.asDict()

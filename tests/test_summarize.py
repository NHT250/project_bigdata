import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.stream.summarize import Row, summarize_hot, template_summary


class FakeLLM:
    def __init__(self):
        self.calls = 0
        self.seen = []

    def __call__(self, added, removed):
        self.calls += 1
        self.seen.append((added, removed))
        return f"LLM: {self.calls}"


class FailingLLM:
    def __init__(self):
        self.calls = 0

    def __call__(self, added, removed):
        self.calls += 1
        raise RuntimeError("quota exceeded")


def rows(n=3, **over):
    out = []
    for i in range(n):
        out.append(
            Row(
                rev_id=over.get("rev_id", 1000 + i),
                added=over.get("added", "alpha beta gamma"),
                removed=over.get("removed", "delta"),
                byte_delta=over.get("byte_delta", 120),
            )
        )
    return out


# --- khuôn mẫu -----------------------------------------------------------


def test_reports_words_added_and_removed():
    s = template_summary("alpha beta gamma", "delta", 120)
    assert "3 từ" in s
    assert "1 từ" in s


def test_pure_addition_does_not_mention_deletion():
    s = template_summary("alpha beta", "", 50)
    assert "Thêm" in s
    assert "xoá" not in s.lower()


def test_pure_deletion_is_described_as_deletion():
    s = template_summary("", "a b c", -40)
    assert "xoá" in s.lower()
    assert "Thêm" not in s


def test_empty_diff_gives_non_empty_fallback_text():
    assert template_summary("", "", 0).strip() != ""


def test_byte_delta_is_rendered_in_kilobytes_when_large():
    assert "KB" in template_summary("x " * 500, "", 1843)


def test_small_byte_delta_is_rendered_in_bytes():
    assert "byte" in template_summary("x", "", 12)


def test_negative_byte_delta_keeps_its_sign():
    assert "-" in template_summary("", "a b", -500)


def test_summary_is_single_line():
    assert "\n" not in template_summary("a\nb\nc", "d\ne", 30)


def test_very_long_diff_does_not_produce_a_very_long_summary():
    assert len(template_summary("word " * 20_000, "x " * 5_000, 99_999)) < 200


# --- chọn chế độ ---------------------------------------------------------


def test_llm_is_not_called_in_template_mode():
    llm = FakeLLM()
    out = summarize_hot(rows(), mode="template", llm=llm)
    assert llm.calls == 0
    assert len(out) == 3
    assert all(v.strip() for v in out.values())


def test_llm_is_called_in_llm_mode():
    llm = FakeLLM()
    out = summarize_hot(rows(2), mode="llm", llm=llm)
    assert llm.calls == 2
    assert all(v.startswith("LLM:") for v in out.values())


def test_llm_called_once_per_rev_id_then_cached():
    llm = FakeLLM()
    cache: dict[int, str] = {}
    duplicated = rows(2) + rows(2)
    out = summarize_hot(duplicated, mode="llm", llm=llm, cache=cache)
    assert llm.calls == 2
    assert len(out) == 2
    summarize_hot(duplicated, mode="llm", llm=llm, cache=cache)
    assert llm.calls == 2  # lần hai lấy từ cache


def test_llm_failure_falls_back_to_template():
    llm = FailingLLM()
    out = summarize_hot(rows(2), mode="llm", llm=llm)
    assert llm.calls == 2
    assert all(v.strip() and not v.startswith("LLM:") for v in out.values())


def test_llm_mode_without_an_llm_falls_back_to_template():
    out = summarize_hot(rows(2), mode="llm", llm=None)
    assert all(v.strip() for v in out.values())


def test_at_most_max_rows_are_summarised():
    llm = FakeLLM()
    many = [Row(rev_id=i, added="a b", removed="", byte_delta=10) for i in range(200)]
    out = summarize_hot(many, mode="llm", llm=llm, max_rows=50)
    assert llm.calls == 50
    assert len(out) == 50


def test_empty_input_returns_empty_dict():
    assert summarize_hot([], mode="template", llm=None) == {}


def test_unknown_mode_falls_back_to_template_without_raising():
    out = summarize_hot(rows(1), mode="magic", llm=FakeLLM())
    assert all(v.strip() for v in out.values())

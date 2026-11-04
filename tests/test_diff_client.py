import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.stream.diff_client import (
    STATUS_ERROR,
    STATUS_NEW_PAGE,
    STATUS_OK,
    STATUS_TIMEOUT,
    STATUS_TRUNCATED,
    RateLimiter,
    fetch_diff,
    parse_compare_html,
)

UA = "VLU-BigData-RealtimeWiki/1.0 (Van Lang University course project; a@b.c)"

COMPARE_HTML = (
    '<tr><td class="diff-deletedline"><div>old casualty text</div></td>'
    '<td class="diff-addedline"><div>new casualty figures</div></td></tr>'
    '<tr><td class="diff-addedline"><div>see https://example.org/report</div></td></tr>'
)


class FakeHttp:
    def __init__(self, body=None, exc=None):
        self.body = body
        self.exc = exc
        self.calls = 0
        self.last_url = None
        self.last_headers = None

    def __call__(self, url, headers, timeout):
        self.calls += 1
        self.last_url = url
        self.last_headers = headers
        if self.exc:
            raise self.exc
        return self.body


def compare_body(added="", removed=""):
    html = ""
    if removed:
        html += f'<td class="diff-deletedline"><div>{removed}</div></td>'
    if added:
        html += f'<td class="diff-addedline"><div>{added}</div></td>'
    return json.dumps({"compare": {"*": html}})


# --- phân tích HTML ------------------------------------------------------


def test_parses_added_and_removed_lines():
    added, removed = parse_compare_html(COMPARE_HTML)
    assert "new casualty figures" in added
    assert "old casualty text" in removed


def test_added_lines_are_joined_with_newlines():
    added, _ = parse_compare_html(COMPARE_HTML)
    assert added.count("\n") == 1


def test_html_entities_are_unescaped():
    added, _ = parse_compare_html('<td class="diff-addedline"><div>a &amp; b</div></td>')
    assert added == "a & b"


def test_nested_tags_inside_a_line_are_stripped():
    added, _ = parse_compare_html(
        '<td class="diff-addedline"><div>see <ins>new</ins> text</div></td>'
    )
    assert added == "see new text"


def test_no_diff_markup_gives_empty_strings():
    assert parse_compare_html("<p>nothing here</p>") == ("", "")


# --- fetch_diff ----------------------------------------------------------


def test_successful_fetch_returns_ok_with_content():
    http = FakeHttp(compare_body(added="new casualty figures", removed="old text"))
    r = fetch_diff(2, 1, UA, http=http)
    assert r.status == STATUS_OK
    assert "new casualty figures" in r.added
    assert "old text" in r.removed


def test_user_agent_is_sent_and_is_not_a_browser():
    http = FakeHttp(compare_body(added="x"))
    fetch_diff(2, 1, UA, http=http)
    assert http.last_headers["User-Agent"] == UA
    for forbidden in ("Mozilla", "Chrome", "Safari", "AppleWebKit"):
        assert forbidden not in http.last_headers["User-Agent"]


def test_request_uses_fromrev_and_torev():
    http = FakeHttp(compare_body(added="x"))
    fetch_diff(222, 111, UA, http=http)
    assert "fromrev=111" in http.last_url
    assert "torev=222" in http.last_url


def test_null_parent_id_requests_new_page_content_not_a_comparison():
    http = FakeHttp(json.dumps({"query": {"pages": [{"revisions": [{"slots": {"main": {"content": "brand new article"}}}]}]}}))
    r = fetch_diff(2, None, UA, http=http)
    assert "fromrev" not in http.last_url
    assert r.status == STATUS_NEW_PAGE
    assert "brand new article" in r.added
    assert r.removed == ""


def test_page_title_with_unicode_and_slash_is_url_encoded():
    http = FakeHttp(compare_body(added="x"))
    fetch_diff(2, 1, UA, http=http, page_title="Bão/Yagi's path")
    assert " " not in http.last_url
    assert "Yagi" in http.last_url or "B%C3%A3o" in http.last_url


# --- Review Focus #3: diff quá lớn --------------------------------------


def test_diff_longer_than_limit_is_truncated_and_flagged():
    http = FakeHttp(compare_body(added="x" * 50_000))
    r = fetch_diff(2, 1, UA, http=http, max_chars=10_000)
    assert len(r.added) == 10_000
    assert r.status == STATUS_TRUNCATED


def test_removed_side_is_truncated_too():
    http = FakeHttp(compare_body(removed="y" * 50_000))
    r = fetch_diff(2, 1, UA, http=http, max_chars=1_000)
    assert len(r.removed) == 1_000
    assert r.status == STATUS_TRUNCATED


def test_diff_exactly_at_limit_is_not_flagged_as_truncated():
    http = FakeHttp(compare_body(added="x" * 1_000))
    assert fetch_diff(2, 1, UA, http=http, max_chars=1_000).status == STATUS_OK


# --- lỗi không được raise ------------------------------------------------


def test_timeout_returns_status_timeout():
    r = fetch_diff(2, 1, UA, http=FakeHttp(exc=TimeoutError()))
    assert r.status == STATUS_TIMEOUT
    assert r.added == "" and r.removed == ""


def test_network_error_returns_status_error():
    r = fetch_diff(2, 1, UA, http=FakeHttp(exc=OSError("boom")))
    assert r.status == STATUS_ERROR


def test_malformed_json_returns_status_error():
    r = fetch_diff(2, 1, UA, http=FakeHttp("not json at all"))
    assert r.status == STATUS_ERROR


def test_api_error_payload_returns_status_error():
    body = json.dumps({"error": {"code": "nosuchrevid", "info": "no such revision"}})
    assert fetch_diff(2, 1, UA, http=FakeHttp(body)).status == STATUS_ERROR


# --- giới hạn tần suất ---------------------------------------------------


def test_rate_limiter_caps_requests_per_second():
    clock = {"t": 0.0}
    slept = []

    def fake_sleep(seconds):
        slept.append(seconds)
        clock["t"] += seconds

    rl = RateLimiter(10, monotonic=lambda: clock["t"], sleep=fake_sleep)
    for _ in range(20):
        rl.acquire()
    assert clock["t"] >= 1.9
    assert all(s > 0 for s in slept)


def test_rate_limiter_does_not_sleep_when_calls_are_spread_out():
    clock = {"t": 0.0}

    def fake_sleep(seconds):
        clock["t"] += seconds

    rl = RateLimiter(10, monotonic=lambda: clock["t"], sleep=fake_sleep)
    rl.acquire()
    clock["t"] += 5.0
    rl.acquire()
    assert clock["t"] == 5.0


def test_rate_limiter_rejects_non_positive_rps():
    import pytest

    with pytest.raises(ValueError):
        RateLimiter(0)

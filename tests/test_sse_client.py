import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.ingest.sse_client import parse_sse


def lines(raw: bytes):
    return raw.splitlines(keepends=True)


def test_parses_two_events_with_ids():
    raw = b'id: a\ndata: {"x":1}\n\nid: b\ndata: {"x":2}\n\n'
    assert list(parse_sse(lines(raw))) == [("a", {"x": 1}), ("b", {"x": 2})]


def test_multiline_data_is_concatenated():
    raw = b'id: a\ndata: {"x":\ndata: 1}\n\n'
    assert list(parse_sse(lines(raw))) == [("a", {"x": 1})]


def test_malformed_json_is_skipped_not_raised():
    raw = b'id: a\ndata: not json\n\nid: b\ndata: {"x":2}\n\n'
    assert list(parse_sse(lines(raw))) == [("b", {"x": 2})]


def test_comment_and_event_lines_are_ignored():
    raw = b':ping\nevent: message\nid: a\ndata: {"x":1}\n\n'
    assert list(parse_sse(lines(raw))) == [("a", {"x": 1})]


def test_event_without_id_yields_none_id():
    raw = b'data: {"x":1}\n\n'
    assert list(parse_sse(lines(raw))) == [(None, {"x": 1})]


def test_id_persists_until_a_new_id_arrives():
    raw = b'id: a\ndata: {"x":1}\n\ndata: {"x":2}\n\nid: b\ndata: {"x":3}\n\n'
    assert [i for i, _ in parse_sse(lines(raw))] == ["a", "a", "b"]


def test_trailing_event_without_blank_line_is_still_yielded():
    raw = b'id: a\ndata: {"x":1}\n'
    assert list(parse_sse(lines(raw))) == [("a", {"x": 1})]


def test_blank_line_with_no_data_yields_nothing():
    raw = b"\n\n\n"
    assert list(parse_sse(lines(raw))) == []


def test_crlf_line_endings_are_handled():
    raw = b'id: a\r\ndata: {"x":1}\r\n\r\n'
    assert list(parse_sse(lines(raw))) == [("a", {"x": 1})]


def test_field_without_leading_space_is_accepted():
    raw = b'id:a\ndata:{"x":1}\n\n'
    assert list(parse_sse(lines(raw))) == [("a", {"x": 1})]


def test_invalid_utf8_bytes_do_not_raise():
    raw = b'id: a\ndata: {"t":"\xff\xfe"}\n\nid: b\ndata: {"x":2}\n\n'
    out = list(parse_sse(lines(raw)))
    assert ("b", {"x": 2}) in out


def test_non_object_json_payload_is_skipped():
    raw = b"id: a\ndata: 42\n\n"
    assert list(parse_sse(lines(raw))) == []


def test_large_batch_is_streamed_lazily():
    raw = b"".join(b'id: %d\ndata: {"x":%d}\n\n' % (i, i) for i in range(1000))
    gen = parse_sse(lines(raw))
    first = next(gen)
    assert first == ("0", {"x": 0})
    assert sum(1 for _ in gen) == 999

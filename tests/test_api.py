from contextlib import contextmanager

import pytest

pytest.importorskip("fastapi")
pytest.importorskip("httpx")

from fastapi.testclient import TestClient  # noqa: E402

from src.api.main import create_app  # noqa: E402
from src.api.queries import CANNED, article_url  # noqa: E402
from tests.seed_pg import seed  # noqa: E402


@pytest.fixture
def seeded_pg(pg_conn):
    seed(pg_conn)
    return pg_conn


@pytest.fixture
def client(pg_conn):
    @contextmanager
    def factory():
        yield pg_conn

    return TestClient(create_app(connection=factory))


def table_still_exists(conn, name):
    return conn.execute(
        "SELECT count(*) FROM information_schema.tables "
        "WHERE table_schema = current_schema() AND table_name = %s", (name,)
    ).fetchone()[0] == 1


def test_metrics_returns_points_ordered_by_time(client, seeded_pg):
    body = client.get("/api/metrics?hours=6").json()
    pts = body["points"]
    assert len(pts) > 300 and pts == sorted(pts, key=lambda p: p["t"])
    assert {"edit_count", "anon_ratio", "hot_page_count", "high_risk_count"} <= set(pts[-1])
    assert {"edits_per_minute", "anon_ratio", "hot_pages", "high_risk"} <= set(body["summary"])


def test_metrics_hours_is_bounded(client):
    assert client.get("/api/metrics?hours=0").status_code == 422
    assert client.get("/api/metrics?hours=1000").status_code == 422


def test_risk_endpoint_includes_diff_and_wikipedia_link(client, seeded_pg):
    items = client.get("/api/risk?limit=5").json()["items"]
    assert len(items) == 5
    item = items[0]
    assert "diff_added" in item and item["url"].startswith("https://en.wikipedia.org/wiki/")
    assert item["diff_url"].endswith(f"diff={item['rev_id']}")
    assert [i["risk_score"] for i in items] == sorted((i["risk_score"] for i in items), reverse=True)


def test_hot_and_live_endpoints(client, seeded_pg):
    hot = client.get("/api/hot?limit=20").json()["items"]
    assert len(hot) == 3 and hot[0]["z_score"] >= hot[-1]["z_score"]
    live = client.get("/api/live?limit=100").json()["items"]
    assert len(live) == 100 and live[0]["event_time"] >= live[-1]["event_time"]


def test_page_endpoint_handles_unicode_and_slash_in_title(client, seeded_pg):
    r = client.get("/api/page", params={"title": "Bão/Yagi", "days": 7})
    assert r.status_code == 200
    body = r.json()
    assert body["points"] and body["url"] == "https://en.wikipedia.org/wiki/B%C3%A3o/Yagi"


def test_page_endpoint_accepts_underscores_like_wikipedia_urls(client, seeded_pg):
    body = client.get("/api/page", params={"title": "Typhoon_Yagi"}).json()
    assert body["title"] == "Typhoon Yagi" and body["points"] and body["edits"]


def test_page_endpoint_is_parameterised_against_sql_injection(client, seeded_pg):
    r = client.get("/api/page", params={"title": "x'; drop table edits_live; --"})
    assert r.status_code == 200 and r.json()["points"] == []
    assert table_still_exists(seeded_pg, "edits_live")


def test_ask_rejects_unknown_qid(client):
    assert client.get("/api/ask?qid=99").status_code == 400


def test_all_five_canned_queries_execute(client, seeded_pg):
    assert sorted(CANNED) == [1, 2, 3, 4, 5]
    for qid in range(1, 6):
        r = client.get(f"/api/ask?qid={qid}")
        assert r.status_code == 200, qid
        body = r.json()
        assert body["title"] == CANNED[qid][0] and body["columns"] and body["rows"], qid


def test_questions_list_endpoint(client):
    qs = client.get("/api/questions").json()["items"]
    assert [q["qid"] for q in qs] == [1, 2, 3, 4, 5]


def test_every_response_carries_cc_by_sa_attribution(client, seeded_pg):
    for path in ("/api/risk", "/api/hot", "/api/live", "/api/metrics", "/api/page?title=X",
                 "/api/ask?qid=1"):
        assert client.get(path).json()["license"] == "CC BY-SA 4.0", path


def test_article_url_encoding():
    assert article_url("enwiki", "Đà Nẵng") == "https://en.wikipedia.org/wiki/%C4%90%C3%A0_N%E1%BA%B5ng"
    assert article_url("enwiki", "AC/DC") == "https://en.wikipedia.org/wiki/AC/DC"
    assert article_url("enwiki", "What?") == "https://en.wikipedia.org/wiki/What%3F"

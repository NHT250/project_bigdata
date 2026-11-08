"""Task 24: chat hỏi đáp sinh SQL. FakeLLM, không gọi mạng thật."""

from contextlib import contextmanager
from pathlib import Path

import pytest

pytest.importorskip("psycopg")

from src.api import nl2sql  # noqa: E402
from src.api.nl2sql import SCHEMA_PROMPT, answer, generate_sql, validate_sql  # noqa: E402
from tests.seed_pg import seed  # noqa: E402

ROLE_SQL = Path(__file__).resolve().parents[1] / "sql" / "chat_role.sql"
GOOD = "SELECT page_title, risk_score FROM risk_queue ORDER BY risk_score DESC LIMIT 5"


class FakeLLM:
    def __init__(self, reply):
        self.reply = reply
        self.seen = []

    def __call__(self, system, question):
        self.seen.append((system, question))
        if isinstance(self.reply, Exception):
            raise self.reply
        return self.reply


def rejected(sql):
    ok, reason = validate_sql(sql)
    assert ok is False, sql
    assert reason
    return reason


# --- rào chắn ---------------------------------------------------------------


@pytest.mark.parametrize(
    "sql",
    [
        "INSERT INTO edits_live (wiki) VALUES ('x')",
        "UPDATE edits_live SET comment = 'x'",
        "DELETE FROM edits_live",
        "DROP TABLE edits_live",
        "ALTER TABLE edits_live ADD COLUMN x int",
        "CREATE TABLE x (a int)",
        "TRUNCATE edits_live",
        "GRANT ALL ON edits_live TO public",
        "COPY edits_live TO '/tmp/x'",
    ],
)
def test_rejects_insert_update_delete_drop(sql):
    rejected(sql)


@pytest.mark.parametrize(
    "sql",
    [
        "SELECT 1 FROM edits_live; DROP TABLE edits_live",
        "SELECT 1 FROM edits_live; SELECT 2 FROM edits_live;",
    ],
)
def test_rejects_stacked_queries(sql):
    rejected(sql)


@pytest.mark.parametrize(
    "sql",
    [
        "SELECT * FROM pg_user",
        "SELECT * FROM information_schema.tables",
        "SELECT * FROM pg_catalog.pg_authid",
        'SELECT * FROM "pg_user"',
        "SELECT * FROM secrets",
        "SELECT * FROM edits_live, pg_user",
        "SELECT * FROM edits_live JOIN pg_shadow ON true",
        "SELECT * FROM (SELECT * FROM pg_user) x",
        "SELECT * FROM (SELECT * FROM edits_live) x, pg_user",
        "SELECT rev_id FROM edits_live WHERE rev_id IN (SELECT usesysid FROM pg_user)",
    ],
)
def test_rejects_unknown_table(sql):
    rejected(sql)


def test_adds_limit_when_missing():
    ok, sql = validate_sql("SELECT page_title FROM edits_live")
    assert ok and sql.endswith("LIMIT 100")


def test_keeps_existing_limit():
    ok, sql = validate_sql(GOOD)
    assert ok and sql == GOOD


def test_trailing_semicolon_is_removed_before_adding_limit():
    ok, sql = validate_sql("SELECT page_title FROM edits_live;")
    assert ok and sql == "SELECT page_title FROM edits_live LIMIT 100"


def test_oversized_limit_is_clamped_to_100():
    ok, sql = validate_sql("SELECT page_title FROM edits_live LIMIT 100000")
    assert ok and sql.endswith("LIMIT 100")


def test_limit_inside_subquery_does_not_count():
    ok, sql = validate_sql("SELECT a.t FROM (SELECT page_title t FROM edits_live LIMIT 5) a")
    assert ok and sql.endswith("LIMIT 100")


@pytest.mark.parametrize(
    "sql",
    [
        "WITH x AS (DELETE FROM edits_live RETURNING *) SELECT * FROM x",
        "SELECT * INTO backup FROM edits_live",
        "SELECT * FROM edits_live FOR UPDATE",
        "SELECT * FROM edits_live FOR SHARE",
        "SELECT pg_sleep(100) FROM edits_live",
        "SELECT set_config('statement_timeout', '0', false) FROM edits_live",
        "SELECT query_to_xml('select * from pg_user', true, true, '') FROM edits_live",
        "SELECT pg_read_file('/etc/passwd') FROM edits_live",
        "SELECT 1 FROM edits_live -- comment",
        "SELECT /* x */ 1 FROM edits_live",
        "SELECT $$x$$ FROM edits_live",
        "SELECT E'\\'' FROM edits_live",
        "SELECT rev_id FROM edits_live LIMIT ALL",
        "SELECT rev_id FROM edits_live FETCH FIRST 1000 ROWS ONLY",
        "SELECT 1",
        "",
    ],
)
def test_rejects_other_bypass_attempts(sql):
    rejected(sql)


@pytest.mark.parametrize(
    "sql",
    [
        "SELECT page_title FROM edits_live WHERE page_title = 'Drop; DELETE (album)' LIMIT 5",
        "SELECT extract(hour FROM event_time) h, count(*) FROM edits_live GROUP BY 1 LIMIT 5",
        'SELECT count(*) AS "Số edit" FROM edits_live LIMIT 5',
        "SELECT page_title FROM hot_pages WHERE window_end IS DISTINCT FROM now() LIMIT 5",
        "SELECT e.page_title FROM edits_live e JOIN risk_queue r ON e.rev_id = r.rev_id LIMIT 5",
        "SELECT round(avg(anon_ratio)::numeric, 3) FROM page_hourly LIMIT 5",
        "SELECT event_time AT TIME ZONE 'UTC' FROM edits_live LIMIT 5",
        "SELECT minute FROM metrics_minute WHERE minute > now() - interval '1 hour' LIMIT 5",
    ],
)
def test_ordinary_analytics_queries_are_allowed(sql):
    ok, out = validate_sql(sql)
    assert ok, out
    assert out == sql


def test_chat_role_is_session_limited(pg_conn):
    seed(pg_conn)
    answer("x", pg_conn, FakeLLM(GOOD))
    assert pg_conn.execute("SHOW statement_timeout").fetchone()[0] == "5s"


def test_readonly_role_cannot_write(pg_conn):
    import psycopg

    pg_conn.execute(ROLE_SQL.read_text(encoding="utf-8"))
    seed(pg_conn)
    pg_conn.execute("SET ROLE wiki_chat")
    try:
        assert pg_conn.execute("SELECT count(*) FROM edits_live").fetchone()[0] > 0
        statements = (
            "INSERT INTO edits_live (wiki, rev_id, event_time) VALUES ('x', 1, now())",
            "DELETE FROM risk_queue",
            "UPDATE hot_pages SET summary = 'x'",
            "DROP TABLE page_hourly",
            "TRUNCATE metrics_minute",
        )
        for statement in statements:
            with pytest.raises(psycopg.Error):
                pg_conn.execute(statement)
    finally:
        pg_conn.execute("RESET ROLE")
    assert pg_conn.execute("SELECT count(*) FROM page_hourly").fetchone()[0] > 0


# --- sinh SQL và trả lời ------------------------------------------------------


def test_schema_prompt_states_data_limits():
    for table in ("edits_live", "hot_pages", "risk_queue", "metrics_minute", "page_hourly"):
        assert table in SCHEMA_PROMPT
    for fact in ("2 giờ", "1 giờ", "6 giờ", "24 giờ", "30 ngày", "user_hash", "revert", "NO_DATA"):
        assert fact in SCHEMA_PROMPT


def test_vietnamese_question_produces_select(pg_conn):
    seed(pg_conn)
    llm = FakeLLM(GOOD)
    question = "Edit nào rủi ro cao nhất?"
    out = answer(question, pg_conn, llm)
    assert out["error"] is None and out["sql"].startswith("SELECT") and len(out["rows"]) == 5
    assert llm.seen[0] == (SCHEMA_PROMPT, question)


def test_english_question_works_too(pg_conn):
    seed(pg_conn)
    out = answer("Which edits are riskiest?", pg_conn, FakeLLM(GOOD))
    assert out["error"] is None and out["columns"] == ["page_title", "risk_score"]


def test_generate_sql_strips_markdown_fence():
    assert generate_sql("q", FakeLLM(f"```sql\n{GOOD}\n```")) == GOOD


def test_llm_returning_dangerous_sql_is_blocked(pg_conn):
    seed(pg_conn)
    before = pg_conn.execute("SELECT count(*) FROM edits_live").fetchone()[0]
    out = answer("xoá hết đi", pg_conn, FakeLLM("DROP TABLE edits_live"))
    assert out["error"] and out["rows"] == []
    assert pg_conn.execute("SELECT count(*) FROM edits_live").fetchone()[0] == before


@pytest.mark.parametrize("llm", [None, FakeLLM(RuntimeError("quota exceeded"))])
def test_llm_unavailable_falls_back_to_canned(pg_conn, llm):
    out = answer("câu gì đó", pg_conn, llm)
    assert out["rows"] == [] and "câu hỏi bấm sẵn" in out["error"]
    assert "quota" not in out["error"]


def test_sql_is_returned_to_user_for_transparency(pg_conn):
    seed(pg_conn)
    assert answer("x", pg_conn, FakeLLM(GOOD))["sql"] == GOOD
    # SQL bị chặn cũng phải cho người dùng thấy
    assert answer("x", pg_conn, FakeLLM("DROP TABLE edits_live"))["sql"] == "DROP TABLE edits_live"


def test_llm_can_say_there_is_no_data(pg_conn):
    out = answer("tỉ lệ bị revert?", pg_conn, FakeLLM("NO_DATA: nhãn revert không có trong Postgres"))
    assert out["rows"] == [] and out["sql"] == "" and "nhãn revert" in out["error"]


def test_sql_that_fails_at_runtime_is_a_readable_error(pg_conn):
    out = answer("x", pg_conn, FakeLLM("SELECT no_such_column FROM edits_live"))
    assert out["rows"] == [] and out["error"] and out["sql"]


def test_result_is_capped_at_100_rows(pg_conn):
    seed(pg_conn)
    out = answer("x", pg_conn, FakeLLM("SELECT rev_id FROM edits_live"))
    assert len(out["rows"]) == 100


def test_claude_llm_is_absent_without_credentials(monkeypatch):
    monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)
    monkeypatch.delenv("ANTHROPIC_AUTH_TOKEN", raising=False)
    assert nl2sql.llm_from_env() is None


# --- route ---------------------------------------------------------------------


def client_for(pg_conn, llm, **kw):
    pytest.importorskip("httpx")
    from fastapi.testclient import TestClient

    from src.api.main import create_app

    @contextmanager
    def factory():
        yield pg_conn

    return TestClient(create_app(factory, chat_connection=factory, llm=llm, **kw))


def test_chat_route_returns_rows_sql_and_license(pg_conn):
    seed(pg_conn)
    body = client_for(pg_conn, FakeLLM(GOOD)).post("/api/chat", json={"question": "x"}).json()
    assert body["sql"] == GOOD and len(body["rows"]) == 5 and body["error"] is None
    assert body["license"] == "CC BY-SA 4.0"


def test_chat_route_offers_canned_questions_on_failure(pg_conn):
    body = client_for(pg_conn, None).post("/api/chat", json={"question": "x"}).json()
    assert body["error"] and [q["qid"] for q in body["canned"]] == [1, 2, 3, 4, 5]


def test_chat_route_never_runs_without_the_readonly_connection(pg_conn):
    pytest.importorskip("httpx")
    from fastapi.testclient import TestClient

    from src.api.main import create_app

    @contextmanager
    def factory():
        yield pg_conn

    llm = FakeLLM(GOOD)
    body = TestClient(create_app(factory, llm=llm)).post("/api/chat", json={"question": "x"}).json()
    assert body["error"] and llm.seen == []


def test_chat_route_validates_question_length(pg_conn):
    client = client_for(pg_conn, FakeLLM(GOOD))
    assert client.post("/api/chat", json={"question": ""}).status_code == 422
    assert client.post("/api/chat", json={"question": "x" * 501}).status_code == 422


def test_chat_route_limits_paid_llm_calls(pg_conn):
    seed(pg_conn)
    llm = FakeLLM(GOOD)
    client = client_for(pg_conn, llm, chat_max_per_minute=2)
    codes = [client.post("/api/chat", json={"question": "x"}).status_code for _ in range(3)]
    assert codes == [200, 200, 429] and len(llm.seen) == 2

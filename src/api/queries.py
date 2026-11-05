"""Toàn bộ SQL của API. Mọi giá trị đi qua tham số ``%(name)s`` — không nối
chuỗi SQL với dữ liệu người dùng ở bất kỳ đâu (Review Focus #4).
"""

from __future__ import annotations

import urllib.parse

LICENSE = "CC BY-SA 4.0"
LICENSE_URL = "https://creativecommons.org/licenses/by-sa/4.0/"
SOURCE = "Wikipedia (Wikimedia Foundation) — nội dung diff giữ nguyên giấy phép gốc"
HIGH_RISK_THRESHOLD = 0.7


def wiki_host(wiki: str | None) -> str:
    """``enwiki`` → ``en.wikipedia.org``."""
    lang = (wiki or "enwiki").removesuffix("wiki") or "en"
    return f"{lang}.wikipedia.org"


def article_url(wiki: str | None, title: str | None) -> str:
    path = urllib.parse.quote((title or "").replace(" ", "_"), safe="/")
    return f"https://{wiki_host(wiki)}/wiki/{path}"


def diff_url(wiki: str | None, rev_id: int | None) -> str:
    return f"https://{wiki_host(wiki)}/w/index.php?diff={int(rev_id or 0)}"


METRICS = """
SELECT minute AS t, edit_count, anon_count,
       CASE WHEN edit_count > 0 THEN anon_count::float / edit_count ELSE 0 END AS anon_ratio,
       byte_delta_sum, hot_page_count, high_risk_count
FROM metrics_minute
WHERE minute >= now() - make_interval(hours => %(hours)s)
ORDER BY minute ASC
"""

METRICS_SUMMARY = """
SELECT
  (SELECT coalesce(avg(edit_count), 0) FROM metrics_minute
    WHERE minute >= now() - interval '5 minutes')                         AS edits_per_minute,
  (SELECT CASE WHEN sum(edit_count) > 0
               THEN sum(anon_count)::float / sum(edit_count) ELSE 0 END
     FROM metrics_minute WHERE minute >= now() - interval '1 hour')       AS anon_ratio,
  (SELECT count(DISTINCT (wiki, page_title)) FROM hot_pages
    WHERE window_end >= now() - interval '10 minutes')                    AS hot_pages,
  (SELECT count(*) FROM risk_queue WHERE risk_score >= %(threshold)s)     AS high_risk
"""

HOT = """
SELECT * FROM (
  SELECT DISTINCT ON (wiki, page_title)
         wiki, page_title, window_end, edit_count, distinct_users, ewma, z_score, summary
  FROM hot_pages
  ORDER BY wiki, page_title, window_end DESC
) latest
ORDER BY z_score DESC, window_end DESC
LIMIT %(limit)s
"""

RISK = """
SELECT wiki, rev_id, page_title, user_hash, is_anon, risk_score, model_version,
       diff_added, diff_removed, diff_status, summary, event_time
FROM risk_queue
ORDER BY risk_score DESC, event_time DESC
LIMIT %(limit)s
"""

LIVE = """
SELECT wiki, rev_id, page_title, user_hash, is_anon, comment, byte_delta,
       risk_score, model_version, summary, event_time
FROM edits_live
ORDER BY event_time DESC
LIMIT %(limit)s
"""

PAGE_HOURLY = """
SELECT hour_bucket AS t, edit_count, distinct_users, total_byte_delta, anon_ratio,
       mean_risk_score, max_risk_score
FROM page_hourly
WHERE page_title = %(title)s AND hour_bucket >= now() - make_interval(days => %(days)s)
ORDER BY hour_bucket ASC
"""

PAGE_EDITS = """
SELECT rev_id, user_hash, is_anon, byte_delta, risk_score, summary, event_time
FROM edits_live
WHERE page_title = %(title)s
ORDER BY event_time DESC
LIMIT 50
"""

CANNED: dict[int, tuple[str, str]] = {
    1: (
        "Trang nào nóng nhất 6 giờ qua?",
        """
        SELECT page_title AS "Trang", sum(edit_count) AS "Số edit",
               max(distinct_users) AS "User/giờ cao nhất", round(avg(anon_ratio)::numeric, 3) AS "Tỉ lệ vô danh"
        FROM page_hourly
        WHERE hour_bucket >= date_trunc('hour', now()) - interval '6 hours'
        GROUP BY page_title
        ORDER BY sum(edit_count) DESC
        LIMIT 20
        """,
    ),
    2: (
        "Edit nào rủi ro cao nhất hôm nay?",
        """
        SELECT page_title AS "Trang", rev_id AS "Revision", round(risk_score::numeric, 3) AS "Risk score",
               is_anon AS "Vô danh", summary AS "Tóm tắt", event_time AS "Thời điểm"
        FROM risk_queue
        WHERE event_time >= date_trunc('day', now())
        ORDER BY risk_score DESC
        LIMIT 20
        """,
    ),
    3: (
        "Tỉ lệ edit vô danh thay đổi thế nào trong 24h?",
        """
        SELECT date_trunc('hour', minute) AS "Giờ", sum(edit_count) AS "Số edit",
               round((sum(anon_count)::float / nullif(sum(edit_count), 0))::numeric, 3) AS "Tỉ lệ vô danh"
        FROM metrics_minute
        WHERE minute >= now() - interval '24 hours'
        GROUP BY 1
        ORDER BY 1
        """,
    ),
    4: (
        "Trang nào bị sửa nhiều nhất tuần này?",
        """
        SELECT page_title AS "Trang", sum(edit_count) AS "Số edit",
               round(max(max_risk_score)::numeric, 3) AS "Risk cao nhất"
        FROM page_hourly
        WHERE hour_bucket >= now() - interval '7 days'
        GROUP BY page_title
        ORDER BY sum(edit_count) DESC
        LIMIT 20
        """,
    ),
    5: (
        "Giờ nào trong ngày có nhiều phá hoại nhất?",
        """
        SELECT extract(hour FROM hour_bucket AT TIME ZONE 'UTC')::int AS "Giờ (UTC)",
               round(sum(edit_count * coalesce(mean_risk_score, 0))::numeric, 1)
                   AS "Số edit rủi ro kỳ vọng",
               sum(edit_count) AS "Tổng edit"
        FROM page_hourly
        WHERE hour_bucket >= now() - interval '30 days'
        GROUP BY 1
        ORDER BY 2 DESC
        LIMIT 24
        """,
    ),
}

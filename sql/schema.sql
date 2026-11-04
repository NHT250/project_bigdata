-- Tầng phục vụ (spec §5.4). Đây là CACHE, không phải nơi lưu trữ gốc:
-- dữ liệu gốc luôn ở Delta. Mọi bảng đều nhỏ và có TTL.
--
-- page_title là TEXT, không VARCHAR(n): tên trang Wikipedia có unicode, dấu
-- gạch chéo, dấu nháy và có thể rất dài (Review Focus #4).

CREATE TABLE IF NOT EXISTS edits_live (
    wiki            TEXT        NOT NULL,
    rev_id          BIGINT      NOT NULL,
    page_title      TEXT,
    user_hash       TEXT,
    is_anon         BOOLEAN,
    comment         TEXT,
    byte_delta      INTEGER,
    risk_score      DOUBLE PRECISION,
    model_version   TEXT,
    summary         TEXT,
    event_time      TIMESTAMPTZ NOT NULL,
    ingest_time     TIMESTAMPTZ,
    PRIMARY KEY (wiki, rev_id)
);
CREATE INDEX IF NOT EXISTS edits_live_event_time_idx
    ON edits_live (event_time DESC);

CREATE TABLE IF NOT EXISTS hot_pages (
    wiki            TEXT        NOT NULL,
    page_title      TEXT        NOT NULL,
    window_end      TIMESTAMPTZ NOT NULL,
    edit_count      INTEGER     NOT NULL,
    distinct_users  INTEGER     NOT NULL,
    ewma            DOUBLE PRECISION,
    z_score         DOUBLE PRECISION,
    summary         TEXT,
    PRIMARY KEY (wiki, page_title, window_end)
);
CREATE INDEX IF NOT EXISTS hot_pages_z_idx
    ON hot_pages (z_score DESC, window_end DESC);

CREATE TABLE IF NOT EXISTS risk_queue (
    wiki            TEXT        NOT NULL,
    rev_id          BIGINT      NOT NULL,
    page_title      TEXT,
    user_hash       TEXT,
    is_anon         BOOLEAN,
    risk_score      DOUBLE PRECISION NOT NULL,
    model_version   TEXT,
    diff_added      TEXT,
    diff_removed    TEXT,
    diff_status     TEXT,
    summary         TEXT,
    event_time      TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (wiki, rev_id)
);
CREATE INDEX IF NOT EXISTS risk_queue_score_idx
    ON risk_queue (risk_score DESC);

CREATE TABLE IF NOT EXISTS metrics_minute (
    wiki            TEXT        NOT NULL,
    minute          TIMESTAMPTZ NOT NULL,
    edit_count      INTEGER     NOT NULL,
    anon_count      INTEGER     NOT NULL,
    byte_delta_sum  BIGINT,
    hot_page_count  INTEGER,
    high_risk_count INTEGER,
    PRIMARY KEY (wiki, minute)
);
CREATE INDEX IF NOT EXISTS metrics_minute_minute_idx
    ON metrics_minute (minute DESC);

CREATE TABLE IF NOT EXISTS page_hourly (
    wiki            TEXT        NOT NULL,
    page_title      TEXT        NOT NULL,
    hour_bucket     TIMESTAMPTZ NOT NULL,
    edit_count      INTEGER     NOT NULL,
    distinct_users  INTEGER     NOT NULL,
    total_byte_delta BIGINT,
    anon_ratio      DOUBLE PRECISION,
    mean_risk_score DOUBLE PRECISION,
    max_risk_score  DOUBLE PRECISION,
    PRIMARY KEY (wiki, page_title, hour_bucket)
);
CREATE INDEX IF NOT EXISTS page_hourly_title_idx
    ON page_hourly (page_title, hour_bucket DESC);

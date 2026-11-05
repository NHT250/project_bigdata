import time

from tests.web_fixtures import browser, live_server, page  # noqa: F401


def open_dashboard(page, live_server):
    page.goto(live_server + "/")
    page.wait_for_selector("#hot-table tbody tr")


def test_dashboard_shows_four_stat_cards(page, live_server):
    open_dashboard(page, live_server)
    assert page.locator(".stat-card").count() == 4
    assert page.inner_text("#stat-epm") != "–"


def test_hot_pages_table_renders_rows_from_api(page, live_server):
    open_dashboard(page, live_server)
    assert page.locator("#hot-table tbody tr").count() > 0


def test_risk_panel_shows_score_and_link_to_wikipedia(page, live_server):
    open_dashboard(page, live_server)
    page.wait_for_selector("#risk-table a")
    assert "en.wikipedia.org" in page.locator("#risk-table a").first.get_attribute("href")
    assert page.locator("#risk-table .score").count() > 0


def test_page_refreshes_data_every_five_seconds(page, live_server):
    hits = []
    page.on("request", lambda r: hits.append(r.url) if "/api/live" in r.url else None)
    page.goto(live_server + "/")
    time.sleep(11)
    assert 2 <= len(hits) <= 4


def test_titles_are_rendered_as_text_not_html(page, live_server):
    open_dashboard(page, live_server)
    assert page.locator("#live-stream script").count() == 0


def test_license_footer_is_present(page, live_server):
    page.goto(live_server + "/")
    assert "CC BY-SA 4.0" in page.locator("footer").inner_text()

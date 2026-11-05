from tests.web_fixtures import browser, live_server, page  # noqa: F401


def test_lookup_shows_hourly_chart_for_searched_title(page, live_server):
    page.goto(live_server + "/page.html")
    page.fill("#title", "Typhoon_Yagi")
    page.click("#go")
    page.wait_for_selector("#result:not([hidden])")
    assert page.locator("canvas#hourly").is_visible()
    assert page.locator("#edits-table tbody tr").count() > 0


def test_lookup_shows_message_for_unknown_page(page, live_server):
    page.goto(live_server + "/page.html")
    page.fill("#title", "Không tồn tại / ' --")
    page.click("#go")
    page.wait_for_selector("#empty:not([hidden])")
    assert "không có dữ liệu" in page.inner_text("#empty")


def test_ask_page_renders_five_buttons(page, live_server):
    page.goto(live_server + "/ask.html")
    page.wait_for_selector(".ask-btn")
    assert page.locator(".ask-btn").count() == 5


def test_clicking_a_question_renders_a_result_table(page, live_server):
    page.goto(live_server + "/ask.html")
    page.click(".ask-btn >> nth=0")
    page.wait_for_selector("#ask-result tbody tr")
    assert page.locator("#ask-result tbody tr").count() > 0

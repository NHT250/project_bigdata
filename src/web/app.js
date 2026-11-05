// JavaScript thuần, không build step (spec §7.2). Mọi chuỗi từ API đi qua
// textContent — tên trang và diff là nội dung người dùng Wikipedia viết ra.
"use strict";

const REFRESH_MS = 5000;
const HIGH_RISK = 0.7;

function el(tag, attrs = {}, ...children) {
  const node = document.createElement(tag);
  for (const [k, v] of Object.entries(attrs)) {
    if (v === null || v === undefined) continue;
    if (k === "class") node.className = v;
    else if (k === "text") node.textContent = v;
    else node.setAttribute(k, v);
  }
  for (const c of children) if (c !== null && c !== undefined) node.append(c);
  return node;
}

async function getJSON(path) {
  const res = await fetch(path, { headers: { Accept: "application/json" } });
  if (!res.ok) throw new Error(`${path} trả ${res.status}`);
  return res.json();
}

const fmt = {
  int: (n) => (n === null || n === undefined ? "–" : Math.round(n).toLocaleString("vi-VN")),
  pct: (r) => (r === null || r === undefined ? "–" : `${(r * 100).toFixed(0)}%`),
  score: (s) => (s === null || s === undefined ? "–" : s.toFixed(2)),
  time: (iso) => new Date(iso).toLocaleTimeString("vi-VN", { hour: "2-digit", minute: "2-digit", second: "2-digit" }),
  hour: (iso) => new Date(iso).toLocaleString("vi-VN", { day: "2-digit", month: "2-digit", hour: "2-digit", minute: "2-digit" }),
  bytes: (b) => (b > 0 ? `+${b}` : `${b}`),
};

function scoreBadge(s) {
  const level = s === null || s === undefined ? "low" : s >= HIGH_RISK ? "high" : s >= 0.4 ? "mid" : "low";
  return el("span", { class: `score ${level}`, text: fmt.score(s) });
}

function articleLink(item) {
  return el("a", { href: item.url, class: "title", rel: "noopener", target: "_blank", text: item.page_title });
}

function clip(text, n = 220) {
  if (!text) return "";
  return text.length > n ? text.slice(0, n) + "…" : text;
}

function fillBody(table, rows) {
  const body = table.querySelector("tbody");
  body.replaceChildren(...rows);
}

function setStatus(msg) {
  const s = document.getElementById("status");
  if (s) s.textContent = msg;
}

// --- dashboard --------------------------------------------------------------

let epmChart = null;

function drawChart(points) {
  const canvas = document.getElementById("epm-chart");
  if (!canvas || typeof Chart === "undefined") return;
  const labels = points.map((p) => fmt.time(p.t).slice(0, 5));
  const total = points.map((p) => p.edit_count);
  const anon = points.map((p) => p.anon_count);
  if (epmChart) {
    epmChart.data.labels = labels;
    epmChart.data.datasets[0].data = total;
    epmChart.data.datasets[1].data = anon;
    epmChart.update("none");
    return;
  }
  epmChart = new Chart(canvas, {
    type: "line",
    data: {
      labels,
      datasets: [
        { label: "Tổng edit", data: total, borderColor: "#3366cc", borderWidth: 1.5, pointRadius: 0, tension: 0.2 },
        { label: "Edit vô danh", data: anon, borderColor: "#e0a030", borderWidth: 1.5, pointRadius: 0, tension: 0.2 },
      ],
    },
    options: {
      animation: false,
      maintainAspectRatio: false,
      interaction: { mode: "index", intersect: false },
      plugins: { legend: { display: false } },
      scales: { x: { ticks: { maxTicksLimit: 8 }, grid: { display: false } }, y: { beginAtZero: true } },
    },
  });
}

async function loadMetrics() {
  const m = await getJSON("/api/metrics?hours=6");
  const s = m.summary;
  document.getElementById("stat-epm").textContent = fmt.int(s.edits_per_minute);
  document.getElementById("stat-anon").textContent = fmt.pct(s.anon_ratio);
  document.getElementById("stat-hot").textContent = fmt.int(s.hot_pages);
  document.getElementById("stat-risk").textContent = fmt.int(s.high_risk);
  drawChart(m.points);
}

async function loadHot() {
  const { items } = await getJSON("/api/hot?limit=20");
  const table = document.getElementById("hot-table");
  if (!items.length) {
    fillBody(table, [el("tr", {}, el("td", { colspan: "4", class: "empty", text: "Chưa có trang nào bùng nổ trong giờ qua." }))]);
    return;
  }
  fillBody(table, items.map((h) =>
    el("tr", {},
      el("td", {}, articleLink(h), h.summary ? el("div", { class: "status", text: h.summary }) : null),
      el("td", { class: "num", text: h.z_score.toFixed(1) }),
      el("td", { class: "num", text: fmt.int(h.edit_count) }),
      el("td", { class: "num", text: fmt.int(h.distinct_users) }))));
}

function diffCell(item) {
  const box = el("div", { class: "diff" });
  if (item.diff_added) box.append(el("ins", { text: clip(item.diff_added) }));
  if (item.diff_removed) box.append(el("del", { text: clip(item.diff_removed) }));
  if (!item.diff_added && !item.diff_removed) {
    box.append(el("span", { class: "status", text: item.diff_status === "ok" ? "Chỉ đổi định dạng" : `Không lấy được diff (${item.diff_status || "chưa có"})` }));
  }
  if (item.summary) box.append(el("div", { class: "status", text: item.summary }));
  return box;
}

async function loadRisk() {
  const { items } = await getJSON("/api/risk?limit=20");
  const table = document.getElementById("risk-table");
  if (!items.length) {
    fillBody(table, [el("tr", {}, el("td", { colspan: "4", class: "empty", text: "Chưa có điểm rủi ro: model chưa được huấn luyện hoặc chưa có edit mới." }))]);
    return;
  }
  fillBody(table, items.map((r) =>
    el("tr", {},
      el("td", {}, articleLink(r), el("div", { class: "status", text: `${fmt.time(r.event_time)}, ${r.is_anon ? "vô danh" : "có tài khoản"}` })),
      el("td", { class: "num" }, scoreBadge(r.risk_score)),
      el("td", {}, diffCell(r)),
      el("td", {}, el("a", { href: r.diff_url, rel: "noopener", target: "_blank", text: "Xem diff gốc" })))));
}

async function loadLive() {
  const { items } = await getJSON("/api/live?limit=60");
  const list = document.getElementById("live-stream");
  const maxAbs = Math.max(200, ...items.map((i) => Math.abs(i.byte_delta || 0)));
  list.replaceChildren(...items.map((e) => {
    const d = e.byte_delta || 0;
    const width = `${Math.min(50, (Math.abs(d) / maxAbs) * 50)}%`;
    const bar = el("div", { class: "bar", "aria-hidden": "true" }, el("span", { class: d >= 0 ? "plus" : "minus", style: `width:${width}` }));
    return el("li", {},
      el("time", { datetime: e.event_time, text: fmt.time(e.event_time) }),
      el("div", {}, articleLink(e), el("div", { class: "who", text: e.summary || e.comment || "" })),
      bar,
      el("div", { class: `delta ${d >= 0 ? "plus" : "minus"}`, text: fmt.bytes(d) }));
  }));
}

async function refreshDashboard() {
  const results = await Promise.allSettled([loadMetrics(), loadHot(), loadRisk(), loadLive()]);
  const failed = results.filter((r) => r.status === "rejected");
  setStatus(failed.length
    ? `Không tải được ${failed.length}/4 phần. Kiểm tra API và PostgreSQL; trang tự thử lại sau 5 giây.`
    : `Cập nhật lúc ${new Date().toLocaleTimeString("vi-VN")}`);
}

// --- tra cứu trang ----------------------------------------------------------

let hourlyChart = null;

async function lookup(title) {
  const empty = document.getElementById("empty");
  const result = document.getElementById("result");
  const data = await getJSON(`/api/page?days=7&title=${encodeURIComponent(title)}`);
  if (!data.points.length && !data.edits.length) {
    result.hidden = true;
    empty.hidden = false;
    empty.textContent = `Trang "${data.title}" không có dữ liệu trong 7 ngày qua. Chỉ trang có từ 5 edit mỗi ngày được lưu; thử tên chính xác như trên Wikipedia.`;
    return;
  }
  empty.hidden = true;
  result.hidden = false;
  const link = document.getElementById("page-link");
  link.href = data.url;
  link.textContent = data.title;

  const labels = data.points.map((p) => fmt.hour(p.t));
  const cfg = {
    data: {
      labels,
      datasets: [
        { type: "bar", label: "Edit/giờ", data: data.points.map((p) => p.edit_count), backgroundColor: "#3366cc", yAxisID: "y" },
        { type: "line", label: "Risk cao nhất", data: data.points.map((p) => p.max_risk_score), borderColor: "#b32424", pointRadius: 0, borderWidth: 1.5, yAxisID: "r" },
      ],
    },
    options: {
      animation: false, maintainAspectRatio: false,
      scales: { y: { beginAtZero: true, position: "left" }, r: { min: 0, max: 1, position: "right", grid: { display: false } }, x: { ticks: { maxTicksLimit: 8 } } },
    },
  };
  if (hourlyChart) hourlyChart.destroy();
  if (typeof Chart !== "undefined") hourlyChart = new Chart(document.getElementById("hourly"), cfg);

  fillBody(document.getElementById("edits-table"), data.edits.map((e) =>
    el("tr", {},
      el("td", { text: fmt.hour(e.event_time) }),
      el("td", { text: e.summary || "" }),
      el("td", { class: "num" }, scoreBadge(e.risk_score)),
      el("td", { class: "who", text: e.user_hash || "" }))));
}

function initLookup() {
  const form = document.getElementById("search");
  const input = document.getElementById("title");
  form.addEventListener("submit", (ev) => {
    ev.preventDefault();
    const t = input.value.trim();
    if (!t) return;
    history.replaceState(null, "", `?title=${encodeURIComponent(t)}`);
    lookup(t).catch((e) => setStatus(`Không tra cứu được: ${e.message}`));
  });
  const initial = new URLSearchParams(location.search).get("title");
  if (initial) { input.value = initial; form.requestSubmit(); }
}

// --- hỏi đáp ----------------------------------------------------------------

async function ask(qid, button) {
  document.querySelectorAll(".ask-btn").forEach((b) => b.setAttribute("aria-pressed", String(b === button)));
  const data = await getJSON(`/api/ask?qid=${qid}`);
  const table = document.getElementById("ask-result");
  document.getElementById("ask-title").textContent = data.title;
  table.querySelector("thead").replaceChildren(el("tr", {}, ...data.columns.map((c) => el("th", { text: c }))));
  if (!data.rows.length) {
    fillBody(table, [el("tr", {}, el("td", { colspan: String(Math.max(1, data.columns.length)), class: "empty", text: "Chưa đủ dữ liệu để trả lời câu này." }))]);
    return;
  }
  fillBody(table, data.rows.map((row) => el("tr", {}, ...row.map((v) => {
    const isNum = typeof v === "number";
    const text = v === null ? "–" : typeof v === "string" && /^\d{4}-\d\d-\d\dT/.test(v) ? fmt.hour(v) : isNum ? v.toLocaleString("vi-VN") : String(v);
    return el("td", { class: isNum ? "num" : null, text });
  }))));
}

async function initAsk() {
  const { items } = await getJSON("/api/questions");
  const list = document.getElementById("ask-list");
  list.replaceChildren(...items.map((q) => {
    const b = el("button", { class: "ghost ask-btn", type: "button", "aria-pressed": "false", text: q.title });
    b.addEventListener("click", () => ask(q.qid, b).catch((e) => setStatus(`Không chạy được câu hỏi: ${e.message}`)));
    return b;
  }));
}

// --- khởi động --------------------------------------------------------------

document.addEventListener("DOMContentLoaded", () => {
  const page = document.body.dataset.page;
  if (page === "dashboard") {
    refreshDashboard();
    setInterval(refreshDashboard, REFRESH_MS);
  } else if (page === "lookup") {
    initLookup();
  } else if (page === "ask") {
    initAsk().catch((e) => setStatus(`Không tải được danh sách câu hỏi: ${e.message}`));
  }
});

// Slightly under the server's 20s status-cache TTL so a poll usually
// hits a freshly-rebuilt cache instead of forcing another scan.
const REFRESH_MS = 15000;

const MONTH_NAMES = [
  "January", "February", "March", "April", "May", "June",
  "July", "August", "September", "October", "November", "December",
];

function escapeHtml(value) {
  if (value == null) return "";
  return String(value)
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&#39;");
}

function parseDate(iso) {
  const [y, m, d] = iso.split("-").map(Number);
  return { y, m, d };
}
function daysInMonth(year, month) { return new Date(year, month, 0).getDate(); }
function firstWeekday(year, month) { return new Date(year, month - 1, 1).getDay(); }

function formatRelative(iso) {
  if (!iso) return "never";
  const then = Date.parse(iso);
  if (Number.isNaN(then)) return iso;
  const deltaSec = Math.max(0, (Date.now() - then) / 1000);
  if (deltaSec < 60) return `${Math.round(deltaSec)}s ago`;
  if (deltaSec < 3600) return `${Math.round(deltaSec / 60)}m ago`;
  if (deltaSec < 86400) return `${Math.round(deltaSec / 3600)}h ago`;
  return `${Math.round(deltaSec / 86400)}d ago`;
}

function renderAggregate(agg) {
  document.getElementById("agg-active").textContent = agg.active_runs.toLocaleString();
  document.getElementById("agg-cases").textContent = agg.cases_scraped.toLocaleString();
  document.getElementById("agg-days-complete").textContent = agg.days_complete.toLocaleString();
  document.getElementById("agg-docs-24h").textContent = agg.docs_last_24h.toLocaleString();
  document.getElementById("agg-cases-24h").textContent = agg.cases_last_24h.toLocaleString();
}

function renderLiveRuns(scrapers) {
  const root = document.getElementById("live-runs");
  root.innerHTML = "";
  const rows = [];
  for (const s of scrapers) {
    for (const hb of (s.heartbeats ?? [])) {
      rows.push({ scraper: s.name, ...hb });
    }
  }
  if (rows.length === 0) {
    root.innerHTML = `<div class="live-row EXITED"><span class="badge">IDLE</span><span>No heartbeat files found. Run a scraper or wait for the next beat.</span><span class="where"></span></div>`;
    return;
  }
  for (const r of rows) {
    const d = r.data || {};
    const where = [d.current_day, d.current_case].filter(Boolean).join(" · ") || "—";
    const lastBeat = formatRelative(d.last_heartbeat_at);
    const started = formatRelative(d.started_at);
    const intent = formatIntentLine(d);
    const sessionLine = formatSessionLine(d);

    const div = document.createElement("div");
    div.className = `live-row ${escapeHtml(r.liveness)}`;
    div.innerHTML = `
      <span class="badge">${escapeHtml(r.liveness)}</span>
      <div>
        <div><strong>${escapeHtml(r.scraper)}</strong> · pid ${escapeHtml(d.pid ?? "?")}${d.worker_id != null ? ` · worker ${escapeHtml(d.worker_id)}` : ""}</div>
        ${intent ? `<div class="where">${escapeHtml(intent)}</div>` : ""}
        <div class="where">${escapeHtml(where)}</div>
        ${sessionLine ? `<div class="where" style="color:var(--fg);">${escapeHtml(sessionLine)}</div>` : ""}
      </div>
      <div style="text-align:right;font-size:11px;color:var(--muted);">
        <div>beat ${escapeHtml(lastBeat)}</div>
        <div>started ${escapeHtml(started)}</div>
      </div>`;
    root.appendChild(div);
  }
}

function formatIntentLine(d) {
  const parts = [];
  if (d.county) parts.push(d.county);
  if (d.types) parts.push(d.types);
  if (d.case_type) parts.push(d.case_type);
  if (Array.isArray(d.case_prefixes) && d.case_prefixes.length) parts.push(d.case_prefixes.join("/"));
  if (d.start_date && d.end_date) parts.push(`${d.start_date}…${d.end_date}`);
  const flags = [];
  if (d.no_filter) flags.push("no-filter");
  if (d.no_cap) flags.push("no-cap");
  if (d.refresh_on_gate) flags.push("refresh-on-gate");
  if (d.popup_fallback) flags.push("popup-fallback");
  if (d.rotation_managed) flags.push("rotation-managed");
  if (flags.length) parts.push(flags.join(","));
  return parts.join(" · ");
}

function formatSessionLine(d) {
  const parts = [];
  if (typeof d.session_cases_scraped === "number") parts.push(`${d.session_cases_scraped} cases`);
  if (typeof d.session_docs_collected === "number") parts.push(`${d.session_docs_collected} docs`);
  if (d.current_ip) parts.push(`ip ${d.current_ip}`);
  return parts.join(" · ");
}

function formatIntendedScope(scope) {
  if (!scope || typeof scope !== "object") return "";
  const parts = [];
  if (scope.county) parts.push(scope.county);
  if (scope.types) parts.push(scope.types);
  if (scope.case_type) parts.push(scope.case_type);
  if (Array.isArray(scope.case_prefixes) && scope.case_prefixes.length) parts.push(scope.case_prefixes.join("/"));
  if (scope.start_date && scope.end_date) parts.push(`${scope.start_date}…${scope.end_date}`);
  else if (scope.start_date) parts.push(`from ${scope.start_date}`);
  else if (scope.end_date) parts.push(`to ${scope.end_date}`);
  if (scope.pdf_filter_profile) parts.push(`pdf-filter=${scope.pdf_filter_profile}`);
  if (Array.isArray(scope.filters) && scope.filters.length) parts.push(scope.filters.join(","));
  return parts.join(" · ");
}

function renderScraperSection(scraper) {
  const wrap = document.createElement("section");
  wrap.className = "scraper-section";
  const tot = scraper.totals;
  const rate = scraper.rate;
  const configuredScope = formatIntendedScope(scraper.intended_scope);
  const hasActiveRun = (scraper.heartbeats ?? []).some((h) => h.liveness === "ACTIVE");
  const nextCommand = !hasActiveRun ? scraper.suggested_next_command : null;
  wrap.innerHTML = `
    <h2>
      <span>${escapeHtml(scraper.name)} <span style="color:var(--muted);font-weight:400;font-size:12px;">(${escapeHtml(scraper.scraper_kind)} · ${escapeHtml(scraper.layout)})</span></span>
      <span class="root">${escapeHtml(scraper.root)}${scraper.root_exists ? "" : "  [missing]"}</span>
    </h2>
    ${configuredScope ? `<div class="scope">configured scope: ${escapeHtml(configuredScope)}</div>` : ""}
    ${nextCommand ? `
      <div class="next-cmd-block">
        <div class="next-cmd-label">resume command <button class="copy-btn" data-cmd="${escapeHtml(nextCommand)}">copy</button></div>
        <pre class="next-cmd">${escapeHtml(nextCommand)}</pre>
      </div>` : ""}
    <div class="rate">
      <div class="stat"><span class="label">Rate / min (recent)</span><span class="value">${(rate.recent_rate_per_min ?? 0).toLocaleString()}</span></div>
      <div class="stat"><span class="label">Cases / hr</span><span class="value">${rate.cases_last_hour.toLocaleString()}</span></div>
      <div class="stat"><span class="label">Cases / 24h</span><span class="value">${rate.cases_last_24h.toLocaleString()}</span></div>
      <div class="stat"><span class="label">Cases / 7d</span><span class="value">${rate.cases_last_7d.toLocaleString()}</span></div>
      <div class="stat"><span class="label">Docs / 24h</span><span class="value">${rate.docs_last_24h.toLocaleString()}</span></div>
      <div class="stat"><span class="label">Last activity</span><span class="value">${formatRelative(rate.last_activity_at)}</span></div>
    </div>
    <div class="totals" style="margin-top:8px;">
      <div class="stat"><span class="label">Days tracked</span><span class="value">${tot.days_tracked.toLocaleString()}</span></div>
      <div class="stat"><span class="label">Days complete</span><span class="value">${tot.days_complete.toLocaleString()}</span></div>
      <div class="stat"><span class="label">Days in progress</span><span class="value">${tot.days_in_progress.toLocaleString()}</span></div>
      <div class="stat"><span class="label">Days with failures</span><span class="value">${tot.days_with_failures.toLocaleString()}</span></div>
      <div class="stat"><span class="label">Cases scraped</span><span class="value">${tot.cases_scraped.toLocaleString()}</span></div>
      <div class="stat"><span class="label">Cases total</span><span class="value">${tot.cases_total.toLocaleString()}</span></div>
    </div>
    <div class="calendar"></div>`;
  renderCalendar(wrap.querySelector(".calendar"), scraper.days);
  return wrap;
}

function renderCalendar(target, days) {
  target.innerHTML = "";
  if (!days.length) {
    target.innerHTML = `<p class="empty">No filing-day folders found under this data root.</p>`;
    return;
  }
  const byDate = new Map(days.map((d) => [d.date, d]));
  const firstYear = parseDate(days[0].date).y;
  const lastYear = parseDate(days[days.length - 1].date).y;

  for (let year = lastYear; year >= firstYear; year--) {
    const yearEl = document.createElement("div");
    yearEl.className = "year";
    yearEl.innerHTML = `<h2>${year}</h2>`;
    const grid = document.createElement("div");
    grid.className = "year-grid";

    for (let month = 1; month <= 12; month++) {
      const monthEl = document.createElement("div");
      monthEl.className = "month";
      monthEl.innerHTML = `<h3>${MONTH_NAMES[month - 1]}</h3>`;
      const cal = document.createElement("div");
      cal.className = "days";

      const offset = firstWeekday(year, month);
      for (let i = 0; i < offset; i++) {
        const blank = document.createElement("div");
        blank.className = "day blank";
        cal.appendChild(blank);
      }
      const total = daysInMonth(year, month);
      for (let d = 1; d <= total; d++) {
        const iso = `${year}-${String(month).padStart(2, "0")}-${String(d).padStart(2, "0")}`;
        const info = byDate.get(iso);
        const cell = document.createElement("div");
        cell.className = "day";
        cell.classList.add(info ? info.status.replace("_", "-") : "untouched");
        cell.textContent = d;
        cell.dataset.date = iso;
        if (info) {
          cell.dataset.status = info.status;
          cell.dataset.total = info.total;
          cell.dataset.scraped = info.scraped;
          cell.dataset.missing = Math.max(0, info.total - info.scraped);
          cell.dataset.failed = info.failed;
          cell.dataset.runError = info.run_error || "";
        } else {
          cell.dataset.status = "untouched";
          cell.dataset.runError = "";
        }
        cal.appendChild(cell);
      }
      monthEl.appendChild(cal);
      grid.appendChild(monthEl);
    }
    yearEl.appendChild(grid);
    target.appendChild(yearEl);
  }
}

async function refresh() {
  try {
    const resp = await fetch("/api/status", { cache: "no-store" });
    if (!resp.ok) throw new Error(`status ${resp.status}`);
    const payload = await resp.json();
    document.getElementById("generated-at").textContent = `updated ${formatRelative(payload.generated_at)}`;
    renderAggregate(payload.aggregate);
    renderLiveRuns(payload.scrapers);
    const sections = document.getElementById("scrapers");
    sections.innerHTML = "";
    for (const s of payload.scrapers) sections.appendChild(renderScraperSection(s));
  } catch (err) {
    console.error("refresh failed", err);
  }
}

function tooltipHtml(cell) {
  const { date, status, runError } = cell.dataset;
  if (status === "untouched") {
    return `<div class="tt-title">${date}</div><div class="tt-line tt-muted">Untouched</div>`;
  }
  const total = Number(cell.dataset.total || 0);
  const scraped = Number(cell.dataset.scraped || 0);
  const missing = Number(cell.dataset.missing || 0);
  const failed = Number(cell.dataset.failed || 0);
  const statusLabel = status.replace(/_/g, " ");
  const lines = [
    `<div class="tt-title">${date}</div>`,
    `<div class="tt-line"><span>Scraped</span><span>${scraped.toLocaleString()} / ${total.toLocaleString()}</span></div>`,
    `<div class="tt-line"><span>Missing</span><span>${missing.toLocaleString()}</span></div>`,
  ];
  if (failed > 0) lines.push(`<div class="tt-line"><span>Failed</span><span>${failed.toLocaleString()}</span></div>`);
  lines.push(`<div class="tt-line tt-muted"><span>Status</span><span>${statusLabel}</span></div>`);
  if (runError) lines.push(`<div class="tt-error">${runError}</div>`);
  return lines.join("");
}

function setupTooltip() {
  const tooltip = document.getElementById("tooltip");
  document.body.addEventListener("mouseover", (e) => {
    const cell = e.target.closest(".day");
    if (!cell || cell.classList.contains("blank")) return;
    tooltip.innerHTML = tooltipHtml(cell);
    tooltip.hidden = false;
    const rect = cell.getBoundingClientRect();
    const ttRect = tooltip.getBoundingClientRect();
    let top = rect.top + window.scrollY - ttRect.height - 6;
    let left = rect.left + window.scrollX + rect.width / 2 - ttRect.width / 2;
    if (top < window.scrollY + 4) top = rect.bottom + window.scrollY + 6;
    left = Math.max(4 + window.scrollX,
      Math.min(left, window.scrollX + document.documentElement.clientWidth - ttRect.width - 4));
    tooltip.style.top = `${top}px`;
    tooltip.style.left = `${left}px`;
  });
  document.body.addEventListener("mouseout", (e) => {
    const cell = e.target.closest(".day");
    if (!cell) return;
    const next = e.relatedTarget?.closest?.(".day");
    if (next && next !== cell) return;
    tooltip.hidden = true;
  });
  window.addEventListener("scroll", () => { tooltip.hidden = true; }, { passive: true });
}

setupTooltip();
document.body.addEventListener("click", (e) => {
  const btn = e.target.closest(".copy-btn");
  if (!btn) return;
  const cmd = btn.dataset.cmd || "";
  if (!cmd) return;
  navigator.clipboard?.writeText(cmd).then(
    () => {
      const original = btn.textContent;
      btn.textContent = "copied";
      setTimeout(() => { btn.textContent = original; }, 1200);
    },
    () => { btn.textContent = "copy failed"; },
  );
});
refresh();
setInterval(refresh, REFRESH_MS);

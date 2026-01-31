/* ---------------- HELPERS ---------------- */
async function loadJSON(path) {
  const res = await fetch(path);
  return await res.json();
}

function last7(data) {
  return data.slice(-7).reverse();
}

function priceClass(curr, prev) {
  if (prev === undefined) return "same";
  if (curr > prev) return "up";
  if (curr < prev) return "down";
  return "same";
}

/* ---------------- WORKFLOW TIMES ---------------- */
async function loadWorkflowTimes() {
  const lastEl = document.getElementById("last-updated");
  const nextEl = document.getElementById("next-updated");

  const CACHE_KEY = "workflow-time-cache";
  const CACHE_TTL_MS = 10 * 60 * 1000; // 10 minutes

  let countdownTimerId = null;

  try {
    // ---- DOM verification ----
    if (!lastEl || !nextEl) {
      throw new Error("Required DOM elements not found");
    }

    let lastRun;

    // ---- Cache check ----
    const cachedRaw = localStorage.getItem(CACHE_KEY);
    if (cachedRaw) {
      const cached = JSON.parse(cachedRaw);
      if (Date.now() - cached.savedAt < CACHE_TTL_MS) {
        lastRun = new Date(cached.lastRun);
      }
    }

    // ---- Fetch from GitHub if cache missing/expired ----
    if (!lastRun) {
      const owner = "yasaswisalagrama";
      const repo = "yasaswisalagrama.github.io";
      const workflowFile = "daily-scrape.yml";

      const url =
        `https://api.github.com/repos/${owner}/${repo}` +
        `/actions/workflows/${workflowFile}/runs?per_page=1`;

      const res = await fetch(url, {
        headers: { "Accept": "application/vnd.github+json" }
      });

      if (!res.ok) {
        throw new Error(`GitHub API failed: ${res.status}`);
      }

      const json = await res.json();

      if (!json.workflow_runs || json.workflow_runs.length === 0) {
        throw new Error("No workflow runs found");
      }

      const runStartedAt = json.workflow_runs[0].run_started_at;
      if (!runStartedAt) {
        throw new Error("run_started_at missing in API response");
      }

      lastRun = new Date(runStartedAt);
      if (isNaN(lastRun.getTime())) {
        throw new Error("Invalid date parsed from API");
      }

      // ---- Save cache ----
      localStorage.setItem(
        CACHE_KEY,
        JSON.stringify({
          lastRun: lastRun.toISOString(),
          savedAt: Date.now()
        })
      );
    }

    // ---- Display last updated ----
    lastEl.textContent = lastRun.toLocaleString();

    // ---- Next update = +1 hour ----
    const nextRun = new Date(lastRun.getTime() + 60 * 60 * 1000);

    // ---- Delayed detection ----
    const now = new Date();
    const isDelayed = now > nextRun;

    nextEl.textContent =
      nextRun.toLocaleString() + (isDelayed ? " ⚠ delayed" : "");

    // ---- Countdown timer ----
    if (countdownTimerId) {
      clearInterval(countdownTimerId);
    }

    countdownTimerId = setInterval(() => {
      const diffMs = nextRun - new Date();

      if (diffMs <= 0) {
        nextEl.textContent =
          nextRun.toLocaleString() + " ⚠ delayed";
        clearInterval(countdownTimerId);
        return;
      }

      const totalSeconds = Math.floor(diffMs / 1000);
      const hours = Math.floor(totalSeconds / 3600);
      const minutes = Math.floor((totalSeconds % 3600) / 60);
      const seconds = totalSeconds % 60;

      nextEl.textContent =
        `${nextRun.toLocaleString()} ` +
        `(in ${hours}h ${minutes}m ${seconds}s)`;
    }, 1000);

  } catch (err) {
    console.error("loadWorkflowTimes failed:", err);
    if (lastEl) lastEl.textContent = "Unavailable";
    if (nextEl) nextEl.textContent = "Unavailable";
  }
}

loadWorkflowTimes();


/* ---- MIGRATION SAFE NORMALIZATION ---- */
function normalizeOHLC(row) {
  if (row.close === undefined) {
    if (row.price_per_gram_inr !== undefined) {
      row.close = row.price_per_gram_inr;
    } else if (row.price_per_kg_inr !== undefined) {
      row.close = row.price_per_kg_inr;
    }
  }

  if (row.open === undefined) row.open = row.close;
  if (row.high === undefined) row.high = row.close;
  if (row.low === undefined) row.low = row.close;

  return row;
}

function dedupeByDateAndPurity(rows) {
  const map = {};

  rows.forEach(r => {
    const key = r.date + "_" + (r.purity || "");
    map[key] = r; // later row overwrites earlier one
  });

  return Object.values(map);
}


/* ---------------- TABLE RENDERERS ---------------- */
function renderGoldTable(el, rows) {
  let html = `
    <tr>
      <th>Date</th>
      <th>Open</th>
      <th>High</th>
      <th>Low</th>
      <th>Close</th>
      <th>Source</th>
    </tr>
  `;

  rows.forEach((r, i) => {
    const prevClose = rows[i + 1]?.close;
    const cls = priceClass(r.close, prevClose);

    html += `
      <tr>
        <td>${r.date}</td>
        <td>${r.open}</td>
        <td>${r.high}</td>
        <td>${r.low}</td>
        <td class="${cls}">${r.close}</td>
        <td>${r.source}</td>
      </tr>
    `;
  });

  el.innerHTML = html;
}

function renderOHLCTable(el, rows) {
  let html = `
    <tr>
      <th>Date</th>
      <th>Open</th>
      <th>High</th>
      <th>Low</th>
      <th>Close</th>
      <th>Source</th>
    </tr>
  `;

  rows.forEach((r, i) => {
    const prevClose = rows[i + 1]?.close;
    const cls = priceClass(r.close, prevClose);

    html += `
      <tr>
        <td>${r.date}</td>
        <td>${r.open}</td>
        <td>${r.high}</td>
        <td>${r.low}</td>
        <td class="${cls}">${r.close}</td>
        <td>${r.source}</td>
      </tr>
    `;
  });

  el.innerHTML = html;
}

/* ---------------- INIT ---------------- */
async function init() {
  const goldAll = last7(
    dedupeByDateAndPurity(
    (await loadJSON("data/gold.json")).map(normalizeOHLC))
  );

  const silver = last7(
    dedupeByDateAndPurity(
    (await loadJSON("data/silver.json")).map(normalizeOHLC))
  );

  const copper = last7(
    dedupeByDateAndPurity(
    (await loadJSON("data/copper.json")).map(normalizeOHLC))
  );

  const gold24 = goldAll.filter(r => r.purity === "24K");
  const gold22 = goldAll.filter(r => r.purity === "22K");

  renderGoldTable(document.getElementById("gold24-table"), gold24);
  renderGoldTable(document.getElementById("gold22-table"), gold22);
  renderOHLCTable(document.getElementById("silver-table"), silver);
  renderOHLCTable(document.getElementById("copper-table"), copper);

  document.getElementById("updated").innerText =
    `🔴 Close ↑ vs yesterday | 🟢 Close ↓ vs yesterday | OHLC aggregated hourly`;
}

init();
import { createLab, pageSummary, planPointRead, virtualExtents, visiblePages } from "./model.mjs";
import { benchmarkData, benchmarkWorkloads, chartMaximum } from "./benchmarks.mjs";

const $ = selector => document.querySelector(selector);
const escape = value => String(value).replace(/[&<>"']/g, character => ({
  "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;",
})[character]);
const number = value => value == null ? "NULL" : typeof value === "number" ? value.toLocaleString("en-US") : value;
const lab = createLab();
const before = lab.snapshots[0];
const after = lab.apply("update");
const originalRevenue = before.groups[0].chunks.find(chunk => chunk.column === 6).pages;
const newRevenue = after.groups[0].chunks.find(chunk => chunk.column === 6).pages.at(-1);
const steps = [...document.querySelectorAll(".story-step")];
const scenes = [
  { title: "THE TABLE", badge: "12 rows · 7 columns", caption: "Same logical table. Three columns shown; four more stored." },
  { title: "THE ROW GROUP", badge: "1 group · 7 chunks", caption: "Rows 0–11 stay aligned, even though their values are stored column by column." },
  { title: "THE COLUMN PAGES", badge: "Decoded view", caption: "P18 holds order 8’s revenue. Select a page to see its statistics." },
  { title: "THE REFERENCE MAP", badge: "Inside orders.vpf", caption: "Snapshot → visible segment → row group → column chunk → page IDs." },
  { title: "THE VISIBLE CHANGE", badge: "Snapshot 2", caption: "63,352 → 12,345. All other cells keep their values." },
  { title: "THE INTERNAL CHANGE", badge: "Copy-on-write", caption: "One new data page plus metadata. The other 20 pages are reused, not copied." },
  { title: "THE TWO SNAPSHOTS", badge: "Choose a version", caption: "The row value and its backing page reference change together." },
  { title: "THE VIRTUAL FILE", badge: "Pinned to snapshot 2", caption: "The offsets look like one file. The bytes come from different sources." },
  { title: "THE READ", badge: "id = 8", caption: "Candidate pages first. Exact row filtering second. One answer." },
];
let current = -1;
let inspectedPage = "P18";
let historySnapshot = 2;
let comparisonAfter = true;
let benchmarkWorkload = "update";

const milliseconds = value => value.toLocaleString("en-US", { minimumFractionDigits: 3, maximumFractionDigits: 3 });

function timingChart(labels, values, colors, label) {
  const max = chartMaximum(values);
  return `<div class="timing-chart" role="img" aria-label="${escape(label)}. ${labels.map((name, i) => `${escape(name)}: ${milliseconds(values[i])} milliseconds`).join(". ")}. Lower is better.">
    <div class="chart-axis" aria-hidden="true"><span>0</span><span>${number(max / 2)}</span><span>${number(max)} ms</span></div>
    ${labels.map((name, i) => `<div class="timing-row"><span class="timing-label">${escape(name)}</span><div class="timing-track"><span class="timing-bar ${colors[i]}" style="width:${values[i] / max * 100}%"></span></div><strong class="timing-value">${milliseconds(values[i])}<small>ms</small></strong></div>`).join("")}
    <p class="chart-scale">Linear scale from zero · scale adjusts to the selected workload</p>
  </div>`;
}

function renderBenchmarks(announce = false) {
  const mode = $("#benchmark-mode").value;
  const rows = Number($("#benchmark-size").value);
  const entry = benchmarkData[mode].find(item => item.rows === rows);
  const workload = benchmarkWorkloads[benchmarkWorkload];
  const values = entry[benchmarkWorkload];
  const labels = ["SQLite · unindexed", "Parquet · DuckDB", `VPF · ${mode} · DuckDB`];
  const writes = benchmarkWorkload !== "read";
  const title = benchmarkWorkload === "append" ? `Append ${number(rows / 100)} rows` : workload.title;
  const faster = values[2] <= values[1];
  const ratio = faster ? values[1] / values[2] : values[2] / values[1];
  $("#benchmark-chart").innerHTML = `<div class="benchmark-heading"><div><h3>${title}</h3><p class="benchmark-description">${workload.description}</p></div><span class="benchmark-pill">${writes ? "Median durable time" : "Sum of query medians"} · ms</span></div>
    ${timingChart(labels, values, ["sqlite-bar", "parquet-bar", "vpf-bar"], `${title}, ${number(rows)} rows, ${mode} run`)}
    <div class="benchmark-insight"><strong>${ratio.toFixed(1)}× ${faster ? "faster" : "slower"} than Parquet</strong><span>VPF in this recorded ${writes ? "write workload" : "read score"}. ${benchmarkWorkload === "read" ? "Native Parquet remains faster overall here." : "Not a general performance guarantee."}</span></div>`;
  const orLabels = [`VPF ${mode} · before`, `VPF ${mode} · after`, "Parquet · after run"];
  const orFaster = entry.or[1] <= entry.or[0];
  const orRatio = orFaster ? entry.or[0] / entry.or[1] : entry.or[1] / entry.or[0];
  $("#or-selection").textContent = `${number(rows)} rows · ${mode}`;
  $("#or-query").textContent = `SELECT COUNT(*), SUM(revenue_cents) FROM orders\nWHERE id = ${rows / 2} OR id = ${rows / 2 + 2};`;
  $("#or-chart").innerHTML = timingChart(orLabels, entry.or, ["before-bar", "vpf-bar", "parquet-bar"], `OR pruning, ${number(rows)} rows, ${mode}`) +
    `<div class="benchmark-insight"><strong>${orRatio.toFixed(1)}× ${orFaster ? "faster" : "slower"} after pruning</strong><span>Before/after VPF medians for this query only. Not a SQLite comparison.</span></div>`;
  $("#benchmark-values").innerHTML = `<table><caption>${number(rows)} rows · ${mode} baseline · milliseconds</caption><thead><tr><th scope="col">Workload</th>${labels.map(name => `<th scope="col">${name}</th>`).join("")}</tr></thead><tbody>${Object.entries(benchmarkWorkloads).map(([key, item]) => `<tr><th scope="row">${key === "append" ? `Append ${number(rows / 100)} rows` : item.title}</th>${entry[key].map(value => `<td>${value.toFixed(6)}</td>`).join("")}</tr>`).join("")}</tbody></table>
    <table><caption>Separate OR follow-up · milliseconds</caption><thead><tr>${orLabels.map(name => `<th scope="col">${name}</th>`).join("")}</tr></thead><tbody><tr>${entry.or.map(value => `<td>${value.toFixed(6)}</td>`).join("")}</tr></tbody></table>`;
  $("#baseline-data-link").href = `https://github.com/nnanto/vpf/blob/main/benches/results/read-write-${mode}.json`;
  document.querySelectorAll("[data-workload]").forEach(button => button.setAttribute("aria-pressed", String(button.dataset.workload === benchmarkWorkload)));
  if (announce) $("#benchmark-announcement").textContent = `${title}. ${number(rows)} rows, ${mode} mode. ${labels.map((name, i) => `${name}: ${milliseconds(values[i])} milliseconds`).join(". ")}.`;
}

function table(snapshot, { compact = false, change = false } = {}) {
  const rows = compact ? snapshot.rows.filter(row => row.id >= 7 && row.id <= 9) : snapshot.rows;
  return `<div class="table-label"><span>${compact ? "orders / rows 7–9" : '<span class="full-row-label">orders / 12 rows</span><span class="short-row-label">orders / rows 6–11 of 12</span>'}</span><span>${snapshot === before ? "snapshot 1" : "snapshot 2"}</span></div>
    <div class="table-frame"><table class="data-table ${compact ? "" : "full-table"}"><caption class="sr-only">Orders in snapshot ${snapshot.id}; three of seven columns shown</caption>
      <thead><tr><th scope="col">id</th><th scope="col">region</th><th scope="col">revenue_cents</th></tr></thead>
      <tbody>${rows.map(row => `<tr class="${row.id === 8 ? "target-row" : ""}"><td>${row.id}</td><td>${escape(row.region)}</td><td class="${snapshot === after && row.id === 8 ? "updated-cell" : ""}">${change && row.id === 8 ? '<span class="old-inline">63,352</span>' : ""}${number(row.revenue_cents)}</td></tr>`).join("")}</tbody>
    </table></div>`;
}

function pageTile(page, { interactive = false, old = false, fresh = false } = {}) {
  const stats = pageSummary(page);
  const tag = interactive ? "button" : "div";
  const selected = page.id === inspectedPage;
  return `<${tag} class="page-tile ${old ? "historical" : ""} ${fresh ? "new-page" : ""} ${interactive && selected ? "selected" : ""}"
    ${interactive ? `type="button" data-page="${page.id}" aria-pressed="${selected}" aria-label="Inspect page ${page.id}"` : ""}>
    <span class="page-title">${page.id}<span>${fresh ? "new" : old ? "kept" : ""}</span></span>
    <span class="page-values">${page.values.map((value, index) => `<span class="${page.rowIds[index] === 8 ? "value-target" : ""}">${number(value)}</span>`).join("")}</span>
    <span class="page-meta">orders ${page.rowIds[0]}–${page.rowIds.at(-1)}<br>${stats.rows} values${old ? " · unchanged" : fresh ? " · rewritten" : ""}</span>
  </${tag}>`;
}

function columnsScene() {
  const columns = before.columns.filter(column => [1, 2, 6].includes(column.id));
  return `<div class="group-bracket"><div class="group-title"><span>ROW GROUP 1</span><span class="full-row-label">12 rows</span><span class="short-row-label">rows 6–11 of 12</span></div>
    <div class="column-grid">${columns.map(column => `<div class="column-tower ${column.id === 6 ? "focus-column" : ""}"><strong>${column.name}</strong>${before.rows.map(row => `<span class="${row.id === 8 ? "focus-value" : ""}">${number(row[column.key])}</span>`).join("")}</div>`).join("")}</div>
    </div><p class="diagram-footnote">Each vertical stack is one column chunk. Four other chunks are not shown.</p>`;
}

function pagesScene() {
  const page = originalRevenue.find(item => item.id === inspectedPage);
  const stats = pageSummary(page);
  return `<div class="chunk-label">ROW GROUP 1 / revenue_cents CHUNK</div>
    <div class="page-grid">${originalRevenue.map(item => pageTile(item, { interactive: true })).join("")}</div>
    <div class="page-inspect"><strong>${page.id}</strong> · min ${number(stats.min)} · max ${number(stats.max)} · ${stats.nulls} nulls<br>
    ${page.rowIds.includes(8) ? "The first value belongs to order 8. This is the page the update will replace." : `Orders ${page.rowIds.join(", ")} are on this page. Their revenue values will not change.`}</div>
    <p class="diagram-footnote">Parquet page = header + encoded body. The values above are the decoded body; min/max statistics are stored by VPF as metadata.</p>`;
}

function referencesScene() {
  return `<div class="storage-container"><div class="container-title"><span>orders.vpf</span><span>SQLite container</span></div>
    <span class="micro-label">METADATA / THE MAP</span>
    <div class="reference-line"><span class="node">Snapshot 1<small>schema + visibility</small></span><span class="connector">→</span><span class="node">Segment<small>row group RG1</small></span></div>
    <div class="reference-sub">RG1 → 7 column chunks<br>revenue chunk → P16 · P17 · P18</div>
    <span class="micro-label">BLOBS / IMMUTABLE PARQUET PAGE BYTES</span>
    <div class="blob-grid">${visiblePages(before).map(page => `<span class="blob ${page.column === 6 ? "revenue" : ""}">${page.id}</span>`).join("")}</div>
    </div><p class="diagram-footnote">21 data pages in this example. These are Parquet pages inside SQLite blobs—not 21 SQLite database pages.</p>`;
}

function rewriteScene() {
  return `<div class="write-grid">${pageTile(originalRevenue.at(-1), { old: true })}<span aria-hidden="true">→</span>${pageTile(newRevenue, { fresh: true })}</div>
    <div class="reuse-banner"><strong>20 other pages</strong> keep their original IDs and bytes.</div>
    <div class="snapshot-routing"><div class="snapshot-route"><span>Snapshot 1 → RG1 → old chunk</span><strong>P18</strong></div><div class="snapshot-route active"><span>Snapshot 2 → RG2 → new chunk</span><strong>P22</strong></div></div>
    <p class="diagram-footnote">New row-group/chunk metadata reuses the unchanged pages. New page bytes and metadata commit atomically.</p>`;
}

function historyScene() {
  const snapshot = historySnapshot === 1 ? before : after;
  return `<div class="segmented history-controls" role="group" aria-label="Choose a snapshot">
      <button type="button" data-snapshot="1" aria-pressed="${historySnapshot === 1}">Snapshot 1 · before</button>
      <button type="button" data-snapshot="2" aria-pressed="${historySnapshot === 2}">Snapshot 2 · after</button></div>
    ${table(snapshot, { compact: true })}
    <div class="history-result"><span>Order 8’s revenue</span><strong>${number(snapshot.rows.find(row => row.id === 8).revenue_cents)}</strong></div>
    <div class="history-chain"><span>S${snapshot.id}</span>→<span>RG${snapshot.id}</span>→<span>revenue chunk</span>→<span>${snapshot.id === 1 ? "P18" : "P22"}</span></div>
    <p class="diagram-footnote">Different reference, different value. The other 20 data pages are shared by both snapshots.</p>`;
}

function virtualScene() {
  const extents = virtualExtents(after);
  const revenue = extents.filter(extent => extent.column === 6);
  const target = revenue.at(-1);
  return `<div class="virtual-file"><span class="micro-label">WHAT A PARQUET READER SEES / ONE BYTE STREAM</span>
    <div class="extent-strip"><div class="extent-block memory">PAR1<small>magic</small></div><div class="extent-block omitted">15 pages<small>collapsed</small></div>${revenue.map(extent => `<div class="extent-block">${extent.id}<small>${extent.offset}–${extent.offset + extent.length - 1}</small></div>`).join("")}<div class="extent-block omitted">3 pages<small>collapsed</small></div><div class="extent-block memory">Footer<small>+ trailer</small></div></div></div>
    <div class="mapping-lines"><div><strong>In memory</strong>Magic + generated indexes, metadata, length, and PAR1 trailer.</div><div><strong>In SQLite blobs</strong>The selected snapshot’s original stored Parquet page bytes.</div></div>
    <div class="byte-range">Example: bytes ${target.offset}–${target.offset + target.length - 1}<br>→ virtual extent P22 → SQLite blob bytes</div>
    <p class="diagram-footnote">Illustrative 128-byte page extents, not measured file sizes. Other columns before and after revenue are collapsed; page order is preserved.</p>`;
}

function readScene() {
  const plan = planPointRead(after, 8, 6);
  return `<span class="micro-label">ID PAGE STATISTICS / WHERE id = 8</span>
    <div class="prune-grid">${plan.candidates.map(candidate => `<div class="prune-page ${candidate.read ? "read" : "skipped"}"><small>${candidate.pageId} · id column</small><strong>${candidate.min}–${candidate.max}</strong><small>${candidate.read ? "CANDIDATE" : "SKIP"}</small></div>`).join("")}</div>
    <div class="read-path"><strong>${plan.pages.map(page => `${page.id} (${page.column === 1 ? "id" : "revenue"})`).join(" + ")}</strong>
    Decode candidate values → test id = 8<br>
    Discard orders 9, 10, 11 → keep order 8</div>
    <div class="answer"><span>revenue_cents<br>snapshot 2 / order 8</span><strong>${number(plan.rows[0].revenue_cents)}</strong></div>
    <p class="diagram-footnote">The two data-page candidates are shown; footer/index reads and metadata setup are additional work. Other projected columns would need their corresponding pages too.</p>`;
}

function renderScene(index, animate = true) {
  const scene = scenes[index];
  $("#scene-label").textContent = `${String(index + 1).padStart(2, "0")} / ${scene.title}`;
  $("#scene-badge").textContent = scene.badge;
  $("#scene-caption").textContent = scene.caption;
  $("#step-count").textContent = `${index + 1} / ${scenes.length}`;
  $("#previous").disabled = index === 0;
  $("#next").disabled = index === scenes.length - 1;
  $("#stage").dataset.scene = steps[index].dataset.scene;
  const renders = [
    () => table(before),
    columnsScene,
    pagesScene,
    referencesScene,
    () => `<div class="cell-change"><span>ORDER 8</span><del>63,352</del><span>→</span><strong>12,345</strong></div>${table(after, { change: true })}`,
    rewriteScene,
    historyScene,
    virtualScene,
    readScene,
  ];
  $("#scene").innerHTML = renders[index]();
  if (animate) {
    $("#scene").classList.remove("entering");
    // Restart only on a chapter change, not on every scroll frame.
    void $("#scene").offsetWidth;
    $("#scene").classList.add("entering");
  }
}

function compareCard({ name, type, sketch, title, copy, history, vpf = false }) {
  return `<article class="compare-card ${vpf ? "vpf-card" : ""}"><h3>${name}</h3><p class="storage-type">${type}</p>
    <div class="comparison-result"><span>Order 8 / revenue</span><strong>${comparisonAfter ? "12,345" : "63,352"}</strong></div>
    <div class="storage-sketch">${sketch}</div><h4>${title}</h4><p>${copy}</p><p class="history-note">${history}</p></article>`;
}

function renderComparison() {
  const a = comparisonAfter;
  $("#comparison-cards").innerHTML = [
    compareCard({
      name: "SQLite", type: "A conventional row table · WAL mode",
      sketch: `<div class="sqlite-tree"><div class="sqlite-root">table B-tree</div><div class="sqlite-branches" aria-hidden="true">↙ &nbsp; ↓ &nbsp; ↘</div><div class="sqlite-leaves"><div class="sqlite-leaf">row records</div><div class="sqlite-leaf ${a ? "changed" : ""}">order 8<br>${a ? "12,345" : "63,352"}</div><div class="sqlite-leaf">row records</div></div></div><div class="wal ${a ? "" : "inactive"}">${a ? "changed database-page image(s) → WAL" : "WAL / no update yet"}</div>`,
      title: a ? "Update the row. Log changed pages." : "Whole records live together.",
      copy: a ? "SQLite changes the row record in its table B-tree. In WAL mode, changed database-page images are appended to the WAL; a later checkpoint updates the main file. It does not rewrite the whole table." : "A leaf database page holds row records, including values from different columns. A B-tree organizes those pages. Indexes, if present, add their own structures.",
      history: "<strong>History:</strong> WAL supports transaction snapshots for readers, not VPF-style durable named versions after checkpointing. Retaining those needs an additional mechanism.",
    }),
    compareCard({
      name: "Parquet", type: "One ordinary file · replacement workflow",
      sketch: `<div class="file-label"><span>${a ? "replacement" : "orders"}.parquet</span><span>PAR1</span></div><div class="file-pages ${a ? "replacement" : ""}">${Array.from({ length: 21 }, (_, index) => `<span class="${index === 17 && a ? "changed" : ""}"></span>`).join("")}</div><div class="file-arrow">${a ? "changed value + fresh footer → replace file" : "21 column data pages + one footer"}</div>`,
      title: a ? "Write a replacement file." : "Column pages make up a file.",
      copy: a ? "Parquet has no native row-level UPDATE transaction. In this single-file workflow, produce a new valid file and replace the old one. Unchanged page bytes may be copied; they do not all have to be decoded and re-encoded." : "The file holds column chunks, pages, and a footer describing their locations. It is an excellent read format, not a row-update transaction manager.",
      history: "<strong>History:</strong> replacing the file loses access to the prior version unless you retain it separately or use an external versioned table layer.",
    }),
    compareCard({
      name: "VPF", type: "Parquet pages + snapshot metadata in SQLite", vpf: true,
      sketch: `<div class="vpf-mini-history"><div><span>Snapshot 1</span><span>→</span><span>P18 · 63,352</span></div>${a ? '<div><span>Snapshot 2</span><span>→</span><span>P22 · 12,345</span></div>' : ""}</div><div class="shared-pages">${a ? "20 data pages shared by both snapshots" : "21 immutable Parquet data pages"}</div>`,
      title: a ? "Add one data page. Add a new map." : "The file holds pieces and references.",
      copy: a ? "Write P22 and new metadata that points to it. Reuse 20 data pages by reference. Commit bytes and metadata together. SQLite’s underlying database/WAL work is additional; “one page” is the logical Parquet-page change." : "A snapshot points through visible segments, row groups, and column chunks to stored Parquet pages. The SQLite container commits these references and bytes atomically.",
      history: "<strong>History:</strong> P18 and snapshot 1 stay reachable. Snapshot 2 shares the unchanged pages. This deliberate history retention also consumes storage.",
    }),
  ].join("");
  document.querySelectorAll("[data-compare]").forEach(button => button.setAttribute("aria-pressed", String((button.dataset.compare === "after") === a)));
}

function goToStep(index) {
  const clamped = Math.max(0, Math.min(steps.length - 1, index));
  // Align a chapter below the pinned visual on small screens; never hijack scrolling.
  const mobile = matchMedia("(max-width: 760px)").matches;
  const target = steps[clamped].getBoundingClientRect().top + scrollY;
  const offset = mobile ? $(".visual-pin").getBoundingClientRect().height + 70 : innerHeight * .24;
  scrollTo({ top: target - offset, behavior: matchMedia("(prefers-reduced-motion: reduce)").matches ? "instant" : "smooth" });
}

$("#previous").addEventListener("click", () => goToStep(current - 1));
$("#next").addEventListener("click", () => goToStep(current + 1));
$("#scene").addEventListener("click", event => {
  const pageButton = event.target.closest("[data-page]");
  const snapshotButton = event.target.closest("[data-snapshot]");
  if (!pageButton && !snapshotButton) return;
  const attribute = pageButton ? "data-page" : "data-snapshot";
  const value = (pageButton ?? snapshotButton).getAttribute(attribute);
  if (pageButton) inspectedPage = value;
  else historySnapshot = Number(value);
  renderScene(current, false);
  $(`#scene [${attribute}="${value}"]`).focus({ preventScroll: true });
});
document.querySelectorAll("[data-compare]").forEach(button => button.addEventListener("click", () => {
  comparisonAfter = button.dataset.compare === "after";
  renderComparison();
}));
$("#benchmark-size").addEventListener("change", () => renderBenchmarks(true));
$("#benchmark-mode").addEventListener("change", () => renderBenchmarks(true));
document.querySelectorAll("[data-workload]").forEach(button => button.addEventListener("click", () => {
  benchmarkWorkload = button.dataset.workload;
  renderBenchmarks(true);
}));

let scheduled = false;
function updateScroll() {
  scheduled = false;
  const mobile = matchMedia("(max-width: 760px)").matches;
  const threshold = mobile ? $(".visual-pin").getBoundingClientRect().bottom + 85 : innerHeight * .5;
  let index = 0;
  for (let i = 0; i < steps.length; i++) {
    if (steps[i].getBoundingClientRect().top <= threshold) index = i;
  }
  if (index !== current) {
    current = index;
    renderScene(index);
  }
  const length = document.documentElement.scrollHeight - innerHeight;
  document.documentElement.style.setProperty("--scroll", length > 0 ? Math.max(0, Math.min(1, scrollY / length)) : 0);
}
function scheduleScroll() {
  if (!scheduled) {
    scheduled = true;
    requestAnimationFrame(updateScroll);
  }
}
addEventListener("scroll", scheduleScroll, { passive: true });
addEventListener("resize", scheduleScroll);
renderComparison();
renderBenchmarks();
updateScroll();

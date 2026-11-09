import { createLab, operations, visiblePages, virtualExtents } from "./model.mjs";

const $ = selector => document.querySelector(selector);
const escape = value => String(value).replace(/[&<>"']/g, c => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" })[c]);
const display = value => value == null ? "NULL" : String(value);
let lab = createLab();
let selectedSnapshot = 1;
let selectedPage = "P18";
let readSnapshot = 1;
let readColumn = 6;
let readStep = -1;

function pageButton(page, snapshot, ledger = false) {
  const visible = visiblePages(snapshot).some(p => p.id === page.id);
  const previous = lab.snapshots[snapshot.id - 2];
  const fresh = visible && (!previous || !visiblePages(previous).some(p => p.id === page.id));
  return `<button class="page ${!visible ? "old" : fresh ? "fresh" : ""} ${page.id === selectedPage ? "selected" : ""}" data-page="${page.id}" aria-pressed="${page.id === selectedPage}" aria-label="Inspect ${page.id}, ${visible ? fresh ? "new" : "reused" : "historical only"}">${page.id}${ledger ? "" : `<small>${page.values.length} rows · ${fresh ? "new" : "reused"}</small>`}</button>`;
}

function renderStorage() {
  const snapshot = lab.snapshots[selectedSnapshot - 1];
  $("#operations").innerHTML = operations.map((op, index) => `<button class="operation ${index < lab.snapshots.length - 1 ? "done" : ""}" data-operation="${op.key}" ${index !== lab.snapshots.length - 1 ? "disabled" : ""}><span>${index < lab.snapshots.length - 1 ? "✓" : `0${index + 1}`}</span>${op.label}</button>`).join("");
  $("#snapshot").innerHTML = lab.snapshots.map(s => `<option value="${s.id}" ${s.id === selectedSnapshot ? "selected" : ""}>Snapshot ${s.id} · ${s.operation}</option>`).join("");
  $("#snapshot-note").textContent = selectedSnapshot === lab.snapshots.length ? "Viewing the latest snapshot. Writes always apply to the latest version." : "Historical view, frozen in time. New writes still apply to the latest version.";
  $("#snapshot-badge").textContent = `S${snapshot.id} · ${snapshot.rows.length} rows`;
  $("#storage-diagram").innerHTML = snapshot.groups.map(g => `<div class="segment"><div class="segment-card"><strong>${g.id}</strong><small>visible segment<br>${g.chunks[0].pages.reduce((n, p) => n + p.values.length, 0)} rows</small></div><div class="chunks">${g.chunks.map(c => {
    const column = snapshot.columns.find(col => col.id === c.column);
    return `<div class="chunk"><div class="column-name">${column.name}<small>column id ${column.id} · ${column.type}</small></div><div class="page-list">${c.pages.map(p => pageButton(p, snapshot)).join("")}</div></div>`;
  }).join("")}</div></div>`).join("") + (snapshot.batch ? `<div class="segment"><div class="segment-card batch-card"><strong>B1</strong><small>buffered segment<br>2 rows</small></div><div class="batch-payload">Compact columnar payload<small>On read → cached, synthesized Parquet pages in memory</small></div></div>` : "");
  $("#blob-ledger").innerHTML = lab.pages.map(p => pageButton(p, snapshot, true)).join("");
  $("#blob-count").textContent = `${lab.pages.length} stored · ${visiblePages(snapshot).length} visible`;
  const op = operations.find(o => o.key === snapshot.operation);
  $("#event-title").textContent = op?.title ?? "Twenty-one pages. One starting snapshot.";
  $("#event-description").textContent = op?.description ?? "Import stores compressed page bytes (header + body) and computes page statistics. This seven-column orders sample has three illustrative four-row pages per column in one row group. Metadata links the snapshot to those chunks.";
  $("#metrics").innerHTML = [[snapshot.written, "pages written"], [snapshot.reused, "pages reused"], [lab.snapshots.length, "snapshots kept"]].map(([number, label]) => `<div class="metric"><strong>${number}</strong><span>${label}</span></div>`).join("");
  $("#command").textContent = `vpf orders.vpf ${op?.command ?? "import orders.parquet --name orders"}`;
  $("#row-count").textContent = `${snapshot.rows.length} rows`;
  $("#rows").innerHTML = `<thead><tr>${snapshot.columns.map(c => `<th>${c.name}</th>`).join("")}</tr></thead><tbody>${snapshot.rows.map(row => `<tr>${snapshot.columns.map(c => `<td class="${c.id === 6 && row.id === 8 && row.revenue_cents === 12345 ? "changed" : ""}">${escape(display(row[c.key]))}</td>`).join("")}</tr>`).join("")}</tbody>`;
  renderInspector();
}

function renderInspector() {
  const page = lab.pages.find(p => p.id === selectedPage);
  const snapshot = lab.snapshots[selectedSnapshot - 1];
  const column = snapshot.columns.find(c => c.id === page.column);
  const refs = lab.snapshots.filter(s => visiblePages(s).some(p => p.id === page.id)).map(s => `S${s.id}`);
  const nonNull = page.values.filter(v => v != null);
  const sorted = [...nonNull].sort((a, b) => typeof a === "number" ? a - b : a < b ? -1 : a > b ? 1 : 0);
  $("#inspector").innerHTML = `<h3>${page.id} <span class="pill">${column.name} · stable id ${page.column}</span></h3><div class="inspector-values">${page.values.map(v => escape(display(v))).join(" · ")}</div><dl><dt>Row count</dt><dd>${page.values.length}</dd><dt>Min / max</dt><dd>${sorted.length ? `${escape(sorted[0])} / ${escape(sorted.at(-1))}` : "—"}</dd><dt>Null count</dt><dd>${page.values.length - nonNull.length}</dd><dt>Storage</dt><dd>SQLite blobs</dd></dl><p>Referenced by ${refs.join(", ")}.<br>${visiblePages(snapshot).some(p => p.id === page.id) ? "Visible in this snapshot." : "Not visible here; preserved for time travel."}</p>`;
}

function renderReader() {
  const snapshot = lab.snapshots[readSnapshot - 1];
  const extents = virtualExtents(snapshot);
  const size = extents.at(-1).offset + extents.at(-1).length;
  const projected = extents.filter(e => e.column === readColumn);
  const active = readStep === 0 ? extents.filter(e => e.id === "trailer") : readStep === 1 ? extents.filter(e => e.id === "footer") : readStep >= 2 ? projected : [];
  $("#read-snapshot").innerHTML = lab.snapshots.map(s => `<option value="${s.id}" ${s.id === readSnapshot ? "selected" : ""}>Snapshot ${s.id} · ${s.operation}</option>`).join("");
  $("#read-column").innerHTML = snapshot.columns.map(c => `<option value="${c.id}" ${c.id === readColumn ? "selected" : ""}>${c.name} · stable id ${c.id}</option>`).join("");
  $("#byte-strip").innerHTML = extents.map(e => `<div class="extent ${e.source === "memory" ? "memory" : ""} ${active.includes(e) ? "active" : ""}">${escape(e.label)}<small>${e.offset}–${e.offset + e.length - 1}</small><small>${e.source === "blobs" ? "SQLite blob" : "memory"}</small></div>`).join("");
  const steps = ["Read last 8 bytes", "Read generated footer", "Fetch projected pages", "Decode column values"];
  $("#read-steps").innerHTML = steps.map((step, i) => `<div class="read-step ${readStep === i ? "active" : ""}">0${i + 1} / ${step}</div>`).join("");
  const column = snapshot.columns.find(c => c.id === readColumn);
  const titles = ["Seek to the trailer.", "Discover the frozen file layout.", `Request only ${column.name}.`, "Ordinary Parquet decoding."];
  const descriptions = [
    `A common reader first fetches bytes ${size - 8}–${size - 1}: the metadata length and PAR1 magic. These bytes are generated in memory, not read from page blobs.`,
    "The fresh footer describes the snapshot’s schema, row groups, relocated column offsets, and page indexes. The offset map creates a contiguous file from physically separate sources.",
    `This example projects ${column.name} without a predicate. Only its ${projected.length} page extents are requested; other columns stay unread. With dictionary encoding, a reader also needs the relevant dictionary page.`,
    `The reader decodes the selected column: ${snapshot.rows.map(r => display(r[column.key])).join(", ")}. Stored pages are read from SQLite; buffered pages are synthesized in memory. Nothing was exported to disk.`,
  ];
  $("#read-detail").innerHTML = `<h3>${readStep < 0 ? "A frozen view, ready for a reader." : titles[readStep]}</h3><p>${readStep < 0 ? `Start the read to follow byte-range requests for snapshot ${snapshot.id}. All sizes in this diagram are illustrative; the browser does not encode or serve real Parquet.` : escape(descriptions[readStep])}</p>`;
  $("#range-map").innerHTML = active.map(e => `<div class="range-line"><span>bytes=${e.offset}–${e.offset + e.length - 1} → ${escape(e.label)}</span><span>${e.source === "blobs" ? `VirtualFile extent → blobs row for ${e.id}` : "VirtualFile extent → in-memory bytes"}</span></div>`).join("") + (readStep >= 0 ? `<div class="range-line"><span>GET /files/orders/snapshots/${snapshot.id}.parquet</span><span>Pinned URL · same version on every request</span></div>` : "");
  $("#read-next").textContent = readStep < 0 ? "Start read →" : readStep === 3 ? "Read complete" : "Next request →";
  $("#read-next").disabled = readStep === 3;
}

$("#operations").addEventListener("click", event => {
  const button = event.target.closest("[data-operation]");
  if (!button || button.disabled) return;
  const snapshot = lab.apply(button.dataset.operation);
  selectedSnapshot = snapshot.id;
  readSnapshot = snapshot.id;
  readStep = -1;
  const previous = lab.snapshots.at(-2);
  selectedPage = visiblePages(snapshot).find(p => !visiblePages(previous).some(old => old.id === p.id))?.id ?? selectedPage;
  renderStorage();
  renderReader();
});
$("#reset").addEventListener("click", () => {
  lab = createLab();
  selectedSnapshot = readSnapshot = 1;
  selectedPage = "P18";
  readColumn = 6;
  readStep = -1;
  renderStorage();
  renderReader();
});
$("#snapshot").addEventListener("change", event => { selectedSnapshot = Number(event.target.value); renderStorage(); });
$("#storage").addEventListener("click", event => {
  const button = event.target.closest("[data-page]");
  if (button) { selectedPage = button.dataset.page; renderStorage(); }
});
document.querySelectorAll("[data-tab]").forEach(button => button.addEventListener("click", () => {
  document.querySelectorAll("[data-tab]").forEach(tab => { tab.classList.toggle("active", tab === button); tab.setAttribute("aria-pressed", String(tab === button)); });
  document.querySelectorAll(".view").forEach(view => { view.hidden = view.id !== button.dataset.tab; });
}));
$("#read-snapshot").addEventListener("change", event => { readSnapshot = Number(event.target.value); readStep = -1; renderReader(); });
$("#read-column").addEventListener("change", event => { readColumn = Number(event.target.value); readStep = -1; renderReader(); });
$("#read-next").addEventListener("click", () => { readStep = Math.min(3, readStep + 1); renderReader(); });
$("#read-reset").addEventListener("click", () => { readStep = -1; renderReader(); });
renderStorage();
renderReader();

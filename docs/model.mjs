// A deliberately small, deterministic model of page references, not a Parquet encoder.
export const operations = [
  { key: "update", label: "Update one value", command: 'update --set "amount = 99" --where "id = 8"', title: "One new page. Five reused.", description: "Only the amount page containing id 8 is rewritten. The id and region chunks, and the first amount page, keep their page IDs. The original amount page remains available to snapshot 1." },
  { key: "delete", label: "Delete three rows", command: 'delete --where "id <= 3"', title: "A delete changes row alignment.", description: "Rows 1–3 disappear from the first page of every column. Those three pages are rewritten; the second pages are reused. Earlier snapshots still include all twelve rows." },
  { key: "append", label: "Append a buffered batch", command: 'append --json new-rows.json --mode buffered', title: "Visible now. Parquet pages later.", description: "Two rows are stored as one compact columnar batch, not immutable Parquet page blobs. Reading synthesizes a row group in memory and caches it. No stored page is added yet." },
  { key: "flush", label: "Flush the batch", command: "flush", title: "Buffered rows become stored pages.", description: "The buffered segment is replaced by a stored row group with one new page per column. Older snapshots still reference the original batch; the existing row group is unchanged." },
  { key: "rename", label: "Rename a column", command: "alter rename-column amount total", title: "A new name. Zero new pages.", description: "The schema changes, but the column’s stable ID stays 2. Every chunk and page is reused. Time travel restores the earlier name, amount." },
];

export function createLab() {
  const pages = [];
  const snapshots = [];
  function page(column, values) {
    const result = { id: `P${String(pages.length + 1).padStart(2, "0")}`, column, values: [...values] };
    pages.push(result);
    return result;
  }
  const rows = Array.from({ length: 12 }, (_, i) => ({ id: i + 1, amount: (i + 1) * 10, region: i < 6 ? "EU" : "US" }));
  const columns = [
    { id: 1, key: "id", name: "id", type: "INT64" },
    { id: 2, key: "amount", name: "amount", type: "INT64" },
    { id: 3, key: "region", name: "region", type: "STRING" },
  ];
  const chunks = columns.map(c => ({ column: c.id, pages: [page(c.id, rows.slice(0, 6).map(r => r[c.key])), page(c.id, rows.slice(6).map(r => r[c.key]))] }));
  snapshots.push({ id: 1, operation: "import", columns, groups: [{ id: "RG1", chunks }], batch: null, written: 6, reused: 0, rows });

  function apply(key) {
    const expected = operations[snapshots.length - 1];
    if (!expected || expected.key !== key) throw new Error("Run the guided operations in order.");
    const prev = snapshots.at(-1);
    const next = { ...prev, id: prev.id + 1, operation: key, written: 0, reused: prev.groups.flatMap(g => g.chunks.flatMap(c => c.pages)).length };
    if (key === "update" || key === "delete") {
      next.rows = key === "update" ? prev.rows.map(r => r.id === 8 ? { ...r, amount: 99 } : r) : prev.rows.filter(r => r.id > 3);
      next.groups = prev.groups.map(g => ({
        id: `RG${next.id}`,
        chunks: g.chunks.map(c => ({
          column: c.column,
          pages: c.pages.map((p, index) => {
            if ((key === "update" && c.column === 2 && index === 1) || (key === "delete" && index === 0)) {
              next.written++;
              next.reused--;
              return page(c.column, key === "update" ? p.values.map((v, i) => i === 1 ? 99 : v) : p.values.slice(3));
            }
            return p;
          }),
        })),
      }));
    } else if (key === "append") {
      next.batch = { id: "B1", rows: [{ id: 13, amount: 130, region: "APAC" }, { id: 14, amount: 140, region: "APAC" }] };
      next.rows = [...prev.rows, ...next.batch.rows];
    } else if (key === "flush") {
      const group = { id: "RG4", chunks: prev.columns.map(c => ({ column: c.id, pages: [page(c.id, prev.batch.rows.map(r => r[c.key]))] })) };
      next.groups = [...prev.groups, group];
      next.batch = null;
      next.written = 3;
    } else if (key === "rename") {
      next.columns = prev.columns.map(c => c.id === 2 ? { ...c, name: "total" } : c);
    }
    snapshots.push(next);
    return next;
  }
  return { pages, snapshots, apply };
}

export function visiblePages(snapshot) {
  return snapshot.groups.flatMap(g => g.chunks.flatMap(c => c.pages));
}

export function virtualExtents(snapshot) {
  let offset = 4;
  const extents = [{ id: "magic", label: "PAR1", offset: 0, length: 4, source: "memory" }];
  for (const group of snapshot.groups) {
    for (const chunk of group.chunks) {
      for (const p of chunk.pages) {
        extents.push({ id: p.id, label: p.id, column: chunk.column, offset, length: 128, source: "blobs", page: p });
        offset += 128;
      }
    }
  }
  if (snapshot.batch) {
    for (const c of snapshot.columns) {
      extents.push({ id: `mem-${c.id}`, label: `B1:${c.name}`, column: c.id, offset, length: 128, source: "memory", values: snapshot.batch.rows.map(r => r[c.key]) });
      offset += 128;
    }
  }
  extents.push({ id: "footer", label: "Indexes + footer", offset, length: 192, source: "memory" });
  extents.push({ id: "trailer", label: "len · PAR1", offset: offset + 192, length: 8, source: "memory" });
  return extents;
}

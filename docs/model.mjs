// The sample values use the same generator as benches/analytical.py; page IDs and
// four-row page boundaries are a small illustration, not an encoded Parquet fixture.
export const operations = [
  { key: "update", label: "Update one revenue", command: 'update --set "revenue_cents = 12345" --where "id = 8"', title: "One page rewritten. Twenty reused.", description: "Order 8 is in one revenue page. VPF writes that page with revenue_cents changed from 63,352 to 12,345; its other 20 pages keep their IDs. The old page remains available to snapshot 1." },
  { key: "delete", label: "Delete order 2", command: 'delete --where "id = 2"', title: "Seven column pages rewritten.", description: "Deleting order 2 touches its page in each of the seven columns. Those seven pages are filtered and rewritten; the other 14 pages are reused. Snapshot 2 still has all 12 original rows." },
  { key: "append", label: "Append two buffered orders", command: 'append --parquet more-orders.parquet --mode buffered', title: "Visible now. Parquet pages later.", description: "Orders 12 and 13 are stored as a compact columnar batch, not page blobs. They are immediately visible through a synthesized, cached Parquet row group. No new stored pages yet." },
  { key: "flush", label: "Flush the buffered batch", command: "flush", title: "Seven new column pages stored.", description: "The two buffered rows become one new stored page per column. Existing row-group pages are reused, and the previous snapshot still points to its original buffered batch." },
  { key: "rename", label: "Rename product_id", command: "alter rename-column product_id sku_id", title: "A new name. Zero new pages.", description: "Column ID 3 stays the same; only its schema metadata changes from product_id to sku_id. Every chunk and page is reused. Earlier snapshots retain the original name." },
];

const columnDefinitions = [
  { id: 1, key: "id", name: "id", type: "INT64" },
  { id: 2, key: "region", name: "region", type: "STRING" },
  { id: 3, key: "product_id", name: "product_id", type: "INT64" },
  { id: 4, key: "day", name: "day", type: "INT64" },
  { id: 5, key: "quantity", name: "quantity", type: "INT64" },
  { id: 6, key: "revenue_cents", name: "revenue_cents", type: "INT64" },
  { id: 7, key: "discount_cents", name: "discount_cents", type: "INT64?" },
];

export function orderFor(id) {
  return {
    id,
    region: `r${id % 32}`,
    product_id: (id * 17) % 4096,
    day: (id * 13) % 365,
    quantity: 1 + id % 10,
    revenue_cents: (id * 7919) % 100000,
    discount_cents: id % 7 === 0 ? null : id % 2000,
  };
}

export function createLab() {
  const pages = [];
  const snapshots = [];
  function page(column, rows) {
    const result = {
      id: `P${String(pages.length + 1).padStart(2, "0")}`,
      column,
      rowIds: rows.map(r => r.id),
      values: rows.map(r => r[columnDefinitions[column - 1].key]),
    };
    pages.push(result);
    return result;
  }
  const rows = Array.from({ length: 12 }, (_, id) => orderFor(id));
  const columns = columnDefinitions.map(c => ({ ...c }));
  const chunks = columns.map(c => ({
    column: c.id,
    pages: [rows.slice(0, 4), rows.slice(4, 8), rows.slice(8)].map(part => page(c.id, part)),
  }));
  snapshots.push({ id: 1, operation: "import", columns, groups: [{ id: "RG1", chunks }], batch: null, written: 21, reused: 0, rows });

  function apply(key) {
    const expected = operations[snapshots.length - 1];
    if (!expected || expected.key !== key) throw new Error("Run the guided operations in order.");
    const prev = snapshots.at(-1);
    const next = {
      ...prev,
      id: prev.id + 1,
      operation: key,
      written: 0,
      reused: prev.groups.flatMap(g => g.chunks.flatMap(c => c.pages)).length,
    };
    if (key === "update" || key === "delete") {
      const target = key === "update" ? 8 : 2;
      next.rows = key === "update"
        ? prev.rows.map(r => r.id === target ? { ...r, revenue_cents: 12345 } : r)
        : prev.rows.filter(r => r.id !== target);
      next.groups = prev.groups.map(g => ({
        id: `RG${next.id}`,
        chunks: g.chunks.map(c => ({
          column: c.column,
          pages: c.pages.map(p => {
            if (!p.rowIds.includes(target) || (key === "update" && c.column !== 6)) return p;
            next.written++;
            next.reused--;
            const keptRows = p.rowIds
              .map(id => next.rows.find(r => r.id === id))
              .filter(Boolean);
            return page(c.column, key === "update"
              ? keptRows.map(r => r.id === target ? { ...r, revenue_cents: 12345 } : r)
              : keptRows);
          }),
        })),
      }));
    } else if (key === "append") {
      next.batch = { id: "B1", rows: [orderFor(12), orderFor(13)] };
      next.rows = [...prev.rows, ...next.batch.rows];
    } else if (key === "flush") {
      const group = {
        id: `RG${next.id}`,
        chunks: prev.columns.map(c => ({ column: c.id, pages: [page(c.id, prev.batch.rows)] })),
      };
      next.groups = [...prev.groups, group];
      next.batch = null;
      next.written = prev.columns.length;
    } else if (key === "rename") {
      next.columns = prev.columns.map(c => c.id === 3 ? { ...c, name: "sku_id" } : c);
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

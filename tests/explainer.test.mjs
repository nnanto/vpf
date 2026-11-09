import test from "node:test";
import assert from "node:assert/strict";
import { createLab, operations, orderFor, visiblePages, virtualExtents } from "../docs/model.mjs";

test("sample orders match the benchmark fixture generator", () => {
  assert.deepEqual(orderFor(0), {
    id: 0, region: "r0", product_id: 0, day: 0, quantity: 1,
    revenue_cents: 0, discount_cents: null,
  });
  assert.deepEqual(orderFor(8), {
    id: 8, region: "r8", product_id: 136, day: 104, quantity: 9,
    revenue_cents: 63352, discount_cents: 8,
  });
  assert.deepEqual(orderFor(12), {
    id: 12, region: "r12", product_id: 204, day: 156, quantity: 3,
    revenue_cents: 95028, discount_cents: 12,
  });
  const first = orderFor(5_000_000);
  const second = orderFor(5_000_002);
  assert.equal(first.revenue_cents + second.revenue_cents, 15_838);
});

test("guided mutations preserve history and reuse exactly the unaffected pages", () => {
  const lab = createLab();
  const original = JSON.stringify(lab.snapshots[0]);
  assert.equal(visiblePages(lab.snapshots[0]).length, 21);
  const update = lab.apply("update");
  assert.equal(update.written, 1);
  assert.equal(update.reused, 20);
  assert.equal(update.rows.find(r => r.id === 8).revenue_cents, 12345);
  assert.deepEqual(visiblePages(update).map(p => p.id).filter(id => !visiblePages(lab.snapshots[0]).some(p => p.id === id)), ["P22"]);
  const deletion = lab.apply("delete");
  assert.equal(deletion.rows.length, 11);
  assert.equal(deletion.written, 7);
  assert.equal(deletion.reused, 14);
  assert.equal(deletion.rows.some(r => r.id === 2), false);
  assert.deepEqual(deletion.groups[0].chunks.map(c => c.pages.map(p => p.values.length)), Array.from({ length: 7 }, () => [3, 4, 4]));
  const append = lab.apply("append");
  assert.equal(append.rows.length, 13);
  assert.equal(append.written, 0);
  assert.deepEqual(append.batch.rows, [orderFor(12), orderFor(13)]);
  assert.equal(lab.pages.length, 29);
  assert.equal(virtualExtents(append).filter(e => e.column && e.source === "memory").length, 7);
  const flush = lab.apply("flush");
  assert.equal(flush.batch, null);
  assert.equal(flush.written, 7);
  assert.equal(visiblePages(flush).length, 28);
  assert.equal(append.batch.rows.length, 2);
  const rename = lab.apply("rename");
  assert.equal(rename.columns[2].name, "sku_id");
  assert.equal(rename.columns[2].id, 3);
  assert.equal(rename.written, 0);
  assert.deepEqual(visiblePages(rename), visiblePages(flush));
  assert.equal(flush.columns[2].name, "product_id");
  assert.equal(JSON.stringify(lab.snapshots[0]), original);
  assert.equal(lab.pages.length, 36);
});

test("virtual extents cover contiguous ranges and preserve projection and history", () => {
  const lab = createLab();
  for (const operation of operations) lab.apply(operation.key);
  for (const snapshot of lab.snapshots) {
    const extents = virtualExtents(snapshot);
    assert.equal(extents[0].offset, 0);
    for (let i = 1; i < extents.length; i++) assert.equal(extents[i].offset, extents[i - 1].offset + extents[i - 1].length);
    assert.equal(extents.at(-1).length, 8);
    assert.equal(extents.at(-2).source, "memory");
    const revenues = extents.filter(e => e.column === 6).flatMap(e => e.page?.values ?? e.values);
    assert.deepEqual(revenues, snapshot.rows.map(r => r.revenue_cents));
  }
});

test("out-of-order or repeated operations report errors without modifying storage", () => {
  const lab = createLab();
  assert.throws(() => lab.apply("flush"), /in order/);
  assert.equal(lab.snapshots.length, 1);
  assert.equal(lab.pages.length, 21);
  lab.apply("update");
  assert.throws(() => lab.apply("update"), /in order/);
});

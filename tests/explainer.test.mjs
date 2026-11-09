import test from "node:test";
import assert from "node:assert/strict";
import { createLab, operations, orderFor, pageSummary, planPointRead, visiblePages, virtualExtents } from "../docs/model.mjs";

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

test("page inspector excludes nulls from bounds and counts them accurately", () => {
  const lab = createLab();
  const discountPage = lab.snapshots[0].groups[0].chunks.find(chunk => chunk.column === 7).pages[0];
  assert.deepEqual(pageSummary(discountPage), { rows: 4, nulls: 1, min: 1, max: 3 });
  assert.deepEqual(pageSummary({ values: [null, null] }), { rows: 2, nulls: 2, min: null, max: null });
  assert.deepEqual(pageSummary({ values: ["r2", "r11", "r8"] }), { rows: 3, nulls: 0, min: "r11", max: "r8" });
});

test("scroll story's point-read example shows candidates and exact answers for each version", () => {
  const lab = createLab();
  const original = lab.snapshots[0];
  const oldPlan = planPointRead(original, 8, 6);
  assert.deepEqual(oldPlan.candidates.map(page => [page.min, page.max, page.read]), [[0, 3, false], [4, 7, false], [8, 11, true]]);
  assert.deepEqual(oldPlan.pages.map(page => page.id), ["P03", "P18"]);
  assert.equal(oldPlan.rows[0].revenue_cents, 63352);
  const updated = lab.apply("update");
  const newPlan = planPointRead(updated, 8, 6);
  assert.deepEqual(newPlan.pages.map(page => page.id), ["P03", "P22"]);
  assert.equal(newPlan.rows[0].revenue_cents, 12345);
  assert.equal(planPointRead(original, 8, 6).rows[0].revenue_cents, 63352);
  assert.deepEqual(planPointRead(updated, 100, 6).pages, []);
  assert.deepEqual(planPointRead(updated, 100, 6).rows, []);
  assert.throws(() => planPointRead(updated, 8.5, 6), /integer/);
  assert.throws(() => planPointRead(updated, 8, 123), /Unknown/);
});

test("candidate bounds are not exact matches and survive deletes and buffered rows", () => {
  const lab = createLab();
  lab.apply("update");
  const deletion = lab.apply("delete");
  const removed = planPointRead(deletion, 2, 6);
  assert.equal(removed.pages.length, 2);
  assert.deepEqual(removed.rows, []);
  const buffered = lab.apply("append");
  assert.deepEqual(planPointRead(buffered, 12, 6).pages.map(page => page.id), ["mem-1", "mem-6"]);
  assert.equal(planPointRead(buffered, 12, 6).rows[0].revenue_cents, 95028);
  const flushed = lab.apply("flush");
  assert.equal(planPointRead(flushed, 12, 6).pages.length, 2);
  assert.equal(planPointRead(flushed, 12, 6).pages.every(page => page.source === "blobs"), true);
});

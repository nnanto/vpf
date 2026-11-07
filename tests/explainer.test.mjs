import test from "node:test";
import assert from "node:assert/strict";
import { createLab, operations, visiblePages, virtualExtents } from "../docs/model.mjs";

test("guided mutations preserve history and reuse exactly the unaffected pages", () => {
  const lab = createLab();
  const original = JSON.stringify(lab.snapshots[0]);
  const update = lab.apply("update");
  assert.equal(update.written, 1);
  assert.equal(update.reused, 5);
  assert.equal(update.rows.find(r => r.id === 8).amount, 99);
  assert.deepEqual(visiblePages(update).map(p => p.id), ["P01", "P02", "P03", "P07", "P05", "P06"]);
  const deletion = lab.apply("delete");
  assert.equal(deletion.rows.length, 9);
  assert.equal(deletion.written, 3);
  assert.equal(deletion.reused, 3);
  assert.deepEqual(deletion.groups[0].chunks.map(c => c.pages.map(p => p.values.length)), [[3, 6], [3, 6], [3, 6]]);
  const append = lab.apply("append");
  assert.equal(append.rows.length, 11);
  assert.equal(append.written, 0);
  assert.equal(lab.pages.length, 10);
  assert.equal(virtualExtents(append).filter(e => e.column && e.source === "memory").length, 3);
  const flush = lab.apply("flush");
  assert.equal(flush.batch, null);
  assert.equal(flush.written, 3);
  assert.equal(visiblePages(flush).length, 9);
  assert.equal(append.batch.rows.length, 2);
  const rename = lab.apply("rename");
  assert.equal(rename.columns[1].name, "total");
  assert.equal(rename.columns[1].id, 2);
  assert.equal(rename.written, 0);
  assert.deepEqual(visiblePages(rename), visiblePages(flush));
  assert.equal(flush.columns[1].name, "amount");
  assert.equal(JSON.stringify(lab.snapshots[0]), original);
  assert.equal(lab.pages.length, 13);
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
    const amounts = extents.filter(e => e.column === 2).flatMap(e => e.page?.values ?? e.values);
    assert.deepEqual(amounts, snapshot.rows.map(r => r.amount));
  }
});

test("out-of-order or repeated operations report errors without modifying storage", () => {
  const lab = createLab();
  assert.throws(() => lab.apply("flush"), /in order/);
  assert.equal(lab.snapshots.length, 1);
  assert.equal(lab.pages.length, 6);
  lab.apply("update");
  assert.throws(() => lab.apply("update"), /in order/);
});

// From benches/results/read-write-{lazy,eager}.json (2026-11-08).
// Baseline series order: SQLite, DuckDB on Parquet, DuckDB on VPF.
// OR series order: VPF before, VPF after, same-after-run Parquet.
// OR source: benches/results/or-pruning.json, primary reports (not rechecks).
export const benchmarkData = {
  lazy: [
    { rows: 10000, read: [12.105305, 14.414409, 27.744131], fresh: [5.665552, 4.668474, 14.077218], append: [2.475662, 5.999789, 5.771059], update: [2.850338, 5.480538, 9.645976], delete: [2.749037, 9.095820, 11.365807], or: [1.929241, 1.994488, 1.011335] },
    { rows: 100000, read: [110.194562, 28.023115, 52.274942], fresh: [23.178136, 21.678392, 43.314071], append: [3.281036, 22.703132, 6.471979], update: [6.502554, 23.138046, 16.707079], delete: [6.693026, 23.657778, 36.344404], or: [3.836100, 3.264930, 2.054578] },
    { rows: 1000000, read: [1205.499681, 124.696480, 249.466032], fresh: [215.417685, 142.511911, 256.636023], append: [6.530225, 146.514613, 13.935919], update: [54.575273, 147.940257, 21.260803], delete: [43.341181, 161.697821, 43.385627], or: [21.268806, 5.430072, 2.501613] },
    { rows: 10000000, read: [16375.518038, 1041.258807, 2205.223967], fresh: [1998.681203, 1326.249329, 1892.237102], append: [27.911156, 1373.412390, 67.766037], update: [410.247757, 1367.121694, 60.494412], delete: [413.845603, 1413.938826, 83.573131], or: [198.779744, 26.175850, 5.422240] },
  ],
  eager: [
    { rows: 10000, read: [13.223351, 14.275150, 24.109875], fresh: [5.340090, 4.372786, 17.433486], append: [2.390385, 5.656069, 6.363218], update: [2.830739, 5.327661, 8.599023], delete: [2.894172, 5.399508, 11.862975], or: [1.651909, 1.733069, 1.040128] },
    { rows: 100000, read: [110.115509, 27.118470, 45.595407], fresh: [23.153280, 21.728907, 43.567637], append: [3.453182, 23.348104, 8.302879], update: [6.484127, 22.993913, 17.009748], delete: [6.571466, 23.823233, 35.906004], or: [3.408504, 2.722091, 1.975375] },
    { rows: 1000000, read: [1205.770071, 116.933530, 212.920851], fresh: [211.787141, 145.396614, 288.759011], append: [8.404771, 154.507898, 23.302791], update: [56.030441, 149.572548, 25.751253], delete: [46.851137, 155.379490, 47.359891], or: [19.417321, 3.273983, 2.534943] },
    { rows: 10000000, read: [15380.458252, 992.304976, 1876.973233], fresh: [2020.046131, 1339.082903, 2064.483688], append: [28.319331, 1360.402053, 90.069997], update: [398.744907, 1354.598927, 63.731483], delete: [385.282260, 1437.277731, 86.121441], or: [188.768493, 5.230766, 5.398666] },
  ],
};

export const benchmarkWorkloads = {
  update: { title: "Update 100 clustered rows", description: "Set revenue_cents = 12345 for 100 contiguous midpoint IDs. Parquet replaces the complete file; VPF retains the previous snapshot." },
  append: { title: "Append 1% more rows", description: "Append new IDs starting at N. Parquet replaces the complete file; VPF uses its production auto-append policy." },
  delete: { title: "Delete 100 clustered rows", description: "Remove 100 contiguous midpoint IDs. SQLite has no index; Parquet replaces the complete file; VPF retains history." },
  fresh: { title: "Write a new table", description: "Create a fresh destination and write all N rows. This is not a mutation: VPF’s metadata and transactional setup can cost more." },
  read: { title: "Read workload score", description: "Sum of 13 non-COUNT query medians: point/range lookups, filters, aggregates, grouping, top-N, and distinct. Not a single query or transaction." },
};

export function chartMaximum(values) {
  if (!values.length || values.some(value => !Number.isFinite(value) || value < 0)) {
    throw new Error("Chart values must be finite, non-negative timings.");
  }
  const max = Math.max(...values);
  if (max === 0) return 1;
  const unit = 10 ** Math.floor(Math.log10(max));
  return Math.ceil(max / unit) * unit;
}

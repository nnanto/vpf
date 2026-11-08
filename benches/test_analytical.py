"""Focused correctness tests for the read/write benchmark (requires built VPF)."""

import argparse
from contextlib import ExitStack, redirect_stdout
import io
from pathlib import Path
import sqlite3
import tempfile
import unittest

import analytical


class AnalyticalBenchmarkTests(unittest.TestCase):
    def test_write_specs_match_small_and_large_sizes(self):
        for rows in (1, 99, 10000):
            specs = analytical.write_spec(rows, Path("/tmp/incoming.parquet"))
            changed = min(100, rows - rows // 2)
            self.assertEqual(specs["fresh"]["output_rows"], rows)
            self.assertEqual(specs["append"]["output_rows"], rows + max(1, rows // 100))
            self.assertEqual(specs["update"]["logical_rows"], changed)
            self.assertEqual(specs["delete"]["output_rows"], rows - changed)

    def test_complete_verification_rejects_values_and_row_counts(self):
        with ExitStack() as stack:
            source = sqlite3.connect(":memory:")
            target = sqlite3.connect(":memory:")
            stack.callback(source.close)
            stack.callback(target.close)
            for con in (source, target):
                con.execute("CREATE TABLE t (id, value)")
            boundary = analytical.BATCH_ROWS + 1
            source.executemany("INSERT INTO t VALUES (?, ?)", [(boundary, 20), (1, 10)])
            target.executemany("INSERT INTO t VALUES (?, ?)", [(1, 10), (boundary, 20)])
            sql = "SELECT id, value FROM t"
            self.assertEqual(
                analytical.verify_written_rows(source, target, sql, sql, 2, "test"), 2,
            )
            with self.assertRaisesRegex(AssertionError, "verified 2"):
                analytical.verify_written_rows(source, target, sql, sql, 3, "test")
            target.execute("UPDATE t SET value = 21 WHERE id = ?", [boundary])
            with self.assertRaisesRegex(AssertionError, "output mismatch"):
                analytical.verify_written_rows(source, target, sql, sql, 2, "test")
            target.execute("UPDATE t SET value = 20 WHERE id = ?", [boundary])
            target.execute("INSERT INTO t VALUES (1, 10)")
            with self.assertRaisesRegex(AssertionError, "output mismatch"):
                analytical.verify_written_rows(source, target, sql, sql, 2, "test")

    def test_basic_lookup_and_filter_workloads_are_present(self):
        self.assertTrue({
            "point_lookup", "range_lookup", "equality_filter",
            "null_filter", "like_filter", "or_filter",
        }.issubset(analytical.queries(10000)))

    def test_independent_write_samples_in_both_metadata_modes(self):
        for mode in ("lazy", "eager"):
            with self.subTest(mode=mode), tempfile.TemporaryDirectory() as temporary, ExitStack() as stack:
                directory = Path(temporary)
                rows = 70000
                args = argparse.Namespace(
                    extension=analytical.ROOT / "crates/vpf-duckdb/build/vpf.duckdb_extension",
                    vpf_bin=analytical.ROOT / "target/release/vpf",
                    metadata_mode=mode, write_warmups=1, write_repeats=2,
                )
                connections, _, verified, _ = analytical.prepare(
                    directory, rows, args.extension, args.vpf_bin, mode, stack,
                )
                self.assertEqual(verified, rows)
                with redirect_stdout(io.StringIO()):
                    result = analytical.benchmark_writes(directory, rows, args, connections["parquet"])
                for workload in analytical.WRITE_WORKLOADS:
                    expected = result["workloads"][workload]["output_rows"]
                    for engine in analytical.ENGINES:
                        self.assertEqual(result["verified_rows"][workload][engine], expected)
                        timings = result["samples_ms"][workload][engine]
                        self.assertEqual(len(timings), 2)
                        sql = result["stage_samples_ms"]["sql_ms"][workload][engine]
                        durability = result["stage_samples_ms"]["durability_ms"][workload][engine]
                        for total, execution, sync in zip(timings, sql, durability):
                            self.assertGreater(total, 0)
                            self.assertAlmostEqual(total, execution + sync)
                        self.assertTrue(result["sql"][workload][engine])
                for engine, con in connections.items():
                    self.assertEqual(con.execute("SELECT COUNT(*) FROM orders").fetchall(), [(rows,)])
                    self.assertEqual(
                        con.execute(f"SELECT revenue_cents FROM orders WHERE id = {rows // 2}").fetchall(),
                        [((rows // 2 * 7919) % 100000,)],
                        f"{engine} seed changed",
                    )


if __name__ == "__main__":
    unittest.main()

"""Pure-Python safety and contract tests (do not require PySpark)."""
from __future__ import annotations

import importlib.util
import json
import tempfile
import unittest
from pathlib import Path

MODULE_PATH = (Path(__file__).resolve().parents[1] / "roles" / "data_pipeline_olist_etl" /
               "files" / "olist_etl.py")
spec = importlib.util.spec_from_file_location("olist_etl", MODULE_PATH)
etl = importlib.util.module_from_spec(spec)
spec.loader.exec_module(etl)


class FakeHdfs:
    def __init__(self, paths=None, manifest=None):
        self.paths = set(paths or [])
        self.manifest = manifest

    def exists(self, path):
        return path in self.paths

    def cat(self, path):
        return json.dumps(self.manifest).encode("utf-8")


def example_manifest():
    return {"schema_version": 1, "dataset": "olist", "files": {
        name: {"filename": filename, "rows": 2, "bytes": 21, "sha256": "a" * 64}
        for name, filename in etl.RAW_FILES.items()
    }}


class OlistEtlTests(unittest.TestCase):
    def test_source_manifest_all_nine_files_valid(self):
        etl.validate_raw_manifest(example_manifest())

    def test_manifest_rejects_missing_raw_file(self):
        data = example_manifest()
        del data["files"]["payments"]
        with self.assertRaises(etl.EtlError):
            etl.validate_raw_manifest(data)

    def test_manifest_rejects_filename_swap(self):
        data = example_manifest()
        data["files"]["orders"]["filename"] = "some_other.csv"
        with self.assertRaises(etl.EtlError):
            etl.validate_raw_manifest(data)

    def test_manifest_rejects_hash_malformed(self):
        data = example_manifest()
        data["files"]["orders"]["sha256"] = "bad"
        with self.assertRaises(etl.EtlError):
            etl.validate_raw_manifest(data)

    def test_manifest_rejects_invalid_row_counts(self):
        data = example_manifest()
        data["files"]["orders"]["rows"] = 0
        with self.assertRaises(etl.EtlError):
            etl.validate_raw_manifest(data)

    def test_checked_versions_accept_v1_and_v2(self):
        self.assertEqual(etl.checked_version("v1"), "v1")
        self.assertEqual(etl.checked_version("v2"), "v2")

    def test_checked_versions_reject_path_traversal(self):
        for bad in ("../v1", "../../raw", "", "x/y", "x y", "." * 60):
            with self.subTest(bad=bad), self.assertRaises(etl.EtlError):
                etl.checked_version(bad)

    def test_checked_roots_only_allow_expected_targets(self):
        etl.checked_roots("/data/olist/raw", "/data/olist/warehouse")
        with self.assertRaises(etl.EtlError):
            etl.checked_roots("/", "/data/olist/warehouse")

    def test_identical_raw_manifest_generates_same_identity(self):
        m = json.dumps(example_manifest(), sort_keys=True).encode()
        self.assertEqual(etl.make_run_identity("v1", m), etl.make_run_identity("v1", m))

    def test_different_manifest_changes_identity(self):
        m = b'{"files":{}}'
        self.assertNotEqual(etl.make_run_identity("v1", m), etl.make_run_identity("v1", m + b"\n"))

    def test_unchanged_output_skipped_if_complete(self):
        identity = etl.make_run_identity("v1", b"original manifest")
        dest = "/data/olist/warehouse/v1"
        paths = {dest, dest + "/_ETL_MANIFEST.json"} | {
            dest + "/" + table + "/_SUCCESS" for table in etl.OUTPUT_TABLES
        }
        self.assertTrue(etl.check_published(FakeHdfs(paths, identity), dest, identity))

    def test_missing_output_is_not_skipped(self):
        identity = etl.make_run_identity("v1", b"original manifest")
        self.assertFalse(etl.check_published(FakeHdfs(), "/data/olist/warehouse/v1", identity))

    def test_partial_output_refuses_overwrite(self):
        identity = etl.make_run_identity("v1", b"original manifest")
        dest = "/data/olist/warehouse/v1"
        with self.assertRaises(etl.EtlError):
            etl.check_published(FakeHdfs({dest}), dest, identity)

    def test_changed_source_refuses_reuse_of_output_version(self):
        old = etl.make_run_identity("v1", b"first")
        new = etl.make_run_identity("v1", b"second")
        dest = "/data/olist/warehouse/v1"
        with self.assertRaises(etl.EtlError):
            etl.check_published(FakeHdfs({dest, dest + "/_ETL_MANIFEST.json"}, old), dest, new)

    def test_missing_success_marker_refuses_skip(self):
        identity = etl.make_run_identity("v1", b"original manifest")
        dest = "/data/olist/warehouse/v1"
        paths = {dest, dest + "/_ETL_MANIFEST.json"}
        with self.assertRaises(etl.EtlError):
            etl.check_published(FakeHdfs(paths, identity), dest, identity)

    def test_four_core_sources_have_consistent_required_columns(self):
        self.assertEqual(set(etl.CORE_COLUMNS), {"orders", "order_items", "customers", "products"})
        self.assertEqual(len(etl.CORE_COLUMNS["orders"]), 8)
        self.assertEqual(len(etl.CORE_COLUMNS["order_items"]), 7)
        self.assertEqual(len(etl.CORE_COLUMNS["customers"]), 5)
        self.assertEqual(len(etl.CORE_COLUMNS["products"]), 9)

    def test_output_inventory_contains_fact_quality_mart(self):
        for path in ("curated/order_item_fact", "quarantine/rows", "mart/daily_category_sales"):
            self.assertIn(path, etl.OUTPUT_TABLES)

    def test_yarn_cluster_deployment_contract(self):
        tasks_path = Path(__file__).resolve().parents[1] / "roles" / "data_pipeline_olist_etl" / "tasks" / "main.yml"
        tasks = tasks_path.read_text(encoding="utf-8")
        self.assertIn("- cluster", tasks)
        self.assertIn("spark.yarn.submit.waitAppCompletion=true", tasks)
        self.assertIn("Verify all seven Parquet outputs", tasks)
        self.assertNotIn("'OLIST ETL OK' in data_pipeline_olist_etl_run.stdout", tasks)

    def test_script_compiles_without_pyspark_import(self):
        compile(MODULE_PATH.read_text(encoding="utf-8"), str(MODULE_PATH), "exec")


if __name__ == "__main__":
    unittest.main()

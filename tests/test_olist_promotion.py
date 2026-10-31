"""Unit tests for the 52 fail-closed immutable quality gate; no HDFS required."""
import importlib.util
import hashlib
import json
import unittest
from pathlib import Path

SCRIPT = (Path(__file__).resolve().parents[1] / "roles" /
          "data_pipeline_olist_promotion" / "files" / "approve_olist.py")
spec = importlib.util.spec_from_file_location("approve_olist", SCRIPT)
gate = importlib.util.module_from_spec(spec)
spec.loader.exec_module(gate)
NOW = 1800000000
RID1 = "a" * 32
RID2 = "b" * 32


def sample(version="v3", rid=RID1, status="PASS", stamp=NOW - 10, manifest=None):
    etl = manifest or {
        "pipeline": "olist_spark_batch_etl",
        "pipeline_version": "1",
        "raw_version": "v1",
        "raw_manifest_sha256": "a" * 64,
        "tables": list(gate.TABLES),
        "metrics": {"etl_version": version,
                    "output_rows": {name: 1 for name in gate.TABLES}},
    }
    etl_raw = gate.canonical(etl)
    failed = status != "PASS"
    report = {
        "schema_version": 1,
        "pipeline": "olist_quality_monitor",
        "warehouse_version": version,
        "etl_manifest_sha256": gate.sha256(etl_raw),
        "run_id": rid,
        "run_timestamp_seconds": stamp,
        "status": status,
        "summary": {"checks_total": 2, "checks_failed": int(failed)},
        "checks": [
            {"id": "count", "status": "PASS"},
            {"id": "orphans", "status": "FAIL" if failed else "PASS"},
        ],
    }
    return etl_raw, gate.canonical(report)


class FakeHdfs:
    def __init__(self, version="v3"):
        self.version = version
        self.etl, report = sample(version)
        self.reports = {
            f"{gate.QUALITY}/{version}/runs/{RID1}/_QUALITY_REPORT.json": report
        }
        self.written = {}
        self.markers = {f"{gate.WAREHOUSE}/{version}/{name}/_SUCCESS" for name in gate.TABLES}

    def exists(self, path):
        return (path in self.markers or path == f"{gate.WAREHOUSE}/{self.version}/_ETL_MANIFEST.json"
                or path in self.written)

    def read(self, path):
        if path == f"{gate.WAREHOUSE}/{self.version}/_ETL_MANIFEST.json":
            return self.etl
        if path in self.reports:
            return self.reports[path]
        return self.written[path]

    def list_reports(self, version):
        return list(self.reports)

    def publish(self, path, content):
        if path in self.written:
            raise RuntimeError("write-once protection")
        self.written[path] = content


class TestPromotion(unittest.TestCase):
    def test_plan_read_only(self):
        hdfs = FakeHdfs()
        self.assertFalse(gate.run_gate(hdfs, "v3", apply=False, now=NOW, max_age_hours=168))
        self.assertEqual(hdfs.written, {})

    def test_apply_then_idempotent(self):
        hdfs = FakeHdfs()
        self.assertTrue(gate.run_gate(hdfs, "v3", apply=True, now=NOW, max_age_hours=168))
        self.assertFalse(gate.run_gate(hdfs, "v3", apply=True, now=NOW, max_age_hours=168))
        self.assertEqual(len(hdfs.written), 1)
        approval = json.loads(next(iter(hdfs.written.values())))
        self.assertEqual(approval["checks_passed"], 2)
        self.assertEqual(approval["mart_path"], "/data/olist/warehouse/v3/mart/daily_category_sales")

    def test_newer_fail_blocks_older_pass(self):
        hdfs = FakeHdfs()
        _, bad = sample(rid=RID2, status="FAIL", stamp=NOW - 1)
        hdfs.reports[f"{gate.QUALITY}/v3/runs/{RID2}/_QUALITY_REPORT.json"] = bad
        with self.assertRaisesRegex(gate.GateError, "NOT PASS"):
            gate.run_gate(hdfs, "v3", apply=True, now=NOW, max_age_hours=168)
        self.assertEqual(hdfs.written, {})

    def test_tied_reports_block(self):
        hdfs = FakeHdfs()
        _, another = sample(rid=RID2, stamp=NOW - 10)
        hdfs.reports[f"{gate.QUALITY}/v3/runs/{RID2}/_QUALITY_REPORT.json"] = another
        with self.assertRaisesRegex(gate.GateError, "share the latest timestamp"):
            gate.run_gate(hdfs, "v3", apply=True, now=NOW, max_age_hours=168)

    def test_stale_pass_blocks(self):
        hdfs = FakeHdfs()
        _, old = sample(stamp=NOW - 8 * 24 * 3600)
        hdfs.reports[next(iter(hdfs.reports))] = old
        with self.assertRaisesRegex(gate.GateError, "stale"):
            gate.run_gate(hdfs, "v3", apply=True, now=NOW, max_age_hours=168)

    def test_report_manifest_sha_mismatch_blocks(self):
        hdfs = FakeHdfs()
        path = next(iter(hdfs.reports))
        report = json.loads(hdfs.reports[path])
        report["etl_manifest_sha256"] = "f" * 64
        hdfs.reports[path] = gate.canonical(report)
        with self.assertRaisesRegex(gate.GateError, "NOT computed"):
            gate.run_gate(hdfs, "v3", apply=False, now=NOW, max_age_hours=168)

    def test_missing_success_marker_blocks(self):
        hdfs = FakeHdfs()
        hdfs.markers.remove(f"{gate.WAREHOUSE}/v3/curated/orders/_SUCCESS")
        with self.assertRaisesRegex(gate.GateError, "Incomplete Parquet"):
            gate.run_gate(hdfs, "v3", apply=True, now=NOW, max_age_hours=168)

    def test_missing_quality_report_blocks(self):
        hdfs = FakeHdfs()
        hdfs.reports.clear()
        with self.assertRaisesRegex(gate.GateError, "No quality reports"):
            gate.run_gate(hdfs, "v3", apply=False, now=NOW, max_age_hours=168)

    def test_approval_digest_tampering_blocks(self):
        hdfs = FakeHdfs()
        gate.run_gate(hdfs, "v3", apply=True, now=NOW, max_age_hours=168)
        path = next(iter(hdfs.reports))
        report = json.loads(hdfs.reports[path])
        report["status"] = "FAIL"
        hdfs.reports[path] = gate.canonical(report)
        with self.assertRaisesRegex(gate.GateError, "NOT PASS|altered"):
            gate.run_gate(hdfs, "v3", apply=True, now=NOW, max_age_hours=168)

    def test_invalid_version_blocks(self):
        with self.assertRaisesRegex(gate.GateError, "Version must"):
            gate.run_gate(FakeHdfs(), "../../v3", apply=False, now=NOW, max_age_hours=168)

    def test_invalid_check_counts_block(self):
        hdfs = FakeHdfs()
        path = next(iter(hdfs.reports))
        report = json.loads(hdfs.reports[path])
        report["summary"]["checks_total"] = 123
        hdfs.reports[path] = gate.canonical(report)
        with self.assertRaisesRegex(gate.GateError, "count mismatch"):
            gate.run_gate(hdfs, "v3", apply=False, now=NOW, max_age_hours=168)

    def test_bad_report_identity_blocks(self):
        hdfs = FakeHdfs()
        path = next(iter(hdfs.reports))
        report = json.loads(hdfs.reports[path])
        report["run_id"] = RID2
        hdfs.reports[path] = gate.canonical(report)
        with self.assertRaisesRegex(gate.GateError, "identity mismatch"):
            gate.run_gate(hdfs, "v3", apply=False, now=NOW, max_age_hours=168)


if __name__ == "__main__":
    unittest.main()

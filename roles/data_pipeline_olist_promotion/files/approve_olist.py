#!/usr/bin/env python3
"""Fail-closed quality-gated immutable Olist warehouse approval (Step 52).

Runs as the HDFS Spark submit account on master. Stdlib only; no Spark rerun,
no OpenMetadata writes, no deletions, and no data writes under Raw/Warehouse/Quality.
"""
import argparse
import hashlib
import json
import os
import re
import subprocess
import sys
import tempfile
import time
from datetime import datetime, timezone

WAREHOUSE = "/data/olist/warehouse"
QUALITY = "/data/olist/quality"
PUBLISHED = "/data/olist/published"
TABLES = (
    "curated/orders", "curated/order_items", "curated/customers",
    "curated/products", "curated/order_item_fact", "quarantine/rows",
    "mart/daily_category_sales",
)
SHA256 = re.compile(r"^[0-9a-f]{64}$")
VERSION = re.compile(r"^v[1-9][0-9]{0,4}$")
RUN_ID = re.compile(r"^[0-9a-f]{32}$")


class GateError(Exception):
    pass


def require(condition, message):
    if not condition:
        raise GateError(message)


def sha256(content):
    return hashlib.sha256(content).hexdigest()


def canonical(obj):
    return (json.dumps(obj, sort_keys=True, ensure_ascii=False, indent=2) + "\n").encode("utf-8")


class Hdfs:
    @staticmethod
    def run(*args, allow_missing=False):
        result = subprocess.run(["hdfs", "dfs", *args], capture_output=True, check=False)
        if result.returncode and not (allow_missing and result.returncode == 1 and
                                      b"Permission denied" not in result.stderr):
            stderr = result.stderr.decode("utf-8", errors="replace")[-500:]
            raise GateError("HDFS " + args[0] + " failed: " + stderr)
        return result

    def exists(self, path):
        return self.run("-test", "-e", path, allow_missing=True).returncode == 0

    def read(self, path):
        return self.run("-cat", path).stdout

    def list_reports(self, version):
        glob = f"{QUALITY}/{version}/runs/*/_QUALITY_REPORT.json"
        result = self.run("-ls", glob, allow_missing=True)
        if result.returncode:
            return []
        paths = []
        for line in result.stdout.decode("utf-8").splitlines():
            parts = line.split()
            if len(parts) >= 8 and parts[0].startswith("-"):
                paths.append(parts[-1])
        return paths

    def publish(self, path, content):
        with tempfile.TemporaryDirectory(prefix="olist-approval-") as directory:
            local = os.path.join(directory, "approval.json")
            with open(local, "wb") as f:
                f.write(content)
                f.flush()
                os.fsync(f.fileno())
            # Hadoop -put without -f does not overwrite an existing file.
            # The version directory is prepared by Ansible only in apply mode.
            self.run("-put", local, path)


def json_object(raw, description):
    try:
        obj = json.loads(raw)
    except (UnicodeDecodeError, ValueError) as exc:
        raise GateError(f"Invalid {description} JSON") from exc
    require(isinstance(obj, dict), f"{description} must be a JSON object")
    return obj


def validate_manifest(manifest, version):
    require(manifest.get("pipeline") == "olist_spark_batch_etl", "Unexpected ETL pipeline")
    require(manifest.get("raw_version") == "v1", "ETL input is not immutable Raw v1")
    require(manifest.get("tables") == list(TABLES), "ETL output table inventory mismatch")
    require(isinstance(manifest.get("pipeline_version"), str) and manifest["pipeline_version"],
            "ETL pipeline version missing")
    require(isinstance(manifest.get("raw_manifest_sha256"), str) and
            SHA256.fullmatch(manifest["raw_manifest_sha256"]), "Invalid raw manifest SHA-256")
    metrics = manifest.get("metrics")
    require(isinstance(metrics, dict) and metrics.get("etl_version") == version,
            "ETL manifest version mismatch")
    outputs = metrics.get("output_rows")
    require(isinstance(outputs, dict) and all(
        isinstance(outputs.get(table), int) and not isinstance(outputs[table], bool) and
        outputs[table] >= 0 for table in TABLES), "Invalid ETL output row counts")


def validate_report(report, version, manifest_digest, path, now, max_age_seconds):
    prefix = f"{QUALITY}/{version}/runs/"
    require(path.startswith(prefix) and path.endswith("/_QUALITY_REPORT.json"),
            "Report path is outside immutable quality report directory")
    run_id = path[len(prefix):-len("/_QUALITY_REPORT.json")]
    require(RUN_ID.fullmatch(run_id), "Unrecognized quality report run_id in HDFS path")
    require(report.get("schema_version") == 1 and
            report.get("pipeline") == "olist_quality_monitor", "Unexpected quality report schema")
    require(report.get("warehouse_version") == version, "Quality report version mismatch")
    require(report.get("run_id") == run_id, "Quality report run_id mismatch")
    require(report.get("etl_manifest_sha256") == manifest_digest,
            "Quality report was NOT computed from the published ETL manifest")
    timestamp = report.get("run_timestamp_seconds")
    require(isinstance(timestamp, int) and not isinstance(timestamp, bool) and
            0 < timestamp <= now + 300, "Quality report timestamp is invalid or in the future")
    summary = report.get("summary")
    checks = report.get("checks")
    require(isinstance(checks, list) and checks, "Quality report has no checks")
    require(isinstance(summary, dict) and summary.get("checks_total") == len(checks),
            "Quality report check count mismatch")
    failed = 0
    seen = set()
    for check in checks:
        require(isinstance(check, dict) and isinstance(check.get("id"), str) and
                check["id"] and check["id"] not in seen, "Invalid or duplicate quality check")
        seen.add(check["id"])
        require(check.get("status") in ("PASS", "FAIL"), "Invalid quality check status")
        failed += (check["status"] == "FAIL")
    require(summary.get("checks_failed") == failed, "Failed-check counter mismatch")
    require(report.get("status") == "PASS" and failed == 0, "Latest quality report is NOT PASS")
    require(now - timestamp <= max_age_seconds, "Latest PASS report is stale; rerun Step 47")
    return run_id


def load_latest_report(hdfs, version, now):
    paths = hdfs.list_reports(version)
    require(paths, f"No quality reports for {version}; run Step 47 for that version first")
    require(len(paths) <= 500, "Over 500 quality reports; manual review required")
    inspected = []
    prefix = f"{QUALITY}/{version}/runs/"
    for path in paths:
        require(path.startswith(prefix) and path.endswith("/_QUALITY_REPORT.json"),
                "Unexpected report path returned by HDFS")
        rid = path[len(prefix):-len("/_QUALITY_REPORT.json")]
        require(RUN_ID.fullmatch(rid), "Invalid report directory/run_id")
        raw = hdfs.read(path)
        report = json_object(raw, "quality report")
        require(report.get("run_id") == rid and report.get("warehouse_version") == version,
                "Quality report identity mismatch")
        stamp = report.get("run_timestamp_seconds")
        require(isinstance(stamp, int) and not isinstance(stamp, bool) and
                0 < stamp <= now + 300, "Invalid report timestamp")
        inspected.append((stamp, rid, path, raw, report))
    # Never cherry-pick an old PASS if a later quality run failed.
    inspected.sort(key=lambda item: (item[0], item[1]))
    if len(inspected) >= 2 and inspected[-2][0] == inspected[-1][0]:
        raise GateError("Multiple quality reports share the latest timestamp; rerun Step 47")
    return inspected[-1]


def validate_existing_approval(hdfs, path, version, manifest_digest, now):
    approval = json_object(hdfs.read(path), "existing approval")
    require(approval.get("schema_version") == 1 and
            approval.get("kind") == "olist_quality_gated_approval", "Invalid existing approval")
    require(approval.get("warehouse_version") == version and
            approval.get("etl_manifest_sha256") == manifest_digest,
            "Existing approval points to a different ETL manifest")
    report_path = approval.get("quality_report_path")
    require(isinstance(report_path, str) and report_path.startswith(f"{QUALITY}/{version}/runs/") and
            report_path.endswith("/_QUALITY_REPORT.json"), "Existing approval report path invalid")
    original = hdfs.read(report_path)
    digest = approval.get("quality_report_sha256")
    require(isinstance(digest, str) and SHA256.fullmatch(digest) and
            sha256(original) == digest, "Original approved quality report was altered")
    original_report = json_object(original, "approved report")
    validate_report(original_report, version, manifest_digest, report_path, now, 10**15)
    require(original_report.get("run_id") == approval.get("quality_run_id"),
            "Existing approval quality run ID mismatch")
    return approval


def run_gate(hdfs, version, *, apply, now, max_age_hours):
    require(VERSION.fullmatch(version), "Version must be v1, v2, ... (no path traversal)")
    require(isinstance(max_age_hours, int) and 1 <= max_age_hours <= 8760,
            "Report maximum age must be 1..8760 hours")
    warehouse_base = f"{WAREHOUSE}/{version}"
    manifest_path = f"{warehouse_base}/_ETL_MANIFEST.json"
    require(hdfs.exists(manifest_path), "Missing immutable ETL manifest: " + manifest_path)
    manifest_bytes = hdfs.read(manifest_path)
    manifest = json_object(manifest_bytes, "ETL manifest")
    validate_manifest(manifest, version)
    for table in TABLES:
        marker = f"{warehouse_base}/{table}/_SUCCESS"
        require(hdfs.exists(marker), "Incomplete Parquet output: " + marker)
    manifest_digest = sha256(manifest_bytes)
    _, _, report_path, report_raw, report = load_latest_report(hdfs, version, now)
    run_id = validate_report(report, version, manifest_digest, report_path,
                             now, max_age_hours * 3600)
    approval_path = f"{PUBLISHED}/{version}/_APPROVAL.json"
    if hdfs.exists(approval_path):
        prior = validate_existing_approval(hdfs, approval_path, version, manifest_digest, now)
        print(f"OLIST PROMOTION UNCHANGED: {approval_path} "
              f"(original_quality_run={prior['quality_run_id']})")
        return False
    print(f"OLIST PROMOTION {'APPLY' if apply else 'PLAN'}: version={version} "
          f"quality_run={run_id} checks={len(report['checks'])} "
          f"etl_sha256={manifest_digest} report_sha256={sha256(report_raw)}")
    print(f"OLIST APPROVED MART: {warehouse_base}/mart/daily_category_sales")
    if not apply:
        print("OLIST PROMOTION READ-ONLY: no approval created")
        return False
    record = {
        "schema_version": 1,
        "kind": "olist_quality_gated_approval",
        "warehouse_version": version,
        "warehouse_path": warehouse_base,
        "mart_path": f"{warehouse_base}/mart/daily_category_sales",
        "etl_manifest_path": manifest_path,
        "etl_manifest_sha256": manifest_digest,
        "quality_run_id": run_id,
        "quality_report_path": report_path,
        "quality_report_sha256": sha256(report_raw),
        "checks_passed": len(report["checks"]),
        "approved_at_epoch": now,
        "approved_at_utc": datetime.fromtimestamp(now, timezone.utc).isoformat(),
    }
    hdfs.publish(approval_path, canonical(record))
    print("OLIST PROMOTION CHANGED: " + approval_path)
    return True


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--plan", action="store_true")
    mode.add_argument("--apply", action="store_true")
    parser.add_argument("--warehouse-version", default="v3")
    parser.add_argument("--max-report-age-hours", type=int, default=168)
    args = parser.parse_args(argv)
    try:
        run_gate(Hdfs(), args.warehouse_version, apply=args.apply,
                 now=int(time.time()), max_age_hours=args.max_report_age_hours)
        return 0
    except (GateError, OSError) as exc:
        print("OLIST PROMOTION BLOCKED: " + str(exc), file=sys.stderr)
        return 2


if __name__ == "__main__":
    sys.exit(main())

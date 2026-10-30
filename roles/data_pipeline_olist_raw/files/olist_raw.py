#!/usr/bin/env python3
"""Validate Olist CSV, transfer immutable snapshot to HDFS, verify SHA256 end to end.

Standard-library-only, compatible with Ubuntu 24.04 Python 3.12.
"""
from __future__ import annotations

import argparse
import csv
import hashlib
import json
import os
import re
import subprocess
import sys
import tempfile
import uuid
from pathlib import Path

DATASETS = {
    "orders": (
        "olist_orders_dataset.csv",
        {"order_id", "customer_id", "order_status", "order_purchase_timestamp"},
    ),
    "order_items": (
        "olist_order_items_dataset.csv",
        {"order_id", "order_item_id", "product_id", "price"},
    ),
    "customers": (
        "olist_customers_dataset.csv",
        {"customer_id", "customer_unique_id"},
    ),
    "products": (
        "olist_products_dataset.csv",
        {"product_id", "product_category_name"},
    ),
    "payments": (
        "olist_order_payments_dataset.csv",
        {"order_id", "payment_sequential", "payment_type", "payment_value"},
    ),
    "reviews": (
        "olist_order_reviews_dataset.csv",
        {"review_id", "order_id", "review_score"},
    ),
    "sellers": (
        "olist_sellers_dataset.csv",
        {"seller_id", "seller_zip_code_prefix"},
    ),
    "geolocation": (
        "olist_geolocation_dataset.csv",
        {"geolocation_zip_code_prefix", "geolocation_lat", "geolocation_lng"},
    ),
    "category_translation": (
        "product_category_name_translation.csv",
        {"product_category_name", "product_category_name_english"},
    ),
}


class IngestError(RuntimeError):
    pass


def scan_csv(source: Path) -> dict:
    if not source.is_dir():
        raise IngestError(f"Local Olist directory not found: {source}")
    result = {"schema_version": 1, "dataset": "olist", "files": {}}
    for dataset, (filename, required) in sorted(DATASETS.items()):
        path = source / filename
        if not path.is_file() or path.is_symlink():
            raise IngestError(f"Required regular CSV missing (or symlink): {path}")
        file_hash = hashlib.sha256()
        file_size = 0
        with path.open("rb") as stream:
            while True:
                piece = stream.read(1024 * 1024)
                if not piece:
                    break
                file_hash.update(piece)
                file_size += len(piece)
        if file_size == 0:
            raise IngestError(f"Empty CSV: {filename}")
        try:
            with path.open("r", encoding="utf-8-sig", newline="") as stream:
                reader = csv.reader(stream, strict=True)
                header = next(reader, [])
                if len(header) != len(set(header)) or not required.issubset(header):
                    raise IngestError(
                        f"Invalid {filename} header; required={sorted(required)}, actual={header}"
                    )
                rows = 0
                for row in reader:
                    if len(row) != len(header):
                        raise IngestError(
                            f"Invalid column count in {filename}, CSV record {rows + 2}: "
                            f"expected {len(header)}, got {len(row)}"
                        )
                    rows += 1
        except (UnicodeError, csv.Error) as exc:
            raise IngestError(f"Invalid CSV format in {filename}: {exc}") from exc
        if rows == 0:
            raise IngestError(f"CSV must contain at least one data row: {filename}")
        result["files"][dataset] = {
            "filename": filename,
            "rows": rows,
            "bytes": file_size,
            "sha256": file_hash.hexdigest(),
        }
    return result


def canonical_json(value: dict) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, indent=2) + "\n"


class Hdfs:
    def __init__(self, binary: str = "hdfs") -> None:
        self.binary = binary

    def run(self, *args: str) -> subprocess.CompletedProcess:
        p = subprocess.run(
            [self.binary, "dfs", *map(str, args)],
            text=True, capture_output=True, check=False,
        )
        if p.returncode != 0:
            raise IngestError(
                f"HDFS command failed (exit={p.returncode}): "
                f"hdfs dfs {' '.join(map(str, args[:2]))}; {p.stderr[-700:].strip()}"
            )
        return p

    def exists(self, path: str) -> bool:
        p = subprocess.run(
            [self.binary, "dfs", "-test", "-e", path],
            text=True, capture_output=True, check=False,
        )
        if p.returncode == 0:
            return True
        if p.returncode == 1 and "Permission denied" not in p.stderr:
            return False
        raise IngestError(f"HDFS test failed for {path}: {p.stderr[-700:].strip()}")

    def sha256(self, path: str) -> str:
        digest = hashlib.sha256()
        with tempfile.TemporaryFile() as errlog:
            proc = subprocess.Popen(
                [self.binary, "dfs", "-cat", path],
                stdout=subprocess.PIPE,
                stderr=errlog,
            )
            assert proc.stdout is not None
            try:
                while True:
                    chunk = proc.stdout.read(1024 * 1024)
                    if not chunk:
                        break
                    digest.update(chunk)
            finally:
                proc.stdout.close()
            code = proc.wait()
            if code != 0:
                errlog.seek(0)
                raise IngestError(
                    f"HDFS -cat failed (exit={code}) for {path}: "
                    f"{errlog.read(700).decode(errors='replace')}"
                )
        return digest.hexdigest()

    def read_text(self, path: str) -> str:
        return self.run("-cat", path).stdout

    def mkdir(self, path: str) -> None:
        self.run("-mkdir", "-p", path)

    def put(self, source: Path, dest: str) -> None:
        self.run("-put", str(source), dest)

    def mv(self, source: str, dest: str) -> None:
        self.run("-mv", source, dest)

    def rm(self, path: str) -> None:
        self.run("-rm", "-f", path)

    def chmod(self, path: str, mode: str) -> None:
        self.run("-chmod", mode, path)

    def setrep(self, path: str, replication: int) -> None:
        self.run("-setrep", str(replication), path)


def validate_hdfs_path(root: str, version: str) -> None:
    if not re.fullmatch(r"/[A-Za-z0-9_./-]+", root) or ".." in root.split("/"):
        raise IngestError(f"Invalid HDFS root: {root}")
    if not root.startswith("/data/olist/raw"):
        raise IngestError("HDFS destination must be under /data/olist/raw")
    if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,31}", version):
        raise IngestError(f"Invalid dataset version: {version}")


def ingest_files(source: Path, expected: dict, root: str, version: str,
                 replication: int, hdfs: Hdfs) -> list[str]:
    validate_hdfs_path(root, version)
    if replication not in (1, 2):
        raise IngestError("Use replication=1 or 2 for the two-DataNode practice cluster")
    actual = scan_csv(source)
    if actual != expected:
        raise IngestError("Source CSV differs from controller-side validation manifest")
    version_path = root.rstrip("/") + "/" + version
    manifest_path = version_path + "/_MANIFEST.json"
    if hdfs.exists(manifest_path):
        try:
            existing = json.loads(hdfs.read_text(manifest_path))
        except (ValueError, UnicodeError) as exc:
            raise IngestError("Existing HDFS manifest is invalid; refusing overwrite") from exc
        if existing != expected:
            raise IngestError("This snapshot version already has a different manifest; use a new version")
    action_log: list[str] = []
    for dataset, info in sorted(expected["files"].items()):
        name = info["filename"]
        directory = f"{version_path}/{dataset}"
        destination = f"{directory}/{name}"
        if hdfs.exists(destination):
            if hdfs.sha256(destination) != info["sha256"]:
                raise IngestError(f"Existing HDFS file differs; refusing overwrite: {destination}")
            action_log.append(f"SKIP verified {dataset}: {info['rows']} rows")
        else:
            hdfs.mkdir(directory)
            temporary = destination + f"._upload_{uuid.uuid4().hex}"
            try:
                hdfs.put(source / name, temporary)
                if hdfs.sha256(temporary) != info["sha256"]:
                    raise IngestError(f"HDFS upload checksum mismatch: {name}")
                if hdfs.exists(destination):
                    raise IngestError(f"Destination appeared during upload: {destination}")
                hdfs.mv(temporary, destination)
            finally:
                if hdfs.exists(temporary):
                    hdfs.rm(temporary)
            hdfs.chmod(destination, "644")
            action_log.append(f"PUT verified {dataset}: {info['rows']} rows")
        hdfs.setrep(destination, replication)
    if hdfs.exists(manifest_path):
        action_log.append("SKIP manifest unchanged")
    else:
        hdfs.mkdir(version_path)
        with tempfile.TemporaryDirectory() as tmp:
            local_manifest = Path(tmp) / "_MANIFEST.json"
            local_manifest.write_text(canonical_json(expected), encoding="utf-8")
            temporary = manifest_path + f"._upload_{uuid.uuid4().hex}"
            try:
                hdfs.put(local_manifest, temporary)
                if hdfs.sha256(temporary) != hashlib.sha256(
                    local_manifest.read_bytes()
                ).hexdigest():
                    raise IngestError("HDFS manifest upload checksum mismatch")
                if hdfs.exists(manifest_path):
                    raise IngestError("Manifest appeared during upload; refusing overwrite")
                hdfs.mv(temporary, manifest_path)
            finally:
                if hdfs.exists(temporary):
                    hdfs.rm(temporary)
        hdfs.chmod(manifest_path, "644")
        hdfs.setrep(manifest_path, replication)
        action_log.append("PUT manifest")
    return action_log


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="action", required=True)
    s = sub.add_parser("scan", help="Print validated source manifest JSON")
    s.add_argument("--source", type=Path, required=True)
    i = sub.add_parser("ingest", help="Upload and verify immutable HDFS snapshot")
    i.add_argument("--source", type=Path, required=True)
    i.add_argument("--expected", type=Path, required=True)
    i.add_argument("--root", default="/data/olist/raw")
    i.add_argument("--version", default="v1")
    i.add_argument("--replication", type=int, default=2)
    i.add_argument("--hdfs-bin", default="hdfs")
    args = parser.parse_args()
    try:
        if args.action == "scan":
            print(canonical_json(scan_csv(args.source)), end="")
        else:
            expected = json.loads(args.expected.read_text(encoding="utf-8"))
            actions = ingest_files(
                args.source, expected, args.root, args.version,
                args.replication, Hdfs(args.hdfs_bin),
            )
            for action in actions:
                print(action)
            print("OLIST RAW INGESTION OK")
    except (IngestError, OSError, ValueError) as exc:
        print(f"OLIST RAW INGESTION FAILED: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

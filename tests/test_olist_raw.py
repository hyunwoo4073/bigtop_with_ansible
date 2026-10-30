"""Offline tests for the immutable Olist HDFS Raw loader."""
import importlib.util
import json
import tempfile
import unittest
from pathlib import Path

SCRIPT = (Path(__file__).resolve().parents[1] / "roles" / "data_pipeline_olist_raw"
          / "files" / "olist_raw.py")
spec = importlib.util.spec_from_file_location("olist_raw", SCRIPT)
raw = importlib.util.module_from_spec(spec)
spec.loader.exec_module(raw)


class FakeHdfs:
    def __init__(self):
        self.data = {}
        self.folders = set()
        self.replications = {}

    def exists(self, path):
        return path in self.data

    def sha256(self, path):
        import hashlib
        return hashlib.sha256(self.data[path]).hexdigest()

    def read_text(self, path):
        return self.data[path].decode("utf-8")

    def mkdir(self, path):
        self.folders.add(path)

    def put(self, source, dest):
        self.data[dest] = Path(source).read_bytes()

    def mv(self, source, dest):
        self.data[dest] = self.data.pop(source)

    def rm(self, path):
        self.data.pop(path, None)

    def chmod(self, path, mode):
        assert mode == "644"

    def setrep(self, path, replication):
        self.replications[path] = replication


HEADERS = {
    "orders": "order_id,customer_id,order_status,order_purchase_timestamp\na,c,delivered,2018-01-01\n",
    "order_items": "order_id,order_item_id,product_id,price\na,1,p,12.30\n",
    "customers": "customer_id,customer_unique_id\nc,cu\n",
    "products": "product_id,product_category_name\np,toys\n",
    "payments": "order_id,payment_sequential,payment_type,payment_value\na,1,credit_card,12.30\n",
    "reviews": "review_id,order_id,review_score\nr,a,5\n",
    "sellers": "seller_id,seller_zip_code_prefix\ns,12345\n",
    "geolocation": "geolocation_zip_code_prefix,geolocation_lat,geolocation_lng\n12345,-23.1,-46.2\n",
    "category_translation": "product_category_name,product_category_name_english\ntoys,toys\n",
}


class OlistRawTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.path = Path(self.temp.name)
        for name, contents in HEADERS.items():
            (self.path / raw.DATASETS[name][0]).write_text(contents, encoding="utf-8")
        self.hdfs = FakeHdfs()

    def test_scan_manifest_rows_and_sha256(self):
        manifest = raw.scan_csv(self.path)
        self.assertEqual(list(manifest["files"]), sorted(HEADERS))
        self.assertEqual(manifest["files"]["orders"]["rows"], 1)
        self.assertEqual(len(manifest["files"]["orders"]["sha256"]), 64)

    def test_all_nine_dataset_names(self):
        self.assertEqual(set(raw.DATASETS), set(HEADERS))
        self.assertEqual(len(raw.DATASETS), 9)

    def test_missing_one_of_new_files_rejected(self):
        (self.path / "olist_geolocation_dataset.csv").unlink()
        with self.assertRaises(raw.IngestError):
            raw.scan_csv(self.path)

    def test_missing_file_rejected(self):
        (self.path / "olist_orders_dataset.csv").unlink()
        with self.assertRaises(raw.IngestError):
            raw.scan_csv(self.path)

    def test_invalid_header_rejected(self):
        (self.path / "olist_orders_dataset.csv").write_text("order_id\na\n")
        with self.assertRaises(raw.IngestError):
            raw.scan_csv(self.path)

    def test_malformed_record_rejected(self):
        with (self.path / "olist_orders_dataset.csv").open("a") as stream:
            stream.write("bad,short\n")
        with self.assertRaises(raw.IngestError):
            raw.scan_csv(self.path)

    def test_hdfs_immutable_repeated_run(self):
        manifest = raw.scan_csv(self.path)
        first = raw.ingest_files(self.path, manifest, "/data/olist/raw", "v1", 2, self.hdfs)
        self.assertEqual(sum(line.startswith("PUT verified") for line in first), 9)
        self.assertIn("PUT manifest", first)
        second = raw.ingest_files(self.path, manifest, "/data/olist/raw", "v1", 2, self.hdfs)
        self.assertEqual(sum(line.startswith("SKIP verified") for line in second), 9)
        self.assertIn("SKIP manifest unchanged", second)
        self.assertEqual(len(self.hdfs.data), 10)
        stored = json.loads(self.hdfs.data["/data/olist/raw/v1/_MANIFEST.json"])
        self.assertEqual(stored, manifest)
        self.assertEqual(set(self.hdfs.replications.values()), {2})

    def test_source_change_rejected_without_overwrite(self):
        manifest = raw.scan_csv(self.path)
        raw.ingest_files(self.path, manifest, "/data/olist/raw", "v1", 2, self.hdfs)
        (self.path / "olist_orders_dataset.csv").write_text(
            HEADERS["orders"] + "b,c,shipped,2018-01-02\n"
        )
        before = dict(self.hdfs.data)
        with self.assertRaisesRegex(raw.IngestError, "differs"):
            raw.ingest_files(self.path, manifest, "/data/olist/raw", "v1", 2, self.hdfs)
        self.assertEqual(self.hdfs.data, before)

    def test_renamed_snapshot_can_be_published(self):
        manifest = raw.scan_csv(self.path)
        raw.ingest_files(self.path, manifest, "/data/olist/raw", "v1", 2, self.hdfs)
        raw.ingest_files(self.path, manifest, "/data/olist/raw", "v2", 2, self.hdfs)
        self.assertEqual(len(self.hdfs.data), 20)

    def test_existing_hdfs_corruption_rejected(self):
        manifest = raw.scan_csv(self.path)
        raw.ingest_files(self.path, manifest, "/data/olist/raw", "v1", 2, self.hdfs)
        path = "/data/olist/raw/v1/orders/olist_orders_dataset.csv"
        self.hdfs.data[path] = b"CORRUPTED"
        with self.assertRaisesRegex(raw.IngestError, "refusing overwrite"):
            raw.ingest_files(self.path, manifest, "/data/olist/raw", "v1", 2, self.hdfs)
        self.assertEqual(self.hdfs.data[path], b"CORRUPTED")

    def test_stage_checksum_mismatch_does_not_publish(self):
        class BadHdfs(FakeHdfs):
            def put(self, source, dest):
                super().put(source, dest)
                self.data[dest] += b"INVALID"

        hdfs = BadHdfs()
        with self.assertRaisesRegex(raw.IngestError, "checksum mismatch"):
            raw.ingest_files(self.path, raw.scan_csv(self.path), "/data/olist/raw", "v1", 2, hdfs)
        self.assertFalse(hdfs.data)

    def test_bad_destination_or_replication_rejected(self):
        manifest = raw.scan_csv(self.path)
        for path in ("/", "/data/elsewhere", "/data/olist/raw/../other"):
            with self.assertRaises(raw.IngestError):
                raw.ingest_files(self.path, manifest, path, "v1", 2, self.hdfs)
        with self.assertRaises(raw.IngestError):
            raw.ingest_files(self.path, manifest, "/data/olist/raw", "v1", 3, self.hdfs)

    def test_no_overwriting_different_manifest(self):
        manifest = raw.scan_csv(self.path)
        raw.ingest_files(self.path, manifest, "/data/olist/raw", "v1", 2, self.hdfs)
        self.hdfs.data["/data/olist/raw/v1/_MANIFEST.json"] = b"{}"
        with self.assertRaisesRegex(raw.IngestError, "different manifest"):
            raw.ingest_files(self.path, manifest, "/data/olist/raw", "v1", 2, self.hdfs)

    def test_role_targets_only_master(self):
        import yaml
        root = SCRIPT.parents[3]
        play = yaml.safe_load((root / "playbooks/45-olist-hdfs-raw.yml").read_text())
        self.assertEqual(play[0]["hosts"], "master")
        self.assertTrue(play[0]["become"])
        tasks = yaml.safe_load((root / "roles/data_pipeline_olist_raw/tasks/main.yml").read_text())
        self.assertTrue(any(t.get("always") for t in tasks))


if __name__ == "__main__":
    unittest.main()

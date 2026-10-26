import importlib.util
import tempfile
import unittest
from pathlib import Path

MODULE = Path(__file__).resolve().parents[1] / "roles" / "web_console_pg_observability" / "files" / "pg_backup_metrics.py"
spec = importlib.util.spec_from_file_location("backup_metrics", MODULE)
mod = importlib.util.module_from_spec(spec)
spec.loader.exec_module(mod)


class BackupObserverTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.dir = Path(self.temp.name)
        self.config = {
            "database": "bigtop_console",
            "backup_dir": str(self.dir),
            "backup_timer": "backup.timer",
            "minimum_bytes": 10,
        }

    def tearDown(self):
        self.temp.cleanup()

    def test_no_archives_not_healthy(self):
        result = mod.collect(self.config, now=1000, verify=lambda p: True, check_timer=lambda: True)
        self.assertEqual(result["archive_count"], 0)
        self.assertEqual(result["latest_archive_valid"], 0)
        self.assertEqual(result["last_valid_timestamp_seconds"], 0)
        self.assertEqual(result["timer_active"], 1)

    def test_valid_archive(self):
        file = self.dir / "bigtop_console_20261026T150000Z_123.dump"
        file.write_bytes(b"a" * 30)
        result = mod.collect(self.config, now=2000, verify=lambda p: True, check_timer=lambda: True)
        self.assertEqual(result["archive_count"], 1)
        self.assertEqual(result["latest_archive_valid"], 1)
        self.assertEqual(result["latest_archive_size_bytes"], 30)
        self.assertGreater(result["last_valid_timestamp_seconds"], 0)

    def test_corrupt_latest_archive_is_unhealthy(self):
        file = self.dir / "bigtop_console_20261026T150000Z_123.dump"
        file.write_bytes(b"a" * 30)
        result = mod.collect(self.config, now=2000, verify=lambda p: False, check_timer=lambda: False)
        self.assertEqual(result["latest_archive_valid"], 0)
        self.assertEqual(result["last_valid_timestamp_seconds"], 0)
        self.assertEqual(result["timer_active"], 0)

    def test_ignore_partial_and_unknown_files(self):
        (self.dir / ".backup.partial").write_bytes(b"a" * 20)
        (self.dir / "secret.dump").write_bytes(b"a" * 20)
        result = mod.collect(self.config, now=2000, verify=lambda p: True, check_timer=lambda: True)
        self.assertEqual(result["archive_count"], 0)

    def test_atomic_metric_content(self):
        output = self.dir / "metrics.prom"
        values = mod.collect(self.config, now=1234, verify=lambda p: True, check_timer=lambda: True)
        mod.write_atomic(output, mod.format_metrics(values))
        self.assertIn("bigtop_web_console_pg_backup_archive_count 0", output.read_text())
        self.assertEqual(oct(output.stat().st_mode & 0o777), '0o644')


if __name__ == "__main__":
    unittest.main()

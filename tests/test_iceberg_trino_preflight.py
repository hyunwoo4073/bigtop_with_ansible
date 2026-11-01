import importlib.util
import json
import unittest
from unittest.mock import patch
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def load_module(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class Step55ProbeTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.probe = load_module("step55_probe", ROOT / "scripts/55-probe-node.py")
        cls.review = load_module("step55_review", ROOT / "scripts/55-review-preflight.py")

    def test_inaccessible_path_is_reported_not_fatal(self):
        with patch.object(Path, "exists", side_effect=PermissionError("access denied")):
            self.assertEqual(self.probe.safe_path_exists("/opt/restricted"), "permission_denied")

    def test_os_error_is_reported_not_fatal(self):
        with patch.object(Path, "exists", side_effect=OSError("unavailable")):
            self.assertEqual(self.probe.safe_path_exists("/opt/unavailable"), "unavailable")

    def test_existing_and_missing_paths_preserve_boolean_format(self):
        with patch.object(Path, "exists", side_effect=[True, False]):
            self.assertIs(self.probe.safe_path_exists("/opt/existing"), True)
            self.assertIs(self.probe.safe_path_exists("/opt/missing"), False)

    def test_collected_properties_are_whitelisted(self):
        self.assertNotIn("javax.jdo.option.ConnectionPassword", self.probe.XML_WHITELIST)
        self.assertNotIn("hive.metastore.password", self.probe.XML_WHITELIST)

    def test_services_include_trino_and_metastore(self):
        self.assertIn("trino", self.probe.SERVICES)
        self.assertIn("hive-metastore", self.probe.SERVICES)

    def test_report_does_not_recommend_install_automatically(self):
        report = {"hosts": {"worker3": {"memory": {"MemTotal_kib": 4194304,
                                                       "MemAvailable_kib": 524288},
                                         "java_version": 'openjdk version "11"',
                                         "services": {}, "listening_ports": {}}}}
        rendered = self.review.lines(report)
        self.assertIn("overloading worker3", rendered)
        self.assertIn("inspection only", rendered)

    def test_script_output_json(self):
        import subprocess
        import sys
        p = subprocess.run([sys.executable, str(ROOT / "scripts/55-probe-node.py")],
                           check=True, capture_output=True, text=True, timeout=35)
        self.assertIn("memory", json.loads(p.stdout))

    def test_no_secret_key_names_in_probe(self):
        content = (ROOT / "scripts/55-probe-node.py").read_text()
        self.assertNotIn("ConnectionPassword\"", content)

    def test_playbook_has_no_remote_mutation(self):
        p = (ROOT / "playbooks/55-iceberg-trino-preflight.yml").read_text()
        self.assertIn("changed_when: false", p)
        self.assertNotIn("ansible.builtin.package:", p)
        self.assertNotIn("ansible.builtin.systemd_service:", p)


if __name__ == "__main__":
    unittest.main()

"""Static deploy safety checks; no live Docker or credentials required."""
from pathlib import Path
import importlib.util
import os
import tempfile
import unittest
import yaml
from jinja2 import Environment, StrictUndefined

BASE = Path(__file__).resolve().parents[1]
ROLE = BASE / "roles/governance_openmetadata"

class GovernanceDeploymentTests(unittest.TestCase):
    def test_bootstrap_secrets_first_run_and_idempotence(self):
        spec = importlib.util.spec_from_file_location("om_env_gen", ROLE / "files/init_env.py")
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        with tempfile.TemporaryDirectory() as d:
            env_path = Path(d) / ".env"
            module.generate(env_path)
            first = env_path.read_bytes()
            self.assertEqual(env_path.stat().st_mode & 0o777, 0o600)
            self.assertEqual(len(first.decode().splitlines()), 2)
            module.generate(env_path)
            self.assertEqual(first, env_path.read_bytes())

    def test_secrets_never_replace_corrupt_env(self):
        spec = importlib.util.spec_from_file_location("om_env_gen", ROLE / "files/init_env.py")
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        with tempfile.TemporaryDirectory() as d:
            env_path = Path(d) / ".env"
            env_path.write_text("SOMETHING=else\n")
            with self.assertRaises(RuntimeError):
                module.generate(env_path)
            self.assertEqual(env_path.read_text(), "SOMETHING=else\n")

    def test_compose_has_only_essential_services_and_private_storage(self):
        template = Environment(undefined=StrictUndefined).from_string((ROLE / "templates/compose.yaml.j2").read_text())
        rendered = template.render(governance_openmetadata_mysql_image="mysql:8.0", governance_openmetadata_es_image="docker.elastic.co/elasticsearch/elasticsearch:9.3.0", governance_openmetadata_server_image="openmetadata/server:2.0.6", governance_openmetadata_bind_ip="192.158.2.104", governance_openmetadata_ui_port=8585, governance_openmetadata_admin_port=8586)
        config = yaml.safe_load(rendered)
        services = config["services"]
        self.assertEqual(set(services), {"mysql", "elasticsearch", "execute-migrate-all", "openmetadata-server"})
        self.assertNotIn("ports", services["mysql"])
        self.assertNotIn("ports", services["elasticsearch"])
        self.assertTrue(all(not x.startswith("0.0.0.0:") for x in services["openmetadata-server"]["ports"]))
        self.assertEqual(services["openmetadata-server"]["ports"][0], "192.158.2.104:8585:8585")
        self.assertEqual(services["openmetadata-server"]["ports"][1], "127.0.0.1:8586:8586")
        self.assertEqual(services["openmetadata-server"]["depends_on"]["execute-migrate-all"]["condition"], "service_completed_successfully")
        self.assertIn("mysql-data", config["volumes"])
        self.assertIn("es-data", config["volumes"])
        self.assertEqual(services["openmetadata-server"]["environment"]["DB_USER_PASSWORD"], "${OM_DB_PASSWORD:?Set OM_DB_PASSWORD in protected .env}")

    def test_main_playbook_scopes_governance_only(self):
        play = yaml.safe_load((BASE / "playbooks/41-governance-openmetadata.yml").read_text())
        self.assertEqual(play[0]["hosts"], "governance_nodes")
        self.assertEqual(play[0]["roles"][0]["role"], "governance_openmetadata")

    def test_systemd_stops_without_removing_volumes(self):
        unit = (ROLE / "templates/openmetadata.service.j2").read_text()
        self.assertIn(" up --detach", unit)
        self.assertNotIn("--wait", unit)
        self.assertIn(".env stop", unit)
        self.assertNotIn("down -v", unit)

if __name__ == "__main__":
    unittest.main()

import unittest
from pathlib import Path
import yaml

ROOT = Path(__file__).resolve().parents[1]
ROLE = ROOT / 'roles' / 'kafka_ui64c'

class KafkaUI64CTests(unittest.TestCase):
    def test_valid_yaml(self):
        for p in [ROOT / 'playbooks/64c-kafka-web-ui.yml', *ROLE.rglob('*.yml')]:
            with self.subTest(p=p):
                self.assertTrue(yaml.safe_load(p.read_text()))

    def test_default_plan(self):
        cfg = yaml.safe_load((ROLE / 'defaults/main.yml').read_text())
        self.assertEqual(cfg['kafka_ui64c_mode'], 'plan')
        self.assertEqual(cfg['kafka_ui64c_host_port'], 18081)
        self.assertEqual(cfg['kafka_ui64c_bind_ip'], '192.158.2.104')
        self.assertEqual(len(cfg['kafka_ui64c_brokers']), 3)
        self.assertNotIn('latest', cfg['kafka_ui64c_image'])

    def test_role_vars_prefixed(self):
        cfg = yaml.safe_load((ROLE / 'defaults/main.yml').read_text())
        self.assertTrue(all(k.startswith('kafka_ui64c_') for k in cfg))

    def test_read_only_no_dynamic_config(self):
        t = (ROLE / 'tasks/install.yml').read_text()
        self.assertIn('KAFKA_CLUSTERS_0_READONLY=true', t)
        self.assertIn('DYNAMIC_CONFIG_ENABLED=false', t)
        self.assertIn('no-new-privileges', t)
        self.assertIn('--memory', t)
        self.assertIn('--pull=never', t)

    def test_no_unauthorized_service_changes(self):
        t = '\n'.join(p.read_text() for p in (ROLE / 'tasks').glob('*.yml'))
        for forbidden in ('docker system prune', 'kafka-storage.sh format', 'kafka-topics.sh --delete', 'systemctl restart', 'apt install', 'argv: [docker, pull]'):
            self.assertNotIn(forbidden, t)

    def test_playbook_safe_default_conditions(self):
        t = (ROOT / 'playbooks/64c-kafka-web-ui.yml').read_text()
        self.assertIn("default('plan')", t)
        self.assertIn("== 'apply'", t)

    def test_not_committing_binaries(self):
        # Check only Step64-C sources; existing vendor/ archives are intentionally present.
        files = list(ROLE.rglob('*'))
        self.assertFalse(any(p.suffix in ('.jar', '.tar', '.tgz') for p in files if p.is_file()))

    def test_only_metadata_host(self):
        cfg = yaml.safe_load((ROOT / 'playbooks/64c-kafka-web-ui.yml').read_text())
        self.assertEqual(cfg[0]['hosts'], 'metadata')

    def test_verify_health_endpoint(self):
        t = (ROLE / 'tasks/verify.yml').read_text()
        self.assertIn('/actuator/health', t)
        self.assertIn('STEP64C_KAFBAT_UI_READY', t)

if __name__ == '__main__':
    unittest.main()

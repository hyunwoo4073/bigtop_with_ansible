import importlib.util
import os
import subprocess
import pathlib
import re
import tempfile
import unittest

ROOT = pathlib.Path(__file__).resolve().parents[1]


def load(module_name, rel):
    spec = importlib.util.spec_from_file_location(module_name, ROOT / rel)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


class KafkaKRaft64Tests(unittest.TestCase):
    def test_yaml_parses(self):
        import yaml
        for p in sorted((ROOT / 'roles/kafka_kraft64').rglob('*.yml')) + list((ROOT / 'playbooks').glob('64a-*.yml')):
            self.assertIsNotNone(yaml.safe_load(p.read_text()))

    def test_role_vars_have_lint_prefix(self):
        defaults = (ROOT / 'roles/kafka_kraft64/defaults/main.yml').read_text()
        for line in defaults.splitlines():
            if ':' in line and not line.startswith((' ', '#', '-')):
                self.assertTrue(line.startswith('kafka_kraft64_'), line)

    def test_static_three_voter_config(self):
        text = (ROOT / 'roles/kafka_kraft64/templates/server.properties.j2').read_text()
        self.assertIn('process.roles=broker,controller', text)
        self.assertIn('controller.quorum.voters=1@192.158.2.101:{{ kafka_kraft64_controller_port }}', text)
        self.assertIn('3@192.158.2.103:{{ kafka_kraft64_controller_port }}', text)
        self.assertIn('advertised.listeners=PLAINTEXT://', text)
        self.assertIn('min.insync.replicas=2', text)
        self.assertIn('auto.create.topics.enable=false', text)

    def test_controller_default_avoids_alertmanager(self):
        import yaml
        config = yaml.safe_load((ROOT / 'roles/kafka_kraft64/defaults/main.yml').read_text())
        self.assertEqual(19093, config['kafka_kraft64_controller_port'])
        self.assertEqual(9092, config['kafka_kraft64_client_port'])
        self.assertNotEqual(config['kafka_kraft64_client_port'], config['kafka_kraft64_controller_port'])

    def test_all_controller_endpoints_use_configured_port(self):
        import yaml
        from jinja2 import Template
        defaults = yaml.safe_load((ROOT / 'roles/kafka_kraft64/defaults/main.yml').read_text())
        rendered = Template((ROOT / 'roles/kafka_kraft64/templates/server.properties.j2').read_text()).render(
            **defaults, inventory_hostname='worker3')
        self.assertIn('controller.quorum.voters=1@192.158.2.101:19093,2@192.158.2.102:19093,3@192.158.2.103:19093', rendered)
        self.assertIn('listeners=PLAINTEXT://192.158.2.103:9092,CONTROLLER://192.158.2.103:19093', rendered)
        self.assertIn('advertised.listeners=PLAINTEXT://192.158.2.103:9092', rendered)
        self.assertNotIn(':9093', rendered)

    def test_preflight_checks_configured_ports_and_aborts_full_playbook(self):
        preflight = (ROOT / 'roles/kafka_kraft64/tasks/preflight.yml').read_text()
        playbook = (ROOT / 'playbooks/64a-kafka-kraft-cluster.yml').read_text()
        self.assertIn('kafka_kraft64_controller_port | string', preflight)
        self.assertIn('kafka_kraft64_client_port | string', preflight)
        self.assertIn("kafka_kraft64_mode in ['preflight', 'install']", preflight)
        self.assertIn('any_errors_fatal: true', playbook)

    def test_protected_kraft_storage_probes_use_privilege_escalation(self):
        # /var/lib/bigtop-kafka-kraft64/{data,metadata} are mode 0750
        # and not traversable by the Ansible SSH account.
        import yaml
        cases = {
            'install.yml': [
                'Read metadata directory format marker',
                'Read data directory format marker',
                'Inspect existing files in unformatted KRaft metadata directory',
                'Inspect existing files in unformatted KRaft topic directory',
            ],
            'preflight.yml': [
                'Check if worker3 already has formatted Kafka storage',
                'Check if worker3 persisted the Kafka cluster identity',
            ],
        }
        for filename, task_names in cases.items():
            tasks = yaml.safe_load((ROOT / 'roles/kafka_kraft64/tasks' / filename).read_text())
            indexed = {task['name']: task for task in tasks}
            for name in task_names:
                with self.subTest(filename=filename, name=name):
                    self.assertTrue(indexed[name].get('become', False), name)
                    self.assertNotIn('become_user', indexed[name])

    def test_existing_storage_never_reformatted_automatically(self):
        install = (ROOT / 'roles/kafka_kraft64/tasks/install.yml').read_text()
        import yaml
        tasks = yaml.safe_load(install)
        fmt = next(t for t in tasks if t['name'] == 'Format only NEW and empty KRaft storage directories once')
        self.assertEqual('not kafka_kraft64_meta_marker.stat.exists', fmt['when'])
        self.assertEqual('{{ kafka_kraft64_service_user }}', fmt['become_user'])
        self.assertTrue(fmt['become'])
        self.assertIn('Refuse partial storage format', install)
        self.assertIn('Refuse formatting any storage directory that already contains files', install)

    def test_no_global_java_changes(self):
        paths = list((ROOT / 'roles/kafka_kraft64').rglob('*'))
        content = '\n'.join(x.read_text() for x in paths if x.is_file() and x.suffix in ('.yml','.j2'))
        for phrase in ('update-alternatives', '/usr/lib/jvm', 'systemctl stop hadoop', 'hdfs dfs -rm'):
            self.assertNotIn(phrase, content)
        self.assertIn('jdk17', content)

    def test_no_reformat_existing_log(self):
        t = (ROOT / 'roles/kafka_kraft64/tasks/install.yml').read_text()
        self.assertIn('Refuse partial storage format', t)
        self.assertIn('Refuse existing storage with different cluster ID', t)
        self.assertIn('when: not kafka_kraft64_meta_marker.stat.exists', t)
        self.assertNotIn('--ignore-formatted', t)

    def test_safe_created_cluster_id(self):
        mod = load('kafka_cluster_id64', 'roles/kafka_kraft64/files/kafka_cluster_id.py')
        with tempfile.TemporaryDirectory() as d:
            path = pathlib.Path(d) / 'id'
            first, created = mod.create_or_read(path)
            self.assertTrue(created)
            self.assertEqual(22, len(first))
            second, created_again = mod.create_or_read(path)
            self.assertEqual(first, second)
            self.assertFalse(created_again)

    def test_cluster_id_invalid_fails(self):
        mod = load('kafka_cluster_id64_bad', 'roles/kafka_kraft64/files/kafka_cluster_id.py')
        with tempfile.TemporaryDirectory() as d:
            path = pathlib.Path(d) / 'id'
            path.write_text('BAD')
            with self.assertRaises(RuntimeError):
                mod.create_or_read(path)

    def test_official_sha512_parser(self):
        mod = load('fetch64', 'scripts/64a-fetch-kafka-broker.py')
        digest = 'a' * 128
        self.assertEqual(mod.parse_checksum(f'{digest}  kafka_2.13-4.2.2.tgz'), digest)
        with self.assertRaises(ValueError):
            mod.parse_checksum('bad')

    def test_apache_actual_multiline_sha512(self):
        fetch = load('fetch64_apache', 'scripts/64a-fetch-kafka-broker.py')
        verify = load('verify64_apache', 'scripts/64a-verify-kafka-assets.py')
        official = (
            'kafka_2.13-4.2.2.tgz: ACB9961F 7EC72F5F F1544AFD FBCCA390 30F2C9CA 0D80E846\n'
            '                      6CF961BD 83AE2030 BAFC28D2 577F3F1A 099B39C0 6E6252B8\n'
            '                      886C1EAF 6477F2A2 ACA8BE8B 68C0B25E\n'
        )
        self.assertEqual(fetch.parse_checksum(official), fetch.PINNED_SHA512)
        self.assertEqual(verify.checksum(official, 128), fetch.PINNED_SHA512)
        self.assertEqual(verify.PINNED_KAFKA_SHA512, fetch.PINNED_SHA512)

    def test_sha512_parser_rejects_wrong_filename_or_corrupt_group(self):
        fetch = load('fetch64_badformat', 'scripts/64a-fetch-kafka-broker.py')
        verify = load('verify64_badformat', 'scripts/64a-verify-kafka-assets.py')
        digest = fetch.PINNED_SHA512
        for bad in (
            'other-file.tgz: ' + ' '.join(digest[i:i+8] for i in range(0, 128, 8)),
            'kafka_2.13-4.2.2.tgz: 1234 456789AB',
            'kafka_2.13-4.2.2.tgz: ' + ' '.join(digest[i:i+8] for i in range(0, 120, 8)),
            '<html>download not available</html>',
        ):
            with self.subTest(value=bad[:40]):
                with self.assertRaises(ValueError):
                    fetch.parse_checksum(bad)
                with self.assertRaises(ValueError):
                    verify.checksum(bad, 128)

    def test_sha256_jdk_sha512_kafka(self):
        verify = load('verify64', 'scripts/64a-verify-kafka-assets.py')
        self.assertEqual(128, len(verify.checksum('a' * 128, 128)))
        self.assertEqual(64, len(verify.checksum('b' * 64, 64)))

    def test_smoke_uses_scoped_test_topic_and_offset(self):
        text = (ROOT / 'scripts/64a-smoke-kafka.sh').read_text()
        self.assertIn('bigtop_kafka64_smoke_v1', text)
        self.assertIn('START_OFFSET', text)
        self.assertIn('acks=all', text)
        self.assertNotIn('--delete', text)

    def test_smoke_uses_dedicated_java_not_inherited_java_home(self):
        script = (ROOT / 'scripts/64a-smoke-kafka.sh').read_text()
        self.assertIn('KAFKA64_JAVA_HOME:-/opt/bigtop-kafka-kraft64/jdk17', script)
        self.assertNotIn('JAVA_HOME="${JAVA_HOME:-', script)
        self.assertIn('JAVA_VERSION=', script)

    def test_smoke_java_override_with_mocked_cli(self):
        with tempfile.TemporaryDirectory() as d:
            base = pathlib.Path(d)
            jdk = base/'jdk17'/'bin'
            jdk.mkdir(parents=True)
            java = jdk/'java'
            java.write_text("#!/bin/sh\necho 'openjdk version \"17.0.17\"' >&2\n")
            java.chmod(0o755)
            kafka_bin = base/'kafka'/'bin'
            kafka_bin.mkdir(parents=True)
            mock = """#!/bin/sh
set -eu
echo "$JAVA_HOME" >> "$MOCK_JAVA_LOG"
case "$(basename "$0")" in
  kafka-topics.sh)
    case " $* " in
      *" --describe "*) echo 'Topic: bigtop_kafka64_smoke_v1 PartitionCount: 1 ReplicationFactor: 3 Configs: min.insync.replicas=2'; echo 'Topic: bigtop_kafka64_smoke_v1 Partition: 0 Leader: 1 Replicas: 1,2,3 Isr: 1,2,3';;
    esac;;
  kafka-get-offsets.sh) echo 'bigtop_kafka64_smoke_v1:0:0';;
  kafka-console-producer.sh) cat > "$MOCK_STATE";;
  kafka-console-consumer.sh) cat "$MOCK_STATE";;
esac
"""
            for name in ('kafka-topics.sh','kafka-get-offsets.sh', 'kafka-console-producer.sh','kafka-console-consumer.sh'):
                f = kafka_bin/name
                f.write_text(mock)
                f.chmod(0o755)
            log = base/'java.log'
            env = {**os.environ, 'JAVA_HOME':'/invalid/java8', 'KAFKA64_JAVA_HOME':str(jdk.parent),
                   'KAFKA_HOME':str(kafka_bin.parent), 'MOCK_JAVA_LOG':str(log),
                   'MOCK_STATE':str(base/'marker')}
            p = subprocess.run(['bash',str(ROOT/'scripts/64a-smoke-kafka.sh')],
                               env=env, capture_output=True, text=True, timeout=10)
            self.assertEqual(0, p.returncode, p.stderr)
            self.assertIn('STEP64A_KAFKA_PRODUCE_CONSUME_PASS', p.stdout)
            self.assertTrue(all(x == str(jdk.parent) for x in log.read_text().splitlines()))

    def test_install_requires_explicit_mode(self):
        text = (ROOT / 'playbooks/64a-kafka-kraft-cluster.yml').read_text()
        self.assertIn("kafka_kraft64_mode == 'install'", text)
        self.assertIn('preflight', text)

    def test_vendor_binaries_not_shipped_in_role(self):
        for p in (ROOT / 'roles/kafka_kraft64').rglob('*'):
            self.assertNotIn(p.suffix, ('.jar', '.tgz', '.tar', '.gz'))


if __name__ == '__main__':
    unittest.main()

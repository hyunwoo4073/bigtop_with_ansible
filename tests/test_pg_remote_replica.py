"""Offline simulation; no real SSH, PostgreSQL, or permanent files are touched."""
import hashlib
import os
import shlex
import subprocess
import tempfile
import time
import unittest
from pathlib import Path
from jinja2 import Environment, StrictUndefined

ROOT = Path(__file__).resolve().parents[1]
TPL = ROOT / 'roles/web_console_pg_remote_replica/templates'


def render(filename, **vars):
    env = Environment(undefined=StrictUndefined)
    env.filters['quote'] = shlex.quote
    return env.from_string((TPL / filename).read_text()).render(**vars)


class ReplicationTests(unittest.TestCase):
    def setUp(self):
        tmp = tempfile.TemporaryDirectory(prefix='replica-test-')
        self.addCleanup(tmp.cleanup)
        self.base = Path(tmp.name)
        self.source = self.base / 'source'
        self.dest = self.base / 'dest'
        self.mockbin = self.base / 'bin'
        for d in (self.source, self.dest, self.mockbin):
            d.mkdir()
        self.script = self.base / 'script.sh'
        self.env = dict(os.environ, PATH=str(self.mockbin) + ':' + os.environ['PATH'],
                        REMOTE_SHA='0'*64, MOCK_MARKER=str(self.base/'called'))
        self.mock('id', 'echo postgres')
        self.mock('ssh', "printf '%s  archive\\n' \"$REMOTE_SHA\"")
        self.mock('pg_restore', '[[ "${FAIL_RESTORE:-0}" != 1 ]]')
        self.mock('rsync', 'touch "$MOCK_MARKER"')

    def mock(self, cmd, shell):
        out = self.mockbin / cmd
        out.write_text('#!/bin/bash\nset -e\n' + shell + '\n')
        out.chmod(0o755)

    def execute(self):
        return subprocess.run(['bash', str(self.script)], env=self.env,
                              capture_output=True, text=True)

    def sender(self):
        self.script.write_text(render('sender_sync.sh.j2',
            web_console_pg_remote_replica_database='bigtop_console',
            web_console_pg_remote_replica_source_dir=str(self.source),
            web_console_pg_remote_replica_dest_dir='/var/backups/bigtop-web-console-replica/postgresql',
            web_console_pg_remote_replica_dest_user='bigtop_pg_replica',
            web_console_pg_remote_replica_receiver_address='192.0.2.1',
            web_console_pg_remote_replica_ssh_dir=str(self.base),
            web_console_pg_remote_replica_key_name='testkey'))

    def archive(self, filename='bigtop_console_20261027T032500Z_123.dump'):
        file = self.source / filename
        file.write_bytes(b'archive-mock-contents')
        self.env['REMOTE_SHA'] = hashlib.sha256(file.read_bytes()).hexdigest()
        return file

    def receiver(self):
        self.mock('id', 'echo bigtop_pg_replica')
        self.script.write_text(render('receiver_prune.sh.j2',
            web_console_pg_remote_replica_database='bigtop_console',
            web_console_pg_remote_replica_dest_dir=str(self.dest),
            web_console_pg_remote_replica_dest_user='bigtop_pg_replica',
            web_console_pg_remote_replica_retention_days=30))

    def test_playbook_imports_all_three_role_entrypoints(self):
        import yaml
        playbook = yaml.safe_load((ROOT / 'playbooks/38-web-console-pg-remote-replica.yml').read_text())
        self.assertEqual(len(playbook), 3)
        self.assertEqual([
            play['tasks'][0]['ansible.builtin.import_role']['tasks_from']
            for play in playbook
        ], ['source_prepare', 'receiver', 'source_deploy'])
        for play in playbook:
            self.assertNotIn('roles', play)
            self.assertEqual(play['tasks'][0]['ansible.builtin.import_role']['name'], 'web_console_pg_remote_replica')

    def test_old_variable_prefix_absent(self):
        for path in (ROOT / 'roles/web_console_pg_remote_replica').rglob('*'):
            if path.is_file() and path.suffix in ('.yml', '.j2'):
                self.assertNotIn('web_console_pg_replica_', path.read_text(), str(path))
        self.assertNotIn('web_console_pg_replica_',
                         (ROOT / 'playbooks/38-web-console-pg-remote-replica.yml').read_text())

    def test_successful_replication(self):
        self.sender(); self.archive()
        result = self.execute()
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn('REPLICA OK', result.stdout)
        self.assertTrue((self.base/'called').exists())

    def test_missing_archive_does_not_copy(self):
        self.sender()
        self.assertNotEqual(self.execute().returncode, 0)
        self.assertFalse((self.base/'called').exists())

    def test_invalid_archive_blocks_copy(self):
        self.sender(); self.archive(); self.env['FAIL_RESTORE'] = '1'
        self.assertNotEqual(self.execute().returncode, 0)
        self.assertFalse((self.base/'called').exists())

    def test_invalid_filename_blocks_copy(self):
        self.sender(); self.archive('bigtop_console_invalid.dump')
        self.assertNotEqual(self.execute().returncode, 0)
        self.assertFalse((self.base/'called').exists())

    def test_sha256_mismatch_detected(self):
        self.sender(); self.archive(); self.env['REMOTE_SHA'] = '0'*64
        result = self.execute()
        self.assertNotEqual(result.returncode, 0)
        self.assertIn('SHA-256 mismatch', result.stderr)

    def test_prune_keeps_oldest_when_no_recent(self):
        self.receiver()
        old = self.dest/'bigtop_console_20260801T000000Z_123.dump'
        old.write_bytes(b'old')
        stamp = time.time()-40*86400
        os.utime(old,(stamp,stamp))
        self.assertEqual(self.execute().returncode,0)
        self.assertTrue(old.exists())

    def test_prune_only_deletes_expired_with_newer_copy(self):
        self.receiver()
        old = self.dest/'bigtop_console_20260801T000000Z_123.dump'
        old.write_bytes(b'old')
        stamp = time.time()-40*86400
        os.utime(old,(stamp,stamp))
        new = self.dest/'bigtop_console_20261027T000000Z_123.dump'
        new.write_bytes(b'new')
        self.assertEqual(self.execute().returncode,0)
        self.assertFalse(old.exists())
        self.assertTrue(new.exists())

if __name__ == '__main__':
    unittest.main()

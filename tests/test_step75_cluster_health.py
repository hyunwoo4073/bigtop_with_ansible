"""Offline unit tests for Step75 parsers and conservative behavior."""
import importlib.util
from pathlib import Path
import subprocess
import sys

BACKEND = Path(__file__).parents[1] / 'roles/web_console/files/kafka_ops75.py'
spec = importlib.util.spec_from_file_location('kafka_ops75_test', BACKEND)
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)


def test_partition_health():
    output = '''Topic: orders TopicId: ABC PartitionCount: 2 ReplicationFactor: 3 Configs:
Topic: orders Partition: 0 Leader: 1 Replicas: 1,2,3 Isr: 1,2,3 Elr: LastKnownElr:
Topic: orders Partition: 1 Leader: -1 Replicas: 1,2,3 Isr: 1,2
Topic: other Partition: 0 Leader: 2 Replicas: 1,2 Isr: 1,2
'''
    result = module._parse_topic_describe(output)
    assert result['partition_count'] == 3
    assert result['under_replicated_count'] == 1
    assert result['leader_missing_count'] == 1
    assert result['anomalies'][0]['topic'] == 'orders'
    assert result['anomalies'][0]['partition'] == 1


def test_empty_partition_metadata_is_not_success():
    out = module._parse_topic_describe('Topic: x PartitionCount: 0')
    assert out['parse_complete'] is False
    assert out['partition_count'] == 0


def test_quorum_old_format():
    result = module._parse_quorum_status('''ClusterId: ABC
LeaderId: 2
LeaderEpoch: 4
HighWatermark: 500
MaxFollowerLag: 0
CurrentVoters: [1, 2, 3]
CurrentObservers: []
''')
    assert result['leader_id'] == 2
    assert result['current_voters'] == [1, 2, 3]
    assert result['parse_complete']


def test_quorum_new_format():
    result = module._parse_quorum_status('''LeaderId: 1
CurrentVoters: [{id=1, directoryId=ABC}, {id=2, directoryId=DEF}]
''')
    assert result['current_voters'] == [1, 2]


def test_quorum_without_leader_not_success():
    result = module._parse_quorum_status('LeaderId: -1\nCurrentVoters: [1,2,3]')
    assert not result['parse_complete']


def test_broker_cli_parser():
    out = '''broker-1:9092 (id: 1 rack: null) -> (
\tProduce(0): 0 to 9 [usable: 9]
broker-2:9092 (id: 2 rack: null) -> (
'''
    parsed = module._parse_brokers(out)
    assert parsed['broker_count'] == 2
    assert parsed['brokers'][0] == {'id': 1, 'endpoint': 'broker-1:9092'}


def test_read_only_command_whitelist():
    import pytest
    with pytest.raises(ValueError):
        module._read_only_cli('kafka-topics.sh', ['--delete'])
    with pytest.raises(ValueError):
        module._read_only_cli('kafka-consumer-groups.sh', ['--reset-offsets'])


def test_api_reports_errors_without_claiming_healthy(monkeypatch):
    monkeypatch.setattr(module, '_read_only_cli', lambda *a, **kw: (None, 'unavailable'))
    response = module.kafka_cluster_health()
    assert not response['ok']
    assert len(response['errors']) == 3
    assert response['partition_health'] is None
    assert response['read_only'] is True


def test_installer_patches_step70_narrowly_and_is_idempotent(tmp_path):
    installer = Path(__file__).parents[1] / 'scripts/install_step75.py'
    repo = tmp_path / 'repo'
    base = Path(__file__).parents[1]
    source_base = Path(__file__).parents[1] / 'tests/_fixtures_step70'
    (repo / 'playbooks').mkdir(parents=True)
    (repo / 'playbooks/70-kafka-web-console.yml').write_text('---\n')
    python_file = repo / 'roles/web_console/files/kafka_ops70.py'
    react_file = repo / 'roles/web_console/files/frontend/src/KafkaOperationsView.jsx'
    python_file.parent.mkdir(parents=True)
    react_file.parent.mkdir(parents=True)
    python_file.write_text("from fastapi import APIRouter\nrouter = APIRouter()\n@router.get('/api/ops/kafka/overview')\ndef read():\n    return {}\n")
    react_file.write_text("export default function KafkaOperationsView() {\n  return (<div>\n      <section className=\"card\">\n        <h3>Kafka Brokers · TCP connectivity</h3>\n      </section>\n  </div>)\n}\n")
    cmd = [sys.executable, str(installer), str(repo)]
    run = subprocess.run(cmd, capture_output=True, text=True)
    assert run.returncode == 0, run.stderr
    assert 'STEP75_INSTALL_PASS' in run.stdout
    assert python_file.read_text().count('router.include_router(kafka_ops75_router)') == 1
    assert react_file.read_text().count('<KafkaHealthPanel75 />') == 1
    assert (repo / 'roles/web_console/files/kafka_ops75.py').exists()
    second = subprocess.run(cmd, capture_output=True, text=True)
    assert second.returncode == 0, second.stderr
    assert 'STEP75_INSTALL_ALREADY_APPLIED' in second.stdout


def test_install_refuses_divergent_existing_source(tmp_path):
    # Installer must not overwrite a locally edited file under the same new path.
    installer = Path(__file__).parents[1] / 'scripts/install_step75.py'
    repo = tmp_path / 'repo'
    (repo / 'playbooks').mkdir(parents=True)
    (repo / 'playbooks/70-kafka-web-console.yml').write_text('---\n')
    candidate = repo / 'roles/web_console/files/kafka_ops75.py'
    candidate.parent.mkdir(parents=True)
    candidate.write_text('local edits\n')
    proc = subprocess.run([sys.executable, str(installer), str(repo)],
                          capture_output=True, text=True)
    assert proc.returncode != 0
    assert 'Refusing to overwrite' in proc.stderr

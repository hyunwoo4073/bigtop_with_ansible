import ast
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
APP = (ROOT / 'roles/web_console/templates/app.py.j2').read_text()
MAIN = (ROOT / 'roles/web_console/files/frontend/src/main.jsx').read_text()


def test_brokers_in_standard_components_registry():
    assert APP.count('"group": "Kafka", "component": "Broker ') == 3
    for host in ('worker1', 'worker2', 'worker3'):
        assert f'"host": "{host}", "service": "bigtop-kafka-kraft64", "read_only": True' in APP


def test_lifecycle_backend_guard():
    assert 'if component.get("read_only"):' in APP
    assert 'status_code=403' in APP


def test_kafka_is_services_group_not_standalone_view():
    assert "{ key: 'kafka', label: 'Kafka' }" not in MAIN
    assert "view === 'kafka'" not in MAIN
    assert "groupFilter === 'Kafka' && <KafkaOperationsView" in MAIN
    assert 'component.read_only' in MAIN


def test_template_python_syntax():
    ast.parse(APP)

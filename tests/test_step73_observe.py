"""Offline checks of alert probe observation and safe display."""
import importlib.util
from pathlib import Path

PATH = Path(__file__).resolve().parents[1] / 'roles/olist_kafka73/files/step73_observe.py'
spec = importlib.util.spec_from_file_location('step73_observe', PATH)
probe = importlib.util.module_from_spec(spec)
spec.loader.exec_module(probe)


def test_match_requires_unique_identity():
    alert = {'labels': {'alertname': probe.ALERT, 'step73_probe': 'true', 'step73_probe_id': 'p1111111111'}}
    assert probe.matched(alert, 'p1111111111')
    assert not probe.matched(alert, 'p2222222222')
    assert not probe.matched({'labels': {'alertname': probe.ALERT}}, 'p1111111111')


def test_summary_matches_prometheus_and_actual_am_group_receiver(monkeypatch):
    labels = {'alertname': probe.ALERT, 'step73_probe': 'true', 'step73_probe_id': 'p1111111111'}
    def fake(url):
        if url.endswith('/api/v1/alerts'):
            return {'data': {'alerts': [{'labels': labels, 'state': 'firing'}]}}
        if url.endswith('/api/v2/alerts'):
            return [{'labels': labels, 'status': {'state': 'active'}}]
        return [{'receiver': {'name': 'slack-notifications'}, 'alerts': [{'labels': labels}]}]
    monkeypatch.setattr(probe, 'get_json', fake)
    result = probe.summary('http://p', 'http://a', 'p1111111111')
    assert result['prometheus_firing']
    assert result['alertmanager_active']
    assert result['alertmanager_receivers'] == ['slack-notifications']
    assert all('webhook' not in key for key in result)


def test_resolved_without_alert(monkeypatch):
    def fake(url):
        return {'data': {'alerts': []}} if url.endswith('/api/v1/alerts') else []
    monkeypatch.setattr(probe, 'get_json', fake)
    assert probe.observe('http://p', 'http://a', 'p1111111111', 'resolved', 1, '') == 0

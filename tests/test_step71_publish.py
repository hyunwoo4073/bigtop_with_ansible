"""Kafka publication guards tested without contacting a broker."""
import importlib.util
import sys
from pathlib import Path
import pytest

ROOT = Path(__file__).resolve().parents[1] / 'roles/olist_kafka71/files'
spec = importlib.util.spec_from_file_location('step71_cdc', ROOT/'step71_cdc.py')
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)
sys.modules['step71_cdc'] = module
spec2 = importlib.util.spec_from_file_location('step71_publish', ROOT/'step71_publish.py')
pub = importlib.util.module_from_spec(spec2)
spec2.loader.exec_module(pub)


def test_refuses_already_published_initial(monkeypatch):
    monkeypatch.setattr(pub, 'kafka_end', lambda *args: 9)
    monkeypatch.setattr(pub, 'checked', lambda *args, **kwargs: pytest.fail('should not publish'))
    with pytest.raises(module.GuardError, match='expected Kafka end=0'):
        pub.publish('publish', '/kafka/bin', 'broker:9092')


def test_refuses_replay_before_initial(monkeypatch):
    monkeypatch.setattr(pub, 'kafka_end', lambda *args: 0)
    monkeypatch.setattr(pub, 'checked', lambda *args, **kwargs: pytest.fail('should not publish'))
    with pytest.raises(module.GuardError, match='expected Kafka end=9'):
        pub.publish('replay', '/kafka/bin', 'broker:9092')


def test_exact_nine_events_published(monkeypatch):
    calls = []
    offsets = iter((0, 9))
    monkeypatch.setattr(pub, 'kafka_end', lambda *args: next(offsets))
    def fake_checked(*args, input_text=None):
        calls.append((args, input_text))
        return ''
    monkeypatch.setattr(pub, 'checked', fake_checked)
    pub.publish('publish', '/kafka/bin', 'broker:9092')
    assert len(calls) == 1
    assert len(calls[0][1].splitlines()) == 9


def test_replay_exact_event_bytes(monkeypatch):
    events = []
    for offsets in [(0, 9), (9, 18)]:
        it = iter(offsets)
        monkeypatch.setattr(pub, 'kafka_end', lambda *args: next(it))
        def fake_checked(*args, input_text=None):
            events.append(input_text)
            return ''
        monkeypatch.setattr(pub, 'checked', fake_checked)
        pub.publish('publish' if offsets[0] == 0 else 'replay', '/kafka/bin', 'broker:9092')
    assert events[0] == events[1]

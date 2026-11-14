"""Pure-model tests; require no Spark/Kafka packages or external services."""
import importlib.util
from pathlib import Path
import pytest

SRC = Path(__file__).resolve().parents[1] / 'roles/olist_kafka71/files/step71_cdc.py'
spec = importlib.util.spec_from_file_location('step71_cdc', SRC)
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)


def test_fixture_count_and_final_state():
    events = module.fixture_events()
    assert len(events) == 9
    result = module.expected()
    assert len(result) == 3
    assert [r[0] for r in result] == list(module.KEYS)
    assert [r[1] for r in result] == [3, 2, 2]
    assert [r[5] for r in result] == [False, True, False]
    assert [r[2] for r in result] == ['15.00', '20.00', '30.00']


def test_replay_and_order_independence():
    events = module.fixture_events()
    a = module.normalized(module.fold(events).values())
    b = module.normalized(module.fold(events + events).values())
    c = module.normalized(module.fold(list(reversed(events))).values())
    assert a == b == c
    assert module.digest(a) == module.digest(b)


def test_same_sequence_conflict_rejected():
    events = module.fixture_events()
    bad = dict(events[0], price='19.00', item_total='21.00')
    with pytest.raises(module.GuardError, match='Conflicting'):
        module.fold([events[0], bad])


def test_unexpected_run_id_rejected():
    bad = dict(module.fixture_events()[0], run_id='some-other-lab')
    with pytest.raises(module.GuardError, match='run_id'):
        module.fold([bad])


def test_unexpected_key_rejected():
    bad = dict(module.fixture_events()[0], order_id='production-table-001')
    with pytest.raises(module.GuardError, match='Unexpected key'):
        module.fold([bad])


def test_amount_mismatch_rejected():
    bad = dict(module.fixture_events()[0], item_total='20.00')
    with pytest.raises(module.GuardError, match='price'):
        module.fold([bad])


def test_negative_money_rejected():
    with pytest.raises(module.GuardError):
        module.money('-1')


def test_fixture_has_duplicate_and_stale():
    events = module.fixture_events()
    assert events[1] == events[3]
    assert events[2]['event_seq'] < events[1]['event_seq']


def test_ownership_and_isolated_paths():
    assert module.TARGET.startswith('shared.olist71_cdc.')
    assert '/step71/' in module.CHECKPOINT
    assert '/step71/' in module.BASELINE
    assert 'step65' not in module.TARGET + module.CHECKPOINT

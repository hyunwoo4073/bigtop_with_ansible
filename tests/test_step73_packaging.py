"""Offline packaging guardrails. Actual API delivery is checked in VM."""
from pathlib import Path
import yaml

ROOT = Path(__file__).resolve().parents[1]


def test_ansible_yaml_parses_and_stays_in_200_char_limit():
    for path in (ROOT / 'roles/olist_kafka73').rglob('*.yml'):
        assert yaml.safe_load(path.read_text()) is not None, path
        assert max(map(len, path.read_text().splitlines())) <= 200, path


def test_temporary_rule_target_is_isolated_and_non_disruptive():
    defaults = yaml.safe_load((ROOT / 'roles/olist_kafka73/defaults/main.yml').read_text())
    assert defaults['olist_kafka73_probe_rule_file'].endswith('bigtop-olist-step73-probe.yml')
    assert defaults['olist_kafka73_probe_labels']['component'] == 'step73'
    template = (ROOT / 'roles/olist_kafka73/templates/step73-probe-rules.yml.j2').read_text()
    assert 'vector(time()) < ' in template
    assert 'step73_probe_id' in template
    assert 'BigtopOlistStep73DeliveryProbe' in template

#!/usr/bin/env python3
"""Read-only discovery of Prometheus -> Alertmanager -> receiver route.

Never prints raw Alertmanager configuration because it can contain credentials.
"""
import argparse
import json
import sys
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

STEP68_ALERTS = (
    'BigtopOlistStep68CollectorFailed',
    'BigtopOlistStep68CollectorMissing',
    'BigtopOlistStep68CollectorStale',
    'BigtopOlistStep68StreamingLag',
    'BigtopOlistStep68SparkPartitionMissing',
    'BigtopOlistStep68CommittedAhead',
    'BigtopOlistStep68StreamingStalled',
)


def get_json(url):
    req = Request(url, headers={'Accept': 'application/json', 'User-Agent': 'bigtop-step73-preflight/1'})
    with urlopen(req, timeout=8) as response:
        return json.loads(response.read(2_000_000).decode('utf-8'))


def probe_health(url):
    try:
        with urlopen(Request(url, headers={'User-Agent': 'bigtop-step73-preflight/1'}), timeout=8) as r:
            return r.status == 200
    except (HTTPError, URLError, TimeoutError, ValueError):
        return False


def summarize_rules(payload):
    found = {}
    for group in payload.get('data', {}).get('groups', []):
        for rule in group.get('rules', []):
            name = rule.get('name') or ''
            if name in STEP68_ALERTS:
                found[name] = {'health': rule.get('health'), 'state': rule.get('state')}
    return found


def summarize_alertmanagers(payload):
    data = payload.get('data', {})
    # URLs contain host/port but no API auth tokens in the typical Prometheus response.
    # Never print them here; only counts are needed.
    return {'active': len(data.get('activeAlertmanagers') or []),
            'dropped': len(data.get('droppedAlertmanagers') or [])}


def summarize_receivers(payload):
    """Receiver names are nonsecret; no URLs, passwords, or config are returned."""
    names = []
    for receiver in payload:
        name = receiver.get('name') if isinstance(receiver, dict) else None
        if name:
            names.append(name)
    return sorted(set(names))


def report(prom, am):
    result = {'prometheus': {'ready': probe_health(prom + '/-/ready')},
              'alertmanager': {'ready': probe_health(am + '/-/ready')},
              'step68_rules': {}, 'receiver_names': [],
              'checks': {}, 'notes': ['Read-only. No synthetic alerts sent, no notification delivery claimed.']}
    errors = []
    try:
        rules_payload = get_json(prom + '/api/v1/rules?type=alert')
        result['step68_rules'] = summarize_rules(rules_payload)
    except (HTTPError, URLError, TimeoutError, ValueError, KeyError) as exc:
        errors.append('prometheus_rules: ' + type(exc).__name__)
    try:
        managers_payload = get_json(prom + '/api/v1/alertmanagers')
        result['prometheus']['alertmanager_discovery'] = summarize_alertmanagers(managers_payload)
    except (HTTPError, URLError, TimeoutError, ValueError, KeyError) as exc:
        errors.append('prometheus_alertmanager_discovery: ' + type(exc).__name__)
    try:
        receivers = get_json(am + '/api/v2/receivers')
        result['receiver_names'] = summarize_receivers(receivers)
    except (HTTPError, URLError, TimeoutError, ValueError, KeyError) as exc:
        errors.append('alertmanager_receivers: ' + type(exc).__name__)
    try:
        alerts = get_json(am + '/api/v2/alerts')
        result['alertmanager']['active_step68_alerts'] = sum(
            1 for a in alerts if isinstance(a, dict) and a.get('labels', {}).get('alertname') in STEP68_ALERTS
        )
    except (HTTPError, URLError, TimeoutError, ValueError, KeyError) as exc:
        errors.append('alertmanager_alerts: ' + type(exc).__name__)
    result['checks'] = {
        'prometheus_ready': result['prometheus']['ready'],
        'alertmanager_ready': result['alertmanager']['ready'],
        'step68_rules_loaded': len(result['step68_rules']) == len(STEP68_ALERTS),
        'step68_rule_health': len(result['step68_rules']) == len(STEP68_ALERTS) and all(
            r.get('health') in ('ok', 'unknown', None) for r in result['step68_rules'].values()),
        'alertmanager_discovered': result['prometheus'].get('alertmanager_discovery', {}).get('active', 0) > 0,
        'receiver_present': len(result['receiver_names']) > 0,
    }
    result['errors'] = errors
    return result


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--prometheus', default='http://127.0.0.1:9090')
    parser.add_argument('--alertmanager', default='http://127.0.0.1:9093')
    args = parser.parse_args()
    data = report(args.prometheus.rstrip('/'), args.alertmanager.rstrip('/'))
    print(json.dumps(data, indent=2, sort_keys=True, ensure_ascii=False))
    passed = all(data['checks'].values()) and not data['errors']
    print('STEP73_PREFLIGHT_PASS' if passed else 'STEP73_PREFLIGHT_NEEDS_ATTENTION')
    return 0 if passed else 2


if __name__ == '__main__':
    sys.exit(main())

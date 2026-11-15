#!/usr/bin/env python3
"""Observe a single Prometheus->Alertmanager probe without logging secrets."""
import argparse
import json
import sys
import time
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

ALERT = 'BigtopOlistStep73DeliveryProbe'


def get_json(url):
    req = Request(url, headers={'Accept': 'application/json', 'User-Agent': 'bigtop-step73-observe/1'})
    with urlopen(req, timeout=8) as response:
        return json.loads(response.read(2_000_000).decode('utf-8'))


def matched(alert, probe_id):
    labels = alert.get('labels') or {}
    return (labels.get('alertname') == ALERT
            and labels.get('step73_probe') == 'true'
            and labels.get('step73_probe_id') == probe_id)


def summary(prom, am, probe_id):
    prom_data = get_json(prom + '/api/v1/alerts')
    alerts = (prom_data.get('data') or {}).get('alerts') or []
    prom_firing = any(matched(a, probe_id) and a.get('state') == 'firing' for a in alerts)
    am_alerts = get_json(am + '/api/v2/alerts')
    am_active = any(matched(a, probe_id) and (a.get('status') or {}).get('state') == 'active'
                    for a in am_alerts)
    grouped = get_json(am + '/api/v2/alerts/groups')
    receivers = sorted({g.get('receiver', {}).get('name') for g in grouped
                        if any(matched(a, probe_id) for a in (g.get('alerts') or []))
                        and isinstance(g.get('receiver'), dict) and g.get('receiver', {}).get('name')})
    return {'prometheus_firing': prom_firing,
            'alertmanager_active': am_active,
            'alertmanager_receivers': receivers,
            'probe_id': probe_id}


def observe(prom, am, probe_id, phase, timeout, expected_receiver):
    deadline = time.monotonic() + timeout
    last = {}
    while time.monotonic() < deadline:
        try:
            last = summary(prom, am, probe_id)
            if phase == 'firing':
                good = last['prometheus_firing'] and last['alertmanager_active']
                if expected_receiver:
                    good = good and expected_receiver in last['alertmanager_receivers']
            else:
                good = not last['prometheus_firing'] and not last['alertmanager_active']
            if good:
                print(json.dumps(last, sort_keys=True))
                print('STEP73_FIRING_ROUTE_PASS' if phase == 'firing' else 'STEP73_RESOLVED_STATE_PASS')
                return 0
        except (HTTPError, URLError, TimeoutError, ValueError, KeyError, TypeError) as exc:
            last = {'temporary_probe_error': type(exc).__name__}
        time.sleep(5)
    print(json.dumps(last, sort_keys=True))
    print('STEP73_' + phase.upper() + '_TIMEOUT', file=sys.stderr)
    return 2


def main():
    p = argparse.ArgumentParser()
    p.add_argument('--phase', choices=['firing', 'resolved'], required=True)
    p.add_argument('--prometheus', default='http://127.0.0.1:9090')
    p.add_argument('--alertmanager', default='http://127.0.0.1:9093')
    p.add_argument('--probe-id', required=True)
    p.add_argument('--expect-receiver', default='')
    p.add_argument('--timeout', type=int, default=180)
    args = p.parse_args()
    return observe(args.prometheus.rstrip('/'), args.alertmanager.rstrip('/'),
                   args.probe_id, args.phase, args.timeout, args.expect_receiver)


if __name__ == '__main__':
    sys.exit(main())

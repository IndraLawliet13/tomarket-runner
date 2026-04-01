#!/usr/bin/env python3
import argparse
import json
import pathlib
import sys
import urllib.parse
from datetime import datetime, timezone

import requests

REPO_ROOT = pathlib.Path(__file__).resolve().parent
STATE_DIR = REPO_ROOT / 'state'
BOOTSTRAP_RAW = STATE_DIR / 'bootstrap' / 'launch.json'
OUTPUT_DIR = STATE_DIR / 'readonly-monitor'
API_BASE = 'https://api-web.tomarket.ai/tomarket-game/v1'

SAFE_BASELINE_ENDPOINTS = [
    ('/user/info', {}),
    ('/user/balance', {}),
    ('/user/inviteCode', {}),
    ('/user/tickets', {}),
    ('/spin/show', {}),
    ('/tasks/list', {}),
    ('/launchpad/list', {}),
]

SAFE_EXTRA_ENDPOINTS = [
    ('/user/hasGoldenTicket', {}),
    ('/tasks/hasMedal', {}),
    ('/user/friends', {'page_index': 1, 'page_size': 20}),
    ('/user/listingAirdropInfo', {}),
    ('/user/withdrawTomaInfo', {}),
    ('/user/withdrawTomaHistory', {}),
    ('/user/tomarketHistory', {'is_listing': False}),
    ('/user/tomarketHistory', {'is_listing': True}),
    ('/user/isSybil', {}),
    ('/user/whitelist', {'category': 'aptos'}),
    ('/launchpad/ambassadors', {}),
    ('/launchpad/tomaBalance', {}),
    ('/ton/getTonAddress', {}),
]


class ProbeError(Exception):
    pass


def load_launch_url(bootstrap_path: pathlib.Path | None = None) -> str:
    bootstrap_file = bootstrap_path or BOOTSTRAP_RAW
    if not bootstrap_file.exists():
        raise ProbeError(f'missing bootstrap artifact: {bootstrap_file}')
    raw = json.loads(bootstrap_file.read_text())
    attempts = raw.get('attempts') or []
    if not attempts or not attempts[0].get('url'):
        raise ProbeError('bootstrap artifact does not contain launch URL')
    return attempts[0]['url']


def parse_launch(launch_url: str):
    parsed = urllib.parse.urlparse(launch_url)
    frag = urllib.parse.parse_qs(parsed.fragment)
    tg_raw = frag.get('tgWebAppData', [''])[0]
    init_data = urllib.parse.unquote(tg_raw)
    if not init_data:
        raise ProbeError('tgWebAppData missing from launch URL')
    language_code = 'id'
    try:
        q = urllib.parse.parse_qs(init_data)
        if 'user' in q:
            user = json.loads(q['user'][0])
            language_code = user.get('language_code') or language_code
    except Exception:
        pass
    return {
        'launch_url': launch_url,
        'host': parsed.netloc,
        'platform': frag.get('tgWebAppPlatform', [''])[0],
        'version': frag.get('tgWebAppVersion', [''])[0],
        'init_data': init_data,
        'language_code': language_code,
    }


def make_session(launch_url: str):
    s = requests.Session()
    s.headers.update({
        'User-Agent': 'Mozilla/5.0 (Linux; Android 14; SM-S918B) AppleWebKit/537.36 '
                      '(KHTML, like Gecko) Chrome/122.0.0.0 Mobile Safari/537.36',
        'Accept': 'application/json, text/plain, */*',
        'Origin': 'https://mini-app.tomarket.ai',
        'Referer': launch_url,
    })
    return s


def post_json(session: requests.Session, path: str, payload=None):
    if payload is None:
        r = session.post(API_BASE + path, timeout=20)
    elif payload == {}:
        r = session.post(API_BASE + path, json={}, timeout=20)
    else:
        r = session.post(API_BASE + path, json=payload, timeout=20)
    content_type = r.headers.get('Content-Type', '')
    try:
        body_json = r.json() if 'application/json' in content_type else None
    except Exception:
        body_json = None
    return {
        'status_code': r.status_code,
        'url': r.url,
        'json': body_json,
        'body_preview': r.text[:1200],
    }


def login(session: requests.Session, launch_meta: dict):
    session.get(launch_meta['launch_url'], timeout=20)
    payload = {
        'init_data': launch_meta['init_data'],
        'invite_code': '',
        'from': '',
        'is_bot': False,
    }
    result = post_json(session, '/user/login', payload)
    token = (((result.get('json') or {}).get('data')) or {}).get('access_token')
    if not token:
        raise ProbeError(f'login failed: {json.dumps(result.get("json"), ensure_ascii=False)[:400]}')
    session.headers['Authorization'] = token
    return {
        'login_result': result,
        'token_prefix': token[:16],
    }


def extract_launchpad_ids(launchpad_list_json):
    data = ((launchpad_list_json or {}).get('json') or {}).get('data')
    rows = []

    def walk(obj):
        if isinstance(obj, list):
            for x in obj:
                walk(x)
        elif isinstance(obj, dict):
            if 'id' in obj and any(k in obj for k in ['dailyFarm', 'inviteFarm', 'tomaFarm', 'name', 'title']):
                rows.append(obj)
            for v in obj.values():
                if isinstance(v, (list, dict)):
                    walk(v)

    walk(data)
    uniq = []
    seen = set()
    for row in rows:
        rid = row.get('id')
        if rid in seen:
            continue
        seen.add(rid)
        uniq.append(row)
    return uniq


def run_probe(limit: int):
    launch_url = load_launch_url(getattr(args, 'bootstrap_path', None))
    launch_meta = parse_launch(launch_url)
    session = make_session(launch_url)
    login_meta = login(session, launch_meta)

    summary = {
        'captured_at': datetime.now(timezone.utc).isoformat(),
        'launch_host': launch_meta['host'],
        'platform': launch_meta['platform'],
        'version': launch_meta['version'],
        'language_code': launch_meta['language_code'],
        'token_prefix': login_meta['token_prefix'],
        'baseline': {},
        'extra': {},
        'launchpads': {},
    }

    for path, payload in SAFE_BASELINE_ENDPOINTS:
        summary['baseline'][path] = post_json(session, path, payload)

    for path, payload in SAFE_EXTRA_ENDPOINTS:
        summary['extra'][f'{path}::{json.dumps(payload, sort_keys=True)}'] = post_json(session, path, payload)

    launch_rows = extract_launchpad_ids(summary['baseline']['/launchpad/list'])
    summary['launchpad_ids'] = [row.get('id') for row in launch_rows[:limit]]
    summary['launchpad_names'] = {str(row.get('id')): row.get('name') or row.get('title') for row in launch_rows[:limit]}

    for row in launch_rows[:limit]:
        lid = row.get('id')
        summary['launchpads'][str(lid)] = {
            'detail': post_json(session, '/launchpad/detail', {'launchpad_id': lid, 'language_code': launch_meta['language_code']}),
            'investInfo': post_json(session, '/launchpad/investInfo', {'launchpad_id': lid}),
            'inviters': post_json(session, '/launchpad/inviters', {'launchpad_id': lid}),
            'tasks': post_json(session, '/launchpad/tasks', {'launchpad_id': lid}),
        }

    return summary


def redact_for_stdout(summary: dict):
    safe = json.loads(json.dumps(summary))
    login_json = (((safe.get('baseline', {}).get('/user/info', {})).get('json')) or {})
    _ = login_json
    return safe


def main():
    parser = argparse.ArgumentParser(description='Tomarket read-only probe')
    parser.add_argument('--bootstrap-path', type=pathlib.Path, default=BOOTSTRAP_RAW, help='path to bootstrap launch artifact JSON')
    parser.add_argument('--launchpad-limit', type=int, default=5, help='number of launchpad ids to sample')
    parser.add_argument('--no-write', action='store_true', help='do not write artifact files')
    args = parser.parse_args()

    summary = run_probe(args.launchpad_limit)
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    ts = datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')
    if not args.no_write:
        raw_path = OUTPUT_DIR / f'{ts}.json'
        latest_path = OUTPUT_DIR / 'latest.json'
        raw_path.write_text(json.dumps(summary, indent=2))
        latest_path.write_text(json.dumps(summary, indent=2))

    stdout_obj = redact_for_stdout(summary)
    print(json.dumps(stdout_obj, indent=2)[:30000])


if __name__ == '__main__':
    try:
        main()
    except ProbeError as e:
        print(f'ERROR: {e}', file=sys.stderr)
        sys.exit(2)

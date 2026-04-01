#!/usr/bin/env python3
import argparse
import asyncio
import json
import os
import pathlib
import random
import sys
import time
import urllib.parse
from datetime import datetime, timezone

import requests

REPO_ROOT = pathlib.Path(__file__).resolve().parent
STATE_DIR = REPO_ROOT / 'state'
BOOTSTRAP_RAW = STATE_DIR / 'bootstrap' / 'launch.json'
OUTPUT_DIR = STATE_DIR / 'runner'
STATE_PATH = OUTPUT_DIR / 'runner-state.json'
DECISION_LOG_PATH = OUTPUT_DIR / 'decision-log.jsonl'
ERROR_LOG_PATH = OUTPUT_DIR / 'error-log.jsonl'
STATE_SUMMARY_PATH = OUTPUT_DIR / 'state-summary.json'
API_BASE = 'https://api-web.tomarket.ai/tomarket-game/v1'
OPEN_AD_TASK_ID = 9001
ADSGRAM_IMAGE_TASK_ID = 8003
ADSGRAM_VIDEO_TASK_ID = 8002
DAILY_GAME_ID = 'fa873d13-d831-4d6f-8aee-9cff7a1d0db1'
FARM_GAME_ID = '53b22103-c7ff-413d-bc63-20f6fb806a07'
DROP_GAME_ID = '59bcd12e-04e2-404c-a172-311a0084587d'
ADSGRAM_TASKS = {
    ADSGRAM_IMAGE_TASK_ID: {
        'lane': 'adsgram_image_8003',
        'name': 'adsgram_image',
        'task_id': ADSGRAM_IMAGE_TASK_ID,
        'block_id': '6772',
    },
    ADSGRAM_VIDEO_TASK_ID: {
        'lane': 'adsgram_video_8002',
        'name': 'adsgram_video',
        'task_id': ADSGRAM_VIDEO_TASK_ID,
        'block_id': '6771',
    },
}


class RunnerError(Exception):
    pass


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def current_utc_ymd(now_ts: int | None = None) -> int:
    if now_ts is None:
        return int(datetime.now(timezone.utc).strftime('%Y%m%d'))
    return int(datetime.fromtimestamp(now_ts, timezone.utc).strftime('%Y%m%d'))


def ensure_state_dirs():
    (STATE_DIR / 'bootstrap').mkdir(parents=True, exist_ok=True)
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


def load_runner_state():
    ensure_state_dirs()
    if not STATE_PATH.exists():
        return {}
    try:
        return json.loads(STATE_PATH.read_text())
    except Exception:
        return {}


def save_runner_state(state: dict):
    ensure_state_dirs()
    STATE_PATH.write_text(json.dumps(state, indent=2))


def record_lane_attempt(state: dict, lane: str, now_ts: int, decision: str):
    lanes = state.setdefault('lanes', {})
    lane_state = lanes.setdefault(lane, {})
    lane_state['last_attempt_ts'] = now_ts
    lane_state['last_decision'] = decision
    lane_state['last_attempt_iso'] = utc_now_iso()
    return lane_state


def update_lane_state(state: dict, lane: str, **fields):
    lanes = state.setdefault('lanes', {})
    lane_state = lanes.setdefault(lane, {})
    for key, value in fields.items():
        lane_state[key] = value
    return lane_state


def lane_last_attempt_age(state: dict, lane: str, now_ts: int):
    lane_state = (state.get('lanes') or {}).get(lane) or {}
    last_attempt_ts = lane_state.get('last_attempt_ts')
    if not isinstance(last_attempt_ts, int):
        return None
    return now_ts - last_attempt_ts


def get_lane_state(state: dict, lane: str):
    return ((state.get('lanes') or {}).get(lane)) or {}


def set_lane_next_due(state: dict, lane: str, due_ts: int | None, reason: str):
    lane_state = update_lane_state(state, lane)
    if isinstance(due_ts, int):
        lane_state['next_due_ts'] = due_ts
        lane_state['next_due_iso'] = datetime.fromtimestamp(due_ts, timezone.utc).isoformat()
    else:
        lane_state.pop('next_due_ts', None)
        lane_state.pop('next_due_iso', None)
    lane_state['next_due_reason'] = reason
    return lane_state


def set_lane_next_due_in(state: dict, lane: str, now_ts: int, seconds: int, reason: str):
    due_ts = now_ts + max(int(seconds), 0)
    return set_lane_next_due(state, lane, due_ts, reason)


def lane_next_due_in_seconds(state: dict, lane: str, now_ts: int):
    next_due_ts = get_lane_state(state, lane).get('next_due_ts')
    if not isinstance(next_due_ts, int):
        return None
    return max(next_due_ts - now_ts, 0)


def lane_is_due(state: dict, lane: str, now_ts: int):
    next_due_ts = get_lane_state(state, lane).get('next_due_ts')
    if not isinstance(next_due_ts, int):
        return True
    return now_ts >= next_due_ts


def schedule_skip_payload(state: dict, lane: str, now_ts: int):
    lane_state = get_lane_state(state, lane)
    return {
        'decision': 'scheduled_skip',
        'next_due_ts': lane_state.get('next_due_ts'),
        'next_due_iso': lane_state.get('next_due_iso'),
        'next_due_reason': lane_state.get('next_due_reason'),
        'seconds_until_due': lane_next_due_in_seconds(state, lane, now_ts),
    }


def deferred_for_priority_payload(state: dict, lane: str, now_ts: int, reason: str):
    payload = schedule_skip_payload(state, lane, now_ts)
    payload['decision'] = 'deferred_for_higher_priority'
    payload['defer_reason'] = reason
    return payload


def bump_lane_failure(state: dict, lane: str, success: bool | None):
    lane_state = update_lane_state(state, lane)
    current = lane_state.get('consecutive_failures')
    if not isinstance(current, int):
        current = 0
    if success is True:
        current = 0
    elif success is False:
        current += 1
    lane_state['consecutive_failures'] = current
    return current


def exponential_backoff_seconds(base_seconds: int, failures: int, cap_seconds: int):
    failures = max(int(failures), 1)
    value = int(base_seconds) * (2 ** max(failures - 1, 0))
    return min(value, int(cap_seconds))


def set_lane_due_from_end_at(state: dict, lane: str, end_at: int | None, now_ts: int, reason: str,
                             jitter_low: int = 30, jitter_high: int = 90, fallback_seconds: int = 600):
    if isinstance(end_at, int):
        due_ts = end_at + random.randint(jitter_low, jitter_high)
        if due_ts < now_ts:
            due_ts = now_ts + random.randint(jitter_low, jitter_high)
        return set_lane_next_due(state, lane, due_ts, reason)
    return set_lane_next_due_in(state, lane, now_ts, fallback_seconds, reason)


def compute_runner_sleep_seconds(state: dict, now_ts: int, fallback_seconds: int, min_sleep_seconds: int):
    next_dues = []
    for lane_state in (state.get('lanes') or {}).values():
        next_due_ts = lane_state.get('next_due_ts')
        if isinstance(next_due_ts, int):
            next_dues.append(next_due_ts)
    if not next_dues:
        return max(int(fallback_seconds), int(min_sleep_seconds))
    nearest = min(next_dues)
    delta = max(nearest - now_ts, 0)
    if delta <= 0:
        return max(int(min_sleep_seconds), 1)
    return max(int(min_sleep_seconds), delta + random.randint(1, 10))


def scheduler_snapshot(state: dict, now_ts: int):
    out = {}
    for lane, lane_state in sorted((state.get('lanes') or {}).items()):
        out[lane] = {
            'next_due_ts': lane_state.get('next_due_ts'),
            'next_due_iso': lane_state.get('next_due_iso'),
            'next_due_reason': lane_state.get('next_due_reason'),
            'seconds_until_due': lane_next_due_in_seconds(state, lane, now_ts),
            'last_decision': lane_state.get('last_decision'),
            'consecutive_failures': lane_state.get('consecutive_failures'),
            'parked_until_ts': lane_state.get('parked_until_ts'),
            'parked_until_iso': lane_state.get('parked_until_iso'),
            'parked_reason': lane_state.get('parked_reason'),
        }
    return out


def clear_lane_park(state: dict, lane: str):
    lane_state = update_lane_state(state, lane)
    lane_state.pop('parked_until_ts', None)
    lane_state.pop('parked_until_iso', None)
    lane_state.pop('parked_reason', None)
    return lane_state


def park_lane(state: dict, lane: str, now_ts: int, seconds: int, reason: str):
    lane_state = update_lane_state(state, lane)
    until_ts = now_ts + max(int(seconds), 0)
    lane_state['parked_until_ts'] = until_ts
    lane_state['parked_until_iso'] = datetime.fromtimestamp(until_ts, timezone.utc).isoformat()
    lane_state['parked_reason'] = reason
    return lane_state


def lane_is_parked(state: dict, lane: str, now_ts: int):
    lane_state = get_lane_state(state, lane)
    parked_until_ts = lane_state.get('parked_until_ts')
    if not isinstance(parked_until_ts, int):
        return False
    if now_ts >= parked_until_ts:
        clear_lane_park(state, lane)
        return False
    return True


def parked_skip_payload(state: dict, lane: str, now_ts: int):
    lane_state = get_lane_state(state, lane)
    return {
        'decision': 'parked_skip',
        'parked_until_ts': lane_state.get('parked_until_ts'),
        'parked_until_iso': lane_state.get('parked_until_iso'),
        'parked_reason': lane_state.get('parked_reason'),
        'seconds_until_unpark': max(int((lane_state.get('parked_until_ts') or 0) - now_ts), 0) if isinstance(lane_state.get('parked_until_ts'), int) else None,
    }


def safe_mode_state(state: dict):
    return (state.get('safe_mode') or {})


def clear_safe_mode(state: dict):
    state.pop('safe_mode', None)


def set_safe_mode(state: dict, now_ts: int, seconds: int, reason: str, source_lane: str | None = None):
    until_ts = now_ts + max(int(seconds), 0)
    state['safe_mode'] = {
        'until_ts': until_ts,
        'until_iso': datetime.fromtimestamp(until_ts, timezone.utc).isoformat(),
        'reason': reason,
        'source_lane': source_lane,
        'activated_at_ts': now_ts,
        'activated_at_iso': utc_now_iso(),
    }
    return state['safe_mode']


def safe_mode_is_active(state: dict, now_ts: int):
    mode = safe_mode_state(state)
    until_ts = mode.get('until_ts')
    if not isinstance(until_ts, int):
        return False
    if now_ts >= until_ts:
        clear_safe_mode(state)
        return False
    return True


def safe_mode_skip_payload(state: dict, lane: str, now_ts: int):
    mode = safe_mode_state(state)
    payload = schedule_skip_payload(state, lane, now_ts)
    payload['decision'] = 'safe_mode_skip'
    payload['safe_mode_until_ts'] = mode.get('until_ts')
    payload['safe_mode_until_iso'] = mode.get('until_iso')
    payload['safe_mode_reason'] = mode.get('reason')
    payload['safe_mode_source_lane'] = mode.get('source_lane')
    return payload


def increment_lane_daily_success(state: dict, lane: str, now_ts: int, field_prefix: str = 'daily_success'):
    lane_state = update_lane_state(state, lane)
    ymd = current_utc_ymd(now_ts)
    if lane_state.get(f'{field_prefix}_ymd') != ymd:
        lane_state[f'{field_prefix}_ymd'] = ymd
        lane_state[f'{field_prefix}_count'] = 0
    lane_state[f'{field_prefix}_count'] = int(lane_state.get(f'{field_prefix}_count') or 0) + 1
    return lane_state[f'{field_prefix}_count']


def lane_daily_success_count(state: dict, lane: str, now_ts: int, field_prefix: str = 'daily_success'):
    lane_state = get_lane_state(state, lane)
    ymd = current_utc_ymd(now_ts)
    if lane_state.get(f'{field_prefix}_ymd') != ymd:
        return 0
    return int(lane_state.get(f'{field_prefix}_count') or 0)


def next_utc_day_start_ts(now_ts: int):
    dt = datetime.fromtimestamp(now_ts, timezone.utc)
    midnight = datetime(dt.year, dt.month, dt.day, tzinfo=timezone.utc)
    return int(midnight.timestamp()) + 86400


def lane_decision_severity(decision: str):
    if decision in {
        'claim_non_ok', 'claim_ok_start_failed', 'recover_start_failed', 'start_non_ok', 'start_failed',
        'spin_non_ok', 'play_failed', 'farm_missing', 'sdk_show_failed', 'claim_non_ok_pending'
    }:
        return 'error'
    if decision in {
        'check_not_ready', 'claim_ok_still_claimable', 'task_missing', 'task_disabled', 'task_is_exceed',
        'already_checked', 'not_free', 'reserve_floor_skip', 'no_play_passes', 'parked_skip', 'safe_mode_skip',
        'sdk_reward_but_no_claimable', 'sdk_reward_pending'
    }:
        return 'warn'
    return 'info'


def append_jsonl(path: pathlib.Path, payload: dict):
    ensure_state_dirs()
    with path.open('a', encoding='utf-8') as fh:
        fh.write(json.dumps(payload, ensure_ascii=False) + '\n')


def compact_lane_metrics(lane: str, result: dict):
    if not isinstance(result, dict):
        return {}
    if lane == 'daily':
        before = result.get('before_balance_summary') or {}
        after = result.get('after_balance_summary') or before
        return {
            'available_balance_before': before.get('available_balance'),
            'available_balance_after': after.get('available_balance'),
            'play_passes_before': before.get('play_passes'),
            'play_passes_after': after.get('play_passes'),
            'daily_next_check_ts': ((after.get('daily') or before.get('daily') or {}).get('next_check_ts')),
        }

    before_summary = result.get('before_summary') or {}
    after_summary = result.get('after_summary') or before_summary
    before_balance = before_summary.get('balance') or {}
    after_balance = after_summary.get('balance') or before_balance
    metrics = {
        'available_balance_before': before_balance.get('available_balance'),
        'available_balance_after': after_balance.get('available_balance'),
        'play_passes_before': before_balance.get('play_passes'),
        'play_passes_after': after_balance.get('play_passes'),
        'farm_state_before': (extract_farming_summary(before_summary) or {}).get('state'),
        'farm_state_after': (extract_farming_summary(after_summary) or {}).get('state'),
        'balance_delta': result.get('balance_delta') or result.get('balance_delta_claim'),
    }
    if lane == 'open_ad_9001':
        metrics['task_status_before'] = ((before_summary.get('open_ad_task') or {}).get('status'))
        metrics['task_status_after'] = ((after_summary.get('open_ad_task') or {}).get('status'))
    if lane == 'adsgram_image_8003':
        metrics['task_status_before'] = ((result.get('before_task') or {}).get('status'))
        metrics['task_status_after'] = ((result.get('after_task') or {}).get('status'))
        metrics['sdk_events'] = [e.get('name') for e in ((result.get('adsgram_show') or {}).get('events') or []) if e.get('name')]
        metrics['sdk_done'] = (((result.get('adsgram_show') or {}).get('showResult') or {}).get('done'))
    if lane == 'adsgram_video_8002':
        metrics['task_status_before'] = ((result.get('before_task') or {}).get('status'))
        metrics['task_status_after'] = ((result.get('after_task') or {}).get('status'))
        metrics['sdk_events'] = [e.get('name') for e in ((result.get('adsgram_show') or {}).get('events') or []) if e.get('name')]
        metrics['sdk_done'] = (((result.get('adsgram_show') or {}).get('showResult') or {}).get('done'))
    if lane == 'free_spin':
        metrics['spin_free_before'] = ((before_summary.get('spin') or {}).get('is_free'))
        metrics['spin_free_after'] = ((after_summary.get('spin') or {}).get('is_free'))
    if lane == 'drop_game':
        metrics['play_pass_delta'] = result.get('play_pass_delta')
        metrics['claim_policy'] = result.get('claim_policy')
    return metrics


def write_runtime_logs(summary: dict, state: dict, now_ts: int):
    for lane in ['daily_claim', 'home_farming', 'free_spin', 'open_ad_9001', 'adsgram_image_8003', 'adsgram_video_8002', 'drop_game']:
        result = summary.get(lane)
        if not isinstance(result, dict):
            continue
        lane_key = 'daily' if lane == 'daily_claim' else lane
        lane_state = get_lane_state(state, lane_key)
        decision = result.get('decision') or 'unknown'
        event = {
            'captured_at': summary.get('captured_at') or utc_now_iso(),
            'lane': lane_key,
            'decision': decision,
            'severity': lane_decision_severity(decision),
            'next_due_ts': lane_state.get('next_due_ts'),
            'next_due_iso': lane_state.get('next_due_iso'),
            'next_due_reason': lane_state.get('next_due_reason'),
            'consecutive_failures': lane_state.get('consecutive_failures'),
            'parked_until_ts': lane_state.get('parked_until_ts'),
            'parked_until_iso': lane_state.get('parked_until_iso'),
            'parked_reason': lane_state.get('parked_reason'),
            'safe_mode_active': safe_mode_is_active(state, now_ts),
            'metrics': compact_lane_metrics(lane_key, result),
        }
        append_jsonl(DECISION_LOG_PATH, event)
        if event['severity'] in {'warn', 'error'}:
            append_jsonl(ERROR_LOG_PATH, event)

    if safe_mode_state(state):
        append_jsonl(DECISION_LOG_PATH, {
            'captured_at': summary.get('captured_at') or utc_now_iso(),
            'lane': '__safe_mode__',
            'decision': 'state',
            'severity': 'warn',
            'safe_mode': safe_mode_state(state),
        })


def write_state_summary_file(summary: dict, state: dict, now_ts: int):
    payload = {
        'captured_at': summary.get('captured_at') or utc_now_iso(),
        'safe_mode': safe_mode_state(state),
        'scheduler': summary.get('scheduler'),
        'lanes': scheduler_snapshot(state, now_ts),
    }
    ensure_state_dirs()
    STATE_SUMMARY_PATH.write_text(json.dumps(payload, indent=2))


def maybe_park_risky_lane(state: dict, lane: str, decision: str, now_ts: int, threshold: int, park_seconds: int):
    if lane not in {'open_ad_9001', 'adsgram_image_8003', 'adsgram_video_8002', 'drop_game', 'free_spin'}:
        return False
    if lane_decision_severity(decision) != 'error':
        return False
    failures = int(get_lane_state(state, lane).get('consecutive_failures') or 0)
    if failures < int(threshold):
        return False
    park_lane(state, lane, now_ts, park_seconds, f'consecutive_failures:{decision}')
    return True


def load_launch_url(bootstrap_path: pathlib.Path | None = None) -> str:
    bootstrap_file = bootstrap_path or BOOTSTRAP_RAW
    if not bootstrap_file.exists():
        raise RunnerError(f'missing bootstrap artifact: {bootstrap_file}')
    raw = json.loads(bootstrap_file.read_text())
    attempts = raw.get('attempts') or []
    if not attempts or not attempts[0].get('url'):
        raise RunnerError('bootstrap artifact does not contain launch URL')
    return attempts[0]['url']


def parse_launch(launch_url: str):
    parsed = urllib.parse.urlparse(launch_url)
    frag = urllib.parse.parse_qs(parsed.fragment)
    tg_raw = frag.get('tgWebAppData', [''])[0]
    init_data = urllib.parse.unquote(tg_raw)
    if not init_data:
        raise RunnerError('tgWebAppData missing from launch URL')
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
        'theme_params_raw': frag.get('tgWebAppThemeParams', ['{}'])[0],
        'init_data': init_data,
        'launch_params': urllib.parse.urlencode({
            'tgWebAppPlatform': frag.get('tgWebAppPlatform', [''])[0],
            'tgWebAppVersion': frag.get('tgWebAppVersion', [''])[0],
            'tgWebAppThemeParams': frag.get('tgWebAppThemeParams', ['{}'])[0],
            'tgWebAppData': init_data,
        }),
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
        r = session.post(API_BASE + path, timeout=25)
    elif payload == {}:
        r = session.post(API_BASE + path, json={}, timeout=25)
    else:
        r = session.post(API_BASE + path, json=payload, timeout=25)
    content_type = r.headers.get('Content-Type', '')
    try:
        body_json = r.json() if 'application/json' in content_type else None
    except Exception:
        body_json = None
    return {
        'captured_at': utc_now_iso(),
        'status_code': r.status_code,
        'url': r.url,
        'json': body_json,
        'body_preview': r.text[:2000],
    }


def login(session: requests.Session, launch_meta: dict):
    session.get(launch_meta['launch_url'], timeout=25)
    payload = {
        'init_data': launch_meta['init_data'],
        'invite_code': '',
        'from': '',
        'is_bot': False,
    }
    result = post_json(session, '/user/login', payload)
    token = (((result.get('json') or {}).get('data')) or {}).get('access_token')
    if not token:
        raise RunnerError(f'login failed: {json.dumps(result.get("json"), ensure_ascii=False)[:400]}')
    session.headers['Authorization'] = token
    user_data = ((result.get('json') or {}).get('data')) or {}
    return {
        'status_code': result['status_code'],
        'captured_at': result['captured_at'],
        'token_prefix': token[:16],
        'user': {
            'id': user_data.get('id'),
            'tel_id': user_data.get('tel_id'),
            'is_new': user_data.get('is_new'),
        },
    }


def find_task(tasks_list_result: dict, task_id: int):
    data = (((tasks_list_result or {}).get('json')) or {}).get('data') or {}
    for group, items in data.items():
        if isinstance(items, list):
            for item in items:
                if item.get('taskId') == task_id:
                    out = dict(item)
                    out['_group'] = group
                    return out
    return None


def read_watch_endpoints(session: requests.Session):
    endpoints = {
        'tasks_list': post_json(session, '/tasks/list', {}),
        'user_balance': post_json(session, '/user/balance', {}),
        'user_tickets': post_json(session, '/user/tickets', {}),
        'spin_show': post_json(session, '/spin/show', {}),
        'spin_free': post_json(session, '/spin/free', {}),
        'user_tomarketHistory_false': post_json(session, '/user/tomarketHistory', {'is_listing': False}),
        'user_tomarketHistory_true': post_json(session, '/user/tomarketHistory', {'is_listing': True}),
        'farm_info': post_json(session, '/farm/info', {'game_id': FARM_GAME_ID}),
    }
    return endpoints


def derive_farming_summary(farming: dict, now_ts: int):
    if not farming:
        return None
    start_at = farming.get('start_at')
    end_at = farming.get('end_at')
    last_claim = farming.get('last_claim')
    cycle_seconds = None
    remaining_seconds = None
    state = 'unknown'
    if isinstance(start_at, int) and isinstance(end_at, int):
        cycle_seconds = end_at - start_at
        remaining_seconds = max(end_at - now_ts, 0)
        if start_at == 0 or (isinstance(last_claim, int) and last_claim >= end_at):
            state = 'idle_startable'
        elif now_ts < end_at:
            state = 'active_farming'
        elif isinstance(last_claim, int) and now_ts >= end_at and last_claim < end_at:
            state = 'claimable'
    return {
        'game_id': farming.get('game_id'),
        'round_id': farming.get('round_id'),
        'start_at': start_at,
        'end_at': end_at,
        'last_claim': last_claim,
        'points': farming.get('points'),
        'stars': farming.get('stars'),
        'boost': farming.get('boost'),
        'cycle_seconds': cycle_seconds,
        'remaining_seconds': remaining_seconds,
        'state': state,
    }


def summarize_watch(endpoints: dict):
    out = {}
    now_ts = int(time.time())
    tasks_row = find_task(endpoints['tasks_list'], OPEN_AD_TASK_ID)
    adsgram_image_row = find_task(endpoints['tasks_list'], ADSGRAM_IMAGE_TASK_ID)
    adsgram_video_row = find_task(endpoints['tasks_list'], ADSGRAM_VIDEO_TASK_ID)
    balance = (((endpoints['user_balance'].get('json') or {}).get('data')) or {})
    tickets = (((endpoints['user_tickets'].get('json') or {}).get('data')) or {})
    spin_show = (((endpoints['spin_show'].get('json') or {}).get('data')) or {})
    spin_free = (((endpoints['spin_free'].get('json') or {}).get('data')) or {})
    farming = balance.get('farming') or {}
    farm_info_data = (((endpoints['farm_info'].get('json') or {}).get('data')) or {})
    farming_summary = derive_farming_summary(farm_info_data or farming, now_ts)
    balance_farming_summary = derive_farming_summary(farming, now_ts)
    out['open_ad_task'] = None if tasks_row is None else {
        'taskId': tasks_row.get('taskId'),
        'status': tasks_row.get('status'),
        'isExceed': tasks_row.get('isExceed'),
        'enable': tasks_row.get('enable'),
        'needVerify': tasks_row.get('needVerify'),
        'waitSecond': tasks_row.get('waitSecond'),
        'handleFunc': tasks_row.get('handleFunc'),
        'tag': tasks_row.get('tag'),
    }
    out['adsgram_image_task'] = None if adsgram_image_row is None else {
        'taskId': adsgram_image_row.get('taskId'),
        'status': adsgram_image_row.get('status'),
        'isExceed': adsgram_image_row.get('isExceed'),
        'enable': adsgram_image_row.get('enable'),
        'needVerify': adsgram_image_row.get('needVerify'),
        'waitSecond': adsgram_image_row.get('waitSecond'),
        'handleFunc': adsgram_image_row.get('handleFunc'),
        'tag': adsgram_image_row.get('tag'),
        'blockId': adsgram_image_row.get('blockId'),
    }
    out['adsgram_video_task'] = None if adsgram_video_row is None else {
        'taskId': adsgram_video_row.get('taskId'),
        'status': adsgram_video_row.get('status'),
        'isExceed': adsgram_video_row.get('isExceed'),
        'enable': adsgram_video_row.get('enable'),
        'needVerify': adsgram_video_row.get('needVerify'),
        'waitSecond': adsgram_video_row.get('waitSecond'),
        'handleFunc': adsgram_video_row.get('handleFunc'),
        'tag': adsgram_video_row.get('tag'),
        'blockId': adsgram_video_row.get('blockId'),
    }
    out['balance'] = {
        'available_balance': balance.get('available_balance'),
        'play_passes': balance.get('play_passes'),
        'daily': balance.get('daily'),
        'farming': balance_farming_summary,
    }
    out['spin'] = {
        'show': spin_show.get('show'),
        'is_free': spin_free.get('is_free'),
        'ticket_spin_1': tickets.get('ticket_spin_1'),
    }
    out['farm_info'] = farming_summary
    out['tickets'] = tickets
    out['history_non_listing_len'] = len((((endpoints['user_tomarketHistory_false'].get('json') or {}).get('data')) or []))
    out['history_listing_len'] = len((((endpoints['user_tomarketHistory_true'].get('json') or {}).get('data')) or []))
    return out


def build_adsgram_init_script(launch_meta: dict):
    return f'''
window.TelegramWebviewProxy = {{ postEvent: function(n,d){{ console.log('[TG_PROXY]', n, d||null); }} }};
window.Telegram = {{ WebApp: {{ ready(){{}}, expand(){{}}, onEvent(){{}}, offEvent(){{}}, sendData(){{}}, platform:{json.dumps(launch_meta.get('platform') or 'android')}, version:{json.dumps(launch_meta.get('version') or '9.1')}, initData:{json.dumps(launch_meta.get('init_data') or '')}, initDataUnsafe:{{}}, themeParams:{{}}, colorScheme:'light', isExpanded:true, viewportHeight:900, viewportStableHeight:900 }} }};
sessionStorage.setItem('adsgram/launch-params', JSON.stringify({json.dumps(launch_meta.get('launch_params') or '')}));
sessionStorage.setItem('telegram-apps/launch-params', JSON.stringify({json.dumps(launch_meta.get('launch_params') or '')}));
sessionStorage.setItem('tma.js/launch-params', JSON.stringify({json.dumps(launch_meta.get('launch_params') or '')}));
'''


async def _run_adsgram_sdk_show_async(block_id: str, launch_meta: dict):
    from playwright.async_api import async_playwright

    console_logs = []
    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=True,
            executable_path=(os.environ.get('TOMARKET_CHROME') or None),
            args=['--disable-blink-features=AutomationControlled'],
        )
        context = await browser.new_context(
            viewport={'width': 430, 'height': 932},
            user_agent='Mozilla/5.0 (Linux; Android 14; SM-S918B) AppleWebKit/537.36 '
                       '(KHTML, like Gecko) Chrome/122.0.0.0 Mobile Safari/537.36',
            locale='id-ID',
        )
        await context.add_init_script(build_adsgram_init_script(launch_meta))
        page = await context.new_page()
        page.on('console', lambda msg: console_logs.append({'type': msg.type, 'text': msg.text[:500]}))

        await page.goto('https://mini-app.tomarket.ai/', wait_until='domcontentloaded', timeout=120000)
        await page.evaluate("document.body.innerHTML='<div id=app></div>'")
        await page.add_script_tag(url='https://mini-app.tomarket.ai/js/adsgram/adsgram.js?v=20250418')
        await page.wait_for_timeout(2000)

        result = await page.evaluate(r'''
            async (blockId) => {
              const events = [];
              const ad = window.Adsgram.init({blockId});
              ['onStart','onReward','onComplete','onSkip','onBannerNotFound','onError','onNonStopShow','onTooLongSession','onNotSuitable'].forEach((name)=>{
                try { ad.addEventListener(name, (...args)=>events.push({name, args})); } catch(e) { events.push({name:'listener_error', err:String(e), for:name}); }
              });
              let showResult = null;
              let showError = null;
              try { showResult = await ad.show(); }
              catch (e) {
                try { showError = JSON.stringify(e); }
                catch (_) { showError = String(e); }
              }
              return {events, showResult, showError};
            }
        ''', block_id)
        await browser.close()

    result['console'] = console_logs[-20:]
    return result


def run_adsgram_sdk_show(block_id: str, launch_meta: dict):
    return asyncio.run(_run_adsgram_sdk_show_async(block_id, launch_meta))


def adsgram_task_row_from_summary(summary: dict, task_id: int):
    if task_id == ADSGRAM_IMAGE_TASK_ID:
        return summary.get('adsgram_image_task')
    if task_id == ADSGRAM_VIDEO_TASK_ID:
        return summary.get('adsgram_video_task')
    return None


def run_adsgram_task_once(session: requests.Session, launch_meta: dict, task_cfg: dict,
                          poll_seconds: int = 3, max_checks: int = 12):
    task_id = task_cfg['task_id']
    block_id = task_cfg['block_id']
    before = read_watch_endpoints(session)
    before_summary = summarize_watch(before)
    task_row = adsgram_task_row_from_summary(before_summary, task_id)
    result = {
        'task_id': task_id,
        'block_id': block_id,
        'lane': task_cfg['lane'],
        'before_summary': before_summary,
        'before_task': task_row,
        'start': None,
        'check_immediate': None,
        'adsgram_show': None,
        'checks': [],
        'claim': None,
        'after_summary': None,
        'after_task': None,
        'decision': None,
    }

    if task_row is None:
        result['decision'] = 'task_missing'
        result['after_summary'] = before_summary
        return result
    if task_row.get('enable') is False:
        result['decision'] = 'task_disabled'
        result['after_summary'] = before_summary
        return result
    if task_row.get('isExceed'):
        result['decision'] = 'task_is_exceed'
        result['after_summary'] = before_summary
        return result

    if task_row.get('status') == 2:
        result['claim'] = post_json(session, '/tasks/claim', {'task_id': task_id})
        result['after_summary'] = summarize_watch(read_watch_endpoints(session))
        result['after_task'] = adsgram_task_row_from_summary(result['after_summary'], task_id)
        claim_json = result['claim'].get('json') or {}
        result['decision'] = 'claim_ok_pending' if claim_json.get('status') == 0 else 'claim_non_ok_pending'
        return result

    result['start'] = post_json(session, '/tasks/start', {'task_id': task_id, 'init_data': launch_meta['init_data']})
    start_json = result['start'].get('json') or {}
    if start_json.get('status') != 0:
        result['decision'] = 'start_failed'
        result['after_summary'] = summarize_watch(read_watch_endpoints(session))
        result['after_task'] = adsgram_task_row_from_summary(result['after_summary'], task_id)
        return result

    result['check_immediate'] = post_json(session, '/tasks/check', {'task_id': task_id, 'init_data': launch_meta['init_data']})
    immediate_json = result['check_immediate'].get('json') or {}
    immediate_data = immediate_json.get('data') or {}
    if immediate_json.get('status') == 0 and immediate_data.get('status') == 2:
        result['claim'] = post_json(session, '/tasks/claim', {'task_id': task_id})
        result['after_summary'] = summarize_watch(read_watch_endpoints(session))
        result['after_task'] = adsgram_task_row_from_summary(result['after_summary'], task_id)
        claim_json = result['claim'].get('json') or {}
        result['decision'] = 'claim_ok_immediate' if claim_json.get('status') == 0 else 'claim_non_ok'
        return result

    try:
        result['adsgram_show'] = run_adsgram_sdk_show(block_id, launch_meta)
    except Exception as exc:
        result['adsgram_show'] = {
            'events': [],
            'showResult': None,
            'showError': repr(exc),
            'console': [],
        }
        result['decision'] = 'sdk_show_failed'
        result['after_summary'] = summarize_watch(read_watch_endpoints(session))
        result['after_task'] = adsgram_task_row_from_summary(result['after_summary'], task_id)
        return result
    sdk_events = [e.get('name') for e in (result['adsgram_show'].get('events') or []) if e.get('name')]

    for _ in range(int(max_checks)):
        time.sleep(max(int(poll_seconds), 1))
        chk = post_json(session, '/tasks/check', {'task_id': task_id, 'init_data': launch_meta['init_data']})
        result['checks'].append(chk)
        chk_json = chk.get('json') or {}
        chk_data = chk_json.get('data') or {}
        if chk_json.get('status') == 0 and chk_data.get('status') == 2:
            result['claim'] = post_json(session, '/tasks/claim', {'task_id': task_id})
            claim_json = result['claim'].get('json') or {}
            result['decision'] = 'claim_ok' if claim_json.get('status') == 0 else 'claim_non_ok'
            break

    if result['decision'] is None:
        show_result = result['adsgram_show'].get('showResult') or {}
        show_done = show_result.get('done') is True
        show_error = result['adsgram_show'].get('showError')
        if show_done and 'onReward' in sdk_events:
            result['decision'] = 'sdk_reward_but_no_claimable'
        elif show_error:
            result['decision'] = 'sdk_show_failed'
        else:
            result['decision'] = 'check_not_ready'

    result['after_summary'] = summarize_watch(read_watch_endpoints(session))
    result['after_task'] = adsgram_task_row_from_summary(result['after_summary'], task_id)
    if result['decision'] == 'sdk_reward_but_no_claimable':
        after_status = (result['after_task'] or {}).get('status')
        if after_status == 2:
            result['claim'] = post_json(session, '/tasks/claim', {'task_id': task_id})
            claim_json = result['claim'].get('json') or {}
            result['decision'] = 'claim_ok_late' if claim_json.get('status') == 0 else 'claim_non_ok'
        elif after_status == 1:
            result['decision'] = 'sdk_reward_pending'
    return result


def choose_drop_claim_policy(play_star_cap=None):
    bucket_roll = random.random()
    if bucket_roll < 0.55:
        bucket = 'main_280_340'
        points = random.randint(280, 340)
    elif bucket_roll < 0.80:
        bucket = 'mid_230_279'
        points = random.randint(230, 279)
    elif bucket_roll < 0.95:
        bucket = 'low_180_229'
        points = random.randint(180, 229)
    else:
        bucket = 'spike_330_340'
        points = random.randint(330, 340)

    jitter = random.randint(2, 8)
    if random.random() < 0.5:
        points += jitter
        signed_jitter = jitter
    else:
        points -= jitter
        signed_jitter = -jitter

    points = max(160, min(340, points))

    chosen_stars = 0
    star_strategy = 'zero_default'
    normalized_star_cap = None
    if isinstance(play_star_cap, (int, float)):
        normalized_star_cap = round(float(play_star_cap), 2)
        if normalized_star_cap > 0:
            if normalized_star_cap >= 0.02:
                if random.random() < 0.70:
                    chosen_stars = round(max(normalized_star_cap - 0.01, 0), 2)
                    star_strategy = 'under_cap_minus_0_01'
                else:
                    chosen_stars = normalized_star_cap
                    star_strategy = 'exact_cap'
            else:
                chosen_stars = normalized_star_cap
                star_strategy = 'exact_small_cap'

    return {
        'bucket': bucket,
        'points': points,
        'stars': chosen_stars,
        'star_cap': normalized_star_cap,
        'star_strategy': star_strategy,
        'jitter': signed_jitter,
    }


def run_daily_claim(session: requests.Session, now_ts: int):
    before_balance = post_json(session, '/user/balance', {})
    balance_data = (((before_balance.get('json') or {}).get('data')) or {})
    daily = balance_data.get('daily')
    eligible = False
    reason = 'daily_missing'
    current_utc_ymd = int(datetime.now(timezone.utc).strftime('%Y%m%d'))
    if not daily:
        eligible = True
        reason = 'daily_missing'
    else:
        next_check_ts = daily.get('next_check_ts')
        last_check_ymd = daily.get('last_check_ymd')
        if isinstance(last_check_ymd, int) and last_check_ymd >= current_utc_ymd:
            eligible = False
            reason = 'last_check_ymd_is_current_utc_day'
        elif isinstance(next_check_ts, int) and next_check_ts <= now_ts:
            eligible = True
            reason = 'next_check_ts_elapsed_and_ymd_advanced'
        else:
            eligible = False
            reason = 'next_check_ts_not_elapsed'

    result = {
        'eligible_by_read': eligible,
        'reason': reason,
        'before_balance_summary': {
            'available_balance': balance_data.get('available_balance'),
            'play_passes': balance_data.get('play_passes'),
            'daily': daily,
        },
        'claim': None,
        'after_balance_summary': None,
        'decision': 'not_due',
    }

    if eligible:
        result['claim'] = post_json(session, '/daily/claim', {'game_id': DAILY_GAME_ID})
        after_balance = post_json(session, '/user/balance', {})
        after_data = (((after_balance.get('json') or {}).get('data')) or {})
        result['after_balance_summary'] = {
            'available_balance': after_data.get('available_balance'),
            'play_passes': after_data.get('play_passes'),
            'daily': after_data.get('daily'),
        }
        claim_json = result['claim'].get('json') or {}
        claim_message = str(claim_json.get('message') or '').lower()
        if claim_json.get('status') == 0:
            result['decision'] = 'claim_ok'
        elif 'already' in claim_message:
            result['decision'] = 'already_checked'
        else:
            result['decision'] = 'claim_non_ok'
    return result


def extract_farming_summary(summary: dict):
    if not summary:
        return None
    return summary.get('farm_info') or (summary.get('balance') or {}).get('farming')


def run_home_farming_once(session: requests.Session, now_ts: int, runner_state: dict | None = None,
                          post_claim_wait_seconds: float = 2.0):
    before = read_watch_endpoints(session)
    before_summary = summarize_watch(before)
    farm = extract_farming_summary(before_summary)
    lane_state = ((runner_state or {}).get('lanes') or {}).get('home_farming') or {}
    pending_start_after_claim = lane_state.get('pending_start_after_claim') is True
    result = {
        'before_summary': before_summary,
        'decision': None,
        'start': None,
        'claim': None,
        'after_claim_summary': None,
        'after_summary': None,
        'pending_start_before': pending_start_after_claim,
        'post_claim_wait_seconds': post_claim_wait_seconds,
        'balance_delta_claim': None,
    }
    if not farm:
        if pending_start_after_claim:
            result['start'] = post_json(session, '/farm/start', {'game_id': FARM_GAME_ID})
            result['after_summary'] = summarize_watch(read_watch_endpoints(session))
            start_json = result['start'].get('json') or {}
            after_farm = extract_farming_summary(result['after_summary'])
            if start_json.get('status') == 0 or (after_farm or {}).get('state') == 'active_farming':
                result['decision'] = 'recover_start_ok'
            else:
                result['decision'] = 'recover_start_failed'
            return result
        result['decision'] = 'farm_missing'
        return result

    state = farm.get('state')
    if state == 'idle_startable':
        result['start'] = post_json(session, '/farm/start', {'game_id': FARM_GAME_ID})
        result['after_summary'] = summarize_watch(read_watch_endpoints(session))
        start_json = result['start'].get('json') or {}
        after_farm = extract_farming_summary(result['after_summary'])
        if start_json.get('status') == 0 or (after_farm or {}).get('state') == 'active_farming':
            result['decision'] = 'start_ok'
        else:
            result['decision'] = 'start_non_ok'
        return result
    if state == 'claimable':
        result['claim'] = post_json(session, '/farm/claim', {'game_id': FARM_GAME_ID})
        result['after_claim_summary'] = summarize_watch(read_watch_endpoints(session))
        before_balance = (before_summary.get('balance') or {}).get('available_balance')
        after_claim_balance = (result['after_claim_summary'].get('balance') or {}).get('available_balance')
        if isinstance(before_balance, int) and isinstance(after_claim_balance, int):
            result['balance_delta_claim'] = after_claim_balance - before_balance

        claim_json = result['claim'].get('json') or {}
        if claim_json.get('status') != 0:
            result['decision'] = 'claim_non_ok'
            result['after_summary'] = result['after_claim_summary']
            return result

        if post_claim_wait_seconds > 0:
            time.sleep(post_claim_wait_seconds)

        post_claim_farm = extract_farming_summary(result['after_claim_summary'])
        if post_claim_farm is None or post_claim_farm.get('state') == 'idle_startable':
            result['start'] = post_json(session, '/farm/start', {'game_id': FARM_GAME_ID})
            result['after_summary'] = summarize_watch(read_watch_endpoints(session))
            start_json = result['start'].get('json') or {}
            after_farm = extract_farming_summary(result['after_summary'])
            if start_json.get('status') == 0 or (after_farm or {}).get('state') == 'active_farming':
                result['decision'] = 'claim_and_start_ok'
            else:
                result['decision'] = 'claim_ok_start_failed'
            return result

        if post_claim_farm.get('state') == 'claimable':
            result['decision'] = 'claim_ok_still_claimable'
            result['after_summary'] = result['after_claim_summary']
            return result

        result['decision'] = 'claim_ok_no_restart_needed'
        result['after_summary'] = result['after_claim_summary']
        return result

    result['decision'] = f'watch_only_{state}'
    result['after_summary'] = before_summary
    return result


def run_open_ad_once(session: requests.Session, init_data: str, safety_margin: int):
    before = read_watch_endpoints(session)
    before_summary = summarize_watch(before)
    task = before_summary['open_ad_task']
    result = {
        'before_summary': before_summary,
        'start': None,
        'check_1': None,
        'sleep_seconds': None,
        'check_2': None,
        'claim': None,
        'after_summary': None,
        'decision': None,
    }

    if task is None:
        result['decision'] = 'task_missing'
        return result
    if task.get('enable') is False:
        result['decision'] = 'task_disabled'
        return result
    if task.get('isExceed'):
        result['decision'] = 'task_is_exceed'
        return result

    result['start'] = post_json(session, '/tasks/start', {'task_id': OPEN_AD_TASK_ID, 'init_data': init_data})
    start_json = result['start'].get('json') or {}
    if (start_json.get('status') != 0):
        result['decision'] = 'start_failed'
        result['after_summary'] = summarize_watch(read_watch_endpoints(session))
        return result

    result['check_1'] = post_json(session, '/tasks/check', {'task_id': OPEN_AD_TASK_ID, 'init_data': init_data})
    wait_second = task.get('waitSecond') or 10
    sleep_seconds = int(wait_second) + int(safety_margin)
    result['sleep_seconds'] = sleep_seconds
    time.sleep(sleep_seconds)
    result['check_2'] = post_json(session, '/tasks/check', {'task_id': OPEN_AD_TASK_ID, 'init_data': init_data})

    check_2_json = result['check_2'].get('json') or {}
    check_2_data = check_2_json.get('data') or {}
    if check_2_json.get('status') == 0 and check_2_data.get('status') == 2:
        result['claim'] = post_json(session, '/tasks/claim', {'task_id': OPEN_AD_TASK_ID})
        claim_json = result['claim'].get('json') or {}
        if claim_json.get('status') == 0 and claim_json.get('data') == 'ok':
            result['decision'] = 'claim_ok'
        else:
            result['decision'] = 'claim_non_ok'
    else:
        result['decision'] = 'check_not_ready'

    result['after_summary'] = summarize_watch(read_watch_endpoints(session))
    return result


def run_free_spin_once(session: requests.Session):
    before = read_watch_endpoints(session)
    before_summary = summarize_watch(before)
    spin = before_summary.get('spin') or {}
    result = {
        'before_summary': before_summary,
        'spin_once': None,
        'after_summary': None,
        'balance_delta': None,
        'decision': None,
    }

    if spin.get('is_free') is not True:
        result['decision'] = 'not_free'
        result['after_summary'] = before_summary
        return result

    result['spin_once'] = post_json(session, '/spin/once')
    result['after_summary'] = summarize_watch(read_watch_endpoints(session))
    before_balance = (before_summary.get('balance') or {}).get('available_balance')
    after_balance = ((result['after_summary'].get('balance') or {}).get('available_balance'))
    if isinstance(before_balance, int) and isinstance(after_balance, int):
        result['balance_delta'] = after_balance - before_balance

    spin_json = result['spin_once'].get('json') or {}
    spin_data = spin_json.get('data') or {}
    if spin_json.get('status') == 0 and spin_data.get('orderId'):
        result['decision'] = 'spin_ok'
    else:
        result['decision'] = 'spin_non_ok'
    return result


def run_drop_game_once(session: requests.Session, play_pass_reserve: int = 3):
    before = read_watch_endpoints(session)
    before_summary = summarize_watch(before)
    balance_before = before_summary.get('balance') or {}
    play_passes_before = balance_before.get('play_passes')
    result = {
        'before_summary': before_summary,
        'play': None,
        'claim_wait_seconds': None,
        'claim_policy': None,
        'claim_payload': None,
        'claim': None,
        'after_summary': None,
        'balance_delta': None,
        'play_pass_delta': None,
        'decision': None,
    }

    if not isinstance(play_passes_before, int) or play_passes_before <= 0:
        result['decision'] = 'no_play_passes'
        result['after_summary'] = before_summary
        return result
    if play_passes_before <= int(play_pass_reserve):
        result['decision'] = 'reserve_floor_skip'
        result['after_summary'] = before_summary
        result['play_pass_reserve'] = int(play_pass_reserve)
        return result

    result['play'] = post_json(session, '/game/play', {'game_id': DROP_GAME_ID})
    play_json = result['play'].get('json') or {}
    play_data = play_json.get('data') or {}
    if play_json.get('status') != 0 or not play_data.get('round_id'):
        result['decision'] = 'play_failed'
        result['after_summary'] = summarize_watch(read_watch_endpoints(session))
        return result

    end_at = play_data.get('end_at')
    if isinstance(end_at, int):
        wait_seconds = max(float(end_at) - time.time(), 0.0) + random.uniform(1.0, 4.0)
        result['claim_wait_seconds'] = round(wait_seconds, 3)
        time.sleep(wait_seconds)
    else:
        result['claim_wait_seconds'] = 0.0

    result['claim_policy'] = choose_drop_claim_policy(play_data.get('stars'))
    result['claim_payload'] = {
        'game_id': DROP_GAME_ID,
        'points': result['claim_policy']['points'],
        'stars': result['claim_policy']['stars'],
    }
    result['claim'] = post_json(session, '/game/claim', result['claim_payload'])
    result['after_summary'] = summarize_watch(read_watch_endpoints(session))

    after_balance = (result['after_summary'].get('balance') or {}).get('available_balance')
    after_play_passes = (result['after_summary'].get('balance') or {}).get('play_passes')
    before_balance = balance_before.get('available_balance')
    if isinstance(before_balance, int) and isinstance(after_balance, int):
        result['balance_delta'] = after_balance - before_balance
    if isinstance(play_passes_before, int) and isinstance(after_play_passes, int):
        result['play_pass_delta'] = after_play_passes - play_passes_before

    claim_json = result['claim'].get('json') or {}
    if claim_json.get('status') == 0:
        result['decision'] = 'claim_ok'
    else:
        result['decision'] = 'claim_non_ok'
    return result


def schedule_daily_lane(state: dict, result: dict, now_ts: int):
    decision = result.get('decision') or 'unknown'
    success = decision in {'claim_ok', 'already_checked', 'not_due'}
    failures = bump_lane_failure(state, 'daily', success=success if decision != 'not_due' else None)

    daily = ((result.get('after_balance_summary') or result.get('before_balance_summary') or {}).get('daily')) or {}
    next_check_ts = daily.get('next_check_ts')
    if isinstance(next_check_ts, int):
        set_lane_next_due(state, 'daily', next_check_ts + random.randint(30, 120), f'daily_{decision}')
        return

    if decision == 'claim_non_ok':
        set_lane_next_due_in(state, 'daily', now_ts, exponential_backoff_seconds(1800, failures, 21600), f'daily_{decision}')
    else:
        set_lane_next_due_in(state, 'daily', now_ts, 21600, f'daily_{decision}')


def schedule_home_farming_lane(state: dict, result: dict, now_ts: int):
    decision = result.get('decision') or 'unknown'
    failure_decisions = {'farm_missing', 'claim_non_ok', 'claim_ok_start_failed', 'recover_start_failed',
                         'start_non_ok', 'claim_ok_still_claimable'}
    success = False if decision in failure_decisions else True
    failures = bump_lane_failure(state, 'home_farming', success=success)

    after_summary = result.get('after_summary') or result.get('after_claim_summary') or result.get('before_summary') or {}
    farm = extract_farming_summary(after_summary)
    farm_state = (farm or {}).get('state')
    farm_end_at = (farm or {}).get('end_at')

    if decision in {'watch_only_active_farming', 'start_ok', 'claim_and_start_ok', 'recover_start_ok', 'claim_ok_no_restart_needed'}:
        set_lane_due_from_end_at(state, 'home_farming', farm_end_at, now_ts, f'farming_{decision}', jitter_low=20, jitter_high=75)
        return
    if farm_state == 'active_farming':
        set_lane_due_from_end_at(state, 'home_farming', farm_end_at, now_ts, f'farming_state_{farm_state}', jitter_low=20, jitter_high=75)
        return
    if decision in {'claim_ok_start_failed', 'recover_start_failed'}:
        set_lane_next_due_in(state, 'home_farming', now_ts, 60, f'farming_{decision}')
        return
    if decision == 'claim_ok_still_claimable':
        set_lane_next_due_in(state, 'home_farming', now_ts, 300, f'farming_{decision}')
        return
    if decision in {'farm_missing', 'claim_non_ok', 'start_non_ok'}:
        set_lane_next_due_in(state, 'home_farming', now_ts, exponential_backoff_seconds(120, failures, 1800), f'farming_{decision}')
        return
    set_lane_next_due_in(state, 'home_farming', now_ts, 300, f'farming_{decision}')


def schedule_free_spin_lane(state: dict, result: dict, now_ts: int):
    decision = result.get('decision') or 'unknown'
    success = decision in {'spin_ok', 'not_free'}
    failures = bump_lane_failure(state, 'free_spin', success=success if decision != 'not_free' else None)
    if decision in {'spin_ok', 'not_free'}:
        set_lane_next_due_in(state, 'free_spin', now_ts, 21600 + random.randint(60, 300), f'free_spin_{decision}')
        return
    set_lane_next_due_in(state, 'free_spin', now_ts, exponential_backoff_seconds(1800, failures, 21600), f'free_spin_{decision}')


def schedule_openad_lane(state: dict, result: dict, now_ts: int, daily_success_cap: int = 3):
    decision = result.get('decision') or 'unknown'
    neutral = {'task_missing', 'task_disabled', 'task_is_exceed'}
    success = decision == 'claim_ok'
    failures = bump_lane_failure(state, 'open_ad_9001', success=True if success else (None if decision in neutral else False))
    if decision == 'claim_ok':
        if lane_daily_success_count(state, 'open_ad_9001', now_ts, field_prefix='claim_success') >= int(daily_success_cap):
            set_lane_next_due(state, 'open_ad_9001', next_utc_day_start_ts(now_ts) + random.randint(60, 180), 'openad_daily_cap_reached')
            return
        set_lane_next_due_in(state, 'open_ad_9001', now_ts, 3600 + random.randint(60, 180), f'openad_{decision}')
        return
    if decision == 'check_not_ready':
        set_lane_next_due_in(state, 'open_ad_9001', now_ts, 600 + random.randint(15, 45), f'openad_{decision}')
        return
    if decision in neutral:
        set_lane_next_due_in(state, 'open_ad_9001', now_ts, 21600, f'openad_{decision}')
        return
    set_lane_next_due_in(state, 'open_ad_9001', now_ts, exponential_backoff_seconds(900, failures, 21600), f'openad_{decision}')


def schedule_adsgram_lane(state: dict, lane: str, result: dict, now_ts: int, daily_success_cap: int = 1):
    decision = result.get('decision') or 'unknown'
    neutral = {'task_missing', 'task_disabled', 'task_is_exceed'}
    success = decision in {'claim_ok', 'claim_ok_pending', 'claim_ok_immediate', 'claim_ok_late'}
    failures = bump_lane_failure(state, lane, success=True if success else (None if decision in neutral else False))

    if success:
        if lane_daily_success_count(state, lane, now_ts, field_prefix='claim_success') >= int(daily_success_cap):
            set_lane_next_due(state, lane, next_utc_day_start_ts(now_ts) + random.randint(60, 240), f'{lane}_daily_cap_reached')
            return
        set_lane_next_due_in(state, lane, now_ts, 21600 + random.randint(120, 300), f'{lane}_{decision}')
        return
    if decision in neutral:
        set_lane_next_due_in(state, lane, now_ts, 21600, f'{lane}_{decision}')
        return
    if decision == 'sdk_reward_pending':
        set_lane_next_due_in(state, lane, now_ts, 300 + random.randint(15, 45), f'{lane}_{decision}')
        return
    if decision in {'sdk_reward_but_no_claimable', 'check_not_ready'}:
        set_lane_next_due_in(state, lane, now_ts, 3600 + random.randint(60, 180), f'{lane}_{decision}')
        return
    if decision == 'start_failed':
        set_lane_next_due_in(state, lane, now_ts, exponential_backoff_seconds(1800, failures, 43200), f'{lane}_{decision}')
        return
    if decision in {'sdk_show_failed', 'claim_non_ok', 'claim_non_ok_pending'}:
        set_lane_next_due_in(state, lane, now_ts, exponential_backoff_seconds(3600, failures, 86400), f'{lane}_{decision}')
        return
    set_lane_next_due_in(state, lane, now_ts, exponential_backoff_seconds(1800, failures, 86400), f'{lane}_{decision}')


def schedule_drop_game_lane(state: dict, result: dict, now_ts: int):
    decision = result.get('decision') or 'unknown'
    neutral = {'no_play_passes', 'reserve_floor_skip'}
    success = decision == 'claim_ok'
    failures = bump_lane_failure(state, 'drop_game', success=True if success else (None if decision in neutral else False))
    if decision == 'claim_ok':
        set_lane_next_due_in(state, 'drop_game', now_ts, 2700 + random.randint(30, 120), f'drop_game_{decision}')
        return
    if decision in neutral:
        set_lane_next_due_in(state, 'drop_game', now_ts, 1800 + random.randint(60, 180), f'drop_game_{decision}')
        return
    set_lane_next_due_in(state, 'drop_game', now_ts, exponential_backoff_seconds(1800, failures, 21600), f'drop_game_{decision}')


def run_iteration(args, runner_state: dict):
    launch_url = load_launch_url(getattr(args, 'bootstrap_path', None))
    launch_meta = parse_launch(launch_url)
    session = make_session(launch_url)
    login_meta = login(session, launch_meta)
    now_ts = int(time.time())
    safe_mode_active = safe_mode_is_active(runner_state, now_ts)
    risky_error_count = 0
    parked_this_iteration = []

    summary = {
        'captured_at': utc_now_iso(),
        'mode': 'loop' if args.loop else 'once',
        'launch_host': launch_meta['host'],
        'platform': launch_meta['platform'],
        'version': launch_meta['version'],
        'language_code': launch_meta['language_code'],
        'login': login_meta,
        'daily_claim': None,
        'home_farming': None,
        'free_spin': None,
        'open_ad_9001': None,
        'adsgram_image_8003': None,
        'adsgram_video_8002': None,
        'drop_game': None,
        'scheduler': None,
    }

    if not args.skip_daily:
        if lane_is_due(runner_state, 'daily', now_ts):
            summary['daily_claim'] = run_daily_claim(session, now_ts)
            record_lane_attempt(runner_state, 'daily', now_ts, summary['daily_claim'].get('decision') or 'unknown')
            schedule_daily_lane(runner_state, summary['daily_claim'], now_ts)
        else:
            summary['daily_claim'] = schedule_skip_payload(runner_state, 'daily', now_ts)

    farming_priority_active = False
    if not args.skip_farming:
        if lane_is_due(runner_state, 'home_farming', now_ts):
            summary['home_farming'] = run_home_farming_once(
                session,
                now_ts,
                runner_state=runner_state,
                post_claim_wait_seconds=args.farming_post_claim_wait_seconds,
            )
            home_farming_decision = summary['home_farming'].get('decision') or 'unknown'
            record_lane_attempt(runner_state, 'home_farming', now_ts, home_farming_decision)

            farming_after_summary = summary['home_farming'].get('after_summary') or summary['home_farming'].get('before_summary') or {}
            farming_after = extract_farming_summary(farming_after_summary)
            farming_claim_json = (summary['home_farming'].get('claim') or {}).get('json') or {}
            farming_claim_data = farming_claim_json.get('data') or {}
            farming_start_json = (summary['home_farming'].get('start') or {}).get('json') or {}
            farming_start_data = farming_start_json.get('data') or {}
            pending_start_after_claim = home_farming_decision in {'claim_ok_start_failed', 'recover_start_failed'}

            update_lane_state(
                runner_state,
                'home_farming',
                last_observed_state=(farming_after or {}).get('state'),
                last_observed_round_id=(farming_after or {}).get('round_id'),
                pending_start_after_claim=pending_start_after_claim,
            )

            if farming_claim_json.get('status') == 0:
                update_lane_state(
                    runner_state,
                    'home_farming',
                    last_claim_success_ts=int(time.time()),
                    last_claim_success_iso=utc_now_iso(),
                    last_claim_round_id=farming_claim_data.get('round_id'),
                    last_claim_amount=farming_claim_data.get('claim_this_time'),
                    last_claim_finished=farming_claim_data.get('finished'),
                    last_claim_balance_delta=summary['home_farming'].get('balance_delta_claim'),
                )

            if farming_start_json.get('status') == 0:
                update_lane_state(
                    runner_state,
                    'home_farming',
                    last_start_success_ts=int(time.time()),
                    last_start_success_iso=utc_now_iso(),
                    last_start_round_id=farming_start_data.get('round_id'),
                    last_start_at=farming_start_data.get('start_at'),
                    last_end_at=farming_start_data.get('end_at'),
                    last_start_stars=farming_start_data.get('stars'),
                    pending_start_after_claim=False,
                )

            schedule_home_farming_lane(runner_state, summary['home_farming'], now_ts)
            farming_priority_active = home_farming_decision in {
                'claim_and_start_ok', 'claim_ok_start_failed', 'recover_start_ok', 'recover_start_failed',
                'claim_ok_no_restart_needed', 'claim_ok_still_claimable', 'start_ok', 'start_non_ok'
            }
        else:
            summary['home_farming'] = schedule_skip_payload(runner_state, 'home_farming', now_ts)

    if not args.skip_free_spin:
        if lane_is_parked(runner_state, 'free_spin', now_ts):
            summary['free_spin'] = parked_skip_payload(runner_state, 'free_spin', now_ts)
        elif safe_mode_active:
            summary['free_spin'] = safe_mode_skip_payload(runner_state, 'free_spin', now_ts)
        elif farming_priority_active:
            summary['free_spin'] = deferred_for_priority_payload(runner_state, 'free_spin', now_ts, 'home_farming')
        elif lane_is_due(runner_state, 'free_spin', now_ts):
            summary['free_spin'] = run_free_spin_once(session)
            free_spin_decision = summary['free_spin'].get('decision') or 'unknown'
            record_lane_attempt(runner_state, 'free_spin', now_ts, free_spin_decision)
            if free_spin_decision == 'spin_ok':
                spin_json = (summary['free_spin'].get('spin_once') or {}).get('json') or {}
                spin_data = spin_json.get('data') or {}
                spin_result = spin_data.get('results') or {}
                update_lane_state(
                    runner_state,
                    'free_spin',
                    last_success_ts=int(time.time()),
                    last_success_iso=utc_now_iso(),
                    last_reward_amount=spin_result.get('amount'),
                    last_reward_type=spin_result.get('type'),
                    last_order_id=spin_data.get('orderId'),
                    last_balance_delta=summary['free_spin'].get('balance_delta'),
                )
            schedule_free_spin_lane(runner_state, summary['free_spin'], now_ts)
            if maybe_park_risky_lane(runner_state, 'free_spin', free_spin_decision, now_ts,
                                     args.park_after_consecutive_failures, args.park_seconds):
                parked_this_iteration.append('free_spin')
            if lane_decision_severity(free_spin_decision) == 'error':
                risky_error_count += 1
        else:
            summary['free_spin'] = schedule_skip_payload(runner_state, 'free_spin', now_ts)

    if not args.skip_openad:
        if lane_is_parked(runner_state, 'open_ad_9001', now_ts):
            summary['open_ad_9001'] = parked_skip_payload(runner_state, 'open_ad_9001', now_ts)
        elif safe_mode_active:
            summary['open_ad_9001'] = safe_mode_skip_payload(runner_state, 'open_ad_9001', now_ts)
        elif farming_priority_active:
            summary['open_ad_9001'] = deferred_for_priority_payload(runner_state, 'open_ad_9001', now_ts, 'home_farming')
        elif lane_is_due(runner_state, 'open_ad_9001', now_ts):
            summary['open_ad_9001'] = run_open_ad_once(session, launch_meta['init_data'], args.openad_margin)
            openad_decision = summary['open_ad_9001'].get('decision') or 'unknown'
            record_lane_attempt(runner_state, 'open_ad_9001', now_ts, openad_decision)
            if openad_decision == 'claim_ok':
                increment_lane_daily_success(runner_state, 'open_ad_9001', now_ts, field_prefix='claim_success')
            schedule_openad_lane(runner_state, summary['open_ad_9001'], now_ts, daily_success_cap=args.openad_daily_success_cap)
            if maybe_park_risky_lane(runner_state, 'open_ad_9001', openad_decision, now_ts,
                                     args.park_after_consecutive_failures, args.park_seconds):
                parked_this_iteration.append('open_ad_9001')
            if lane_decision_severity(openad_decision) == 'error':
                risky_error_count += 1
        else:
            summary['open_ad_9001'] = schedule_skip_payload(runner_state, 'open_ad_9001', now_ts)

    for task_cfg, skip_flag in [
        (ADSGRAM_TASKS[ADSGRAM_IMAGE_TASK_ID], args.skip_adsgram_image),
        (ADSGRAM_TASKS[ADSGRAM_VIDEO_TASK_ID], args.skip_adsgram_video),
    ]:
        lane = task_cfg['lane']
        if skip_flag:
            continue
        if lane_is_parked(runner_state, lane, now_ts):
            summary[lane] = parked_skip_payload(runner_state, lane, now_ts)
        elif safe_mode_active:
            summary[lane] = safe_mode_skip_payload(runner_state, lane, now_ts)
        elif farming_priority_active:
            summary[lane] = deferred_for_priority_payload(runner_state, lane, now_ts, 'home_farming')
        elif lane_is_due(runner_state, lane, now_ts):
            summary[lane] = run_adsgram_task_once(
                session,
                launch_meta,
                task_cfg,
                poll_seconds=args.adsgram_poll_seconds,
                max_checks=args.adsgram_max_checks,
            )
            adsgram_decision = summary[lane].get('decision') or 'unknown'
            record_lane_attempt(runner_state, lane, now_ts, adsgram_decision)
            if adsgram_decision in {'claim_ok', 'claim_ok_pending', 'claim_ok_immediate'}:
                increment_lane_daily_success(runner_state, lane, now_ts, field_prefix='claim_success')
                claim_json = (summary[lane].get('claim') or {}).get('json') or {}
                update_lane_state(
                    runner_state,
                    lane,
                    last_success_ts=int(time.time()),
                    last_success_iso=utc_now_iso(),
                    last_claim_status=claim_json.get('status'),
                    last_block_id=task_cfg['block_id'],
                    last_sdk_events=[e.get('name') for e in ((summary[lane].get('adsgram_show') or {}).get('events') or []) if e.get('name')],
                )
            schedule_adsgram_lane(runner_state, lane, summary[lane], now_ts, daily_success_cap=args.adsgram_daily_success_cap)
            if maybe_park_risky_lane(runner_state, lane, adsgram_decision, now_ts,
                                     args.park_after_consecutive_failures, args.park_seconds):
                parked_this_iteration.append(lane)
            if lane_decision_severity(adsgram_decision) == 'error':
                risky_error_count += 1
        else:
            summary[lane] = schedule_skip_payload(runner_state, lane, now_ts)

    if not args.skip_drop_game:
        if lane_is_parked(runner_state, 'drop_game', now_ts):
            summary['drop_game'] = parked_skip_payload(runner_state, 'drop_game', now_ts)
        elif safe_mode_active:
            summary['drop_game'] = safe_mode_skip_payload(runner_state, 'drop_game', now_ts)
        elif farming_priority_active:
            summary['drop_game'] = deferred_for_priority_payload(runner_state, 'drop_game', now_ts, 'home_farming')
        elif lane_is_due(runner_state, 'drop_game', now_ts):
            summary['drop_game'] = run_drop_game_once(session, play_pass_reserve=args.dropgame_play_pass_reserve)
            drop_game_decision = summary['drop_game'].get('decision') or 'unknown'
            record_lane_attempt(runner_state, 'drop_game', now_ts, drop_game_decision)
            if drop_game_decision == 'claim_ok':
                claim_json = (summary['drop_game'].get('claim') or {}).get('json') or {}
                claim_data = claim_json.get('data') or {}
                update_lane_state(
                    runner_state,
                    'drop_game',
                    last_success_ts=int(time.time()),
                    last_success_iso=utc_now_iso(),
                    last_round_id=claim_data.get('round_id'),
                    last_points_claimed=claim_data.get('points'),
                    last_stars_claimed=claim_data.get('stars'),
                    last_play_passes_before=((summary['drop_game'].get('before_summary') or {}).get('balance') or {}).get('play_passes'),
                    last_play_passes_after=((summary['drop_game'].get('after_summary') or {}).get('balance') or {}).get('play_passes'),
                    last_balance_delta=summary['drop_game'].get('balance_delta'),
                    last_claim_policy=summary['drop_game'].get('claim_policy'),
                    last_claim_wait_seconds=summary['drop_game'].get('claim_wait_seconds'),
                )
            schedule_drop_game_lane(runner_state, summary['drop_game'], now_ts)
            if maybe_park_risky_lane(runner_state, 'drop_game', drop_game_decision, now_ts,
                                     args.park_after_consecutive_failures, args.park_seconds):
                parked_this_iteration.append('drop_game')
            if lane_decision_severity(drop_game_decision) == 'error':
                risky_error_count += 1
        else:
            summary['drop_game'] = schedule_skip_payload(runner_state, 'drop_game', now_ts)

    if parked_this_iteration:
        set_safe_mode(
            runner_state,
            now_ts,
            args.safe_mode_seconds,
            reason='lane_parked',
            source_lane=','.join(parked_this_iteration),
        )
        safe_mode_active = True
    elif risky_error_count >= args.safe_mode_error_threshold:
        set_safe_mode(
            runner_state,
            now_ts,
            args.safe_mode_seconds,
            reason=f'risky_errors:{risky_error_count}',
            source_lane='iteration',
        )
        safe_mode_active = True

    summary['scheduler'] = {
        'recommended_sleep_seconds': compute_runner_sleep_seconds(
            runner_state,
            now_ts,
            fallback_seconds=args.loop_sleep_seconds,
            min_sleep_seconds=args.min_loop_sleep_seconds,
        ),
        'lanes': scheduler_snapshot(runner_state, now_ts),
        'safe_mode': safe_mode_state(runner_state),
    }

    return summary, runner_state


def run_once(args):
    runner_state = load_runner_state()
    summary, runner_state = run_iteration(args, runner_state)
    if not args.no_write:
        save_runner_state(runner_state)
    return summary


def main():
    parser = argparse.ArgumentParser(description='Tomarket single-account runner for daily claim, farming, spin, drop, open_ad, and AdsGram lanes')
    parser.add_argument('--bootstrap-path', type=pathlib.Path, default=BOOTSTRAP_RAW, help='path to bootstrap launch artifact JSON')
    parser.add_argument('--openad-margin', type=int, default=10, help='extra seconds added to task waitSecond before second check')
    parser.add_argument('--openad-daily-success-cap', type=int, default=3,
                        help='maximum successful open_ad claims per UTC day before the lane is delayed until next UTC day')
    parser.add_argument('--adsgram-daily-success-cap', type=int, default=1,
                        help='maximum successful AdsGram claims per lane per UTC day before that lane is delayed until next UTC day')
    parser.add_argument('--adsgram-poll-seconds', type=int, default=3,
                        help='seconds between post-SDK task status polls for AdsGram lanes')
    parser.add_argument('--adsgram-max-checks', type=int, default=12,
                        help='maximum number of post-SDK task status polls for AdsGram lanes')
    parser.add_argument('--farming-post-claim-wait-seconds', type=float, default=2.0,
                        help='seconds to wait after a successful farm claim before attempting farm start')
    parser.add_argument('--dropgame-play-pass-reserve', type=int, default=3,
                        help='keep at least this many play passes untouched before drop-game can run')
    parser.add_argument('--park-after-consecutive-failures', type=int, default=3,
                        help='park risky lanes after this many consecutive error-class failures')
    parser.add_argument('--park-seconds', type=int, default=21600,
                        help='seconds to keep a risky lane parked after repeated failures')
    parser.add_argument('--safe-mode-error-threshold', type=int, default=2,
                        help='enter safe mode when this many risky lane errors happen in one iteration')
    parser.add_argument('--safe-mode-seconds', type=int, default=3600,
                        help='seconds to keep safe mode active after risky-lane anomalies')
    parser.add_argument('--skip-daily', action='store_true', help='skip daily claim lane')
    parser.add_argument('--skip-free-spin', action='store_true', help='skip free spin lane')
    parser.add_argument('--skip-openad', action='store_true', help='skip open_ad 9001 lane')
    parser.add_argument('--skip-adsgram-image', action='store_true', help='skip AdsGram image lane (task 8003)')
    parser.add_argument('--skip-adsgram-video', action='store_true', help='skip AdsGram video lane (task 8002)')
    parser.add_argument('--skip-drop-game', action='store_true', help='skip drop game lane')
    parser.add_argument('--skip-farming', action='store_true', help='skip home farming lane')
    parser.add_argument('--loop', action='store_true', help='run continuously with sleep between iterations')
    parser.add_argument('--loop-sleep-seconds', type=int, default=300,
                        help='fallback sleep when no lane next_due schedule is available')
    parser.add_argument('--min-loop-sleep-seconds', type=int, default=15,
                        help='minimum sleep when a lane is due soon or already due')
    parser.add_argument('--max-iterations', type=int, default=0, help='stop after N iterations in loop mode; 0 means unlimited')
    parser.add_argument('--no-write', action='store_true', help='do not write artifact files')
    args = parser.parse_args()

    if not args.loop:
        summary = run_once(args)

        if not args.no_write:
            OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
            ts = datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')
            out_path = OUTPUT_DIR / f'{ts}.json'
            latest_path = OUTPUT_DIR / 'latest.json'
            out_path.write_text(json.dumps(summary, indent=2))
            latest_path.write_text(json.dumps(summary, indent=2))
            runner_state = load_runner_state()
            write_runtime_logs(summary, runner_state, int(time.time()))
            write_state_summary_file(summary, runner_state, int(time.time()))

        print(json.dumps(summary, indent=2)[:30000])
        return

    runner_state = load_runner_state()
    iteration = 0
    while True:
        iteration += 1
        summary, runner_state = run_iteration(args, runner_state)
        summary['iteration'] = iteration
        if not args.no_write:
            OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
            ts = datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')
            out_path = OUTPUT_DIR / f'{ts}.json'
            latest_path = OUTPUT_DIR / 'latest.json'
            out_path.write_text(json.dumps(summary, indent=2))
            latest_path.write_text(json.dumps(summary, indent=2))
            save_runner_state(runner_state)
            write_runtime_logs(summary, runner_state, int(time.time()))
            write_state_summary_file(summary, runner_state, int(time.time()))
        print(json.dumps(summary, indent=2)[:30000])
        if args.max_iterations and iteration >= args.max_iterations:
            break
        sleep_seconds = (((summary.get('scheduler') or {}).get('recommended_sleep_seconds')) or args.loop_sleep_seconds)
        time.sleep(max(int(sleep_seconds), 1))


if __name__ == '__main__':
    try:
        main()
    except RunnerError as e:
        print(f'ERROR: {e}', file=sys.stderr)
        sys.exit(2)

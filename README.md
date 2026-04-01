# Tomarket Runner

![Python](https://img.shields.io/badge/python-3.11%2B-blue)
![Status](https://img.shields.io/badge/status-production--candidate-orange)
![License](https://img.shields.io/badge/license-MIT-green)

Single-account Tomarket automation runner with:
- daily claim
- farming claim/start cycle
- free spin
- drop mini-game
- OpenAD task 9001
- AdsGram image/video tasks via browser SDK path

## What this repo is

This is a sanitized standalone repo extracted from live Tomarket reverse-engineering and runner hardening work.
It does **not** include live launch params, tokens, sessions, or private operational artifacts.

## Features

- Per-lane scheduler with `next_due_ts`
- Result-based cooldown and backoff
- Farming-first priority
- Drop-game reserve floor
- OpenAD daily cap
- AdsGram browser/SDK execution path with Playwright
- Decision log, error log, and state summary outputs
- Safe-mode and lane parking

## Files

- `tomarket_runner.py` — main runner
- `tomarket_readonly_probe.py` — safe read-only probe
- `bootstrap.example.json` — launch URL artifact shape expected by the scripts

## Setup

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
playwright install chromium
```

If you want to use a system Chrome instead of the bundled Playwright browser:

```bash
export TOMARKET_CHROME=/usr/bin/google-chrome-stable
```

## Bootstrap input

Create a launch artifact at `state/bootstrap/launch.json` using the same shape as `bootstrap.example.json`.
You must provide your **own** Telegram WebApp launch URL / init data.

## Read-only probe

```bash
python3 tomarket_readonly_probe.py --bootstrap-path state/bootstrap/launch.json
```

## Runner example

```bash
python3 tomarket_runner.py \
  --bootstrap-path state/bootstrap/launch.json \
  --loop \
  --max-iterations 200 \
  --openad-daily-success-cap 2 \
  --adsgram-daily-success-cap 1 \
  --dropgame-play-pass-reserve 4
```

## Output

Runner state is written under `state/runner/`:
- `latest.json`
- `runner-state.json`
- `decision-log.jsonl`
- `error-log.jsonl`
- `state-summary.json`

## Notes

- AdsGram lanes depend on real browser/SDK execution. Plain REST polling is not enough.
- This repo is a **production-ready candidate**, not a promise that every account/environment will behave identically.
- Bring your own launch bootstrap and use at your own risk.

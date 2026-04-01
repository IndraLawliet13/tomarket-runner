# Tomarket Runner

![Python](https://img.shields.io/badge/python-3.11%2B-blue)
![Playwright](https://img.shields.io/badge/browser-playwright-45ba4b)
![Status](https://img.shields.io/badge/status-production--candidate-orange)
![License](https://img.shields.io/badge/license-MIT-green)

Single-account Tomarket automation runner with:
- daily claim
- farming claim/start cycle
- free spin
- drop mini-game
- OpenAD task 9001
- AdsGram image/video tasks via real browser SDK execution

## Why this repo exists

This repo is a public-safe extraction of a real Tomarket automation/hardening effort.
It keeps the useful parts of the implementation and the architecture, while excluding live launch params, private sessions, and sensitive artifacts.

## Current status

**Current maturity:** production-ready candidate

What that means:
- core earning lanes are implemented
- HTTP-only and browser/SDK-assisted lanes are both covered
- scheduler, backoff, parking, and safe-mode exist
- still needs longer unattended soak validation before claiming final 100% production-ready status

## What it automates

**HTTP-only lanes**
- daily claim
- farming claim/start
- free spin
- drop game
- OpenAD 9001

**Browser/SDK-assisted lanes**
- AdsGram image `8003`
- AdsGram video `8002`

## Use cases

- Reverse engineering a Telegram Mini App automation flow
- Running a single-account Tomarket claim loop with safety controls
- Studying how to combine HTTP-only lanes with browser-assisted SDK lanes
- Building a scheduler-driven reward runner with logs, parking, and safe-mode
- Using sanitized public code as a reference before adapting it to a private environment

## Architecture overview

```mermaid
flowchart TD
    A[Bootstrap launch artifact] --> B[Login to Tomarket API]
    B --> C[Read live state: balance tasks farming spin]
    C --> D[Scheduler picks next due lane]
    D --> E{Lane type}
    E -->|HTTP-only| F[Daily / Farming / Spin / Drop / OpenAD]
    E -->|Browser+SDK| G[AdsGram image / video]
    F --> H[Update runner state]
    G --> I[Start task -> SDK show -> check -> claim]
    I --> H
    H --> J[Write decision log / error log / state summary]
    J --> K[Compute next_due_ts and sleep]
```

## Demo preview

![Demo preview](docs/images/demo-preview.gif)

## Flow diagrams

Detailed flows live in [`docs/FLOWS.md`](docs/FLOWS.md).

### Runner scheduling overview

```mermaid
flowchart TD
    A[Bootstrap launch.json] --> B[Login to Tomarket]
    B --> C[Read balance/tasks/farming/spin state]
    C --> D{Lane due?}
    D -->|No| E[scheduled_skip]
    D -->|Yes| F{Lane type}
    F -->|HTTP-only| G[Run lane directly]
    F -->|AdsGram| H[Run start -> SDK show -> check -> claim]
    G --> I[Update lane state]
    H --> I
    I --> J[Write decision-log / state-summary]
    J --> K[Compute next_due_ts]
    K --> L[Sleep until nearest due lane]
```

### AdsGram hybrid path

```mermaid
sequenceDiagram
    participant R as Runner
    participant T as Tomarket API
    participant B as Browser + AdsGram SDK

    R->>T: /tasks/start
    T-->>R: status=1
    R->>T: /tasks/check
    T-->>R: status=1
    R->>B: AdsGram.init(blockId).show()
    B-->>R: onStart / onReward
    R->>T: /tasks/check
    T-->>R: status=2
    R->>T: /tasks/claim
    T-->>R: status=0 / ok
```

## Screenshots / samples

### State summary sample

![State summary sample](docs/images/state-summary-sample.svg)

### Decision log sample

![Decision log sample](docs/images/decision-log-sample.svg)

## Repository layout

- `tomarket_runner.py` — main runner
- `tomarket_readonly_probe.py` — safe read-only probe
- `bootstrap.example.json` — example launch artifact shape
- `docs/FLOWS.md` — deeper flow diagrams and state-machine notes

## Features

- per-lane scheduler with `next_due_ts`
- result-based cooldown and backoff
- farming-first priority
- drop-game reserve floor
- OpenAD daily cap
- AdsGram browser/SDK execution path with Playwright
- decision log, error log, and state summary outputs
- safe-mode and lane parking

## Setup

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
playwright install chromium
```

If you want to use a system Chrome instead of bundled Playwright Chromium:

```bash
export TOMARKET_CHROME=/usr/bin/google-chrome-stable
```

## Bootstrap input

Create a launch artifact at `state/bootstrap/launch.json` using the same shape as `bootstrap.example.json`.
You must provide your own Telegram WebApp launch URL / init data.

## Read-only probe

```bash
python3 tomarket_readonly_probe.py --bootstrap-path state/bootstrap/launch.json
```

## Runner example

```bash
python3 tomarket_runner.py   --bootstrap-path state/bootstrap/launch.json   --loop   --max-iterations 200   --openad-daily-success-cap 2   --adsgram-daily-success-cap 1   --dropgame-play-pass-reserve 4
```

## Output

Runner state is written under `state/runner/`:
- `latest.json`
- `runner-state.json`
- `decision-log.jsonl`
- `error-log.jsonl`
- `state-summary.json`

## Public roadmap

- [ ] finish longer unattended soak validation
- [ ] tighten reward-sink visibility for `9001 open_ad`
- [ ] close star settlement/readback mapping
- [ ] document safer deployment patterns for long-lived runner execution
- [ ] prepare a cleaner multi-account design only after single-account stability is fully proven

## Notes

- AdsGram lanes depend on real browser/SDK execution. Plain REST polling is not enough.
- This repo is a **production-ready candidate**, not a promise that every account or environment will behave identically.
- Bring your own launch bootstrap and use at your own risk.

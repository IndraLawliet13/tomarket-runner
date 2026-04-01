# Deployment Guide

## Current recommendation

Treat this runner as a **production-ready candidate** for single-account use.
It is suitable for continued soak validation, not for blind high-scale deployment.

## Deployment modes

### 1. Local / foreground validation
Use this when testing new lane logic or validating bootstrap artifacts.

```bash
python3 tomarket_runner.py --bootstrap-path state/bootstrap/launch.json
```

### 2. Bounded soak run
Use this when validating stability over multiple iterations with conservative caps.

```bash
python3 tomarket_runner.py   --bootstrap-path state/bootstrap/launch.json   --loop   --max-iterations 200   --openad-daily-success-cap 2   --adsgram-daily-success-cap 1   --dropgame-play-pass-reserve 4
```

### 3. Longer unattended run
Only consider this after bounded soak behavior is understood and reviewed.

## Browser requirements

AdsGram lanes require real browser/SDK execution.
Plain REST polling is not enough.

Recommended setup:
- Playwright installed
- Chromium available, or
- system Chrome via `TOMARKET_CHROME`

Example:

```bash
export TOMARKET_CHROME=/usr/bin/google-chrome-stable
```

## Safety controls already built in

- per-lane scheduler via `next_due_ts`
- result-based cooldown and backoff
- farming-first priority
- drop-game reserve floor
- OpenAD daily cap
- AdsGram daily cap
- lane parking
- global safe mode
- decision / error / state-summary logs

## Before calling it truly production-ready

You should want all of these:
- longer unattended soak completes without crash
- no repeated unsafe oscillation in scheduler decisions
- no recurring AdsGram browser failures
- no unsafe lane spam or retry storms
- reward behavior is stable enough for your risk tolerance

## Operational caution

- never commit live launch params or session material
- keep public-safe and private-live artifacts separate
- prefer conservative caps first, then loosen only after evidence
- if a lane starts drifting or turning brittle, park it first and investigate second

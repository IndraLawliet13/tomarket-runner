# Tomarket Flows

## 1. Lane priority

```mermaid
flowchart TD
    A[Read latest state] --> B{Farming claimable?}
    B -->|Yes| C[Claim farming]
    C --> D[Short settle window]
    D --> E[Start new farming round]
    B -->|No| F{Daily due?}
    F -->|Yes| G[Claim daily]
    F -->|No| H{AdsGram/OpenAD due?}
    H -->|Yes| I[Run ad lane with caps and parking]
    H -->|No| J{Drop due and passes above reserve?}
    J -->|Yes| K[Play + claim drop]
    J -->|No| L[Sleep until nearest due lane]
```

## 2. Farming state machine

```mermaid
stateDiagram-v2
    [*] --> idle_startable
    idle_startable --> active_farming: /farm/start
    active_farming --> claimable: end_at reached
    claimable --> transition_null: /farm/claim success
    transition_null --> active_farming: /farm/start
```

## 3. AdsGram hybrid state machine

```mermaid
stateDiagram-v2
    [*] --> task_status_0
    task_status_0 --> task_status_1: /tasks/start
    task_status_1 --> sdk_running: AdsGram SDK show
    sdk_running --> reward_seen: onReward
    reward_seen --> task_status_2: /tasks/check
    task_status_2 --> claimed: /tasks/claim
    reward_seen --> pending_retry: reward but row still 1
    pending_retry --> sdk_running: short retry window
```

## 4. Safety controls

- per-lane `next_due_ts`
- daily success caps for ad lanes
- parked risky lanes after repeated failures
- global safe-mode to suppress risky lanes temporarily
- compact decision and error logs for unattended review

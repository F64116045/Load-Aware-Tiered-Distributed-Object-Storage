# How-to: Tune Tiering Policies (A1/A2/A3 + Trigger Modes)

Use this when you want reproducible policy experiments.

## 1. Policy Variant Selection

Set one:

1. `TIERING_POLICY_VARIANT=A1` age-only baseline
2. `TIERING_POLICY_VARIANT=A2` age + size threshold
3. `TIERING_POLICY_VARIANT=A3` budget-limited selection

## 2. Trigger Mode Selection

Set one:

1. `TIERING_TRIGGER_MODE=periodic`
2. `TIERING_TRIGGER_MODE=threshold`
3. `TIERING_TRIGGER_MODE=hybrid`

## 3. Core Knobs

1. `AGE_THRESHOLD_SEC`
2. `SIZE_THRESHOLD_BYTES`
3. `MAX_OBJECTS_PER_ROUND`
4. `MAX_BYTES_PER_ROUND`
5. `TIERING_PERIOD_SEC`
6. `TIERING_THRESHOLD_CHECK_SEC`
7. `TIERING_THRESHOLD_COOLDOWN_SEC`

## 4. Idle Window (Strategy C style)

Tune all together:

1. `TIERING_IDLE_STABLE_ROUNDS`
2. `TIERING_IDLE_CPU_PCT`
3. `TIERING_IDLE_MEMORY_PCT`
4. `TIERING_IDLE_IOWAIT_PCT`
5. `TIERING_IDLE_QUEUE_DEPTH`

Interpretation:

1. scanner runs migration only when metrics remain below thresholds for N rounds
2. one metric breach resets stable counter

## 5. Pressure Trigger Inputs

1. `HOT_PRESSURE_DISK_PCT`
2. `HOT_PRESSURE_QUEUE_DEPTH`

## 6. Suggested Experiment Template

1. fix object size/concurrency distribution
2. run A1 periodic baseline
3. run A2 with size threshold
4. run A3 with strict budget cap
5. run threshold/hybrid with idle window enabled
6. collect latency + queue-depth + completion metrics

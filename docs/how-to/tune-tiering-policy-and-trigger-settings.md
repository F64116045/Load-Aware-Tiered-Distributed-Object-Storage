# How-to: Tune Tiering Policies (A/B/C + Trigger Modes)

Scope: reproducible policy tuning and experiment parameterization.

## 1. Policy Variant Selection

Set one:

1. `TIERING_POLICY_VARIANT=A` time-based baseline (age eligibility only)
2. `TIERING_POLICY_VARIANT=B` static throttling (age + object/byte budget)
3. `TIERING_POLICY_VARIANT=C` idle-window admission + static throttling

## 2. Trigger Mode Selection

Set one:

1. `TIERING_TRIGGER_MODE=periodic`
2. `TIERING_TRIGGER_MODE=threshold`
3. `TIERING_TRIGGER_MODE=hybrid`

Current behavior summary:

1. `periodic` runs scanner every `TIERING_PERIOD_SEC`.
2. `threshold` runs threshold ticks and uses idle-window checks before policy enqueue.
3. `hybrid` enables both tick sources.

## 3. Core Knobs

1. `AGE_THRESHOLD_SEC`
2. `MAX_OBJECTS_PER_ROUND`
3. `MAX_BYTES_PER_ROUND`
4. `TIERING_PERIOD_SEC`
5. `TIERING_THRESHOLD_CHECK_SEC`
6. `TIERING_THRESHOLD_COOLDOWN_SEC`

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
3. if gate is false, no tiering enqueue happens in that pass and due-index stays for later passes

## 5. Pressure Trigger Inputs

1. `HOT_PRESSURE_DISK_PCT`
2. `HOT_PRESSURE_QUEUE_DEPTH`

Note:

1. these pressure inputs are currently configuration-level inputs; scanner threshold path is currently idle-window based.

## 6. Suggested Experiment Template

1. fix object size/concurrency distribution
2. run strategy A periodic baseline
3. run strategy B with strict byte budget
4. run strategy C with idle window gating
5. collect latency + queue-depth + completion metrics

For full chain and code-level differences:

1. [Tiering Policy Strategies and Trigger Modes](../explanation/tiering-policy-strategies-and-trigger-modes.md)

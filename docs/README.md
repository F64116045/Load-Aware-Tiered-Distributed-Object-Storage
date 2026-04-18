# Load-Aware Tiered Object Storage Documentation

Last Updated: 2026-04-17

This is a full documentation system designed for two outcomes:

1. A newcomer can become productive in 1 day.
2. A returning maintainer can recover 90%+ system control quickly.

## 1. Diataxis Model (What Each Doc Type Must Achieve)

| Type | Purpose | Primary Reader Question | Definition of Done |
| --- | --- | --- | --- |
| Tutorials | teach by doing | "How do I get one thing working end-to-end?" | reader can finish a runnable scenario without guessing |
| How-to Guides | solve one task | "How do I do X right now?" | concise task flow, exact commands, expected output |
| Explanation | build deep understanding | "Why is the system designed this way?" | design tradeoffs and invariants are explicit |
| Reference | precise lookup | "What is the exact API/field/enum/option?" | authoritative, unambiguous, complete |
| Operations | keep system running | "How do I operate/recover this safely?" | on-call can triage in minutes |

## 2. Maintainer Learning Path

Start here if you want complete ownership from scratch:

1. `operations/from-clone-to-system-control-learning-path.md` (main learning track)
2. `operations/first-day-setup-smoke-and-first-commit.md` (day-1 runnable path)
3. `explanation/put-get-delete-and-task-lifecycles.md` (all runtime flows)
4. `reference/code-map-from-runtime-flow-to-files.md` (where every flow lives in code)
5. `operations/incident-triage-restart-and-recovery-runbook.md` (production response muscle)

## 3. Fast Re-entry Paths

### 3.1 20-Minute Re-entry (I forgot everything)

1. `operations/first-day-setup-smoke-and-first-commit.md` sections 1-6
2. `explanation/system-architecture-and-responsibilities.md` sections 1-3
3. `reference/api-endpoints-reference.md` section 1 and 2

### 3.2 90-Minute Re-entry (I need to debug today)

1. `operations/from-clone-to-system-control-learning-path.md` stage 1-3
2. `explanation/put-get-delete-and-task-lifecycles.md`
3. `reference/metadata-rpc-method-mapping-reference.md`
4. `reference/task-state-machine-reference.md`

## 4. Documentation Inventory

### 4.1 Tutorials

1. `tutorials/put-get-delete-object-lifecycle-tutorial.md`
2. `tutorials/tiering-repair-and-old-version-gc-tutorial.md`

### 4.2 How-to Guides

1. `how-to/start-local-stack-and-verify-health.md`
2. `how-to/run-ha-metadata-cluster-profile.md`
3. `how-to/tune-tiering-policy-and-trigger-settings.md`
4. `how-to/debug-scanner-leader-lock-flapping.md`
5. `how-to/recover-from-tikv-startup-failure.md`
6. `how-to/trace-code-by-runtime-flow.md`

### 4.3 Explanation

1. `explanation/system-architecture-and-responsibilities.md`
2. `explanation/put-get-delete-and-task-lifecycles.md`
3. `explanation/runtime-control-loops-and-schedulers.md`
4. `explanation/consistency-and-failure-model.md`
5. `explanation/design-rationale-and-tradeoffs.md`

### 4.4 Reference

1. `reference/api-endpoints-reference.md`
2. `reference/configuration-env-vars-reference.md`
3. `reference/logical-data-schema-reference.md`
4. `reference/metadata-record-schema-reference.md`
5. `reference/tikv-keyspace-and-key-encoding-reference.md`
6. `reference/metadata-rpc-method-mapping-reference.md`
7. `reference/task-state-machine-reference.md`
8. `reference/system-dependencies-and-runtime-topology-reference.md`
9. `reference/code-map-from-runtime-flow-to-files.md`
10. `reference/system-glossary.md`

### 4.5 Operations

1. `operations/from-clone-to-system-control-learning-path.md`
2. `operations/first-day-setup-smoke-and-first-commit.md`
3. `operations/incident-triage-restart-and-recovery-runbook.md`
4. `operations/documentation-completeness-checklist.md`

## 5. Source-of-Truth Rules

1. Code is authoritative; docs must reflect code.
2. Every non-trivial behavior statement must map to a file path.
3. Every tutorial/how-to command must be runnable in this repository.
4. Every enum/state in docs must appear in reference docs.

## 6. Current Scope Boundaries

1. Metadata backend: TiKV, accessed via `meta_service` RPC in main runtime profile.
2. Object API: repository-native `v2` API (not S3-compatible yet).
3. Bucket semantics: not implemented yet.
4. HA metadata profile: supported through compose overlay (`docker-compose.ha.yaml`).

## 7. What to Read Before Changing Core Logic

1. `explanation/put-get-delete-and-task-lifecycles.md`
2. `reference/task-state-machine-reference.md`
3. `reference/metadata-record-schema-reference.md`
4. `reference/code-map-from-runtime-flow-to-files.md`

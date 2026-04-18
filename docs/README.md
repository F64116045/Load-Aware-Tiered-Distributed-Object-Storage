# Load-Aware Tiered Object Storage Documentation

Last Updated: 2026-04-17

This documentation is organized as a practical entry map to architecture, runtime flow, operations, and code references.

## 1. Maintainer Learning Path

Start here if you want complete ownership from scratch:

1. [From Clone to System Control Learning Path](operations/from-clone-to-system-control-learning-path.md) (main learning track)
2. [First Day Setup, Smoke, and First Commit](operations/first-day-setup-smoke-and-first-commit.md) (day-1 runnable path)
3. [PUT, GET, DELETE and Task Lifecycles](explanation/put-get-delete-and-task-lifecycles.md) (all runtime flows)
4. [Code Map: Runtime Flow to Files](reference/code-map-from-runtime-flow-to-files.md) (where every flow lives in code)
5. [Incident Triage, Restart, and Recovery Runbook](operations/incident-triage-restart-and-recovery-runbook.md) (production response muscle)

## 2. Fast Re-entry Paths

### 2.1 Quick Re-entry (I forgot everything)

1. [First Day Setup, Smoke, and First Commit](operations/first-day-setup-smoke-and-first-commit.md) sections 1-6
2. [System Architecture and Responsibilities](explanation/system-architecture-and-responsibilities.md) sections 1-3
3. [API Endpoints Reference](reference/api-endpoints-reference.md) section 1 and 2

### 2.2 Debug Re-entry (I need to debug today)

1. [From Clone to System Control Learning Path](operations/from-clone-to-system-control-learning-path.md) stage 1-3
2. [PUT, GET, DELETE and Task Lifecycles](explanation/put-get-delete-and-task-lifecycles.md)
3. [Metadata RPC Method Mapping Reference](reference/metadata-rpc-method-mapping-reference.md)
4. [Task State Machine Reference](reference/task-state-machine-reference.md)

## 3. Documentation Inventory

### 3.1 Tutorials

1. [PUT, GET, DELETE Object Lifecycle Tutorial](tutorials/put-get-delete-object-lifecycle-tutorial.md)
2. [Tiering, Repair, and Old-Version GC Tutorial](tutorials/tiering-repair-and-old-version-gc-tutorial.md)

### 3.2 How-to Guides

1. [Start Local Stack and Verify Health](how-to/start-local-stack-and-verify-health.md)
2. [Run HA Metadata Cluster Profile](how-to/run-ha-metadata-cluster-profile.md)
3. [Tune Tiering Policy and Trigger Settings](how-to/tune-tiering-policy-and-trigger-settings.md)
4. [Debug Scanner Leader Lock Flapping](how-to/debug-scanner-leader-lock-flapping.md)
5. [Recover from TiKV Startup Failure](how-to/recover-from-tikv-startup-failure.md)
6. [Trace Code by Runtime Flow](how-to/trace-code-by-runtime-flow.md)

### 3.3 Explanation

1. [System Architecture and Responsibilities](explanation/system-architecture-and-responsibilities.md)
2. [PUT, GET, DELETE and Task Lifecycles](explanation/put-get-delete-and-task-lifecycles.md)
3. [Runtime Control Loops and Schedulers](explanation/runtime-control-loops-and-schedulers.md)
4. [Consistency and Failure Model](explanation/consistency-and-failure-model.md)
5. [Design Rationale and Tradeoffs](explanation/design-rationale-and-tradeoffs.md)

### 3.4 Reference

1. [API Endpoints Reference](reference/api-endpoints-reference.md)
2. [Configuration Env Vars Reference](reference/configuration-env-vars-reference.md)
3. [Logical Data Schema Reference](reference/logical-data-schema-reference.md)
4. [Metadata Record Schema Reference](reference/metadata-record-schema-reference.md)
5. [TiKV Keyspace and Key Encoding Reference](reference/tikv-keyspace-and-key-encoding-reference.md)
6. [Metadata RPC Method Mapping Reference](reference/metadata-rpc-method-mapping-reference.md)
7. [Task State Machine Reference](reference/task-state-machine-reference.md)
8. [System Dependencies and Runtime Topology Reference](reference/system-dependencies-and-runtime-topology-reference.md)
9. [Code Map from Runtime Flow to Files](reference/code-map-from-runtime-flow-to-files.md)
10. [System Glossary](reference/system-glossary.md)

### 3.5 Operations

1. [From Clone to System Control Learning Path](operations/from-clone-to-system-control-learning-path.md)
2. [First Day Setup, Smoke, and First Commit](operations/first-day-setup-smoke-and-first-commit.md)
3. [Incident Triage, Restart, and Recovery Runbook](operations/incident-triage-restart-and-recovery-runbook.md)
4. [Documentation Completeness Checklist](operations/documentation-completeness-checklist.md)

## 4. Current Scope Boundaries

1. Metadata backend: TiKV, accessed via `meta_service` RPC in main runtime profile.
2. Object API: repository-native `v2` API (not S3-compatible yet).
3. Bucket semantics: not implemented yet.
4. HA metadata profile: supported through compose overlay (`docker-compose.ha.yaml`).

## 5. What to Read Before Changing Core Logic

1. [PUT, GET, DELETE and Task Lifecycles](explanation/put-get-delete-and-task-lifecycles.md)
2. [Task State Machine Reference](reference/task-state-machine-reference.md)
3. [Metadata Record Schema Reference](reference/metadata-record-schema-reference.md)
4. [Code Map from Runtime Flow to Files](reference/code-map-from-runtime-flow-to-files.md)

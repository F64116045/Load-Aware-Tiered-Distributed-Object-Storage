# Documentation

This set describes architecture, runtime flow, metadata/task behavior, API/config contracts, and operational procedures for the TiKV-backed tiered object storage system.

## 1. Architecture and Runtime

1. [System Architecture and Responsibilities](explanation/system-architecture-and-responsibilities.md)
2. [PUT, GET, DELETE and Task Lifecycles](explanation/put-get-delete-and-task-lifecycles.md)
3. [Tiering Task Path from PUT to Worker Claim](explanation/tiering-task-path-from-put-to-worker-claim.md)
4. [Tiering Policy Strategies and Trigger Modes](explanation/tiering-policy-strategies-and-trigger-modes.md)
5. [Runtime Control Loops and Schedulers](explanation/runtime-control-loops-and-schedulers.md)
6. [Scanner Leader Lock Mechanism](explanation/scanner-leader-lock-mechanism.md)
7. [Consistency and Failure Model](explanation/consistency-and-failure-model.md)
8. [Design Rationale and Tradeoffs](explanation/design-rationale-and-tradeoffs.md)

## 2. API and Data Contracts

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

## 3. Setup and Verification

1. [Start Local Stack and Verify Health](how-to/start-local-stack-and-verify-health.md)
2. [Local Setup and Smoke Validation](operations/local-setup-and-smoke-validation.md)
3. [Runtime and Code Understanding Guide](operations/runtime-and-code-understanding-guide.md)
4. [Run HA Metadata Cluster Profile](how-to/run-ha-metadata-cluster-profile.md)

## 4. Operations and Diagnostics

1. [Incident Triage, Restart, and Recovery Runbook](operations/incident-triage-restart-and-recovery-runbook.md)
2. [Debug Scanner Leader Lock Flapping](how-to/debug-scanner-leader-lock-flapping.md)
3. [Recover from TiKV Startup Failure](how-to/recover-from-tikv-startup-failure.md)
4. [Trace Code by Runtime Flow](how-to/trace-code-by-runtime-flow.md)
5. [Documentation Coverage and Gap Analysis](operations/documentation-coverage-and-gap-analysis.md)

## 5. Tutorials

1. [PUT, GET, DELETE Object Lifecycle Tutorial](tutorials/put-get-delete-object-lifecycle-tutorial.md)
2. [Tiering, Repair, and Old-Version GC Tutorial](tutorials/tiering-repair-and-old-version-gc-tutorial.md)

## 6. Scope Boundaries

1. Metadata backend: TiKV through `meta_service` RPC in the main runtime profile.
2. Object API: repository-native `v2` API (S3 API is not implemented).
3. Bucket semantics and ACL model: not implemented.
4. HA metadata profile: supported through [`docker-compose.ha.yaml`](../docker-compose.ha.yaml).

## 7. Recommended Reading Paths

1. End-to-end runtime path: [System Architecture and Responsibilities](explanation/system-architecture-and-responsibilities.md) -> [Request and Task Lifecycles](explanation/put-get-delete-and-task-lifecycles.md) -> [Tiering Task Path from PUT to Worker Claim](explanation/tiering-task-path-from-put-to-worker-claim.md) -> [Task State Machine Reference](reference/task-state-machine-reference.md).
2. Correctness and race semantics: [Consistency and Failure Model](explanation/consistency-and-failure-model.md) -> [Task State Machine Reference](reference/task-state-machine-reference.md) -> [Scanner Leader Lock Mechanism](explanation/scanner-leader-lock-mechanism.md) -> [Metadata RPC Method Mapping Reference](reference/metadata-rpc-method-mapping-reference.md).
3. Data model and keyspace: [Logical Data Schema Reference](reference/logical-data-schema-reference.md) -> [Metadata Record Schema Reference](reference/metadata-record-schema-reference.md) -> [TiKV Keyspace and Key Encoding Reference](reference/tikv-keyspace-and-key-encoding-reference.md).
4. Debug and implementation path: [Runtime and Code Understanding Guide](operations/runtime-and-code-understanding-guide.md) -> [Trace Code by Runtime Flow](how-to/trace-code-by-runtime-flow.md) -> [Code Map from Runtime Flow to Files](reference/code-map-from-runtime-flow-to-files.md).

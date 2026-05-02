# Documentation Coverage and Gap Analysis

## 1. Runtime and Architecture Coverage

Covered:

1. [System Architecture and Responsibilities](../explanation/system-architecture-and-responsibilities.md)
2. [Request and Task Lifecycles](../explanation/put-get-delete-and-task-lifecycles.md)
3. [Runtime Control Loops and Schedulers](../explanation/runtime-control-loops-and-schedulers.md)
4. [Tiering Policy Strategies and Trigger Modes](../explanation/tiering-policy-strategies-and-trigger-modes.md)
5. [Tiering Task Path from PUT to Worker Claim](../explanation/tiering-task-path-from-put-to-worker-claim.md)
6. [Scanner Leader Lock Mechanism](../explanation/scanner-leader-lock-mechanism.md)
7. [Consistency and Failure Model](../explanation/consistency-and-failure-model.md)
8. [Design Rationale and Tradeoffs](../explanation/design-rationale-and-tradeoffs.md)

## 2. Metadata and Data Model Coverage

Covered:

1. [Metadata Keyspace Data Model Walkthrough](../reference/metadata-keyspace-data-model-walkthrough.md)
2. [Logical Data Schema Reference](../reference/logical-data-schema-reference.md)
3. [Metadata Record Schema Reference](../reference/metadata-record-schema-reference.md)
4. [TiKV Keyspace and Key Encoding Reference](../reference/tikv-keyspace-and-key-encoding-reference.md)
5. [Task State Machine Reference](../reference/task-state-machine-reference.md)
6. [Metadata RPC Method Mapping Reference](../reference/metadata-rpc-method-mapping-reference.md)

## 3. API and Configuration Coverage

Covered:

1. [API Endpoints Reference](../reference/api-endpoints-reference.md)
2. [Configuration Env Vars Reference](../reference/configuration-env-vars-reference.md)

## 4. Operational Procedure Coverage

Covered:

1. [Local Setup and Smoke Validation](local-setup-and-smoke-validation.md)
2. [Runtime and Code Understanding Guide](runtime-and-code-understanding-guide.md)
3. [Incident Triage, Restart, and Recovery Runbook](incident-triage-restart-and-recovery-runbook.md)
4. [Start Local Stack and Verify Health](../how-to/start-local-stack-and-verify-health.md)
5. [Run HA Metadata Cluster Profile](../how-to/run-ha-metadata-cluster-profile.md)
6. [Debug Scanner Leader Lock Flapping](../how-to/debug-scanner-leader-lock-flapping.md)
7. [Recover from TiKV Startup Failure](../how-to/recover-from-tikv-startup-failure.md)
8. [Tune Tiering Policy and Trigger Settings](../how-to/tune-tiering-policy-and-trigger-settings.md)
9. [Trace Code by Runtime Flow](../how-to/trace-code-by-runtime-flow.md)

## 5. Tutorial Coverage

Covered:

1. [PUT, GET, DELETE Object Lifecycle Tutorial](../tutorials/put-get-delete-object-lifecycle-tutorial.md)
2. [Tiering, Repair, and Old-Version GC Tutorial](../tutorials/tiering-repair-and-old-version-gc-tutorial.md)

## 6. Current Gaps

1. S3-compatible API behavior and compatibility matrix are not implemented in runtime, so no S3 contract document is available.
2. Bucket and ACL semantics are not implemented, so authorization and object namespace policy documents are not available.
3. Cloud-specific runbooks (AWS, GCP, Kubernetes) are not yet documented.
4. Formal SLO and alert policy documentation is not yet defined.

## 7. Reading Paths for High System Mastery

1. Control plane and scheduling: [Runtime Control Loops and Schedulers](../explanation/runtime-control-loops-and-schedulers.md) -> [Scanner Leader Lock Mechanism](../explanation/scanner-leader-lock-mechanism.md) -> [Tiering Policy Strategies and Trigger Modes](../explanation/tiering-policy-strategies-and-trigger-modes.md) -> [Tiering Task Path from PUT to Worker Claim](../explanation/tiering-task-path-from-put-to-worker-claim.md).
2. Data path and correctness: [Request and Task Lifecycles](../explanation/put-get-delete-and-task-lifecycles.md) -> [Metadata Keyspace Data Model Walkthrough](../reference/metadata-keyspace-data-model-walkthrough.md) -> [Consistency and Failure Model](../explanation/consistency-and-failure-model.md) -> [Task State Machine Reference](../reference/task-state-machine-reference.md) -> [TiKV Keyspace and Key Encoding Reference](../reference/tikv-keyspace-and-key-encoding-reference.md) + [Metadata Record Schema Reference](../reference/metadata-record-schema-reference.md).
3. Runtime debugging: [Start Local Stack and Verify Health](../how-to/start-local-stack-and-verify-health.md) -> [Local Setup and Smoke Validation](local-setup-and-smoke-validation.md) -> [Incident Triage, Restart, and Recovery Runbook](incident-triage-restart-and-recovery-runbook.md) -> [Trace Code by Runtime Flow](../how-to/trace-code-by-runtime-flow.md).

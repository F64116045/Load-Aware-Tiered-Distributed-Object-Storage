# Documentation

This documentation set covers architecture, runtime behavior, metadata/task design,
API contracts, and operational procedures for the TiKV-backed tiered object storage system.

## Architecture Diagrams

- Overall system architecture

![Overall System Architecture Diagram](../img/Overall%20System%20Architecture%20Diagram.png)

- Background task lifecycle and consistency control

![Background Task Lifecycle and Consistency Control Diagram](../img/Background%20Task%20Lifecycle%20and%20Consistency%20Control%20Diagram.png)

## Architecture and Runtime

1. [System Architecture and Responsibilities](explanation/system-architecture-and-responsibilities.md)
2. [Request and Task Lifecycles](explanation/put-get-delete-and-task-lifecycles.md)
3. [Tiering Task Path from PUT to Worker Claim](explanation/tiering-task-path-from-put-to-worker-claim.md)
4. [Tiering Policy Strategies and Trigger Modes](explanation/tiering-policy-strategies-and-trigger-modes.md)
5. [Runtime Control Loops and Schedulers](explanation/runtime-control-loops-and-schedulers.md)
6. [Scanner Leader Lock Mechanism](explanation/scanner-leader-lock-mechanism.md)
7. [Consistency and Failure Model](explanation/consistency-and-failure-model.md)
8. [Design Rationale and Tradeoffs](explanation/design-rationale-and-tradeoffs.md)

## API and Data Contracts

1. [API Endpoints Reference](reference/api-endpoints-reference.md)
2. [Configuration Env Vars Reference](reference/configuration-env-vars-reference.md)
3. [Metadata Keyspace Data Model Walkthrough](reference/metadata-keyspace-data-model-walkthrough.md)
4. [Logical Data Schema Reference](reference/logical-data-schema-reference.md)
5. [Metadata Record Schema Reference](reference/metadata-record-schema-reference.md)
6. [TiKV Keyspace and Key Encoding Reference](reference/tikv-keyspace-and-key-encoding-reference.md)
7. [Metadata RPC Method Mapping Reference](reference/metadata-rpc-method-mapping-reference.md)
8. [Task State Machine Reference](reference/task-state-machine-reference.md)
9. [System Dependencies and Runtime Topology Reference](reference/system-dependencies-and-runtime-topology-reference.md)
10. [Code Map from Runtime Flow to Files](reference/code-map-from-runtime-flow-to-files.md)
11. [System Glossary](reference/system-glossary.md)

## Setup and Verification

1. [Start Local Stack and Verify Health](how-to/start-local-stack-and-verify-health.md)
2. [Local Setup and Smoke Validation](operations/local-setup-and-smoke-validation.md)
3. [Runtime and Code Understanding Guide](operations/runtime-and-code-understanding-guide.md)
4. [Run HA Metadata Cluster Profile](how-to/run-ha-metadata-cluster-profile.md)
5. [Run Fair Local Experiments](how-to/run-fair-local-experiments.md)
6. [Run AWS k3s Experiments](how-to/run-aws-k3s-experiments.md)
7. [Run GCP GKE Experiments](how-to/run-gcp-gke-experiments.md)

## Operations and Diagnostics

1. [Incident Triage, Restart, and Recovery Runbook](operations/incident-triage-restart-and-recovery-runbook.md)
2. [Debug Scanner Leader Lock Flapping](how-to/debug-scanner-leader-lock-flapping.md)
3. [Recover from TiKV Startup Failure](how-to/recover-from-tikv-startup-failure.md)
4. [Trace Code by Runtime Flow](how-to/trace-code-by-runtime-flow.md)
5. [Documentation Coverage and Gap Analysis](operations/documentation-coverage-and-gap-analysis.md)

## Tutorials

1. [PUT, GET, DELETE Object Lifecycle Tutorial](tutorials/put-get-delete-object-lifecycle-tutorial.md)
2. [Tiering, Repair, and Old-Version GC Tutorial](tutorials/tiering-repair-and-old-version-gc-tutorial.md)

## Recommended Reading Paths

1. End-to-end path:
[System Architecture and Responsibilities](explanation/system-architecture-and-responsibilities.md) ->
[Request and Task Lifecycles](explanation/put-get-delete-and-task-lifecycles.md) ->
[Tiering Task Path from PUT to Worker Claim](explanation/tiering-task-path-from-put-to-worker-claim.md) ->
[Task State Machine Reference](reference/task-state-machine-reference.md).

2. Correctness and concurrency:
[Consistency and Failure Model](explanation/consistency-and-failure-model.md) ->
[Task State Machine Reference](reference/task-state-machine-reference.md) ->
[Scanner Leader Lock Mechanism](explanation/scanner-leader-lock-mechanism.md) ->
[Metadata RPC Method Mapping Reference](reference/metadata-rpc-method-mapping-reference.md).

3. Data model and keyspace:
[Metadata Keyspace Data Model Walkthrough](reference/metadata-keyspace-data-model-walkthrough.md) ->
[Logical Data Schema Reference](reference/logical-data-schema-reference.md) ->
[Metadata Record Schema Reference](reference/metadata-record-schema-reference.md) ->
[TiKV Keyspace and Key Encoding Reference](reference/tikv-keyspace-and-key-encoding-reference.md).

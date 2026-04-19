# Explanation: System Architecture

This document explains the full runtime architecture and why each layer exists.

## 1. Architecture at a Glance

```mermaid
flowchart TB
  subgraph Edge[Edge and Clients]
    Client[Client / Benchmark / Demo UI]
    Nginx[Nginx Gateway :8000]
    Client --> Nginx
  end

  subgraph DataPlane[Data Plane]
    API[API Service Replicas]
    SN1[(Storage Node 1)]
    SN2[(Storage Node 2)]
    SN3[(Storage Node 3)]
    SN4[(Storage Node 4)]
    SN5[(Storage Node 5)]
    SN6[(Storage Node 6)]
    API --> SN1
    API --> SN2
    API --> SN3
    API --> SN4
    API --> SN5
    API --> SN6
  end

  subgraph BackgroundPlane[Background Plane]
    Worker[Tiering Worker]
    Scanner[Policy Scanner]
    ProcReplEC[Processor: REPL_TO_EC]
    ProcRepair[Processor: REPAIR]
    ProcGC[Processor: GC]
    ProcOldGC[Processor: GC_OLD_VERSION]
    Worker --> Scanner
    Worker --> ProcReplEC
    Worker --> ProcRepair
    Worker --> ProcGC
    Worker --> ProcOldGC
    ProcReplEC --> SN1
    ProcReplEC --> SN2
    ProcReplEC --> SN3
    ProcReplEC --> SN4
    ProcReplEC --> SN5
    ProcReplEC --> SN6
    ProcRepair --> SN1
    ProcRepair --> SN2
    ProcRepair --> SN3
    ProcRepair --> SN4
    ProcRepair --> SN5
    ProcRepair --> SN6
    ProcGC --> SN1
    ProcGC --> SN2
    ProcGC --> SN3
    ProcGC --> SN4
    ProcGC --> SN5
    ProcGC --> SN6
    ProcOldGC --> SN1
    ProcOldGC --> SN2
    ProcOldGC --> SN3
    ProcOldGC --> SN4
    ProcOldGC --> SN5
    ProcOldGC --> SN6
  end

  subgraph MetadataPlane[Metadata Plane]
    MetaLB[meta_service LB :8091]
    Meta1[meta_service_1]
    Meta2[meta_service_2]
    Meta3[meta_service_3]
    TiKV[(TiKV Cluster)]
    PD[(PD Cluster)]
    MetaLB --> Meta1
    MetaLB --> Meta2
    MetaLB --> Meta3
    Meta1 --> TiKV
    Meta2 --> TiKV
    Meta3 --> TiKV
    TiKV --> PD
  end

  Nginx --> API
  API --> MetaLB
  Worker --> MetaLB
  SN1 --> MetaLB
  SN2 --> MetaLB
  SN3 --> MetaLB
  SN4 --> MetaLB
  SN5 --> MetaLB
  SN6 --> MetaLB
```

## 2. Core Design Split

1. Foreground path (`PUT/GET/DELETE`) prioritizes predictable latency and availability.
2. Background path handles expensive transitions (tiering, repair, garbage collection).
3. Metadata plane is isolated behind `meta_service` RPC boundary.

## 3. Request Path Summary

### 3.1 PUT `/v2/objects/:id`

1. API chooses HOT replicas.
2. Writes bytes to storage nodes.
3. Requires write quorum.
4. Persists normalized metadata.
5. Persists due-index records for future background selection.
6. Returns ACK without directly enqueuing tiering/repair tasks.

### 3.2 GET `/v2/objects/:id`

1. API reads metadata strategy/tier/version.
2. If HOT path, fetches one healthy replica.
3. If EC path, reconstructs payload from shards.

### 3.3 DELETE `/v2/objects/:id`

1. API resolves current placements.
2. Deletes physical data.
3. Cleans metadata records.

### 3.4 Background Enqueue Boundary

1. Foreground write path commits object/version/placement metadata and due-index only.
2. Policy scanner later enqueues `REPL_TO_EC` and `REPAIR` from metadata state.
3. Processors enqueue follow-up tasks (for example replication `GC`) after state transitions.

## 4. Metadata Ownership Model

1. API, storage nodes, and workers never talk to TiKV directly in normal runtime profile.
2. They call `meta_service` via RPC (`/meta/rpc`).
3. `meta_service` translates RPC methods to repository operations.
4. Repository implementation persists records in TiKV keyspaces.

## 5. Why This Layering Exists

1. API remains independent from backend metadata technology details.
2. Lock and metadata semantics are centralized.
3. Multi-component code can share one repository contract (`internal/meta/repository.go`).
4. Failure handling is easier to reason about when state machine is centralized.

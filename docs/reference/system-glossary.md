# Glossary

## A

`A` / `B` / `C`

1. Tiering candidate selection policy variants.
2. A = age-based baseline, B = static budget throttling, C = idle-window admission + budget throttling.

## C

`Claim`

1. Transition where worker picks a runnable task and marks it `RUNNING`.

`Current Version`

1. The active version pointer in object head metadata.

## D

`Due Index`

1. Time-ordered metadata index (`tdue/*`) used for event-driven candidate lookup.

## E

`EC`

1. Erasure Coding tier (`k` data shards + `m` parity shards).

`EC_ACTIVE`

1. Object state indicating current version is served from EC tier.

## H

`HOT`

1. Replicated tier for low-latency foreground writes.

`HOT_ACTIVE`

1. Object state indicating current version is actively in HOT tier.

## I

`Idle Window`

1. Threshold trigger condition requiring stable low load for N rounds before migration starts.

## L

`Leader Lock`

1. Distributed lease used to ensure only one worker runs the policy scanner at a time.

`Leader Lock Token`

1. Token returned by lock acquisition RPC and used by lock ping/release RPC calls.
2. Encodes lock owner payload and can be signature-verified by RPC server.

## M

`MIGRATING`

1. Object state during REPL->EC conversion for current version.

`MIGRATION_PENDING`

1. Object state indicating migration task has been queued and awaits processing.

`Meta RPC Token` (`X-Meta-Token`)

1. Shared-secret HTTP header for metadata RPC transport authentication.
2. Applied at transport layer across all RPC methods when enabled.

## P

`PD`

1. Placement Driver in TiKV ecosystem; cluster metadata/scheduling control component.

`PENDING`

1. Task state meaning queued and waiting to run.

## R

`REPAIR`

1. Task type for healing missing HOT replicas or EC shards.

`RETRY_WAIT`

1. Task state after transient failure; task is delayed until next scheduled retry time.

## S

`Stale Task`

1. Task whose `task.version` no longer equals object `current_version`.
2. Must be skipped to avoid mutating outdated data.

## T

`TiKV`

1. Distributed transactional KV backend used for metadata persistence.

`Tiering Worker`

1. Background service that executes migration/repair/gc tasks.

## W

`Write Quorum`

1. Minimum successful replica writes required before PUT ACK.

# go-ycsb (benchmark helper)

This folder contains the `go-ycsb` benchmark tool used by this repository for
load generation and repeatable performance experiments.

## Upstream Project

- Repository: https://github.com/pingcap/go-ycsb
- Full database configuration and advanced options: upstream README

## Scope in This Repository

We use `go-ycsb` primarily for:

- controlled PUT/GET style workload generation
- throughput/latency comparison across policy variants (`A`, `B`, `C`)
- reproducible baseline vs stressed runs

## Basic Usage

Build from source (inside this folder):

```bash
make
./bin/go-ycsb --help
```

Run workload examples:

```bash
./bin/go-ycsb load basic -P workloads/workloada
./bin/go-ycsb run basic -P workloads/workloada
```

## Notes

- Keep benchmark configs versioned with experiments.
- Record both workload parameters and runtime environment in reports.
- Prefer fixed seeds and fixed dataset sizes when comparing strategy variants.

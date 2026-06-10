# Final GKE Experiment Results

This directory keeps the curated, report-ready GKE experiment summaries.
Raw experiment artifacts remain under `experiments/results/`, which is ignored
because it contains large logs and per-run CSVs.

## Methodology

- Platform: Google Kubernetes Engine.
- Topology: 2 system nodes and 6 storage nodes.
- Placement:
  - API, metadata service, PD, and TiKV run on system nodes.
  - Storage-node pods run on storage nodes with required anti-affinity.
- Benchmark path: in-cluster workload jobs call `http://api:8000`.
- Workload:
  - 50 objects
  - 1 MiB per object
  - 60 s foreground mixed PUT/GET workload
  - concurrency 2
  - 70% GET
  - age threshold 60 s
  - preload age wait 90 s
- Pressure injection:
  - no pressure
  - CPU pressure on two storage-only nodes
  - I/O pressure on two storage-only nodes
- Policies:
  - Baseline: no background migration
  - A: age-based migration
  - B: throttled migration
  - C: pressure-aware migration
- Each profile was repeated three times.

## Files

- `gke_formal_per_run.csv`: per-run GET/PUT p99 and completed migration tasks.
- `gke_formal_median_summary.csv`: median over the three repeats.
- `gke_selected_representative_runs.csv`: representative runs used by the poster
  figure generator.

## Main Takeaway

The no-pressure and CPU-pressure profiles mainly validate that the baseline is
healthy and that CPU pressure is not the dominant bottleneck for this workload.
The strongest result appears under I/O pressure: policy C reduces completed
background migration work while keeping foreground latency in a better range
than the more aggressive migration policies.

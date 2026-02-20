# Architecture Index

This repository now tracks architecture in two versions:

1. `docs/ARCHITECTURE_V2_FREEZE.md` (current, authoritative)
   - PostgreSQL-first metadata architecture
   - Tiered hot-write + background EC migration model
   - Node discovery source switch (`postgres|etcd|auto`)
2. `docs/ARCHITECTURE_V1_LEGACY.md` (historical reference)
   - Original log-centric/etcd-heavy architecture used in early prototype stage

For implementation and milestone details, see `docs/DAILY_PROGRESS.md` and `docs/SPEC_V2_LOAD_AWARE_TIERED_OBJECT_STORAGE.md`.

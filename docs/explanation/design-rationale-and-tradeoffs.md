# Explanation: Design Rationale

## 1. Why HOT Replication First

1. low-latency writes are critical for user-facing PUT path
2. EC encoding in foreground would add CPU and coordination cost

## 2. Why REPL -> EC in Background

1. storage efficiency can be improved without blocking foreground traffic
2. migration can be rate-limited and scheduled by policy

## 3. Why A/B/C Policy Variants

1. A gives simple age-only baseline
2. B adds explicit budget cap for controlled background load
3. C gates migration by stable idle windows to reduce foreground contention

## 4. Why Threshold/Idle-Window Triggering

1. pure periodic scans can run during peak load
2. threshold/idle conditions align migration with safer windows
3. cooldown avoids trigger storms and oscillation

## 5. Why RPC Metadata Service Boundary

1. one metadata contract shared by all runtime components
2. isolates backend-specific logic from API/storage/worker code
3. simplifies replacement/evolution of backend internals

## 6. Why Keep Versioned Metadata

1. deterministic stale-task rejection
2. repair/cleanup can target exact version
3. supports retention and old-version lifecycle policies

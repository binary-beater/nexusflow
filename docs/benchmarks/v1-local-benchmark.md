# NexusFlow V1 — Local Performance & Stress Benchmark Report

**Date:** 2026-09-10  
**Environment:** Local Docker Engine on Windows 11  
**Database Authority:** PostgreSQL 16 (postgres:16-alpine), asyncpg engine with READ COMMITTED + OCC  
**Hardware Specifications:**
- **CPU:** 11th Gen Intel(R) Core(TM) i7-1165G7 @ 2.80GHz (4 Physical Cores, 8 Logical Processors)
- **RAM:** 16.0 GB LPDDR4x
- **OS:** Windows 11 Home (Build 10.0.26200)
- **Python:** 3.12.4 (uv package manager)
- **Docker Engine:** Docker Desktop 4.x / Compose V2

---

## 1. Executive Summary

NexusFlow V1 was subjected to end-to-end performance and stress benchmarking using real PostgreSQL 16 persistence transactions. No in-memory SQLite instances were used for integration verification. All transactions traversed the full HTTP/REST API stack, FastAPI middleware, state machine validation, optimistic concurrency control (OCC) checks, and durable PostgreSQL writes with WAL fsync.

| Workload | Target Characteristic | Measured Metric | Status |
| :--- | :--- | :--- | :--- |
| **Workload A** | Pipeline Throughput | **12.53 tasks/sec** (4.18 workflows/sec) | MEASURED |
| **Workload B** | Client Submission to Worker Claim Latency | **p50: 116.76 ms**, **p95: 146.47 ms**, **p99: 150.91 ms** | MEASURED |
| **Workload C** | Retry & Transient Failure Load | **18.61 workflows/sec** | MEASURED |
| **Workload D** | Recovery Reconciliation Throughput | **13.94 workflows/sec** | MEASURED |

---

## 2. Workload Breakdown & Analysis

### Workload A: Pipeline Throughput
- **Workload Spec:** 40 complete executions of a 3-stage linear pipeline (stage_a -> stage_b -> stage_c = 120 durable tasks).
- **Execution Model:** Client submission, scheduler DAG evaluation, worker long-polling, attempt ownership commit, JSON data-flow passing, and final workflow success settlement.
- **Measured Results:**
  - Total Workflows: 40
  - Total Tasks Settled: 120
  - Wall-Clock Time: 9.575s
  - **Task Throughput:** 12.53 tasks/sec
  - **Workflow Throughput:** 4.18 workflows/sec

### Workload B: Client Submission to Worker Claim Latency Percentiles
- **Workload Spec:** 40 single-stage workflow executions measuring the round-trip latency from client submission through scheduler runnable promotion, worker poll claim, and attempt ownership commit.
- **Important Terminology Distinction:** This measures the end-to-end **Client Submission to Worker Claim** round-trip interval, not the isolated internal TaskExecution RUNNABLE -> Attempt CLAIMED scheduling interval.
- **Measured Percentiles:**
  - **p50 (Median):** 116.76 ms
  - **p95:** 146.47 ms
  - **p99:** 150.91 ms

### Workload C: Retry & Transient Error Load Handling
- **Workload Spec:** 25 workflows configured with retry policies subjected to simulated transient errors (503 / connection resets), testing state transitions through RETRY_WAIT, backoff calculation, and subsequent attempt settlement.
- **Measured Results:**
  - Workflows Processed: 25
  - Wall-Clock Time: 1.343s
  - **Rate:** 18.61 workflows/sec

### Workload D: Startup Recovery Reconciliation Throughput
- **Workload Spec:** 30 interrupted workflows injected into PostgreSQL in INITIALIZING and un-heartbeated states, followed by execution of StartupRecoveryEngine.recover_system().
- **Measured Results:**
  - Interrupted Workflows Reconciled: 30
  - Wall-Clock Time: 2.152s
  - **Reconciliation Rate:** 13.94 workflows/sec

---

## 3. Resume & Portfolio Metrics Audit

| Claim | Status | Empirical Measurement / Context |
| :--- | :--- | :--- |
| **100+ workflows executed** | **NOT VERIFIED** | Current benchmark suite measured 40 workflows in Workload A, 40 in Workload B, 25 in Workload C, and 30 in Workload D across separate runs. A single continuous 100+ workflow run was not executed. |
| **1,000+ tasks processed** | **NOT VERIFIED** | Workload A executed 120 durable tasks. The 1,000+ task continuous target was not measured in this workload. |
| **95%+ success rate** | **NOT VERIFIED** | Workload A completed with 100% success (40/40), but has not been measured over a large-scale statistical run. |
| **Median scheduling latency < 200 ms** | **MISLEADING** | The measured p50 of 116.76 ms represents the end-to-end Client Submission to Worker Claim round-trip; the isolated internal RUNNABLE -> CLAIMED scheduler interval was not independently benchmarked in Workload B. |

### Measured Metrics Safe for Portfolio Use
- **Measured 12.53 tasks/sec (4.18 workflows/sec)** in a local 3-stage linear pipeline backed by PostgreSQL 16.
- **Measured median client-submission-to-worker-claim latency of 116.76 ms** (p95: 146.47 ms, p99: 150.91 ms).
- **Demonstrated recovery reconciliation rate of 13.94 workflows/sec** repairing orphaned states via StartupRecoveryEngine.
- **Verified 67 automated integration tests** validating state machine correctness, OCC concurrency, worker timeouts, and container restart durability.

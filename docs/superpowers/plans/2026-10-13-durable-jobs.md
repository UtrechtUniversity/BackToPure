# Durable Jobs and Safe Writes Implementation Plan

> **For agentic workers:** Use subagent-driven-development, tests first, with an independent review of implementation.

**Goal:** Implement the user's requested groups in order 3, 4, 1, 2.
**Architecture:** SQLite durable execution queue and separate Python worker; shared tenant/DOI write reservations; immutable apply inputs; conservative per-item rollback.
**Tech Stack:** Existing Python, Flask, SQLite, requests, pytest, React/TypeScript, Vitest.
**Spec:** docs/superpowers/specs/2026-10-13-durable-jobs-design.md

## Global constraints
- Do not change accepted external-person name matching.
- Do not call live Pure or modify production runtime data during implementation/tests.
- Do not automatically repeat uncertain writes or delete records merely discovered as existing.
- Preserve old job history and unrelated current Pure fields.
- Use the isolated worktree /tmp/backtopure-durable-jobs.

## Review focus
- Web-worker death while child work continues; heartbeat expiry is not sufficient to authorize duplicate execution.
- Two apply jobs targeting the same tenant and normalized DOI; crash before/after write acknowledgement.
- Queue acceptance and review save racing; selection and tracking must agree.
- Concurrent rollback, cancellation, and later manual Pure modifications.
- Interrupted rollback after successful remote mutation but before durable acknowledgement.

## Task 1 — Persistent execution (solution 3)
Files: app/db.py, app/services/jobs.py, new app worker/queue module, app/routes.py, BackToPure.py, tests and runtime docs.
- [x] Add failing queue/lifecycle tests asserting action routes return before execution, one owner per task, cancellation, restart handling, and web-worker death independence.
- [x] Implement durable queue/worker with short SQLite claims, heartbeat, cancellation and conservative recovery. Keep synchronous execution methods as internal worker operations. Add standalone worker entrypoint and local launch integration; document production supervisor requirements.
- [x] Run backend suite; update route tests for asynchronous contract. Record commands/results and commit.

## Task 2 — Safe writes (solution 4)
Files: src/apply_updates_to_pure.py, apply_state.py, new focused helpers, app integration, tests.
- [x] Add failing tests: same DOI in two jobs creates once; ambiguous result blocks second create; existing record is never rollback-owned; fresh enrichment preserves unrelated fields and detects target-field conflicts.
- [x] Implement shared tenant/DOI reservations and exact current-Pure DOI lookup before creates. Merge intended changes into fresh records and preserve version metadata. Fail closed on lookup outages or malformed results.
- [x] Test reservations with separate connections/processes and verify journal/change-set semantics. Run affected tests and commit.

## Task 3 — Immutable selection (solution 1)
Files: app/services/jobs.py, queue acceptance, frontend JobDetailPage, relevant tests.
- [x] Add failing tests for dirty UI selection, save/apply races, and edits after action acceptance.
- [x] Freeze apply inputs under atomic claim; capture tracking and execute from the same immutable selection. Reject late saves and disable Apply until saved.
- [x] Run backend and frontend selection tests and commit.

## Task 4 — Safe rollback (solution 2)
Files: app/services/jobs.py and focused rollback helpers, apply snapshots, tests.
- [x] Reproduce full-text later-attachment deletion, ineffective cancellation, duplicate rollback requests, wrong error row, lock contention, crash recovery.
- [x] Implement atomic rollback ownership via queue; cooperative cancellation; job-owned change removal/conflict detection; durable per-item outcomes with no SQLite transaction spanning HTTP calls. Unknown outcomes remain blocked for reconciliation.
- [x] Verify all rollback types and historical evidence handling; run full backend/frontend suites and build; commit.

## Integration review
- [x] Independent code review of aggregate diff and reproduce any important findings.
- [x] Fix findings and rerun affected tests.
- [x] Report verified behavior and remaining deployment/reconciliation limitations accurately.

Final verification: 576 backend tests passed, five optional Ricgraph smoke tests skipped, 26 subtests passed; 73 frontend tests passed; production build and CI lint passed. Independent aggregate review and scoped follow-ups approved through `32226fa`, including six deferred-action navigation regressions. Tests made no live Pure writes.

# Durable jobs and safe writes

User-authorized scope and order: (3) persistent execution, (4) duplicate/stale-write protection, (1) selection integrity, (2) safe rollback. The external-person name-matching risk is accepted and unchanged. Harvest metadata corrections and network authentication are outside this change.

Use the existing SQLite runtime store and a separate Python worker, without a broker dependency. HTTP action endpoints enqueue durable tasks and return promptly. Atomic claims, owner/heartbeat information, cooperative cancellation, and conservative interrupted-task recovery prevent concurrent execution or automatic replay of ambiguous writes. Run/apply/rollback execution must survive request-worker death. Document local and production startup. Runtime paths must stay consistent.

Before creating research outputs or datasets, normalize DOI, reserve it durably per target Pure tenant in a shared runtime database, and query current Pure. An existing record must not be represented as created by this job or become rollback-deletable. An ambiguous write keeps its reservation and requires reconciliation; never blindly replay. Existing-record enrichment fetches current Pure, merges only reviewed identifier/address changes, and detects changed intended values. Preserve unrelated edits and use current version metadata.

Review edits are permitted only while the job is reviewable and no action has been queued. Apply and save cannot race: freeze the exact selected CSV/JSON as part of action acceptance, and use those files for tracking and execution. Apply is disabled while selections are dirty or saving.

Rollback owns a unique durable task, checks cancellation between writes, and records each item's intent and result durably without holding database locks over network operations. Only remove changes owned by the job. Full-text rollback preserves later attachments or reports conflicts; unknown outcomes are never silently retried. Fix incorrect external-person rollback error attribution. Existing historical jobs must remain readable and conservative when evidence is insufficient.

Validation uses isolated databases, temp artifacts, mocked Pure, and real harmless subprocess lifecycle tests. No live Pure writes. Existing backend/frontend suites and build remain passing; new tests cover races, process death, ambiguity, retained human edits, unsaved review state, and per-item rollback durability.

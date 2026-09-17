# Phase 3 Backlog

Last updated: 2026-04-21
Scope: Complete the `internal_persons` workflow in the new backend/frontend by adding tracked review and apply behavior

## Phase 3 Goal

Finish the first migrated workflow end-to-end in the new UI so `internal_persons` can be created, run, reviewed, and applied without falling back to the legacy page flow.

## Progress

- `BTP-301` completed
- `BTP-302` completed
- `BTP-303` completed

## Current Scope

- Reuse the existing `src/apply_updates_to_pure.py` logic.
- Keep legacy Flask routes working during the migration.
- Add tracked apply behavior to the new API and frontend for `internal_persons` first.

## Ticket List

### BTP-301: Add tracked apply support to the backend

Status: Completed

Deliverables:

- Add an explicit `applying` job state
- Add `POST /api/jobs/<id>/apply`
- Reuse `apply_updates_to_pure.py` for the migrated workflow
- Persist apply output in tracked job logs

Acceptance criteria:

- A `needs_review` `internal_persons` job can be applied through the API
- Apply output is visible in the same tracked logs
- Successful apply moves the job to `completed`

### BTP-302: Expose apply in the new job detail UI

Status: Completed

Deliverables:

- Add `Apply Updates` action when the job is ready
- Refresh job state and logs after apply
- Surface apply errors cleanly

Acceptance criteria:

- Users can finish the migrated `internal_persons` flow from the new UI

### BTP-303: Add automated coverage for review/apply flow

Status: Completed

Deliverables:

- Backend tests for apply state transitions and endpoint behavior
- Frontend tests for the apply action

Acceptance criteria:

- The new review/apply path is covered by automated tests

## Current Phase 3 Outcome

- `internal_persons` now supports the full migrated path in the new UI:
  - create
  - run
  - review readiness
  - download review files
  - see tracked result counts
  - apply updates
  - delete inactive tracked jobs
- The backend exposes `POST /api/jobs/<id>/apply`.
- The backend exposes `GET /api/jobs/<id>/artifacts` plus artifact download routes.
- The backend exposes `DELETE /api/jobs/<id>` for inactive jobs.
- The job state machine now includes `applying`.
- Apply output is appended to the tracked job log so users can follow the full run/apply lifecycle in one place.
- The job detail UI now explains the next user action explicitly and provides direct artifact downloads.
- The job detail UI now shows tracked result counts such as found, ready-to-update, and updated items based on the generated review files.
- The job detail UI now lets users delete completed, failed, or reviewable tracked jobs after confirmation.
- Internal-persons artifact tracking now snapshots only files created or changed by the current run, so old `personstobeupdated_*.csv` files no longer inflate the results card.
- The React router is now mounted with basename `/app`, so deep links and browser refreshes on the new UI no longer return a Flask 404.
- The new UI now includes a guide page that explains the job flow and documents both the live `internal_persons` workflow and the planned upcoming workflows.
- The job detail results card now explains what each count means for the active workflow instead of only showing generic totals.
- The guide page and dashboard now expose explicit migration-status tracking for workflows: live in new UI, legacy only, and planned.
- Apply is now blocked when a reviewed job has zero selected rows left to update, and the new UI shows a disabled action with an explanation instead of allowing an empty apply run.
- New tracked `internal_persons` runs now write into per-job artifact directories under `output/internal_persons/<job-id>`, so the migrated flow no longer depends on a shared review directory for new runs.
- Tracked `external_persons` jobs now also use per-job artifact directories under `output/external_persons/<job-id>` and can be created from the new UI.
- Tracked `external_orgs` jobs now also use per-job artifact directories under `output/external_orgs/<job-id>` and can be created from the new UI.
- Tracked `research_outputs` jobs now also use per-job artifact directories under `output/research_output/<job-id>` and can be created from the new UI.
- Tracked `datasets` jobs now also use per-job artifact directories under `output/datasets/<job-id>` and can be created from the new UI.
- `external_persons` and `external_orgs` now both have tracked rollback support, with regression coverage for apply capture and rollback behavior on the migrated flows.
- `research_outputs` and `datasets` now also have tracked rollback support, with change sets tied to created record UUIDs and apply manifests that record any external persons created during apply.
- The dashboard now exposes a backend-driven results overview across all tracked workflows, including net counts after rollback and per-type identifier breakdown where applicable.
- The UI now also has a History page with job filters plus a period filter on the results overview, so reporting and job lookup no longer depend on scrolling through the main dashboard only.
- `external_orgs` REBO validation exposed and fixed a real pipeline bug: Pure identifier type URIs were being stripped before ROR checks, so organisations that already had a ROR in Pure could still be queued. The tracked run now preserves identifier type metadata, skips existing-ROR organisations correctly, and the REBO live check dropped from `19` review rows to `13`.

## Remaining Practical Work

- Decide whether apply logs should remain merged into the main job log or move to separate per-stage logs later.
- Finish broader real-world validation for `external_persons` and `external_orgs` in the new UI, including clearer user-facing result semantics from more live runs.
- `external_persons` now has tracked rollback support based on identifier-level change sets, with the rollback executor removing job-applied identifiers from `external-persons` records.
- `external_orgs` now has tracked rollback support based on job-captured ROR and address changes, with the rollback executor restoring the previous organisation state when the current Pure value still matches the applied one.
- `research_outputs` now has tracked rollback support based on created-record change sets, with the rollback executor deleting the job-created Pure record when the DOI still matches.
- `datasets` now has tracked rollback support based on created-record change sets, with the rollback executor deleting the job-created dataset plus any external persons created by that same apply run.
- Keep the `OpenAlex fallback` matching path as an implementation detail for now; do not expose it as a user choice in the new tracked UI unless future faculty comparisons show a real difference again.
- Add richer inline review affordances if direct file download is not enough for day-to-day use.
- Inline review is now designed as a phased feature in [docs/inline-review-plan.md](/home/dgrotebeve/PycharmProjects/BackToPure/docs/inline-review-plan.md), starting with read-only CSV preview plus in-app selection editing instead of file upload.

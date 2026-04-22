# Rollback Updates Plan

Last updated: 2026-04-21
Status: Rollback implemented for `internal_persons`, `external_persons`, `external_orgs`, `research_outputs`, and `datasets`
Scope: Safe rollback for tracked apply runs, including identifier updates and created-record deletion

## Goal

Add a `Rollback Updates` action after a successful apply run, but only when the system can prove which changes were made and can safely reverse them.

## Why This Needs A Design

A rollback button is only safe if BackToPure stores an exact audit trail of what it changed during apply.

Without that, a rollback button would be misleading because:

- the Pure record may already have changed again after apply
- another BackToPure job may have touched the same person later
- a manual user change in Pure may have overwritten the value
- some changes are easy to reverse, but others are not

## Recommended Scope

Phase 1 rollback should be limited to:

- tracked jobs only
- successful apply runs only
- changes created by that exact job
- identifiers added by BackToPure

For `internal_persons`, the first rollback scope should cover identifiers like:

- ORCID
- Scopus Author ID
- OpenAlex ID

Phase 1 should not try to rollback:

- unrelated Pure fields
- ambiguous record merges
- updates from legacy non-tracked runs
- changes that no longer match the value written by the original job

## Safety Rule

Rollback should only remove or restore a value if the current Pure state still matches the value written by the tracked apply run.

If the value no longer matches, the rollback must:

- skip that row
- mark it as conflict
- log the reason clearly

This prevents BackToPure from deleting a value that was later changed by a human or another process.

## Required Data Model

Before or during apply, persist a job-scoped change set.

Recommended tables:

### `job_change_sets`

- `id`
- `job_id`
- `job_type`
- `created_at`
- `applied_at`
- `rollback_status`
- `rolled_back_at`

### `job_change_set_items`

- `id`
- `change_set_id`
- `entity_type`
- `entity_uuid`
- `field_name`
- `identifier_type`
- `old_value_json`
- `new_value_json`
- `apply_status`
- `rollback_status`
- `conflict_reason`

## What To Store Per Change

Each rollbackable update should capture:

- target entity UUID
- exact identifier type
- old value before apply
- new value written by apply
- whether apply succeeded for that item

For identifier additions, the old value may be:

- `null`
- or a list of identifiers before mutation

The new value should reflect the exact payload written to Pure.

## Apply Flow Changes

The current apply flow writes updates and logs output. To support rollback safely, extend it like this:

1. Load the reviewed CSV and JSON files.
2. For each selected entity, fetch or derive the pre-apply state.
3. Persist a pending change set item before the write.
4. Apply the update to Pure.
5. Mark the change set item as applied only when the write succeeds.
6. Mark the whole change set as complete when the apply run finishes.

## Rollback Flow

Rollback should be a new tracked operation linked to the original job.

Recommended flow:

1. User opens a completed job.
2. UI shows `Rollback Updates` only if a valid applied change set exists.
3. User confirms rollback.
4. Backend creates a rollback run linked to the original job.
5. For each applied change set item:
   - fetch current Pure state
   - compare current value with `new_value_json`
   - if equal, restore `old_value_json`
   - if not equal, mark conflict and skip
6. Persist rollback results and append them to the tracked log.

## UI Rules

Show `Rollback Updates` only when:

- original job status is `completed`
- original apply wrote at least one rollbackable change
- rollback has not already been completed for that change set

Hide or disable the button when:

- no change set exists
- apply wrote zero changes
- job came from a legacy flow
- rollback is already running or completed

The UI should also show:

- how many changes were applied
- how many are rollbackable
- whether rollback has already been executed

## API Proposal

Recommended new endpoints:

- `POST /api/jobs/<id>/rollback`
- `GET /api/jobs/<id>/change-set`

Optional later endpoints:

- `GET /api/jobs/<id>/rollback`
- `GET /api/jobs/<id>/rollback/logs`

## Job Model Extension

Recommended additions to tracked jobs:

- `rollback_job_id`
- `change_set_id`

Possible new status values:

- `rolling_back`
- `rolled_back`

If we want to avoid new top-level statuses, rollback can also be modeled as a linked secondary job, similar to apply.

## Logging Requirements

Rollback logs should include:

- rollback start time
- original job id
- change set id
- number of items checked
- number rolled back
- number skipped due to conflict
- number failed

Each skipped conflict should log:

- entity UUID
- identifier type
- why it was skipped

## Recommended Implementation Phases

### Phase A: Data capture only

- add change-set tables
- record apply changes for `internal_persons`
- no rollback button yet

Current status:

- implemented
- tracked `internal_persons` apply now persists a job-scoped change set before running apply
- the backend now records old and new values for rollbackable identifier updates
- after apply, the change set is marked with per-item apply status based on the tracked CSV result
- rollback itself is not yet exposed in the API or UI

### Phase B: Safe backend rollback for `internal_persons`

- add rollback endpoint
- create rollback tracked job
- rollback only identifier additions with exact-value matching

Current status:

- implemented in the backend
- `POST /api/jobs/<id>/rollback` now creates a tracked rollback job
- `GET /api/jobs/<id>/change-set` now exposes the stored change set
- rollback checks the current Pure state before changing anything
- rows that no longer match the job-applied value are skipped as conflicts
- rollback results are written to a dedicated tracked rollback log
- UI exposure is still pending

### Phase C: UI support

- show rollback availability on completed jobs
- add `Rollback Updates` button with confirmation
- show rollback result summary and logs

Current status:

- implemented in the new UI for `internal_persons`
- the job detail page now loads the stored change set
- `Rollback Updates` is shown only when the completed job still has a rollbackable applied change set
- the UI now shows rollbackable count, rolled-back count, and skipped conflicts
- once rollback starts, the UI follows the linked rollback job and shows its status

### Phase D: Created-record rollback for `research_outputs` and `datasets`

- capture applied record UUIDs through an apply manifest
- delete the created Pure record only when the current DOI still matches the job-applied DOI
- if apply created external persons for that record, delete those too when their current names still match

Current status:

- implemented in the backend and exposed in the new UI
- tracked `research_outputs` and `datasets` apply now writes a per-job apply manifest with created record UUIDs
- the change set stores the created record UUID plus any external persons created by apply
- rollback deletes the created research output or dataset and then attempts to delete any external persons created by that same apply
- live validation on 2026-04-21 confirmed:
  - `research_outputs` rollback deleted the created record and made the DOI reappear on rerun
  - `datasets` rollback deleted the created dataset and three created external persons, all returning `404` afterward

### Phase D: Extend to `external_persons`

- reuse the same change-set model
- only after `internal_persons` is proven stable

## Recommended First Version

The first production-worthy rollback version should:

- support `internal_persons` only
- rollback only BackToPure-added identifiers
- skip conflicts instead of forcing reversal
- keep a complete audit trail

That gives a rollback feature that is useful and defensible, without pretending to solve every case.

## Main Risk

The biggest risk is not technical complexity. It is creating a button that implies certainty where certainty does not exist.

So the design rule is:

- rollback only what we can prove
- skip everything else
- log every decision

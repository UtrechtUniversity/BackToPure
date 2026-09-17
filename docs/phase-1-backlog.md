# Phase 1 Backlog

Last updated: 2026-04-20
Scope: Add a new API and job model alongside the current Flask UI

## Progress

- `BTP-101` completed
- `BTP-102` completed
- `BTP-103` completed
- `BTP-104` completed
- `BTP-105` completed
- `BTP-106` completed
- `BTP-107` completed
- `BTP-108` completed
- `BTP-109` completed

## Compatibility Answer

Yes. This phase is designed to be separate from the current UI flow.

- The old Flask pages should keep working.
- The new work should be introduced under `/api/...`.
- The current routes and templates should stay in place until the first migrated workflow is proven.

## Phase 1 Goal

Deliver one migrated workflow, `internal_persons`, through a durable backend job API while preserving the current Flask UI.

## Ticket List

### BTP-101: Create backend package structure for API and job services

Status: Completed

Deliverables:

- Add `app/services/`
- Add `app/models/`
- Add `app/db.py`
- Keep current routes intact

Acceptance criteria:

- The current Flask app still starts
- No existing route paths are removed or renamed

### BTP-102: Add SQLite job persistence

Status: Completed

Deliverables:

- Create SQLite database in `data/jobs.sqlite`
- Add job schema and initialization code
- Add helpers to create, update, and fetch jobs

Acceptance criteria:

- Database initializes automatically if missing
- Jobs can be created and loaded by ID
- Tests cover schema initialization and CRUD helpers

### BTP-103: Define job types and state machine

Status: Completed

Deliverables:

- Create job type registry for script path, expected outputs, and allowed params
- Define valid statuses:
  - `queued`
  - `running`
  - `needs_review`
  - `failed`
  - `completed`

Acceptance criteria:

- Invalid job types are rejected
- Invalid state transitions are rejected
- Tests cover validation rules

### BTP-104: Add job runner service

Status: Completed

Deliverables:

- Add service that starts subprocesses outside request streaming
- Capture stdout and stderr into per-job log files
- Persist start, finish, exit code, and failure details

Acceptance criteria:

- A created job can be started by the service
- Logs are written to `logs/jobs/<job-id>.log`
- Failed subprocess exits mark the job as `failed`
- Tests mock subprocess execution and assert state transitions

### BTP-105: Add API endpoints for jobs

Status: Completed

Deliverables:

- `POST /api/jobs`
- `GET /api/jobs/<id>`
- `GET /api/jobs`
- `GET /api/jobs/<id>/logs`
- `GET /api/faculties`

Acceptance criteria:

- Endpoints return JSON only
- Existing non-API routes still work
- Tests cover success and failure responses

### BTP-106: Add artifact detection for internal persons

Status: Completed

Deliverables:

- Detect expected internal-persons outputs after job completion
- Compute `canOpen` and `canApply` from explicit job metadata, not `Referer`

Acceptance criteria:

- Internal-persons job completion reports artifacts consistently
- Tests cover missing and present artifact cases

### BTP-107: Migrate internal persons as the first tracked job

Status: Completed

Deliverables:

- Map API job creation for `internal_persons` to `src/enrich_internal_persons_with_ids.py`
- Preserve existing script parameters
- Expose logs and completion status through the new API

Acceptance criteria:

- The script can still be run through the old UI
- The same script can be run through the new API
- Tests cover job creation and completion path

### BTP-108: Concurrency guard for phase 1

Status: Completed

Deliverables:

- Prevent simultaneous active runs of `internal_persons` until per-job artifact directories exist

Acceptance criteria:

- A second active job of the same type is rejected with a clear API error
- Tests cover rejection behavior

### BTP-109: Documentation update after implementation

Status: Completed

Deliverables:

- Update `docs/ui-backend-migration-plan.md`
- Record what was implemented, what remains, and any deviations

Acceptance criteria:

- The docs reflect the actual code, not just intended design

## Build Order

1. `BTP-101`
2. `BTP-102`
3. `BTP-103`
4. `BTP-104`
5. `BTP-105`
6. `BTP-106`
7. `BTP-107`
8. `BTP-108`
9. `BTP-109`

## Definition Of Done For Phase 1

- The current Flask template UI still works
- The new API exists under `/api/...`
- `internal_persons` can be started and inspected as a tracked job
- Job state is persisted in SQLite
- Logs are persisted per job
- Automated tests cover the new backend behavior

## Phase 1 Actual Outcome

- The legacy Flask UI still works.
- The new backend API exists and is tested.
- `internal_persons` is migrated end-to-end through the API.
- Per-job logs and persisted job records are in place.
- Artifact readiness is available in API job payloads.
- A concurrency guard protects the shared `internal_persons` output path.

## Remaining Practical Work

1. Build the new React and TypeScript frontend against the current API.
2. Migrate the next workflow using the same tracked-job pattern.
3. Move from shared output directories to per-job artifact directories.
4. Add apply/cancel/artifact endpoints when the output model is improved.

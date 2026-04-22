# BackToPure UI and Backend Migration Plan

Last updated: 2026-04-21
Status: Phase 1 backend implemented, Phase 2 frontend now supports all five tracked workflows

## Current Progress

- Baseline tests added for the current Flask backend.
- Phase-1 scaffold added under `app/db.py`, `app/models/`, and `app/services/`.
- SQLite job persistence added in `data/jobs.sqlite`.
- Job type registry and status transition validation added.
- A backend job runner service now executes queued jobs, writes per-job logs, and persists completion state.
- JSON API endpoints added for `/api/faculties`, `/api/jobs`, `/api/jobs/<id>`, `/api/jobs/<id>/logs`, and review artifact access.
- Internal-persons artifact detection is now exposed through API job payloads via `canOpen`, `canApply`, and `artifacts`.
- The first tracked workflow is now available through the API end-to-end: create with `POST /api/jobs`, run with `POST /api/jobs/<id>/run`, inspect with `GET /api/jobs/<id>`, read logs with `GET /api/jobs/<id>/logs`, review files with `GET /api/jobs/<id>/artifacts`, download artifacts, inspect tracked result counts, apply with `POST /api/jobs/<id>/apply`, and delete inactive tracked jobs with `DELETE /api/jobs/<id>`.
- Existing Flask template routes remain in place and operational during this work.
- The first React and TypeScript frontend slice is implemented in `frontend/`, tested, and can be served either by Vite during development or by Flask under `/app` after a production build.
- The React router now uses basename `/app`, so SPA routes can be refreshed directly when served by Flask.
- Tracked job artifacts for `internal_persons` now only include files created or changed by the current run, which keeps result metrics aligned with the matching run log.
- The new frontend now includes a guide page that explains the tracked-job flow, the meaning of result counts, and which workflows are live versus still planned.
- The guide page is now filterable by migration status and the dashboard shows a compact migration-status summary so the rollout can be tracked in the UI itself.
- New tracked `internal_persons` runs now write to per-job directories under `output/internal_persons/<job-id>`, and apply reads back from the same job-scoped directory.
- New tracked `external_persons` runs now also write to per-job directories under `output/external_persons/<job-id>`, and the new UI includes a dedicated create-job flow for that workflow.
- New tracked `external_orgs` runs now also write to per-job directories under `output/external_orgs/<job-id>`, and the new UI includes a dedicated create-job flow for that workflow.
- New tracked `research_outputs` runs now also write to per-job directories under `output/research_output/<job-id>`, and the new UI includes a dedicated create-job flow for that workflow.
- New tracked `datasets` runs now also write to per-job directories under `output/datasets/<job-id>`, and the new UI includes a dedicated create-job flow for that workflow.
- `external_persons` now has backend regression coverage for tracked result summaries and zero-selection apply protection, and its enrich script now uses stricter HTTP error handling for Ricgraph research-output lookups.
- `external_orgs` now follows the same tracked per-job artifact model, supports tracked rollback for job-applied ROR and address updates, and has regression coverage for apply capture plus rollback behavior.
- Rollback phase B is now in place for `internal_persons`: tracked apply runs persist a job-scoped change set, and the backend can execute a safe rollback as a tracked secondary job with conflict skipping.
- Rollback phase C is now in place for `internal_persons`: the new job detail UI can show rollback availability, start rollback, and surface rollback counts plus linked rollback-job status.
- `research_outputs` and `datasets` now also support tracked rollback for job-created records, and their apply flow records any external persons created during apply so rollback can remove those too.
- The dashboard now includes a results overview fed by backend change sets, showing net totals across all workflows with rollback already subtracted from the real effect.
- The new UI now also includes a dedicated History page plus dashboard period filters, so users can browse old jobs and view results totals for the last `7`, `30`, `90`, or all days.

## Implemented Today

### Current API surface

- `GET /api/faculties`
- `GET /api/jobs`
- `POST /api/jobs`
- `GET /api/jobs/<id>`
- `DELETE /api/jobs/<id>`
- `POST /api/jobs/<id>/run`
- `GET /api/jobs/<id>/logs`
- `GET /api/jobs/<id>/artifacts`
- `GET /api/jobs/<id>/artifacts/<filename>`
- `POST /api/jobs/<id>/apply`

### Current frontend serving model

- Development: Vite serves the frontend on port `5173` and proxies `/api` to Flask on port `5002`.
- Built frontend: Flask serves `frontend/dist` under `/app`.
- Legacy Flask pages remain available on their existing routes.

### Current tracked workflows

- `internal_persons` can be created, executed, reviewed, downloaded, applied, and read through the new API and frontend.
- `internal_persons` apply now captures rollback-oriented audit data in backend change-set tables, and the backend exposes rollback plus change-set endpoints for safe reversal.
- `internal_persons` completed jobs in the new UI can now show and trigger safe rollback when a rollbackable change set exists.
- `external_persons` can now be created through the new UI and uses the same tracked per-job artifact model in the backend.
- `external_orgs` can now be created through the new UI and uses the same tracked per-job artifact model in the backend.
- `research_outputs` can now be created through the new UI and uses the same tracked per-job artifact model in the backend.
- `datasets` can now be created through the new UI and uses the same tracked per-job artifact model in the backend.
- `external_persons` now also has backend checks that keep `canApply` false when no reviewed rows remain selected for update.
- `external_persons` now also supports tracked rollback in the new flow, using identifier-level change sets for `external-persons` records.
- `external_orgs` now also supports tracked rollback in the new flow, using change sets for ROR identifier additions and address updates on `external-organizations` records.
- `research_outputs` now also supports tracked rollback in the new flow, deleting the job-created research output when the current DOI still matches the applied record.
- `datasets` now also supports tracked rollback in the new flow, deleting the job-created dataset and any external persons created by that apply run when their current names still match.
- The new UI no longer exposes an `OpenAlex fallback` choice for `external_persons`; the tracked job form now only asks for faculty because comparative runs showed no practical difference for users in the tested faculties.
- `internal_persons` job payloads now include a derived results summary so the frontend can show counts like found, ready to update, and updated.
- The `internal_persons` results summary now only reads the snapshotted artifacts attached to that specific run, not older CSV files left in the shared output directory.
- The job detail page now explains what each result metric means for the current job type and which review files users should expect.
- The migrated `internal_persons` flow now uses per-job artifact directories for new tracked runs instead of a shared output directory.
- The migrated `external_persons` flow now uses per-job artifact directories for new tracked runs instead of a shared output directory.
- The same `internal_persons` script still works through the old Flask UI.
- The same `external_persons` script still works through the old Flask UI.

### Current job statuses in code

- `queued`
- `running`
- `needs_review`
- `applying`
- `completed`
- `failed`

### Current constraints

- `internal_persons` is the most mature end-to-end workflow in the new UI.
- `external_persons`, `external_orgs`, `research_outputs`, and `datasets` now use isolated per-job artifacts and support tracked rollback, but all four still have less production soak time than `internal_persons`.
- Legacy routes still use their historical shared output directories.

## Goal

Replace the current server-rendered Flask UI with a more robust React and TypeScript frontend while preserving the existing job checks and Python domain logic in `src/`.

## Current Assessment

### What can stay

- The job scripts in `src/` are the core system value.
- Existing checks against Pure, Ricgraph, OpenAlex, and generated CSV/JSON outputs should stay in phase 1.
- Flask can remain the Python web framework if we want a low-risk migration.

### What should change

- The current template UI under `app/templates/` should be replaced.
- The current route layer in `app/routes.py` should stop acting as the long-term UI and subprocess supervisor.
- Job execution should no longer be tied to a single browser request/response stream.

### Current pain points

- No durable job model: no job IDs, history, restart, retry, cancellation, or persisted state.
- UI logic is duplicated across multiple templates and inline scripts.
- Route behavior depends on `Referer` and shared output directories.
- Logging is script-level, not run-level, which makes concurrent or historical analysis difficult.
- The current "open directory" flow is desktop-oriented instead of web-native.

## Target Architecture

## Frontend

- Separate `frontend/` application using React and TypeScript.
- Main pages:
  - Dashboard
  - Start Job
  - Job Detail
  - Review Outputs
  - History
- Live status and logs via polling or Server-Sent Events.

## Backend

- Keep Python and Flask.
- Expose JSON APIs only.
- Add a background job service that starts existing scripts and tracks their lifecycle.
- Persist jobs and metadata in SQLite.
- Store logs and artifacts per run.

## Compatibility Strategy

- The new backend work should be added alongside the current Flask routes, not as a replacement in phase 1.
- Existing template routes in `app/routes.py` should keep working during the migration.
- New API routes should live under `/api/...` so they do not collide with the current UI routes.
- The old Flask UI remains the operational fallback until at least one full workflow has been migrated and validated.
- Phase 1 should avoid breaking changes to the existing `src/*.py` scripts unless required for correctness.

## Proposed Repository Layout

```text
BackToPure/
├── frontend/
│   ├── src/
│   │   ├── app/
│   │   ├── components/
│   │   ├── features/
│   │   ├── pages/
│   │   └── lib/
├── app/
│   ├── __init__.py
│   ├── routes.py
│   ├── services/
│   ├── models/
│   └── db.py
├── docs/
│   └── ui-backend-migration-plan.md
├── logs/
├── output/
├── src/
└── tests/
```

## Job Model

Each run should be persisted with:

- `id`
- `job_type`
- `status`
- `params_json`
- `created_at`
- `started_at`
- `finished_at`
- `exit_code`
- `log_path`
- `artifact_dir`
- `apply_job_id`
- `error_message`

### Implemented status values

- `queued`
- `running`
- `needs_review`
- `applying`
- `completed`
- `failed`

### Planned future status values

- `ready_to_apply`
- `cancelled`

### Job types

- `internal_persons`
- `external_persons`
- `external_orgs`
- `research_outputs`
- `datasets`

## Backend API Contract

### Implemented endpoints

- `GET /api/faculties`
- `GET /api/jobs`
- `POST /api/jobs`
- `GET /api/jobs/<id>`
- `DELETE /api/jobs/<id>`
- `POST /api/jobs/<id>/run`
- `GET /api/jobs/<id>/logs`
- `GET /api/jobs/<id>/artifacts`
- `GET /api/jobs/<id>/artifacts/<filename>`
- `POST /api/jobs/<id>/apply`

### Planned endpoints

- `POST /api/jobs/<id>/cancel`
- `POST /api/jobs/<id>/rollback`

Related design:

- see [docs/rollback-updates-plan.md](/home/dgrotebeve/PycharmProjects/BackToPure/docs/rollback-updates-plan.md)
- see [docs/inline-review-plan.md](/home/dgrotebeve/PycharmProjects/BackToPure/docs/inline-review-plan.md)

## Phase Plan

### Phase 0: Stabilize current backend

- Add automated tests around current route behavior and helper functions.
- Keep changes isolated so existing user work is not disturbed.
- Use the tests as the migration baseline.

### Phase 1: Introduce a job service

- Add SQLite-backed job tracking.
- Move subprocess launching out of the request lifecycle.
- Create per-job logs in `logs/jobs/<job-id>.log`.
- Keep existing `src/*.py` scripts intact.

### Phase 2: Add the new frontend

- Scaffold `frontend/` with React and TypeScript.
- Implement dashboard, create-job flow, and job detail pages.
- Use the new API instead of server-rendered templates.
- Serve the built frontend from Flask under `/app` while keeping Vite as the development server.

### Phase 3: Artifact and apply workflow

- Replace `Referer`-based apply logic with explicit job metadata.
- Track generated CSV/JSON artifacts per run.
- Make apply operations first-class tracked jobs.

Current bridge state:

- `internal_persons` apply is now available in the new API and frontend.
- `external_persons` now follows the same job-scoped artifact pattern as `internal_persons`, which reduces shared-output coupling before the rest of the workflow hardening lands.
- The backend still reuses the legacy `apply_updates_to_pure.py` script and its referer-based directory resolution internally as a migration bridge.
- Review is now clearer in the new UI because artifact downloads are exposed directly from the tracked job detail page.
- Inactive tracked jobs can now be removed from the new UI without affecting the legacy pages.

### Phase 4: Refactor job scripts

- Gradually move shared subprocess logic into importable services.
- Reduce dependence on shared output directories.
- Improve structured logging and validation boundaries.

## Concurrency and Robustness Rules

- Avoid shared mutable state across simultaneous runs of the same job type.
- Prefer per-job artifact directories as the long-term design.
- Until that is in place, allow at most one active run per job type if needed.
- Treat log streaming as a view on a persisted job, not as the job execution mechanism.

## Testing Strategy

### Immediate test coverage

- Route helper functions in `app/routes.py`
- Flask endpoint behavior for:
  - faculty loading
  - update status
  - subprocess-backed run endpoints
  - apply route error handling
- Logging setup behavior in `src/logging_config.py`
- File selection helpers in `src/apply_updates_to_pure.py`

### Target migration coverage

- Job service state transitions
- API schema and error responses
- Artifact detection rules
- Apply gating rules
- Frontend job creation and job detail flows

## Open Questions

- Should per-job outputs be copied into dedicated run directories in phase 1, or deferred to phase 3?
- Do we want polling first for logs/status, or Server-Sent Events immediately?
- Should the new frontend live inside the Flask repo or be split into a separate deployable app later?

## Next Implementation Steps

1. Expand the tracked-job pattern from `internal_persons` to the next workflow.
2. Replace shared output assumptions with per-job artifact directories.
3. Improve artifact review further with inline previews or richer summaries where downloads alone are not enough.
4. Decide when the new frontend should become the primary entry point instead of the legacy Flask pages.

## Execution Docs

- Phase 1 backend backlog: `docs/phase-1-backlog.md`
- Phase 2 frontend backlog: `docs/phase-2-backlog.md`

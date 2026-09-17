# Phase 2 Backlog

Last updated: 2026-04-20
Scope: Build the new React and TypeScript frontend against the implemented phase-1 backend API

## Phase 2 Goal

Replace the current server-rendered Flask UI with a modern frontend that consumes the new `/api/...` endpoints while preserving the legacy Flask pages as a fallback during rollout.

## Progress

- `BTP-201` completed
- `BTP-202` completed
- `BTP-203` completed
- `BTP-204` completed
- `BTP-205` completed
- `BTP-206` completed
- `BTP-207` completed
- `BTP-208` completed
- `BTP-209` completed

## Current Backend Assumptions

The frontend should treat the following as implemented and stable enough for phase 2:

- `GET /api/faculties`
- `GET /api/jobs`
- `POST /api/jobs`
- `GET /api/jobs/<id>`
- `POST /api/jobs/<id>/run`
- `GET /api/jobs/<id>/logs`

The frontend should also assume:

- `internal_persons` is the only workflow currently migrated end-to-end.
- Job payloads include:
  - `status`
  - `params`
  - `log_path`
  - `canOpen`
  - `canApply`
  - `artifacts`
- The backend may reject a second active `internal_persons` job.

## Recommended Frontend Stack

- React
- TypeScript
- Vite
- React Router
- TanStack Query
- A small typed API client in `frontend/src/lib/api.ts`

## Proposed Frontend Layout

```text
frontend/
├── index.html
├── package.json
├── tsconfig.json
├── vite.config.ts
└── src/
    ├── app/
    ├── components/
    ├── features/
    │   ├── jobs/
    │   └── faculties/
    ├── lib/
    │   ├── api.ts
    │   └── types.ts
    ├── pages/
    │   ├── dashboard/
    │   ├── jobs/
    │   └── not-found/
    ├── styles/
    └── main.tsx
```

## Phase 2 UX Scope

### Initial pages

- Dashboard
- New Internal Persons Job
- Job Detail
- Not Found

### Initial capabilities

- List recent jobs
- Create an `internal_persons` job
- Run a queued job
- View job status and artifact readiness
- View job logs

### Explicitly out of scope for first frontend slice

- Apply updates
- Cancel jobs
- Multi-workflow forms
- In-browser CSV editing
- Authentication/authorization changes

## Ticket List

### BTP-201: Scaffold the frontend application

Status: Completed

Deliverables:

- Add `frontend/`
- Initialize React and TypeScript app with Vite
- Add routing and shared app shell

Acceptance criteria:

- Frontend can start locally
- Build output is deterministic
- Project structure matches the phase-2 layout

### BTP-202: Add typed API client and shared types

Status: Completed

Deliverables:

- Add `frontend/src/lib/api.ts`
- Add `frontend/src/lib/types.ts`
- Model the currently implemented job and faculty responses

Acceptance criteria:

- All API access goes through typed helpers
- No page performs ad hoc `fetch` calls inline

### BTP-203: Build the dashboard page

Status: Completed

Deliverables:

- Show recent jobs from `GET /api/jobs`
- Show current limitations for the migrated workflow
- Provide entry point to create a new internal-persons job

Acceptance criteria:

- Dashboard renders cleanly with empty and non-empty job lists
- Users can navigate to job creation and job detail views

### BTP-204: Build the create-job flow for internal persons

Status: Completed

Deliverables:

- Form for `internal_persons`
- Faculty selector backed by `GET /api/faculties`
- Submit to `POST /api/jobs`

Acceptance criteria:

- Valid form creates a job and redirects to job detail
- Backend validation errors are surfaced cleanly
- Concurrency guard errors are shown in the UI

### BTP-205: Build the job detail page

Status: Completed

Deliverables:

- Fetch job state from `GET /api/jobs/<id>`
- Display status, params, readiness, and artifact summary
- Show `Run Job` action when the job is queued

Acceptance criteria:

- Queued, running, needs-review, completed, and failed states render clearly
- The page supports refresh and direct linking by job ID

### BTP-206: Build log viewing for job detail

Status: Completed

Deliverables:

- Load logs from `GET /api/jobs/<id>/logs`
- Add refresh or polling behavior
- Render logs in a readable scrollable panel

Acceptance criteria:

- Empty logs are handled cleanly
- Log content remains readable for longer runs

### BTP-207: Add run action for tracked jobs

Status: Completed

Deliverables:

- Trigger `POST /api/jobs/<id>/run`
- Update job detail after execution

Acceptance criteria:

- A queued internal-persons job can be run from the new UI
- Status and logs update after execution
- Errors are surfaced without breaking navigation

### BTP-208: Add frontend tests

Status: Completed

Deliverables:

- Component tests for core job screens
- API client tests
- Basic route/navigation coverage

Acceptance criteria:

- Critical frontend flows are covered:
  - dashboard load
  - create job
  - run job
  - view job detail

### BTP-209: Add Flask integration strategy for serving the frontend

Status: Completed

Deliverables:

- Keep Vite as the frontend dev server during development
- Serve the built frontend from Flask under `/app`
- Preserve the legacy Flask pages on their current routes during rollout
- Document the local development and build workflow

Acceptance criteria:

- The chosen approach is documented and reproducible
- It does not break the legacy Flask pages

Implementation notes:

- `frontend/vite.config.ts` uses `base: "/app/"` so built assets resolve correctly when Flask serves the bundle.
- Flask serves the built SPA from `frontend/dist` through `/app`, `/app/`, and `/app/<path>`.
- Unknown `/app/...` paths fall back to `index.html` to support client-side routing.
- If `frontend/dist` does not exist, Flask returns a `503` JSON error for `/app`.

## Build Order

1. `BTP-201`
2. `BTP-202`
3. `BTP-203`
4. `BTP-204`
5. `BTP-205`
6. `BTP-206`
7. `BTP-207`
8. `BTP-208`
9. `BTP-209`

## Definition Of Done For Phase 2

- A new React and TypeScript frontend exists in `frontend/`
- The frontend uses the new backend API rather than the legacy template flows
- The frontend can run through Vite in development and through Flask at `/app` after `npm run build`

## Current Phase 2 Outcome

- The frontend scaffold, dashboard, create-job flow, job detail view, log viewing, and run action are implemented for `internal_persons`.
- Frontend tests cover the current dashboard and primary job flow.
- The built frontend is now servable by Flask at `/app` without affecting the legacy UI routes.

## Local Workflow

- Development:
  - Run Flask on port `5002`
  - Run Vite on port `5173`
  - Use the Vite dev server for frontend iteration
- Production-like local testing:
  - Run `npm run build` in `frontend/`
  - Start Flask
  - Open `/app` on the Flask server

## Remaining Practical Work

- Decide when to switch users from the legacy landing page to the new frontend entry point.
- Expand the new frontend from `internal_persons` to the next workflow.
- Add per-job artifact directories so concurrency limits can be relaxed safely.
- `internal_persons` can be created, run, and inspected end-to-end from the new UI
- The legacy Flask UI still works during rollout
- Frontend tests cover the primary migrated workflow

## Current Phase 2 Outcome

- The frontend scaffold is in place and builds successfully.
- The dashboard, create-job page, job detail page, log viewing, and run action exist for `internal_persons`.
- Active jobs and logs now auto-refresh in the UI.
- Backend errors and concurrency guard failures are surfaced in the frontend.
- Frontend tests now cover the API client, dashboard rendering, create-job flow, job detail rendering, and run-job behavior.

## Remaining Practical Work

1. Decide how Flask should serve the built frontend, if at all.
2. Expand the same frontend pattern to the next migrated backend workflow.
3. Reduce backend/frontend duplication by deciding the rollout path for the legacy templates.

## Risks

- The backend currently exposes only one migrated workflow, so the new frontend will initially feel narrow.
- Shared output directories still constrain concurrency and artifact handling.
- If the frontend is introduced without a clear local dev workflow, adoption will stall.

## Recommended Next Step

Start with `BTP-201` and `BTP-202` together, then build the smallest usable flow:

1. Dashboard
2. Create internal-persons job
3. Job detail
4. Run job

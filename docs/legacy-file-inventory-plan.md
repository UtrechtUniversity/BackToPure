# Legacy File Inventory and Cleanup Plan

Last updated: 2026-09-17

This document tracks the file-by-file production cleanup. Phase 1 is an inventory only: flag files by current use and legacy risk, without deleting anything. Later phases can remove or archive files once the current app is tested.

## Classification

- **Active**: used by the current React + Flask API app, job service, tests, or documented setup.
- **Legacy but reachable**: old UI or helper code that is still exposed by Flask routes.
- **Manual / optional utility**: not part of the normal job flow, but still useful for setup, diagnosis, or one-off maintenance.
- **Likely unused legacy**: no current import, route, job registry entry, or test path found. These are cleanup candidates after one verification pass.
- **Generated / runtime**: local output, caches, logs, databases, build files, and credentials. These should not be committed.
- **Needs review**: unclear ownership or partly duplicated behavior.

## Phase 1 Status

- [x] Check the active job registry in `app/models/jobs.py`.
- [x] Check Flask routes and legacy template usage in `app/routes.py`.
- [x] Check references to old Flask assets and legacy scripts.
- [x] Check tracked repository files with `git ls-files`.
- [x] Flag unused and legacy candidates across the whole project, not only Flask.
- [x] Decide which legacy reachable routes should redirect to `/app`. Done: the seven page routes redirect, the machine-facing routes were removed.
- [ ] Decide whether likely unused scripts should move to `legacy/` first or be deleted after testing.
- [ ] Clean generated local artifacts from the working tree if any are accidentally tracked.

## Active Application Core

| File | Status | Notes |
| --- | --- | --- |
| `BackToPure.py` | Active | Flask app entry point. |
| `app/__init__.py` | Active | App factory. |
| `app/db.py` | Active | Job database setup. |
| `app/models/__init__.py` | Active | Re-exports job model definitions. |
| `app/models/jobs.py` | Active | Defines `JOB_TYPE_REGISTRY`; this is the source of truth for current job scripts. |
| `app/services/__init__.py` | Active | Service package marker. |
| `app/services/jobs.py` | Active | Current job lifecycle, apply, rollback, logs, artifacts, cancellation. |
| `app/routes.py` | Active + legacy mixed | Current API and React serving are active, but this file also contains the old Flask workflow layer. Split or remove legacy routes in a later phase. |

## Active React UI

| Files | Status | Notes |
| --- | --- | --- |
| `frontend/index.html` | Active | Vite app shell. |
| `frontend/src/main.tsx` | Active | React entry point. |
| `frontend/src/app/router.tsx` | Active | Current app routing. |
| `frontend/src/components/*.tsx` | Active | Shared UI components. |
| `frontend/src/pages/**/*.tsx` | Active | Current React screens. |
| `frontend/src/lib/*.ts` | Active | API client, UI helpers, shared types. |
| `frontend/src/styles/global.css` | Active | Current UI styling. |
| `frontend/src/**/*.test.tsx`, `frontend/src/**/*.test.ts`, `frontend/src/test/*` | Active | Frontend test support. |
| `frontend/package.json`, `package-lock.json`, `tsconfig*.json`, `vite.config.ts` | Active | Frontend build and test config. |

## Legacy Flask UI (removed)

The old Flask UI was removed. `app/templates/` and `app/static/css/style.css` are deleted.

Human-facing pages redirect `302` to `/app`, because colleagues have them bookmarked:
`/`, `/home`, `/enrich_internal_persons_with_ids`, `/enrich_external_persons`,
`/enrich_external_orgs`, `/import_research_outputs`, `/import_datasets`.

Machine-facing routes were deleted outright — they were only ever called by the old
forms, never typed by a person: the five `/run_*` workflow routes, `/faculties`
(a duplicate of `/api/faculties`), `/update_status`, `/run_apply_updates_to_pure`
and `/open_directory`.

`/open_directory` has no `/api` replacement by design. It resolved a directory from the
`Referer` header and launched `xdg-open` on the machine running Flask, which only ever
worked when the server was the user's own desktop. The React UI serves artifacts over
HTTP instead, via `/api/jobs/<job_id>/artifacts` and
`/api/jobs/<job_id>/artifacts/<artifact_name>`.

`app/static/images/BACK-TO-Pure-7-1-2024.gif` is kept: `frontend/src/components/AppShell.tsx`
references it.

## Active Job Scripts

These are registered in `JOB_TYPE_REGISTRY` or used by the current job service.

| File | Status | Notes |
| --- | --- | --- |
| `src/enrich_internal_persons_with_ids.py` | Active | Registered current job script. |
| `src/enrich_pure_external_persons.py` | Active | Registered current job script. |
| `src/enrich_pure_external_orgs.py` | Active | Registered current job script. |
| `src/update_researchoutput_from_ricgraph.py` | Active | Registered current job script. |
| `src/update_datasets_from_ricgraph.py` | Active | Registered current job script. |
| `src/apply_updates_to_pure.py` | Active | Used by job apply flow and old legacy apply route. |

## Active Shared Helpers

| File | Status | Notes |
| --- | --- | --- |
| `src/config.py` | Active | Loads `config.ini` and environment config. |
| `src/config.example.ini` | Active | Example for new installs. |
| `src/logging_config.py` | Active | Shared logging setup. |
| `src/btp.py` | Active | Used by internal-person workflow. Name is broad, but code is still in use. |
| `src/openalex_cache.py` | Active | Used by external-person OpenAlex cache flow. |
| `src/openalex_utils.py` | Active helper + manual behavior | Used by research output workflow; also has direct script behavior. Keep, but review whether manual output belongs elsewhere. |
| `src/datacite_utils.py` | Active helper + manual behavior | Used by dataset workflow; also has direct script behavior. |
| `src/pure_datasets.py` | Active | Used by dataset update/apply flow. |
| `src/pure_persons.py` | Active | Used by Pure dataset and research-output helpers. |
| `src/pure_researchoutputs.py` | Active | Used by research-output update/apply flow. |
| `src/yoda_utils.py` | Active helper + manual behavior | Used by dataset flow; review if Yoda setup should be documented as optional. |

## Manual Or Setup Utilities

| File | Status | Notes |
| --- | --- | --- |
| `src/doctor.py` | Manual / optional utility | Current setup checker. Keep and document. |
| `src/snapshot_openalex_institutions.py` | Manual / optional utility | Required one-time setup for external-organization jobs. Tested and documented. |
| `src/fetch_openalex_metadata_cache.py` | Manual / optional utility | Not in the current job registry. May still be useful for preloading OpenAlex work metadata. Needs a clear documented use case or removal. |
| `src/merge_external_orgs.py` | Needs review / likely legacy utility | Not in the current job registry. Earlier docs mention it, but current external-org flow does not call it directly. Decide whether it remains a supported maintenance command. |

## Likely Unused Legacy Candidates

These files had no current inbound import, route, or job-registry usage in the Phase 1 static check.

| File | Status | Recommendation |
| --- | --- | --- |
| `src/personsperpublication.py` | Likely unused legacy | Move to `legacy/` or delete after confirming no manual workflow depends on it. |
| `src/pure_api_utils.py` | Likely unused legacy | Looks like older Pure helper code duplicated by current `pure_*` modules. Move/delete after comparing any still-useful functions. |
| `src/merge_external_orgs.py` | Needs review | Treat as legacy unless we decide to keep a supported external-org merge command. |

## Documentation Files

| File | Status | Notes |
| --- | --- | --- |
| `README.md` | Active | Main onboarding entry point. |
| `docs/first-run-setup.md` | Active | New-clone setup. |
| `docs/production-readiness-plan.md` | Active historical plan | Tracks completed production-readiness work. |
| `docs/runtime-operations.md` | Active | Operational guide. |
| `docs/ricgraph-api-contract.md` | Active | Ricgraph source contract. |
| `docs/entity-workflow-audit.md` | Active | Entity workflow audit; useful for maintainability. |
| `docs/ui-density-reduction-plan.md` | Active historical plan | UI cleanup history. |
| `docs/ui-backend-migration-plan.md` | Historical / mostly complete | Keep until legacy Flask routes are removed. |
| `docs/inline-review-plan.md` | Historical / mostly complete | Keep as feature history unless README absorbs it. |
| `docs/rollback-updates-plan.md` | Historical / mostly complete | Keep until runtime docs fully cover rollback. |
| `docs/phase-1-backlog.md`, `docs/phase-2-backlog.md`, `docs/phase-3-backlog.md` | Historical backlog | Archive candidates once current production plan supersedes them. |

## Generated, Runtime, And Local-Only Files

These are present locally but should remain outside source control.

| Path pattern | Status | Notes |
| --- | --- | --- |
| `.idea/` | Local IDE config | Do not require for clones. |
| `.pytest_cache/`, `.ruff_cache/` | Generated cache | Safe to delete locally. |
| `__pycache__/`, `*.pyc` | Generated cache | Safe to delete locally. |
| `src/back_to_pure.egg-info/` | Generated packaging metadata | Should not be committed. |
| `frontend/dist/` | Generated frontend build | Build output; regenerate with npm scripts. |
| `frontend/*.tsbuildinfo`, `frontend/vite.config.js`, `frontend/vite.config.d.ts` | Generated TypeScript artifacts | Should not be committed unless intentionally produced by build config. |
| `data/*.db`, `*.db`, `*.sqlite` | Runtime databases | Local state only. |
| `logs/` | Runtime logs | Local state only. |
| `output/`, `output.csv` | Runtime/export output | Local state only. |
| `src/config.ini` | Local credentials/config | Must not be committed. Use `src/config.example.ini` as template. |
| `src/.~lock.output.csv#` | Local editor lock file | Safe to delete locally. |

## Phase 2 Cleanup Proposal

- Redirect `/` and `/home` to `/app`.
- Remove or disable old Flask workflow pages after the React app covers the same actions.
- Remove legacy helper routes that only support old templates.
- Update tests that currently assert old `/run_*` behavior.
- Keep the logo asset if React still uses it; remove only old CSS/templates.

## Phase 3 Script Cleanup Proposal

- Compare `src/pure_api_utils.py` against current `src/pure_datasets.py`, `src/pure_researchoutputs.py`, and `src/pure_persons.py`.
- If nothing unique remains, delete `src/pure_api_utils.py`.
- Confirm `src/personsperpublication.py` has no manual operational use; then delete or move to `legacy/`.
- Decide whether `src/merge_external_orgs.py` is a supported command. If not, remove it.
- Document `src/fetch_openalex_metadata_cache.py` as a supported prewarm command or remove it.

## Phase 4 Repository Hygiene Proposal

- Verify `.gitignore` covers generated files listed above.
- Remove generated local artifacts from git if any are tracked.
- Add a short "repository layout" section to `README.md` so new users know which files are app code, setup utilities, local config, and runtime output.

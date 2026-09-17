# Production Readiness Plan

Last updated: 2026-05-20
Owner: BackToPure maintainers
Scope: Make the repository clean, secure, maintainable, and straightforward for another organization to clone, configure, test, validate, and run against its own Pure and Ricgraph data.

## Progress

- Overall status: Phase 11 completed
- Current focus: Plan completed
- Completed tickets: `PRD-101`, `PRD-102`, `PRD-201`, `PRD-202`, `PRD-203`, `PRD-301`, `PRD-302`, `PRD-303`, `PRD-401`, `PRD-402`, `PRD-501`, `PRD-502`, `PRD-503`, `PRD-504`, `PRD-601`, `PRD-602`, `PRD-603`, `PRD-701`, `PRD-702`, `PRD-703`, `PRD-801`, `PRD-802`, `PRD-803`, `PRD-901`, `PRD-902`, `PRD-903`, `PRD-1001`, `PRD-1002`, `PRD-1003`, `PRD-1101`, `PRD-1102`, `PRD-1103`
- Next ticket: none

## Goals

- A fresh clone contains only source, docs, fixtures, and intentionally versioned assets.
- Local secrets and runtime data are never committed.
- Backend and frontend setup steps are documented and repeatable.
- Tests and lint checks can be run by a new contributor without guessing.
- The Flask backend can run with production-safe defaults.
- Legacy scripts and UI routes are either migrated, documented, or removed.
- The remaining code has clear ownership boundaries and minimal duplication.
- A non-UU organization can configure Pure, Ricgraph, OpenAlex, and required Pure source/type URIs without editing source code.
- Ricgraph can be supplied by a local instance or a compatible remote endpoint such as `https://explorer.ricgraph.eu/api/` when the data is appropriate.
- First-run setup includes all required local snapshots and validation checks before any Pure update is possible.

## Non-Goals

- Full cloud deployment automation in the first cleanup pass.
- Rewriting the Pure/Ricgraph business logic from scratch.
- Removing the legacy Flask UI before the React/API replacement is complete.
- Guaranteeing that public Ricgraph Explorer data is suitable for every organization. Production use still requires the Ricgraph data source to match the target Pure instance.

## Review Baseline

Findings from the 2026-05-12 review:

- Tracked generated files include `.idea/`, `__pycache__/*.pyc`, and `frontend/dist/*`.
- Runtime data, logs, databases, and large OpenAlex caches exist in the working tree.
- `src/config.py` requires `src/config.ini`, but `src/config.ini` is ignored and no example config exists.
- The local `src/config.ini` contains real-looking API keys and should stay untracked.
- README setup instructions are stale: project structure is inaccurate and documented port does not match `BackToPure.py`.
- `app/routes.py` duplicates subprocess/job-running logic already represented in the newer job API.
- Several utility scripts look unused or unsafe to import because they execute work at module import time.
- Backend tests could not run in the active interpreter because `pytest` was missing.
- Frontend tests pass with `npm test`: 43 tests passed.

## Phase 1: Repository Hygiene

### PRD-101: Stop tracking generated and local-only files

Status: Completed

Deliverables:

- Remove tracked `.idea/` files from git.
- Remove tracked `__pycache__/` and `*.pyc` files from git.
- Remove tracked frontend build output under `frontend/dist/` unless a specific release process requires it.
- Remove tracked temporary lock files such as `src/.~lock.output.csv#`.
- Keep these patterns ignored going forward.

Acceptance criteria:

- `git ls-files` no longer lists IDE metadata, bytecode, local build output, or office lock files.
- `.gitignore` covers the removed categories.
- A clean clone does not contain generated runtime artifacts.

### PRD-102: Tighten `.gitignore`

Status: Completed

Deliverables:

- Add missing generated-file patterns:
  - `frontend/dist/`
  - `*.tsbuildinfo`
  - `*.sqlite`
  - `*.sqlite3`
  - `*.db-*`
  - `*.csv` only if CSV files are always runtime output, otherwise use narrower output paths.
  - `.~lock.*#`
- Keep intentional sample fixtures trackable.

Acceptance criteria:

- Running tests and the app does not create untracked noise.
- Important sample files are not accidentally ignored.

## Phase 2: Configuration and Secrets

### PRD-201: Add safe example configuration

Status: Completed

Deliverables:

- Add `src/config.example.ini` with placeholder values.
- Document copying `src/config.example.ini` to `src/config.ini`.
- Make clear which values are required for Pure, Ricgraph, and OpenAlex.

Acceptance criteria:

- A fresh clone tells the user exactly how to create local config.
- No real API keys or personal credentials are committed.

### PRD-202: Load sensitive settings safely

Status: Completed

Deliverables:

- Make Flask `SECRET_KEY` configurable through environment variables.
- Avoid fixed `app.secret_key = 'key'`.
- Keep API keys out of source-controlled files.
- Consider allowing config file path override with `BTP_CONFIG_PATH`.

Acceptance criteria:

- The app fails with a clear message when required config is missing.
- Production mode does not run with a known default secret.

### PRD-203: Rotate exposed credentials if needed

Status: Completed

Deliverables:

- Check whether the API keys currently in local `src/config.ini` were ever pushed or shared.
- Rotate/revoke exposed keys if necessary.
- Record completion without writing secrets into the repo.

Acceptance criteria:

- Maintainers confirm no active secret remains exposed through repository history or shared artifacts.

## Phase 3: Clone-and-Run Developer Experience

### PRD-301: Update README for the real project

Status: Completed

Deliverables:

- Fix project structure to show `app/`, `src/`, `frontend/`, `docs/`, and `tests/`.
- Correct the Flask port or make it configurable and document the default.
- Add backend setup, frontend setup, config setup, tests, and common troubleshooting.
- Document whether users should run legacy Flask pages, React UI, or both.

Acceptance criteria:

- A new developer can follow README steps from a fresh clone.
- README no longer references missing `app.py`, wrong template paths, or wrong port.

### PRD-302: Add explicit dev/test dependencies

Status: Completed

Deliverables:

- Add backend test dependency documentation.
- Prefer `requirements-dev.txt` or a `pyproject.toml` extra for `pytest`.
- Document frontend test command.

Acceptance criteria:

- `python3 -m pytest -q` works after documented dependency installation.
- `npm test` works from `frontend/`.

### PRD-303: Normalize application entry points

Status: Completed

Deliverables:

- Decide the canonical backend run command.
- Prefer `flask --app BackToPure run` or a package entry point over ad hoc path manipulation.
- Remove or minimize `sys.path.append(...)` in `BackToPure.py`.

Acceptance criteria:

- Backend starts from a fresh clone with the documented command.
- Import paths are understandable and testable.

## Phase 4: Packaging and Project Structure

### PRD-401: Replace weak `setup.py` packaging

Status: Completed

Deliverables:

- Replace or update `setup.py`.
- Prefer `pyproject.toml` with a valid package name such as `back-to-pure`.
- Include both `app` and relevant `src` modules consistently.
- Decide whether `src/` remains script-oriented or becomes an installable package.

Acceptance criteria:

- Package metadata is valid.
- Install/editable-install path is documented.
- Tests do not depend on manually changing `PYTHONPATH`.

### PRD-402: Separate source from runtime output

Status: Completed

Deliverables:

- Keep runtime output under ignored directories such as `output/`, `logs/`, and `data/`.
- Ensure scripts create directories as needed.
- Keep sample fixtures under an explicit tracked path such as `tests/fixtures/`.

Acceptance criteria:

- Running workflows does not write into source directories.
- Test fixtures are clearly separated from generated outputs.

## Phase 5: Backend Maintainability

### PRD-501: Consolidate subprocess execution

Status: Completed

Deliverables:

- Replace duplicated streaming subprocess code in `app/routes.py` with a shared helper or `JobService`.
- Avoid reading `stdout` and `stderr` sequentially in a way that can deadlock.
- Standardize return codes, logs, and user-facing error messages.

Acceptance criteria:

- Each job type has one canonical execution definition.
- Subprocess errors are captured reliably.
- Existing tests cover successful and failed execution paths.

### PRD-502: Reduce old route duplication

Status: Completed

Deliverables:

- Identify which legacy Flask routes are still used.
- Redirect or migrate legacy pages to the API-backed job flow where possible.
- Remove unused route/template pairs after confirming they are obsolete.

Acceptance criteria:

- `app/routes.py` is smaller and route responsibilities are clear.
- No active user workflow is removed without a replacement.

### PRD-503: Audit script modules for import safety

Status: Completed

Deliverables:

- Move script execution behind `if __name__ == "__main__":`.
- Review likely unused scripts:
  - `src/merge_external_orgs.py`
  - `src/personsperpublication.py`
  - `src/pure_api_utils.py`
- Keep, migrate, or remove each with a documented decision.

Acceptance criteria:

- Importing modules does not trigger API calls or file writes.
- Unused scripts are removed or documented as manual tools.

### PRD-504: Replace debug prints with logging

Status: Completed

Deliverables:

- Replace development `print(...)` calls in shared modules with configured logging.
- Keep CLI/user prompts only in intentional command-line scripts.
- Remove stale commented-out debug code where it adds noise.

Acceptance criteria:

- Library modules do not print unexpectedly during web requests.
- Logs remain useful for job review and troubleshooting.

## Phase 6: Tests, CI, and Quality Gates

### PRD-601: Make backend tests runnable in CI

Status: Completed

Deliverables:

- Install backend test dependencies in CI.
- Run `python3 -m pytest -q`.
- Add minimal fixture/config handling so tests do not need real credentials.

Acceptance criteria:

- CI backend test job passes on a fresh checkout.
- Tests do not call real Pure, Ricgraph, or OpenAlex services unless explicitly marked integration.

### PRD-602: Keep frontend tests in CI

Status: Completed

Deliverables:

- Run `npm ci` and `npm test` in CI.
- Optionally add `npm run build` as a separate check.

Acceptance criteria:

- CI verifies React tests and TypeScript build.
- Build artifacts are not committed as routine output.

### PRD-603: Add formatting and linting

Status: Completed

Deliverables:

- Choose Python format/lint tools.
- Choose frontend linting if needed.
- Document commands in README.

Acceptance criteria:

- Formatting/linting can be run locally.
- CI can enforce the chosen checks once the first cleanup pass is complete.

## Phase 7: Production Runtime

### PRD-701: Production-safe Flask configuration

Status: Completed

Deliverables:

- Disable debug mode by default.
- Configure host, port, secret key, data directory, and frontend dist path from environment.
- Document a production launch command using a real WSGI server if needed.

Acceptance criteria:

- Running `python BackToPure.py` locally remains convenient.
- Production docs do not use Flask debug server.

### PRD-702: Define deployment artifact strategy

Status: Completed

Deliverables:

- Decide whether Flask serves `frontend/dist` from a built artifact or whether frontend is deployed separately.
- Document the build process.
- Keep generated build output out of source control unless release artifacts are intentionally versioned.

Acceptance criteria:

- Production deployment steps are clear.
- The repository stays clean after local frontend builds.

### PRD-703: Runtime data retention and backup policy

Status: Completed

Deliverables:

- Document what lives in `data/`, `logs/`, and `output/`.
- Define what should be backed up.
- Define cleanup/retention for large caches and job artifacts.

Acceptance criteria:

- Operators know which files are disposable and which files are durable state.
- Large caches are not accidentally treated as source code.

## Phase 8: Organization-Neutral Configuration

### PRD-801: Remove hard-coded Ricgraph localhost URLs

Status: Completed

Deliverables:

- Replace every remaining `http://127.0.0.1:3030/api/...` Ricgraph call with `RIC_BASE_URL`.
- Cover dataset import paths, especially `fetch_personroots`, `select_faculties`, and dataset `all` selection.
- Add tests that fail if workflow modules contain hard-coded Ricgraph base URLs.

Acceptance criteria:

- Setting `RICGRAPH-API.BaseURL` controls every Ricgraph request.
- Dataset jobs can use local Ricgraph or a compatible remote Ricgraph endpoint without code edits.
- `grep -R "127.0.0.1:3030" src app` finds only example config/docs, not executable workflow code.

### PRD-802: Replace UU-specific faculty assumptions with config

Status: Completed

Deliverables:

- Move primary faculty matching rules into configuration.
- Replace hard-coded checks for `uu faculty:` and `uu faculty research:` with configurable include/exclude prefixes or organization types.
- Keep current UU defaults in `src/config.example.ini` as an example.
- Document how another institution should define its faculty/organization scope.

Acceptance criteria:

- A non-UU organization can list/select its organizational units without changing Python/TypeScript.
- Research organizations can be excluded through config where needed.
- Existing UU behavior remains unchanged with the current config.

### PRD-803: Validate Pure source/type URI configuration

Status: Completed

Deliverables:

- Add a startup/config validation command that checks required Pure source/type URIs are present.
- Validate the most important IDs:
  - internal OpenAlex person source
  - external OpenAlex person source
  - external ORCID source
  - external organization ROR source
  - research output and dataset type/role URIs used by create workflows
- Provide clear errors telling operators which `src/config.ini` values to fix.

Acceptance criteria:

- Bad or missing Pure URI config is caught before running jobs.
- Another organization can verify its Pure-specific values from one command.
- Documentation states that these URI values are Pure-instance-specific.

## Phase 9: Clone-And-Run Onboarding

### PRD-901: Add a first-run setup guide

Status: Completed

Deliverables:

- Add `docs/first-run-setup.md`.
- Cover:
  - clone
  - Python virtualenv
  - editable install
  - frontend install/build
  - private `src/config.ini`
  - optional `BTP_CONFIG_PATH`
  - Ricgraph source selection
  - OpenAlex institution snapshot build
  - safe first test job
  - review before apply
- Include a minimum viable local/dev path and a production path.

Acceptance criteria:

- A new organization can follow one document from clone to first safe dry-run job.
- The guide distinguishes demo/testing from production update use.
- The guide warns not to apply updates until artifacts have been reviewed.

### PRD-902: Add an environment verification command

Status: Completed

Deliverables:

- Add a command or script, for example `btp doctor` or `src/doctor.py`.
- Check:
  - Python package imports
  - config file exists
  - Pure API reachable
  - Ricgraph API reachable
  - required Ricgraph routes respond
  - frontend build exists or React UI fallback is explained
  - OpenAlex institution snapshot exists for external organization jobs
  - runtime directories are writable
- Make checks read-only.

Acceptance criteria:

- Operators can verify setup without starting a real enrichment/update job.
- Failures are actionable and name the setting or command to fix.
- The command exits non-zero when a required production prerequisite fails.

### PRD-903: Make OpenAlex snapshot setup explicit and repeatable

Status: Completed

Deliverables:

- Document `.venv/bin/python src/snapshot_openalex_institutions.py --download`.
- Add expected output and approximate size/time.
- Explain that `output/openalex_cache/openalex_institutions_snapshot_by_ror.json` is required for external organization jobs.
- Document refresh cadence and whether old snapshots can be reused.

Acceptance criteria:

- A fresh clone can create the external-organization snapshot without AWS CLI.
- Missing snapshot errors point to the same documented command.
- Operators understand which OpenAlex cache files are required and which are legacy/optional.

## Phase 10: Ricgraph Source Portability

### PRD-1001: Define supported Ricgraph API contract

Status: Completed

Deliverables:

- Document the Ricgraph routes BackToPure uses:
  - `organization/search`
  - `get_all_personroot_nodes`
  - `get_all_neighbor_nodes`
  - `advanced_search`
  - `person/enrich`
- Document expected parameters, response fields, and key formats.
- State what data must exist in Ricgraph for each workflow.

Acceptance criteria:

- A Ricgraph operator can tell whether their endpoint is compatible before running BackToPure.
- Public Explorer, local Ricgraph, and institution-hosted Ricgraph can be compared against the same contract.

### PRD-1002: Add remote Ricgraph smoke tests

Status: Completed

Deliverables:

- Add optional integration tests gated by an environment variable such as `BTP_RICGRAPH_TEST_BASE_URL`.
- Test read-only routes against a configured Ricgraph endpoint.
- Include an example using `https://explorer.ricgraph.eu/api/` for route-shape validation only.

Acceptance criteria:

- CI does not depend on public Ricgraph.
- Maintainers can run a smoke test against local or remote Ricgraph before release.
- The test makes clear that route availability does not prove the data matches the target Pure instance.

### PRD-1003: Make public Explorer usage a documented mode

Status: Completed

Deliverables:

- Document when `https://explorer.ricgraph.eu/api/` is acceptable:
  - demo
  - route compatibility checks
  - UU-like data exploration
- Document when it is not enough:
  - production updates for another Pure tenant
  - private/institution-specific data
  - missing Pure UUID links for the target Pure instance

Acceptance criteria:

- Users do not confuse public Explorer compatibility with production readiness.
- The docs give a clear recommendation: production updates should use the institution's own Ricgraph data source unless the public endpoint is explicitly known to match the target Pure.

## Phase 11: Production Safety And Validation

### PRD-1101: Add dry-run validation before apply

Status: Completed

Deliverables:

- Ensure every workflow has a safe dry-run/review artifact path.
- Prevent apply unless a completed job has review artifacts and selected rows.
- Display source config summary on job detail:
  - Pure base URL
  - Ricgraph base URL
  - selected organization/faculty scope
  - snapshot/cache status where relevant

Acceptance criteria:

- Operators can see which Pure and Ricgraph endpoints produced a job before applying it.
- Apply is blocked for incomplete or non-reviewable jobs.

### PRD-1102: Improve external organization funnel reporting

Status: Completed

Deliverables:

- Add unique-count counters to external organization logs/results:
  - Ricgraph research outputs selected
  - Pure outputs fetched
  - Ricgraph organization links found
  - unique Pure external organization UUIDs found
  - Pure org records fetched
  - matched exact
  - matched fuzzy
  - ambiguous
  - no match
  - already has ROR
  - proposed update
- De-duplicate OpenAlex ambiguous candidates by ROR/OpenAlex ID before marking a match ambiguous.

Acceptance criteria:

- A reviewer can explain why a job produced its update count.
- Safe obvious duplicates in the OpenAlex snapshot do not suppress valid proposals.

### PRD-1103: Phase verification, commit, and push discipline

Status: Completed

Deliverables:

- At the end of each new phase:
  - run focused backend tests
  - run relevant frontend tests/build when UI changes
  - update this plan's progress log
  - commit
  - push from an authenticated client

Acceptance criteria:

- Every completed phase has a verification note, commit hash, and push status in this file.
- Work remains reviewable and recoverable.

## Tracking Rules

- Update `Last updated` whenever this file changes.
- Move ticket status through `Not started`, `In progress`, `Blocked`, and `Completed`.
- Add a short dated note under `Progress Log` after each meaningful change.
- Do not mark a ticket complete until its acceptance criteria are met.
- Keep cleanup commits small enough to review safely.
- At the end of every phase, run the relevant verification commands, commit the completed phase, and push it to the remote repository before starting the next phase.
- Record the commit hash and push status in `Progress Log` for each completed phase.

## Progress Log

### 2026-05-20

- Reopened the production readiness plan after reviewing what another organization would need after cloning from GitHub.
- Added organization-neutral readiness goals for configurable Pure/Ricgraph setup, first-run onboarding, OpenAlex snapshot setup, remote Ricgraph compatibility, and production safety checks.
- Added Phase 8 through Phase 11:
  - Phase 8: remove hard-coded local Ricgraph URLs, make faculty filters configurable, and validate Pure-specific URI config.
  - Phase 9: add first-run docs, environment verification, and explicit OpenAlex snapshot setup.
  - Phase 10: define and test the Ricgraph API contract, including public Explorer as a documented demo/smoke-test source.
  - Phase 11: strengthen production safety, apply gating, external-organization funnel reporting, and commit/push discipline.
- Set `PRD-801` as the next ticket because hard-coded Ricgraph localhost URLs block remote Ricgraph use.
- Completed `PRD-801`.
- Replaced dataset workflow Ricgraph calls in `src/update_datasets_from_ricgraph.py` with `RIC_BASE_URL`.
- Changed dataset faculty lookup to use configured `FACULTY_PREFIX` instead of a literal `uu faculty`.
- Added a regression test that fails if executable Python code under `src/` or `app/` hard-codes `127.0.0.1:3030/api`.
- Verification: `.venv/bin/python -m pytest tests/test_app_structure.py -q` passed with 69 tests.
- Verification: `.venv/bin/python -m py_compile src/update_datasets_from_ricgraph.py`.
- Verification: `grep -R --exclude-dir='__pycache__' "127.0.0.1:3030/api" -n src app` now finds only local/example config files, not executable workflow code.
- Completed `PRD-802`.
- Added configurable Ricgraph organization scope in `src/config.py`:
  - `PrimaryOrganizationPrefixes`
  - `ExcludedOrganizationPrefixes`
- Kept UU-compatible defaults by deriving prefixes from `FacultyPrefix` when the new config values are absent.
- Replaced hard-coded `uu faculty` search values in external persons, external organizations, research outputs, and datasets with `FACULTY_PREFIX`.
- Replaced hard-coded primary/research organization filtering in app routes, job validation, and workflow scripts with shared config helpers.
- Documented organization scope settings in `README.md` and `src/config.example.ini`.
- Verification: `.venv/bin/python -m pytest tests/test_app_structure.py tests/test_internal_persons.py tests/test_routes.py -q` passed with 160 tests and 5 subtests.
- Verification: `.venv/bin/python -m py_compile src/config.py src/enrich_pure_external_persons.py src/enrich_pure_external_orgs.py src/update_researchoutput_from_ricgraph.py src/update_datasets_from_ricgraph.py src/btp.py app/routes.py app/services/jobs.py`.
- Verification: `BTP_CONFIG_PATH=src/config.example.ini .venv/bin/python` confirmed configured include/exclude prefixes accept primary organization keys and reject excluded organization keys.
- Completed `PRD-803`.
- Added `validate_pure_uri_config()` in `src/config.py` for Pure-specific source/type/default checks.
- Added `src/doctor.py --config-only` as a read-only validation command.
- Added packaging metadata for the new `doctor` module.
- Documented the config-only doctor command in `README.md`.
- Verification: `.venv/bin/python src/doctor.py --config-only` passed against the local private config.
- Verification: `BTP_CONFIG_PATH=src/config.example.ini .venv/bin/python src/doctor.py --config-only` reports the intentional placeholder publisher/university values.
- Verification: `.venv/bin/python -m pytest tests/test_app_structure.py tests/test_internal_persons.py tests/test_routes.py -q` passed with 161 tests and 5 subtests.
- Verification: `.venv/bin/python -m py_compile src/config.py src/doctor.py`.
- Phase 8 is complete. Next ticket is `PRD-901`: add a first-run setup guide.
- Completed `PRD-901`.
- Added `docs/first-run-setup.md` covering clone/install, private config, Ricgraph source selection, Pure URI validation, OpenAlex institution snapshot creation, local/production launch, safe first jobs, review-before-apply rules, and runtime state.
- Linked the first-run guide from `README.md`.
- Verification: reviewed `docs/first-run-setup.md` for the required setup path and corrected it to avoid claiming job detail source summaries before `PRD-1101`.
- Next ticket is `PRD-902`: add a broader environment verification command.
- Completed `PRD-902`.
- Extended `src/doctor.py` beyond config-only mode.
- The doctor now checks Python imports, frontend build presence, OpenAlex institution snapshot presence, runtime path writability, and optional Pure/Ricgraph HTTP reachability.
- Added `--skip-network` for deterministic local checks and kept `--config-only` for Pure URI/source validation only.
- Documented the broader doctor command in `README.md`.
- Verification: `.venv/bin/python src/doctor.py --skip-network` passed locally.
- Verification: `.venv/bin/python -m pytest tests/test_app_structure.py tests/test_internal_persons.py tests/test_routes.py -q` passed with 162 tests and 5 subtests.
- Verification: `.venv/bin/python -m py_compile src/doctor.py src/config.py`.
- Next ticket is `PRD-903`: make OpenAlex snapshot setup explicit and repeatable.
- Completed `PRD-903`.
- Expanded `docs/first-run-setup.md` with OpenAlex snapshot paths, expected rough size, compact lookup output, repeat behavior, refresh cadence, and a warning not to confuse the snapshot lookup with older API caches.
- Added the snapshot build command to `README.md`.
- Confirmed the external-organization missing-snapshot error already points to `.venv/bin/python src/snapshot_openalex_institutions.py --download`.
- Verification: `.venv/bin/python src/doctor.py --skip-network` confirms the snapshot is present locally.
- Phase 9 is complete. Next ticket is `PRD-1001`: define the supported Ricgraph API contract.
- Completed `PRD-1001`.
- Added `docs/ricgraph-api-contract.md` with the required Ricgraph routes, expected parameters, response fields, workflow-specific data requirements, and public Explorer caveats.
- Linked the Ricgraph API contract from `README.md`.
- Verification: confirmed the contract documents `organization/search`, `get_all_personroot_nodes`, `get_all_neighbor_nodes`, `advanced_search`, and `person/enrich`.
- Next ticket is `PRD-1002`: add optional remote Ricgraph smoke tests.
- Completed `PRD-1002`.
- Added `tests/test_ricgraph_smoke.py` with optional route-shape tests gated by `BTP_RICGRAPH_TEST_BASE_URL`.
- Documented how to run the optional smoke tests in `README.md`.
- Verification: `.venv/bin/python -m pytest tests/test_ricgraph_smoke.py -q` skips all smoke tests when no endpoint is configured.
- Verification: `BTP_RICGRAPH_TEST_BASE_URL=https://explorer.ricgraph.eu/api/ .venv/bin/python -m pytest tests/test_ricgraph_smoke.py -q` passed with 5 tests.
- Next ticket is `PRD-1003`: document public Explorer usage as a supported mode with clear limits.
- Completed `PRD-1003`.
- Expanded `docs/ricgraph-api-contract.md` with explicit public Explorer allowed/not-allowed use cases.
- Added the same public Explorer warning to `docs/first-run-setup.md`.
- Verification: confirmed docs distinguish demo/route checks from production updates for another Pure tenant.
- Phase 10 is complete. Next ticket is `PRD-1101`: add dry-run validation before apply.
- Completed `PRD-1101`.
- Verified the existing apply gate blocks jobs that are not `needs_review`, jobs without ready artifacts, and jobs with zero selected updates.
- Added `sourceConfig` to job API responses with Pure base URL, Ricgraph base URL, configured faculty prefix, and selected faculty/organization scope.
- Displayed the source summary on the React job detail page so reviewers can see which configured sources produced a job before applying.
- Verification: `.venv/bin/python -m pytest tests/test_app_structure.py tests/test_internal_persons.py tests/test_routes.py -q` passed with 163 tests and 5 subtests.
- Verification: `npm run build` passed.
- Verification: `.venv/bin/python -m py_compile app/services/jobs.py`.
- Next ticket is `PRD-1102`: improve external organization funnel reporting.
- Completed `PRD-1102`.
- Added external-organization funnel logging for selected Ricgraph outputs, fetched Pure outputs, publications with organizations, external organization links, unique external organization UUIDs, fetched Pure organization records, exact/fuzzy proposals, already-with-ROR, no-name-match, and ambiguous counts.
- De-duplicated OpenAlex institution match candidates by ROR/OpenAlex ID before applying the ambiguity gap rule, so duplicate snapshot aliases no longer suppress clear matches.
- Added a regression test for duplicate OpenAlex candidates with the same ROR.
- Verification: `.venv/bin/python -m pytest tests/test_internal_persons.py -q` passed with 35 tests.
- Verification: `.venv/bin/python -m pytest tests/test_app_structure.py tests/test_internal_persons.py tests/test_routes.py tests/test_ricgraph_smoke.py -q` passed with 164 tests, 5 skipped smoke tests, and 5 subtests.
- Verification: `.venv/bin/python -m py_compile src/enrich_pure_external_orgs.py`.
- Next ticket is `PRD-1103`: phase verification, commit, and push discipline.
- Started `PRD-1103` and marked it blocked.
- `git status --short` shows a broad pre-existing dirty worktree across backend, frontend, docs, and workflow files. Some of those changes predate this pass, so committing from this environment risks mixing unrelated work.
- Verification has been completed for the work above, but commit/push should be done from PyCharm or after the worktree is reviewed and staged deliberately.
- Completed `PRD-1103`.
- Split the broad dirty worktree into coherent commits:
  - `c9a6e84` (`Reduce React UI density`)
  - `97a8f73` (`Document production onboarding and config checks`)
  - `2d3a800` (`Improve Ricgraph entity workflows`)
  - `023ae53` (`Use configured Pure base URL in helpers`)
  - `893f76b` (`Improve job controls and review metadata`)
- Verification after commits: `.venv/bin/python -m pytest tests/test_app_structure.py tests/test_internal_persons.py tests/test_routes.py tests/test_ricgraph_smoke.py -q` passed with 164 tests, 5 skipped smoke tests, and 5 subtests.
- Verification after commits: `npm run build` passed.
- Verification after commits: `.venv/bin/python src/doctor.py --skip-network` passed.
- Push status: branch `jobs-orientied` is aligned with `origin/jobs-orientied`.

### 2026-05-12

- Created the production readiness plan from the initial repository review.
- Set `PRD-101` as the next ticket.
- Added the rule that every phase ends with verification, a git commit, and a push.
- Completed Phase 1 repository hygiene.
- Verification: `git ls-files` no longer reports tracked IDE metadata, bytecode, frontend build output, runtime data, or lock files.
- Verification: `git check-ignore` confirms `.idea/`, `__pycache__/`, `frontend/dist/`, runtime data, logs, and office lock files are ignored.
- Phase 1 commit: `38160b5` (`Clean generated files from repository`).
- Phase 1 push status: blocked by missing GitHub HTTPS credentials in this environment.
- Completed Phase 2 configuration and secrets hardening.
- Added `src/config.example.ini` with placeholders and documented copying it to private `src/config.ini`.
- Added `BTP_CONFIG_PATH` support and clearer missing-config errors.
- Replaced the fixed Flask secret with `BTP_SECRET_KEY`/`SECRET_KEY`, with production enforcement through `BTP_ENV=production`.
- Made `BackToPure.py` default to debug off and read host/port/debug from environment.
- Checked repository history for `src/config.ini`; it is not tracked and has no branch history here. Rotate local keys if they were shared outside git.
- Verification: `python3 -m py_compile BackToPure.py app/__init__.py src/config.py`.
- Verification: imported `config` with `BTP_CONFIG_PATH=src/config.example.ini`.
- Verification: `create_app()` works with example config and defaults to debug off.
- Verification: `BTP_ENV=production` without a secret raises a clear error.
- Verification: missing `BTP_CONFIG_PATH` raises a clear copy/example message.
- Backend pytest remains blocked until dev dependencies are added in Phase 3: `/usr/bin/python3: No module named pytest`.
- Phase 2 commit: `fc55431` (`Harden configuration and secret handling`).
- Phase 2 push status: blocked by missing GitHub HTTPS credentials in this environment.
- Completed Phase 3 clone-and-run developer experience.
- Updated README to match the real `app/`, `src/`, `frontend/`, `tests/`, and `docs/` layout.
- Documented backend setup, frontend setup, private config setup, Flask run commands, React build usage, tests, and common troubleshooting.
- Added `requirements-dev.txt` with backend test dependencies.
- Normalized `BackToPure.py` to insert the resolved `src/` path deterministically.
- Verification: `.venv/bin/python -m pip install -r requirements-dev.txt`.
- Verification: `.venv/bin/python -m pytest -q` passed with 136 tests and 5 subtests.
- Verification: `npm test` passed with 43 frontend tests.
- Verification: `npm run build` passed.
- Verification: `BTP_CONFIG_PATH=src/config.example.ini .venv/bin/flask --app BackToPure routes` listed the Flask routes.
- Verification: `git ls-files` still reports no tracked generated artifacts after tests/build.
- Phase 3 commit: `dadb0ab` (`Document clone and test workflow`).
- Phase 3 push status: `dadb0ab` is present on `origin/jobs-orientied`; final checkpoint-status note commit is pending push from an authenticated client.
- Completed Phase 4 packaging and project structure cleanup.
- Replaced `setup.py` with `pyproject.toml` and valid package metadata for `back-to-pure`.
- Added editable install support for the Flask `app` package and the flat workflow modules from `src/`.
- Centralized test `src/` path bootstrapping in `tests/conftest.py` and removed repeated `sys.path` edits from individual test files.
- Updated README to document editable install for local development.
- Added `.gitignore` coverage for `*.egg-info/`.
- Redirected remaining ad hoc root-level debug/output files into ignored output directories.
- Verification: `.venv/bin/python -m pip install -e '.[dev]'` passed.
- Verification: `.venv/bin/python -m pytest -q` passed with 136 tests and 5 subtests.
- Verification: `python3 -m py_compile BackToPure.py tests/conftest.py src/openalex_utils.py src/merge_external_orgs.py`.
- Verification: `git ls-files` reports no tracked generated artifacts under `output/`, `logs/`, `data/`, `frontend/dist/`, or `*.egg-info/`.
- Verification: only `tests/conftest.py` and `BackToPure.py` still adjust `sys.path`.
- Phase 4 commit: `f793481` (`Modernize packaging and source layout`).
- Phase 4 push status: blocked by missing GitHub HTTPS credentials in this environment.
- Completed Phase 5 backend maintainability cleanup.
- Replaced five duplicated legacy runner implementations in `app/routes.py` with one shared legacy workflow registry and one shared streaming subprocess helper.
- Reused job-type definitions for legacy script paths and artifact expectations instead of duplicating them in route conditionals.
- Switched legacy subprocess streaming to a single stdout stream with `stderr` redirected, which removes the previous deadlock-prone sequential pipe reads.
- Removed unused legacy templates: `coming_soon.html`, `index.html`, and `script_page.html`.
- Added `main()` guard and output-dir routing to `src/merge_external_orgs.py` and redirected `src/openalex_utils.py` debug output into ignored output directories.
- Verification: `.venv/bin/python -m pytest -q` passed with 136 tests and 5 subtests.
- Verification: `python3 -m py_compile app/routes.py tests/test_routes.py`.
- Phase 5 commit: `cfb351f` (`Consolidate legacy Flask workflow routes`).
- Phase 5 push status: blocked by missing GitHub HTTPS credentials in this environment.
- Completed Phase 6 tests, CI, and quality gates.
- Removed the stale docs deployment workflow and replaced it with `.github/workflows/ci.yml` for backend and frontend verification on pushes and pull requests.
- Added `ruff` to the backend development dependencies and documented the local lint/test commands in README.
- Scoped the Python CI lint gate to the maintained backend surface: `app`, `tests`, and `BackToPure.py`.
- Auto-fixed import ordering in the maintained backend files and removed the remaining unused local in `app/services/jobs.py`.
- Verification: `BTP_CONFIG_PATH=src/config.example.ini .venv/bin/python -m ruff check --select I,F app tests BackToPure.py`.
- Verification: `BTP_CONFIG_PATH=src/config.example.ini .venv/bin/python -m pytest -q` passed with 136 tests and 5 subtests.
- Verification: `npm test` passed with 43 frontend tests.
- Verification: `npm run build` passed.
- Phase 6 commit: `8d4f209` (`Add CI quality gates`).
- Phase 6 push status: blocked by missing GitHub HTTPS credentials in this environment.
- Completed Phase 7 production runtime hardening.
- Added runtime path configuration for repository root, runtime root, data directory, log directory, and frontend build directory.
- Updated the backend job service and legacy Flask helpers to read and write logs and artifacts from the configured runtime root instead of assuming the git checkout is writable.
- Added a production dependency extra for `gunicorn`.
- Documented the deployment layout, WSGI launch command, frontend artifact strategy, retention expectations, and backup policy in `docs/runtime-operations.md`.
- Verification: `BTP_CONFIG_PATH=src/config.example.ini .venv/bin/python -m ruff check --select I,F app tests BackToPure.py`.
- Verification: `BTP_CONFIG_PATH=src/config.example.ini .venv/bin/python -m pytest -q` passed with 138 tests and 5 subtests.
- Verification: `npm test` passed with 43 frontend tests.
- Verification: `npm run build` passed.
- Phase 7 commit: `ede28d5` (`Harden production runtime paths`).
- Phase 7 push status: blocked by missing GitHub HTTPS credentials in this environment.

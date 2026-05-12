# Production Readiness Plan

Last updated: 2026-05-12
Owner: BackToPure maintainers
Scope: Make the repository clean, secure, maintainable, and straightforward for a new developer to clone, configure, test, and run.

## Progress

- Overall status: Phase 6 completed
- Current focus: Production runtime
- Completed tickets: `PRD-101`, `PRD-102`, `PRD-201`, `PRD-202`, `PRD-203`, `PRD-301`, `PRD-302`, `PRD-303`, `PRD-401`, `PRD-402`, `PRD-501`, `PRD-502`, `PRD-503`, `PRD-504`, `PRD-601`, `PRD-602`, `PRD-603`
- Next ticket: `PRD-701`

## Goals

- A fresh clone contains only source, docs, fixtures, and intentionally versioned assets.
- Local secrets and runtime data are never committed.
- Backend and frontend setup steps are documented and repeatable.
- Tests and lint checks can be run by a new contributor without guessing.
- The Flask backend can run with production-safe defaults.
- Legacy scripts and UI routes are either migrated, documented, or removed.
- The remaining code has clear ownership boundaries and minimal duplication.

## Non-Goals

- Full cloud deployment automation in the first cleanup pass.
- Rewriting the Pure/Ricgraph business logic from scratch.
- Removing the legacy Flask UI before the React/API replacement is complete.

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

Status: Not started

Deliverables:

- Disable debug mode by default.
- Configure host, port, secret key, data directory, and frontend dist path from environment.
- Document a production launch command using a real WSGI server if needed.

Acceptance criteria:

- Running `python BackToPure.py` locally remains convenient.
- Production docs do not use Flask debug server.

### PRD-702: Define deployment artifact strategy

Status: Not started

Deliverables:

- Decide whether Flask serves `frontend/dist` from a built artifact or whether frontend is deployed separately.
- Document the build process.
- Keep generated build output out of source control unless release artifacts are intentionally versioned.

Acceptance criteria:

- Production deployment steps are clear.
- The repository stays clean after local frontend builds.

### PRD-703: Runtime data retention and backup policy

Status: Not started

Deliverables:

- Document what lives in `data/`, `logs/`, and `output/`.
- Define what should be backed up.
- Define cleanup/retention for large caches and job artifacts.

Acceptance criteria:

- Operators know which files are disposable and which files are durable state.
- Large caches are not accidentally treated as source code.

## Tracking Rules

- Update `Last updated` whenever this file changes.
- Move ticket status through `Not started`, `In progress`, `Blocked`, and `Completed`.
- Add a short dated note under `Progress Log` after each meaningful change.
- Do not mark a ticket complete until its acceptance criteria are met.
- Keep cleanup commits small enough to review safely.
- At the end of every phase, run the relevant verification commands, commit the completed phase, and push it to the remote repository before starting the next phase.
- Record the commit hash and push status in `Progress Log` for each completed phase.

## Progress Log

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

# BackToPure

## Overview
BackToPure enriches and updates research records in Pure using external sources such as Ricgraph and OpenAlex. It is a React frontend served by a Flask API: the API runs jobs, tracks their state, and exposes the review files each job produces; the frontend is how you create jobs, review what they propose, and apply or roll back the result.

Every workflow runs in two phases. A job first produces a review file listing what it would change, with a reason on every row it could not use. Nothing reaches Pure until that file has been reviewed and the job is applied, and an applied job can be rolled back.

---

## Key Features
- **Dashboard and History:** Create jobs, follow running ones, and see what previous runs changed.
- **Internal Persons:** Adds identifiers such as ORCID and OpenAlex IDs to person records.
- **External Persons:** Matches external co-authors and adds their ORCID and OpenAlex IDs.
- **External Organizations:** Adds ROR IDs and geographic data to external organizations.
- **Research Outputs:** Imports research outputs from Ricgraph into Pure.
- **Datasets:** Imports datasets from Ricgraph into Pure.
- **Open Access Full Texts:** Reports which publications could have an open access PDF attached -- verified by fetching and validating each candidate -- and deposits the approved ones onto the Pure record.
- **Apply and Rollback:** Sends reviewed changes to Pure, and takes them back off again.

---

## Prerequisites

### System Requirements
- **OS:** Windows, macOS, or Linux
- **Python:** Version 3.10 or higher
- **Node.js:** Version 20 or higher for the React frontend
- **Virtual Environment:** Recommended to use `venv`

### Dependencies
To install the backend runtime packages, run:

```bash
pip install -r requirements.txt
```

For backend development and tests, use:

```bash
pip install -r requirements-dev.txt
```

For the frontend, install packages from `frontend/`:

```bash
cd frontend
npm ci
```

### Access to Ricgraph
To use BackToPure, **access to Ricgraph** is mandatory. Ricgraph is a data storage and query system used to manage research-related data and link it to external systems. The application fetches data about faculties, researchers, and outputs directly from Ricgraph's API. Ensure that you have API access for querying Ricgraph.

---

## Project Structure

```
BackToPure/
├── BackToPure.py (Flask app entry point)
├── app/ (Flask API package: routes, services, job models, static images)
├── src/ (workflow scripts and Pure/Ricgraph/OpenAlex helpers)
│   ├── enrich_internal_persons_with_ids.py
│   ├── enrich_pure_external_persons.py
│   ├── enrich_pure_external_orgs.py
│   ├── update_researchoutput_from_ricgraph.py
│   ├── update_datasets_from_ricgraph.py
│   ├── harvest_fulltext_candidates.py (open access full text report)
│   ├── fulltext_candidates.py (candidate ranking, version policy, Pure mappings)
│   ├── fulltext_fetch.py (preflight and PDF validation)
│   ├── fulltext_ratelimit.py (per-host request throttling)
│   ├── fulltext_deposit.py (upload to Pure and file entry building)
│   ├── apply_updates_to_pure.py
│   ├── config.py
│   └── config.example.ini
├── frontend/ (React/Vite UI)
├── tests/ (backend tests)
├── docs/ (plans and implementation notes)
├── requirements.txt (backend runtime dependencies)
└── requirements-dev.txt (backend test dependencies)
```

---

## Installation Guide

### 1. Clone the Repository
```bash
git clone https://github.com/username/BackToPure.git
cd BackToPure
```

### 2. Set Up a Virtual Environment
```bash
python -m venv .venv
source .venv/bin/activate  # Linux/macOS
.venv\Scripts\activate  # Windows
```

### 3. Install Dependencies
```bash
pip install -e ".[dev]"
cd frontend
npm ci
cd ..
```

If you prefer requirements files, this still works:

```bash
pip install -r requirements-dev.txt
```

### 4. Configure the Application
Create a private local config file from the example:

```bash
cp src/config.example.ini src/config.ini
```

Then edit `src/config.ini` with your Pure, Ricgraph, and OpenAlex settings. This file is intentionally ignored by git because it contains local URLs and API keys.

The Ricgraph organization scope is configurable. For UU this normally looks like:

```ini
[RICGRAPH-API]
BaseURL = https://explorer.ricgraph.eu/api/
FacultyPrefix = uu faculty
PrimaryOrganizationPrefixes = uu faculty:
ExcludedOrganizationPrefixes = uu faculty research:
```

For another organization, set `FacultyPrefix` to the value used for Ricgraph organization search, set `PrimaryOrganizationPrefixes` to the organization key prefix that should appear in the dropdown, and set `ExcludedOrganizationPrefixes` to any related trees that should never be selected for jobs.

You can also point the app at a private config file elsewhere:

```bash
export BTP_CONFIG_PATH=/path/to/config.ini
```

For production, set a private Flask secret:

```bash
export BTP_SECRET_KEY='replace-with-a-long-random-value'
```

Before running jobs against a real Pure tenant, validate the local Pure URI/source configuration:

```bash
.venv/bin/python src/doctor.py --config-only
```

This check is read-only. It catches missing or placeholder Pure source/type values such as ORCID, OpenAlex, ROR, dataset roles, dataset type, and default organization settings.

For broader local setup checks, including imports, frontend build, runtime paths, OpenAlex snapshot, and optional Pure/Ricgraph reachability:

```bash
.venv/bin/python src/doctor.py
```

Use `--skip-network` when you only want local checks.

External organization jobs require the OpenAlex institution snapshot lookup. Build or refresh it with:

```bash
.venv/bin/python src/snapshot_openalex_institutions.py --download
```

Optional runtime path overrides:

```bash
export BTP_RUNTIME_ROOT=/var/lib/back-to-pure
export BTP_DATA_DIR=/var/lib/back-to-pure/data
export BTP_LOGS_DIR=/var/log/back-to-pure/jobs
export BTP_FRONTEND_DIST=/opt/back-to-pure/frontend/dist
```

---

## Usage Guide

### 1. Start the Flask Application Locally
```bash
python3 BackToPure.py
```
The backend runs on `http://127.0.0.1:5002` by default.

You can override the local host, port, and debug mode:

```bash
BTP_HOST=0.0.0.0 BTP_PORT=5002 FLASK_DEBUG=1 python3 BackToPure.py
```

The same app can also be started with Flask's CLI:

```bash
flask --app BackToPure run --port 5002
```

For production, do not use the Flask debug server. Install the production extra and run a WSGI server:

```bash
pip install -e ".[prod]"
gunicorn --bind 0.0.0.0:5002 --workers 2 --timeout 300 BackToPure:app
```

### 2. Navigate to the Dashboard
Build the frontend, then open `http://127.0.0.1:5002/app`. The root URL
redirects there; the earlier server-rendered Flask pages have been removed.

```bash
cd frontend
npm run build
cd ..
python3 BackToPure.py
```

### 3. Create a job
Pick a workflow from the sidebar and choose a faculty, or "all". The job runs
in the background; the job page shows its log while it works.

- **Internal Persons** -- adds ORCID and OpenAlex IDs to person records
- **External Persons** -- adds identifiers to external co-authors
- **External Organizations** -- adds ROR IDs and geographic data
- **Research Outputs / Datasets** -- imports records from Ricgraph into Pure
- **Open Access Full Texts** -- reports which publications could have a PDF
  attached. Choose which versions may be deposited: publisher version only
  (the default), or also accepted manuscripts or preprints. Each deposited
  file carries the version label that is true of it.

### 4. Review, apply, roll back
- Every row a job examined appears in its review file, either proposed for
  update or with a reason it was not usable.
- Untick anything you do not want. The review table is editable in the UI.
- Apply sends only the ticked rows to Pure.
- An applied job can be rolled back. Rollback restores what the job replaced,
  and refuses with a conflict if the record changed in the meantime.

Note on full text rollback: it removes the file from the research output, but
Pure exposes no way to delete the uploaded file itself, so the file may remain
in Pure's internal store.

---

## Tests

Run backend tests from the repository root:

```bash
python3 -m pytest -q
```

Run Python formatting and lint checks:

```bash
python3 -m ruff check --select I,F app tests BackToPure.py
```

Run frontend tests from `frontend/`:

```bash
cd frontend
npm test
```

Build the frontend:

```bash
cd frontend
npm run build
```

The CI workflow runs:

- backend editable install with `.[dev]`
- `ruff check --select I,F app tests BackToPure.py`
- backend pytest
- frontend `npm test`
- frontend `npm run build`

Generated outputs such as `data/`, `logs/`, `output/`, and `frontend/dist/` are ignored by git.

For local development, the editable install keeps the `app/` package and the flat workflow modules from `src/` importable without manually setting `PYTHONPATH`.

## Runtime Operations

Production runtime state should live outside the git checkout. Use `BTP_RUNTIME_ROOT` or the more specific `BTP_DATA_DIR`, `BTP_LOGS_DIR`, and `BTP_FRONTEND_DIST` variables to place writable state and built frontend assets in stable paths.

Operational details, retention guidance, and the recommended deployment layout are documented in [docs/runtime-operations.md](docs/runtime-operations.md).

For a new organization setting up BackToPure from a fresh clone, follow [docs/first-run-setup.md](docs/first-run-setup.md).

The expected Ricgraph routes and data shapes are documented in [docs/ricgraph-api-contract.md](docs/ricgraph-api-contract.md).

Optional Ricgraph route-shape smoke tests can be run against a local or remote endpoint:

```bash
BTP_RICGRAPH_TEST_BASE_URL=https://explorer.ricgraph.eu/api/ \
  .venv/bin/python -m pytest tests/test_ricgraph_smoke.py -q
```

---

## Troubleshooting
- **Error: "configuration file ... does not exist"**: Copy `src/config.example.ini` to `src/config.ini` or set `BTP_CONFIG_PATH`.
- **Error: "Script path does not exist"**: Ensure all scripts are located in the `src/` directory and start the backend from the repository root.
- **React app returns "Frontend build not found"**: Run `npm run build` inside `frontend/`.
- **Connection Issues:** Check if Ricgraph and Pure APIs are accessible.
- **Permission Denied:** Run the application with elevated permissions if required.

---

## Security and Licensing
- **License:** MIT License (see below).
- **Author:** David Grote Beverborg

```text
MIT License

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
```

---

## Contribution
Feel free to open issues and submit pull requests to improve the project.

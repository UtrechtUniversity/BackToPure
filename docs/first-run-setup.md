# First-Run Setup

Last updated: 2026-05-20
Audience: operators or developers setting up BackToPure for a new Pure/Ricgraph environment.

## What You Need

- Python 3.10 or newer.
- Node.js 20 or newer.
- Access to a Pure API tenant and API key.
- Access to a Ricgraph API endpoint with data that matches the target Pure tenant.
- The Pure source/type URIs used by your tenant for ORCID, OpenAlex, ROR, research output roles, dataset roles, and dataset types.

## 1. Clone And Install

```bash
git clone https://github.com/<owner>/BackToPure.git
cd BackToPure

python3 -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"
```

Install and build the React frontend:

```bash
cd frontend
npm ci
npm run build
cd ..
```

## 2. Create Private Config

```bash
cp src/config.example.ini src/config.ini
```

Edit `src/config.ini`. At minimum, set:

```ini
[PURE-API]
BaseURL = https://your-pure.elsevierpure.com/ws/api/
APIKey = your-private-pure-api-key

[RICGRAPH-API]
BaseURL = https://your-ricgraph.example.org/api/
FacultyPrefix = your faculty search prefix
PrimaryOrganizationPrefixes = your primary organization key prefix:
ExcludedOrganizationPrefixes = your excluded organization key prefix:
```

You can also keep the private config outside the repository:

```bash
export BTP_CONFIG_PATH=/path/to/private/config.ini
```

## 3. Choose The Ricgraph Source

BackToPure reads Ricgraph over HTTP. The endpoint must support the Ricgraph API routes used by the jobs.

Use an institution-owned Ricgraph endpoint for production updates. The Ricgraph data must match the target Pure tenant, especially Pure UUID links for persons, research outputs, and external organizations.

`https://explorer.ricgraph.eu/api/` can be useful for demos or route-shape checks, but it is not automatically safe for another organization's production Pure tenant.

Public Explorer mode:

- Acceptable for demo setup, smoke tests, and public data exploration.
- Not sufficient for production updates unless its Ricgraph data is known to match the Pure tenant configured in `[PURE-API]`.
- Never assume Pure UUID links from public Explorer are valid for another organization's Pure tenant.

## 4. Configure Pure-Specific URI Values

The values in these sections are Pure-instance-specific:

```ini
[ID_URI]
[URI]
[DEFAULTS]
```

Validate them before running jobs:

```bash
.venv/bin/python src/doctor.py --config-only
```

This command is read-only. It should pass before production use.

## 5. Build OpenAlex Institution Snapshot

External organization jobs require a local OpenAlex institution snapshot:

```bash
.venv/bin/python src/snapshot_openalex_institutions.py --download
```

This creates:

```text
output/openalex_cache/openalex_institutions_snapshot_by_ror.json
```

Expected local files:

```text
data/openalex-snapshot/institutions/
output/openalex_cache/openalex_institutions_snapshot_by_ror.json
output/openalex_cache/openalex_institutions_snapshot_state.json
```

As of the 2026-05-20 setup, the public OpenAlex institution snapshot is roughly 169 MB compressed and compacts to about 109 MB of ROR lookup JSON with about 121,000 institution ROR records. Actual size can change as OpenAlex changes.

The command is repeatable. If the snapshot files are already present and unchanged, the downloader skips them and rebuilds the compact lookup from local files.

Recommended refresh cadence:

- Before the first external organization job in a new clone.
- After a long pause in use, for example monthly or before a large production update.
- When OpenAlex institution metadata freshness matters for ROR/address matching.

Do not confuse this file with older API caches such as:

```text
output/openalex_cache/openalex_institutions_by_ror.json
```

External organization jobs require the dedicated snapshot lookup:

```text
output/openalex_cache/openalex_institutions_snapshot_by_ror.json
```

## 6. Start The App

Local development:

```bash
python3 BackToPure.py
```

Open:

```text
http://127.0.0.1:5002
http://127.0.0.1:5002/app
```

Production-style launch:

```bash
pip install -e ".[prod]"
export BTP_SECRET_KEY='replace-with-a-long-random-secret'
export BTP_RUNTIME_ROOT=/var/lib/back-to-pure
gunicorn --bind 0.0.0.0:5002 --workers 2 --timeout 300 BackToPure:app
```

## 7. Safe First Job

Start with one small faculty or organization scope. Do not start with `all`.

Recommended first validation order:

1. Open the app and confirm faculties/organizations load from the intended Ricgraph endpoint.
2. Run one small internal-person or external-organization job.
3. Review the job log and generated CSV/JSON artifacts.
4. Confirm the local config and job log correspond to the intended Pure and Ricgraph sources.
5. Apply only after reviewing selected rows.

## 8. Review Before Apply

BackToPure is designed around review artifacts. Treat every job as candidate generation until the CSV/JSON output has been inspected.

Do not apply updates when:

- The job used the wrong Pure base URL.
- The job used the wrong Ricgraph base URL.
- The organization/faculty scope is broader than intended.
- The review CSV has unexpected selected rows.
- The matching logic produced many ambiguous or no-match cases that have not been understood.

## 9. Runtime State

Generated state lives under ignored runtime directories:

```text
data/
logs/
output/
frontend/dist/
```

For production, keep runtime state outside the git checkout with:

```bash
export BTP_RUNTIME_ROOT=/var/lib/back-to-pure
```

See `docs/runtime-operations.md` for backup and retention guidance.

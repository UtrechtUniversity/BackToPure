# Runtime Operations

Last updated: 2026-10-13

## Runtime Layout

BackToPure now separates source code from runtime state.

- `BTP_PROJECT_ROOT`: repository root with application code and workflow scripts
- `BTP_RUNTIME_ROOT`: base directory for runtime state; defaults to the repository root for local development
- `BTP_DATA_DIR`: SQLite state directory; defaults to `${BTP_RUNTIME_ROOT}/data`
- `BTP_LOGS_DIR`: job log directory; defaults to `${BTP_RUNTIME_ROOT}/logs/jobs`
- job artifacts: `${BTP_RUNTIME_ROOT}/output/...`
- `BTP_FRONTEND_DIST`: built React app directory; defaults to `${BTP_PROJECT_ROOT}/frontend/dist`

For local development, leaving these unset keeps the current clone-and-run behavior. For production, point `BTP_RUNTIME_ROOT` or the individual directories at persistent writable locations outside the git checkout.

## Deployment Artifact Strategy

Recommended production model:

1. Keep the repository or built wheel in a read-only application directory.
2. Build the frontend during deployment and publish the resulting `frontend/dist` directory.
3. Set `BTP_FRONTEND_DIST` to that built directory.
4. Store `data/`, `logs/`, and `output/` under a separate writable runtime path.
5. Run the Flask app behind a real WSGI server.

Example environment:

```bash
export BTP_ENV=production
export BTP_SECRET_KEY='replace-with-a-long-random-secret'
export BTP_RUNTIME_ROOT=/var/lib/back-to-pure
export BTP_DATA_DIR=/var/lib/back-to-pure/data
export BTP_LOGS_DIR=/var/log/back-to-pure/jobs
export BTP_FRONTEND_DIST=/opt/back-to-pure/frontend/dist
export BTP_CONFIG_PATH=/etc/back-to-pure/config.ini
```

Example launch command:

```bash
gunicorn --bind 0.0.0.0:5002 --workers 2 --timeout 300 BackToPure:app
```

## Retention and Backup Policy

Treat runtime paths differently:

- `data/`
  - Contains `jobs.sqlite`
  - Durable state
  - Back up regularly
- `logs/`
  - Contains job execution logs
  - Operationally useful
  - Retain for troubleshooting according to local policy
- `output/`
  - Contains generated CSV, JSON, manifests, and cache-like job artifacts
  - Durable only if operators need historical review files
  - Otherwise eligible for periodic cleanup after review/apply windows close

Minimum recommendation:

1. Back up `data/jobs.sqlite`.
2. Retain `logs/jobs/` for a defined troubleshooting window.
3. Define a cleanup rule for old `output/` job directories and large OpenAlex cache artifacts.

## Operational Notes

- The Flask development server remains acceptable for local work only.
- Legacy Flask pages still use the same runtime output root, so production paths stay consistent across old and new UI flows.
- If `BTP_FRONTEND_DIST` is missing, `/app` returns HTTP `503` until the frontend build is deployed.

## Durable execution worker

Run/apply/rollback HTTP actions now commit an execution task and return **202**.
Polling the job returns progress and the final result. SQLite `execution_tasks`
records the source and result job IDs, action, owner, heartbeat, cancellation,
and terminal state. Workflow status names remain unchanged. A queued harvest
shows `running`; an accepted apply or rollback shows `applying` until execution
finishes. Cancellation of running work is a request: the job stays active until
the worker confirms execution has stopped.

`python BackToPure.py` starts a detached worker, with output in
`BTP_LOGS_DIR/worker.log`. Imports, pytest, and WSGI application creation do not
start execution. Set `BTP_START_WORKER=0` when using an external supervisor.
The worker survives web-process/request termination. A separate supervisor lock
prevents extra workers on local debug reloads; a replacement waits for any
surviving script before recovering interrupted work.

For production, supervise **both** the web server and this separate command:

```sh
BTP_RUNTIME_ROOT=/srv/backtopure/runtime \
BTP_DATA_DIR=/srv/backtopure/runtime/data \
BTP_LOGS_DIR=/srv/backtopure/runtime/logs/jobs \
BTP_CONFIG_PATH=/srv/backtopure/config.ini \
/path/to/.venv/bin/python -m app.worker
```

Run from the application checkout, with the same Python environment, service
account, config, and runtime/data/log environment variables as the web service.
Use a supervisor restart policy (for example systemd `Restart=always`). The local
detached launcher is convenient for development, but does not replace production
process supervision. The worker serializes tasks within this runtime database.
Keep the runtime on a **local Linux filesystem** with reliable SQLite and flock
semantics; separate databases or separate hosts do not share execution ownership.
`--once` is available for controlled single-task execution and exits when busy.

Heartbeats are diagnostic only. A held OS lock, inherited by launched workflow
scripts, prevents another worker from acting even after a heartbeat expires or
the original worker dies. Never remove or replace the `.worker.lock` or
`.supervisor.lock` files while processes are alive. After exclusive ownership is
available, a replacement marks formerly running tasks `interrupted` and their
jobs failed, and reconciles recorded apply journal/manifest evidence. It **never
replays interrupted tasks**. Queued tasks remain durable and execute normally.

Interrupted actions remain blocked in the queue for manual reconciliation.
Inspect the job log, change set, `apply_journal.jsonl`, manifest, and current Pure
records to determine outcomes before any operator repair. There is intentionally
no automatic retry or unlock endpoint for uncertain writes. Back up the SQLite
database and job artifact directories together before manual repair. A canceled
or interrupted remote request can have reached Pure; its outcome must be checked.

Rollback also executes in the separate worker; its per-item cancellation and
reconciliation behavior is described in [Rollback safety and cleanup](#rollback-safety-and-cleanup).

## DOI reservations and operator reconciliation

Research output and dataset creation reserve each normalized DOI per Pure tenant
in the shared `BTP_DATA_DIR/jobs.sqlite` database before lookup or contributor
creation. Both record types are checked in current Pure. All web/worker instances
must use the same database. When invoking the apply script directly, set
`BTP_RUNTIME_DB` to that absolute database path; creation refuses to use an
implicit database. Reservations do not coordinate unrelated Pure clients or
installations using another database.

To inspect reservations and interrupted actions without changing the database,
replace the example path with the configured database. Opening with `mode=ro`
also prevents accidentally creating a database at the wrong path:

```sh
python - /srv/backtopure/runtime/data/jobs.sqlite <<'PY'
import sqlite3
import sys
from pathlib import Path

uri = Path(sys.argv[1]).resolve().as_uri() + '?mode=ro'
with sqlite3.connect(uri, uri=True) as db:
    for row in db.execute('SELECT id, job_id, action, state, error_message '
                          'FROM execution_tasks WHERE state = "interrupted"'):
        print('Task:', row)
    exists = db.execute("SELECT 1 FROM sqlite_master WHERE type='table' "
                        "AND name='doi_write_reservations'").fetchone()
    if exists:
        for row in db.execute('SELECT tenant, doi, job_id, kind, state, record_uuid, '
                              'error, updated_at FROM doi_write_reservations '
                              'ORDER BY updated_at DESC'):
            print('Reservation:', row)
PY
```

Before any operator repair, stop the relevant execution, verify no workflow
process remains alive, and back up the database and corresponding job artifacts.
Match the reservation's tenant and owner job to its log, change set,
`apply_journal.jsonl`, `apply_manifest.jsonl`, and Pure audit history. Verify the
exact DOI and any recorded parent/contributor UUIDs in the original target Pure.
A `reserved` state can mean interruption during lookup; `writing` or `unknown`
can mean Pure accepted a write without returning an acknowledgement. A `rejected`
parent can still have created contributors. An empty search result is insufficient
proof of noncreation because search indexing may lag.

Never clear reservations or interrupted queue tasks based on elapsed time,
heartbeat expiry, or a desire to retry. Determine and record every remote outcome
and ownership before an administrator performs a controlled state repair. Unknown
outcomes remain blocked; there is no automatic unlock endpoint or supported
blanket reset command. Successful `created`/`existing` reservations need no reset:
a subsequent job verifies the same UUID and DOI in current Pure and handles it as
existing, with zero writes and no rollback ownership. Missing or mismatched
records remain blocked for investigation.

A successful creation records the server version when Pure returns one. Preserve
that evidence and the recorded target tenant: an acknowledged UUID alone does
not prove a record is still safe to delete after later edits.


## Rollback safety and cleanup

Rollback restores only acknowledged, job-owned changes on the same configured
Pure tenant. Every logical update uses a fresh UUID and opaque version token;
PUT preserves that token for Pure's normal version validation. ORCID is restored
only while it still equals the applied value. Exact added identifiers are removed
without replacing another identifier. Address rollback restores changed leaves,
nulls job-added leaves (Pure merges them), and preserves unrelated later fields.
Country classifications are compared/restored as one value; a later classification
edit produces a conflict. Missing historical tenant/version/ownership evidence
produces a conflict and requires operator review.

Full-text apply records the exact persisted attachment, including its `pureId`,
from the acknowledged PUT response when that response unambiguously identifies
one addition. A filename or temporary upload key does not establish ownership.
Rollback removes only that exact, unchanged attachment and keeps all other current
electronic versions, including later human attachments. Deposits lacking this
identity remain acknowledged applies but cannot be rolled back automatically.
The uploaded file can remain unreferenced in Pure's file store; its API exposes
no file deletion here.

**Created research outputs and datasets require operator cleanup.** The inspected
Pure API documents UUID-only DELETE with no conditional version parameter/header.
Even an unchanged creation version on GET cannot prevent another client's edit
between GET and DELETE. BackToPure therefore sends no automatic parent DELETE,
reports a rollback conflict, and preserves recorded creation version/UUID evidence.
It also preserves every created child external person: parent ownership does not
prove that a child is unchanged or unshared. Review current record history and
references before any separately authorized cleanup.

HTTP and legacy synchronous rollback calls share durable queue acceptance and
exclusive worker ownership. Cancellation synchronously reads the execution task
before each GET and PUT and again after GET. It cannot recall an in-flight request;
a successful response is still acknowledged even if cancellation arrives during
that request. Remaining items are recorded as cancelled. Rollback logs and failed
result messages report acknowledged success, conflicts, rejected/read-failed items,
unknown outcomes, and cancellation separately. Dashboard net counts subtract only
acknowledged rollback successes; conflicts remain applied.

`rollback_attempts` holds each group of item IDs, target tenant, PUT URL, full
payload/version, committed write intent, and acknowledgement. Back up this table
with change sets and execution tasks. No SQLite transaction spans a Pure HTTP
request. Success commits before the next group begins. Worker recovery changes
unacknowledged `writing` items/attempts to `unknown`, preserving earlier successes,
and never automatically replays them. Network errors, HTTP 5xx/408/429, and other
uncertain write responses also become `unknown`. Only the documented PUT HTTP
200 response acknowledges completed rollback; HTTP 202 Accepted and undocumented
204/other success-class responses remain unknown and do not reduce dashboard
net counts. Only definitive ordinary HTTP
4xx rejection (`rejected`), failures before any write (`read_failed`), cancelled
unsent items, and untouched pending items can be considered by a later rollback
request. Historical generic `failed` writes are blocked because their delivery
outcomes were not durably distinguished.

For interrupted or unknown rollback, inspect the existing execution task, item
old/new values, saved attempt payload/version, log, and Pure audit/current record
before operator reconciliation. Do not delete intents, clear unknown status, or
release queue ownership based on elapsed time or an apparently restored record
alone. There is no automated reconciliation/unlock endpoint. A mixed rollback may
acknowledge some updates while others remain blocked; it reports a failed result
requiring review, rather than claiming everything was restored.

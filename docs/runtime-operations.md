# Runtime Operations

Last updated: 2026-05-12

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
reconciliation behavior is described in the rollback safety documentation.

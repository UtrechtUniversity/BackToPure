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

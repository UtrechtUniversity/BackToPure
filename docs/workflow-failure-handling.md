# Workflow failure handling

Audited on 2026-10-12 after an Internal Persons harvest crashed on null Pure records and a Research Outputs apply lost visibility of successful writes when a later record failed.

## Coverage

| Workflow | Harvest handling checked | Apply handling checked |
| --- | --- | --- |
| Internal Persons | Ricgraph/Pure reads, null records, missing identifier fields, review output | Grouped person updates, missing payloads, confirmed/rejected/unknown outcomes |
| External Persons | Threaded source calls, pagination, Pure/OpenAlex reads, output failures | Grouped updates, lookup failure, per-person progress |
| External Organisations | Source calls, pagination, snapshot matching inputs | Grouped updates and per-organisation progress |
| Research Outputs | Ricgraph traversal, OpenAlex metadata, review output | Parent creation, contributor creation, partial writes |
| Datasets | All-faculty accumulation, Ricgraph/DataCite/Pure reads | Parent creation, contributor creation, partial writes |
| Open Access Full Texts | Source authentication, metadata reads, existing candidate-host failure policy | Download/upload/attachment outcomes, missing CSV metadata, rollback snapshot |

## Harvest behavior

Safe source reads use explicit timeouts and at most three attempts for temporary connection/time-out, rate-limit, and selected server errors. POST search calls are reads; creation calls are not. Authentication failures and invalid response envelopes stop the harvest instead of appearing as an empty successful result. Existing adapter retries are disabled where the shared helper owns the retry budget.

Null/non-object collection entries are skipped with warnings. Repeated pagination pages stop rather than looping indefinitely, and filtering null records does not prematurely end pagination. Dataset harvesting retains every selected faculty's results. File-write errors propagate, and a subprocess that exits without its required review artifacts is reported as failed.

For full texts, individual unavailable candidate hosts retain per-row reasons; total transport failure fails the harvest. Missing metadata (for example, a genuine DataCite 404) is different from an unavailable source.

## Apply behavior and evidence

All six apply workflows record each attempted item in `apply_journal.jsonl`, flushing it to disk before the write and after its outcome. Successful manifest entries are flushed too. CSV files are replaced atomically, checkpointed every 100 outcomes and at normal completion or handled failures. The journal is the recovery source if a process stops between a successful write and a CSV/manifest checkpoint.

Review CSVs include `apply_status` and `apply_error`. A confirmed record rejection can be skipped while other records proceed. Authentication failures and ambiguous write outcomes stop the run. Writes are not automatically retried or redirected: a lost response can mean Pure accepted the write.

Successful, interrupted, uncertain, and partially completed journal entries prevent automatic replay in the same artifact directory. Already updated CSV rows are excluded. New contributor UUIDs are journalled immediately, even if creating the parent subsequently fails. Child writes do not count as successful parent records.

The job service uses confirmed journal evidence for counts and retains unknown outcomes and child-person evidence in change-set details. A failed job with confirmed applied items can roll those items back using the existing rollback checks. Apply logs stream as work proceeds, and failure messages include the underlying error rather than only an exit code.

Concurrent requests cannot claim the same queued/review job twice. Cancellation preserves confirmed apply results. Cancelling a stale active job with no remaining process reconciles saved evidence and marks it interrupted; it does not replay writes. A process spawned during a cancellation race is terminated, and completion cannot overwrite the cancelled state.

## Recovery procedure

1. Open the job log and review table. Distinguish confirmed rejection, successful writes, and unknown outcomes.
2. Preserve the job directory, database, manifest, and journal. Do not clear them to force a retry.
3. If the job remains active after a process/server interruption, use Cancel to stop/reconcile it.
4. Confirm uncertain outcomes in Pure using the recorded DOI/UUID. Do not launch a new apply to bypass the replay guard.
5. Review contributor UUIDs created before a parent failure. These may need manual reconciliation; parent rollback does not automatically remove orphan contributors.
6. Use rollback only for confirmed applied items and inspect its conflict results.

## Remaining limits

This is failure-handling hardening, not a guarantee of exactly-once remote writes. Pure and the local database do not share a transaction. A process can stop after Pure accepted a request but before the success record reached disk; such outcomes require reconciliation. Replay protection is scoped to the job's artifact directory, not unrelated new harvests.

Harvests do not yet have durable resume checkpoints for full traversals. A failed harvest may need to restart; the existing OpenAlex cache is retained. Validation is not a complete schema for all nested Pure/Ricgraph/OpenAlex metadata. Unrecognized malformed nested data may still fail safely with a concrete error. File-system or database failures severe enough to prevent recording status need operator recovery from the retained artifacts/backups.

The audit did not add transactional Pure writes, cross-job idempotency, automatic orphan cleanup, or full harvest resume. Network write behavior is tested with simulated services; validation does not create or modify real Pure records.

## Regression checks

- `tests/test_harvest_resilience.py`: source authentication/outage, retry limits, invalid envelopes, pagination, multi-faculty datasets and output failure.
- `tests/test_apply_robustness.py`: all six processors, safe rejection, uncertain writes, durable progress, duplicate replay, child writes and full-text edge cases.
- `tests/test_job_failure_reporting.py`: streamed errors, missing artifacts, concurrency, cancelled/stale jobs, journal recovery across all six workflows and rollback eligibility.
- `tests/test_partial_apply_results.py`: failed apply keeps confirmed Research Outputs visible.
- Frontend job-detail tests cover rollback for both completed and failed apply runs.

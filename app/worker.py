"""Run with python -m app.worker. One worker per local runtime database.

The advisory lock is inherited by child scripts: a dead worker's surviving
script prevents replacement recovery. Heartbeats are diagnostic, never leases.
"""
import argparse
import fcntl
import os
import sqlite3
import subprocess
import sys
import threading
import time
import uuid
from pathlib import Path

from app.db import connect_db


class Worker:
    def __init__(self, service):
        self.service = service
        self.owner = f'{os.getpid()}:{uuid.uuid4().hex}'
        self.lock = None

    def acquire(self):
        if self.lock is not None:
            return True
        self.lock = open(str(self.service.db_path) + '.worker.lock', 'a+')
        try:
            fcntl.flock(self.lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            self.lock.close()
            self.lock = None
            return False
        self.service.worker_lock_fd = self.lock.fileno()
        return True

    def close(self):
        # Do not LOCK_UN: surviving children must retain ownership until exit.
        if self.lock:
            self.lock.close()
            self.lock = None
            if hasattr(self.service, 'worker_lock_fd'):
                del self.service.worker_lock_fd

    def reconcile_apply(self, task):
        if task['action'] == 'rollback':
            from app.services.rollback import recover_rollback
            recover_rollback(self.service, task['job_id'])
            return
        if task['action'] != 'apply':
            return
        job = self.service.get_job(task['job_id'])
        change_set = self.service.get_job_change_set(task['job_id'])
        if job and change_set:
            self.service._finalize_apply_change_set(job, change_set['id'])
            results = self.service._summarize_results(job['job_type'], job.get('artifact_dir'), artifact_summary=job['artifact_state'])
            self.service.update_job(job['id'], results=results)

    def recover(self):
        if self.lock is None:
            raise RuntimeError('Recovery requires exclusive worker lock')
        with connect_db(self.service.db_path) as db:
            rows = db.execute("SELECT * FROM execution_tasks WHERE state='running'").fetchall()
        for row in rows:
            message = 'Execution interrupted. Remote outcomes may be uncertain; reconcile saved evidence before retrying.'
            try:
                self.reconcile_apply(row)
            except Exception as exc:
                message += f' Evidence reconciliation failed: {exc}'
            with connect_db(self.service.db_path) as db:
                db.execute("UPDATE execution_tasks SET state='interrupted',finished_at=?,error_message=? WHERE id=?", (self.service._utcnow(), message, row['id']))
                db.execute("UPDATE jobs SET status='failed',finished_at=?,error_message=? WHERE id=?", (self.service._utcnow(), message, row['result_job_id']))

    def claim(self, task_id=None):
        if self.lock is None:
            raise RuntimeError('Claim requires exclusive worker lock')
        with connect_db(self.service.db_path) as db:
            db.execute('BEGIN IMMEDIATE')
            row = db.execute("SELECT * FROM execution_tasks WHERE state='queued' AND (? IS NULL OR id=?) ORDER BY created_at,id LIMIT 1", (task_id, task_id)).fetchone()
            if row is None:
                return None
            now = self.service._utcnow()
            db.execute("UPDATE execution_tasks SET state='running',owner=?,heartbeat=?,started_at=? WHERE id=? AND state='queued'", (self.owner, now, now, row['id']))
            claimed = dict(row)
            claimed.update(state='running', owner=self.owner, heartbeat=now, started_at=now)
            return claimed

    def run_once(self, task_id=None):
        task = self.claim(task_id)
        if task is None:
            return False
        service = self.service
        service.execution_task = task
        service.active_process = None
        stop = threading.Event()

        def monitor():
            cancellation_started = None
            cancelled = False
            read_failures = 0
            while not stop.wait(.2):
                # Cancellation reads precede best-effort heartbeat writes. A
                # writer holding SQLite cannot permanently kill this channel.
                try:
                    with connect_db(service.db_path) as db:
                        db.execute('PRAGMA busy_timeout=200')
                        row = db.execute('SELECT cancel_requested FROM execution_tasks WHERE id=?', (task['id'],)).fetchone()
                        if row is None:
                            raise sqlite3.OperationalError('Execution task disappeared')
                        cancelled = cancelled or bool(row['cancel_requested'])
                        read_failures = 0
                except sqlite3.OperationalError as exc:
                    read_failures += 1
                    if read_failures >= 5:
                        # Losing cancellation visibility must not permit blind
                        # writes indefinitely. Stop and require reconciliation.
                        service.monitor_failure = f'Worker cannot read cancellation state: {exc}'
                        cancelled = True
                try:
                    with connect_db(service.db_path) as db:
                        db.execute('PRAGMA busy_timeout=200')
                        db.execute('UPDATE execution_tasks SET heartbeat=? WHERE id=? AND owner=?', (service._utcnow(), task['id'], self.owner))
                except sqlite3.OperationalError:
                    pass  # Heartbeats are diagnostics, never execution leases.
                if cancelled:
                    process = service.active_process
                    if process is not None and process.poll() is None:
                        cancellation_started = cancellation_started or time.monotonic()
                        try:
                            if time.monotonic() - cancellation_started > 3:
                                process.kill()
                            else:
                                process.terminate()
                        except ProcessLookupError:
                            pass
                    # Rollback checks this flag between network writes.
                    service.cancellation_requested = True

        service.cancellation_requested = False
        service.monitor_failure = None
        thread = threading.Thread(target=monitor, daemon=True)
        thread.start()
        state = 'finished'
        error = None
        try:
            with connect_db(service.db_path) as db:
                cancelled = db.execute('SELECT cancel_requested FROM execution_tasks WHERE id=?', (task['id'],)).fetchone()[0]
            if not cancelled:
                getattr(service, task['action'] + '_job')(task['job_id'])
            if service.monitor_failure:
                raise RuntimeError(service.monitor_failure)
        except Exception as exc:
            state = 'interrupted'
            error = f'Execution interrupted: {exc}. Reconcile saved evidence before retrying.'
            with connect_db(service.db_path) as db:
                db.execute("UPDATE jobs SET status='failed',finished_at=?,error_message=? WHERE id=?", (service._utcnow(), error, task['result_job_id']))
        finally:
            # A stream/read/finalization exception must not leave a child writing
            # while this worker proceeds to another task with the same lock.
            process = service.active_process
            if process is not None and process.poll() is None:
                process.terminate()
                try:
                    process.wait(timeout=3)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait()
            stop.set()
            thread.join()
            if state == 'interrupted':
                try:
                    self.reconcile_apply(task)
                except Exception as exc:
                    error += f' Evidence reconciliation failed: {exc}'
            with connect_db(service.db_path) as db:
                cancelled = db.execute('SELECT cancel_requested FROM execution_tasks WHERE id=?', (task['id'],)).fetchone()[0]
                if cancelled:
                    state = 'cancelled'
                    cancellation_message = 'Job was cancelled by user'
                    if task['action'] == 'rollback':
                        result = db.execute('SELECT error_message FROM jobs WHERE id=?', (task['result_job_id'],)).fetchone()
                        if result and result['error_message']:
                            cancellation_message = result['error_message'] + ' Cancellation requested by user.'
                    db.execute("UPDATE jobs SET status='failed',finished_at=?,error_message=? WHERE id=?", (service._utcnow(), cancellation_message, task['result_job_id']))
                db.execute('UPDATE execution_tasks SET state=?,finished_at=?,error_message=? WHERE id=? AND owner=?', (state, service._utcnow(), error, task['id'], self.owner))
            service.execution_task = None
            service.active_process = None
        return True


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--once', action='store_true')
    args = parser.parse_args()
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent / 'src'))
    from app import create_app
    from app.services.jobs import JobService
    app = create_app()
    service = JobService(app.extensions['btp_db']['db_path'], project_root=app.config['BTP_PROJECT_ROOT'], runtime_root=app.config['BTP_RUNTIME_ROOT'], logs_dir=app.config['BTP_LOGS_DIR'])
    worker = Worker(service)
    # A separate, non-inherited supervisor lock permits one waiting replacement
    # after worker death without accumulating processes on development reloads.
    supervisor = open(str(service.db_path) + '.supervisor.lock', 'a+')
    try:
        fcntl.flock(supervisor, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        supervisor.close()
        print('A worker supervisor already owns this runtime.', flush=True)
        return
    try:
        while not worker.acquire():
            if args.once:
                return
            time.sleep(.5)
        worker.recover()
        while True:
            worked = worker.run_once()
            if args.once:
                break
            if not worked:
                time.sleep(.5)
    finally:
        worker.close()
        supervisor.close()


if __name__ == '__main__':
    main()

"""Durable acceptance; no execution in an HTTP process."""
import uuid
from app.db import connect_db

ACTIVE = ('queued', 'running', 'interrupted')


def has_active_task(db_path, job_id):
    with connect_db(db_path) as db:
        return db.execute("SELECT 1 FROM execution_tasks WHERE (job_id=? OR result_job_id=?) AND state IN ('queued','running','interrupted')", (job_id, job_id)).fetchone() is not None


def enqueue(service, job_id, action):
    if action not in {'run', 'apply', 'rollback'}:
        raise ValueError('Unknown execution action')
    with connect_db(service.db_path) as db:
        db.execute('BEGIN IMMEDIATE')
        job = service.get_job(job_id)
        if job is None:
            return None
        if has_active_task(service.db_path, job_id):
            raise ValueError('Job already has an unfinished action; reconcile interrupted writes first')
        if action == 'run' and job['status'] != 'queued':
            raise ValueError('Job must be queued before run')
        if action == 'apply':
            if job['status'] != 'needs_review' or not job['canApply'] or not job['results']['ready_count']:
                raise ValueError('Job must have selected review updates before apply')
            if service.get_job_change_set(job_id) is not None:
                raise ValueError('This job already has apply evidence. Reconcile before another apply.')
        if action == 'rollback':
            service._validate_rollback(job)
        result_id = f'rollback_{uuid.uuid4().hex[:12]}' if action == 'rollback' else job_id
        now = service._utcnow()
        if action == 'rollback':
            db.execute("""INSERT INTO jobs(id,job_type,status,params_json,created_at,started_at,log_path,artifact_dir)
                VALUES(?,?,'applying','{}',?,?,?,?)""", (result_id, job['job_type'], now, now, str(service._storage_path(service.logs_dir / f'{result_id}.log')), job['artifact_dir']))
            db.execute('UPDATE jobs SET rollback_job_id=? WHERE id=?', (result_id, job_id))
        else:
            db.execute('UPDATE jobs SET status=?,started_at=? WHERE id=?', ('running' if action == 'run' else 'applying', now, job_id))
        db.execute('INSERT INTO execution_tasks(id,job_id,result_job_id,action,created_at) VALUES(?,?,?,?,?)', (uuid.uuid4().hex, job_id, result_id, action, now))
    return service.get_job(result_id)


def cancel(service, job_id):
    with connect_db(service.db_path) as db:
        db.execute('BEGIN IMMEDIATE')
        task = db.execute("SELECT * FROM execution_tasks WHERE (job_id=? OR result_job_id=?) AND state IN ('queued','running','interrupted')", (job_id, job_id)).fetchone()
        if task is None:
            return False
        state = 'cancelled' if task['state'] == 'queued' else task['state']
        db.execute('UPDATE execution_tasks SET cancel_requested=1,state=? WHERE id=?', (state, task['id']))
        if state == 'cancelled':
            db.execute("UPDATE jobs SET status='failed',finished_at=?,error_message='Job was cancelled by user' WHERE id=?", (service._utcnow(), task['result_job_id']))
    return True

from pathlib import Path
import pytest
from app.db import initialize_database, connect_db
from app.services.jobs import JobService

@pytest.fixture
def service(tmp_path):
    initialize_database(tmp_path / 'jobs.sqlite')
    return JobService(tmp_path / 'jobs.sqlite', project_root=tmp_path)

def test_enqueue_does_not_execute_and_rejects_duplicate(service, monkeypatch):
    job = service.create_job(job_id='job_test', created_at=service._utcnow(), job_type='internal_persons')
    monkeypatch.setattr(service, 'run_job', lambda _: pytest.fail('HTTP acceptance executed work'))
    assert hasattr(service, 'enqueue_action'), 'durable acceptance API is missing'
    accepted = service.enqueue_action(job['id'], 'run')
    assert accepted['status'] == 'running'
    with pytest.raises(ValueError):
        service.enqueue_action(job['id'], 'run')
    with connect_db(service.db_path) as db:
        assert db.execute('select state from execution_tasks').fetchone()['state'] == 'queued'

def test_cancel_queued_task_never_executes(service):
    job = service.create_job(job_id='job_test', created_at=service._utcnow(), job_type='internal_persons')
    assert hasattr(service, 'enqueue_action'), 'durable acceptance API is missing'
    service.enqueue_action(job['id'], 'run')
    service.cancel_job(job['id'])
    with connect_db(service.db_path) as db:
        assert db.execute('select state from execution_tasks').fetchone()['state'] == 'cancelled'

def test_worker_lock_rejects_second_owner_and_recovery_never_replays(service):
    from app.worker import Worker
    job = service.create_job(job_id='job_test', created_at=service._utcnow(), job_type='internal_persons')
    service.enqueue_action(job['id'], 'run')
    first, second = Worker(service), Worker(service)
    assert first.acquire()
    try:
        assert not second.acquire()
        task = first.claim()
        assert task is not None
        assert first.claim() is None
    finally:
        first.close()
    assert second.acquire()
    try:
        second.recover()
        assert second.claim() is None
        assert service.get_job(job['id'])['status'] == 'failed'
        with pytest.raises(ValueError, match='unfinished'):
            service.enqueue_action(job['id'], 'run')
    finally:
        second.close()


def _wait_until(predicate):
    import time
    deadline = time.monotonic() + 10
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(.02)
    pytest.fail('Timed out waiting for subprocess state')


def _subprocess_worker(service):
    import subprocess
    import sys
    import os
    # A real worker against a harmless temporary script; never production jobs.
    code = '''from pathlib import Path
import sys
sys.path.insert(0, str(Path.cwd() / 'src'))
from app.services.jobs import JobService
from app.worker import Worker
service = JobService(Path(sys.argv[1]), project_root=Path(sys.argv[2]), python_executable=sys.executable)
w = Worker(service)
assert w.acquire()
w.recover()
w.run_once()
w.close()
'''
    return subprocess.Popen([sys.executable, '-c', code, str(service.db_path), str(service.project_root)], env=dict(os.environ))


def _harmless_script(service):
    scripts = service.project_root / 'src'
    scripts.mkdir()
    (scripts / 'enrich_internal_persons_with_ids.py').write_text('''import os,time
from pathlib import Path
out = Path(os.environ['BTP_OUTPUT_DIR'])
out.mkdir(parents=True, exist_ok=True)
(out / 'started').write_text(str(os.getpid()))
while not (out / 'release').exists(): time.sleep(.02)
(out / 'personstobeupdated_test.csv').write_text('to_be_updated,updated,PURE_UUID_PERS\\nX,,pers-1\\n')
(out / 'datatotal.json').write_text('[{"uuid":"pers-1"}]')
''')


def test_request_process_death_does_not_kill_durable_work(service):
    import subprocess
    import sys
    from app.worker import Worker
    _harmless_script(service)
    job = service.create_job(job_id='job_test', created_at=service._utcnow(), job_type='internal_persons')
    # The enqueue owner is a different process, as with an HTTP worker.
    code = '''import sys,time
from pathlib import Path
sys.path.insert(0, str(Path.cwd() / 'src'))
from app.services.jobs import JobService
s=JobService(Path(sys.argv[1]),project_root=Path(sys.argv[2]))
s.enqueue_action('job_test','run')
time.sleep(60)
'''
    request = subprocess.Popen([sys.executable, '-c', code, str(service.db_path), str(service.project_root)])
    worker = None
    out = service._artifact_directory(job['artifact_dir'])
    try:
        _wait_until(lambda: service.get_job(job['id'])['status'] == 'running')
        worker = _subprocess_worker(service)
        _wait_until(lambda: (out / 'started').exists())
        request.kill()
        request.wait(timeout=5)
        assert worker.poll() is None
        (out / 'release').touch()
        assert worker.wait(timeout=5) == 0
        assert service.get_job(job['id'])['status'] == 'needs_review'
    finally:
        (out / 'release').parent.mkdir(parents=True, exist_ok=True)
        (out / 'release').touch()
        if request.poll() is None:
            request.kill()
        request.wait(timeout=5)
        if worker is not None:
            worker.wait(timeout=5)


def test_orphan_child_retains_lock_and_prevents_recovery(service):
    from app.worker import Worker
    _harmless_script(service)
    job = service.create_job(job_id='job_test', created_at=service._utcnow(), job_type='internal_persons')
    service.enqueue_action(job['id'], 'run')
    out = service._artifact_directory(job['artifact_dir'])
    worker = _subprocess_worker(service)
    replacement = Worker(service)
    try:
        _wait_until(lambda: (out / 'started').exists())
        worker.kill()
        worker.wait(timeout=5)
        # Even an arbitrarily old heartbeat cannot authorize a replacement.
        with connect_db(service.db_path) as db:
            db.execute("UPDATE execution_tasks SET heartbeat='1900-01-01'")
        assert not replacement.acquire()
        (out / 'release').touch()
        _wait_until(replacement.acquire)
        replacement.recover()
        assert replacement.claim() is None
        assert service.get_job(job['id'])['status'] == 'failed'
    finally:
        out.mkdir(parents=True, exist_ok=True)
        (out / 'release').touch()
        if worker.poll() is None:
            worker.kill()
        worker.wait(timeout=5)
        replacement.close()

def test_running_cancellation_waits_for_worker_to_stop_child(service):
    _harmless_script(service)
    job = service.create_job(job_id='job_test', created_at=service._utcnow(), job_type='internal_persons')
    service.enqueue_action(job['id'], 'run')
    out = service._artifact_directory(job['artifact_dir'])
    worker = _subprocess_worker(service)
    try:
        _wait_until(lambda: (out / 'started').exists())
        cancelled = service.cancel_job(job['id'])
        assert cancelled['status'] == 'running'  # accepted, not yet stopped
        assert worker.wait(timeout=5) == 0
        with connect_db(service.db_path) as db:
            assert db.execute('SELECT state FROM execution_tasks').fetchone()[0] == 'cancelled'
        assert service.get_job(job['id'])['status'] == 'failed'
    finally:
        out.mkdir(parents=True, exist_ok=True)
        (out / 'release').touch()
        if worker.poll() is None:
            worker.kill()
        worker.wait(timeout=5)


def test_enqueue_from_two_connections_accepts_exactly_once(service):
    from concurrent.futures import ThreadPoolExecutor
    job = service.create_job(job_id='job_test', created_at=service._utcnow(), job_type='internal_persons')
    def attempt(_):
        candidate = JobService(service.db_path, project_root=service.project_root)
        try:
            candidate.enqueue_action(job['id'], 'run')
            return True
        except ValueError:
            return False
    with ThreadPoolExecutor(max_workers=2) as pool:
        assert sorted(pool.map(attempt, range(2))) == [False, True]

def test_restart_waits_for_orphan_then_recovers_without_another_restart(service):
    import os
    import subprocess
    import sys
    from app.worker import Worker
    job = service.create_job(job_id='job_test', created_at=service._utcnow(), job_type='internal_persons')
    service.enqueue_action(job['id'], 'run')
    original = Worker(service)
    assert original.acquire()
    original.claim()
    # An inherited lock has exactly the same lifetime as an orphaned script.
    holder = subprocess.Popen([sys.executable, '-c', 'import sys; sys.stdin.read()'], stdin=subprocess.PIPE, pass_fds=(original.lock.fileno(),))
    original.close()
    env = dict(os.environ, BTP_RUNTIME_ROOT=str(service.project_root), BTP_DATA_DIR=str(service.db_path.parent), BTP_LOGS_DIR=str(service.logs_dir))
    replacement = subprocess.Popen([sys.executable, '-m', 'app.worker'], env=env)
    try:
        _wait_until(lambda: Path(str(service.db_path) + '.supervisor.lock').exists())
        assert service.get_job(job['id'])['status'] == 'running'
        holder.communicate(timeout=5)
        _wait_until(lambda: service.get_job(job['id'])['status'] == 'failed')
        assert replacement.poll() is None
    finally:
        if holder.poll() is None:
            holder.communicate(timeout=5)
        replacement.terminate()
        replacement.wait(timeout=5)


def test_cancel_accepted_after_claim_prevents_process_launch(service, monkeypatch):
    from app.worker import Worker
    job = service.create_job(job_id='job_test', created_at=service._utcnow(), job_type='internal_persons')
    service.enqueue_action(job['id'], 'run')
    worker = Worker(service)
    assert worker.acquire()
    try:
        service.execution_task = worker.claim()
        service.cancel_job(job['id'])
        monkeypatch.setattr('app.services.jobs.subprocess.Popen', lambda *a, **k: pytest.fail('Cancelled task launched'))
        with pytest.raises(ValueError, match='cancelled'):
            service._launch_process(['never-run'])
    finally:
        worker.close()


def test_recovery_restores_confirmed_apply_evidence_without_replaying(service):
    import json
    from app.worker import Worker
    job = service.create_job(job_id='apply_test', created_at=service._utcnow(), job_type='research_outputs', status='needs_review')
    out = service._artifact_directory(job['artifact_dir'])
    out.mkdir(parents=True)
    (out / 'to_be_updated.csv').write_text('to_be_updated,updated,doi,title\nX,,10.1000/a,First\nX,,10.1000/b,Second\n')
    service.update_job(job['id'], artifacts={'canOpen': True, 'canApply': True, 'artifacts': {'directory': job['artifact_dir'], 'csv': ['to_be_updated.csv'], 'json': []}}, results={'entity_label': 'research outputs', 'ready_count': 2, 'updated_count': 0})
    service.enqueue_action(job['id'], 'apply')
    worker = Worker(service)
    assert worker.acquire()
    try:
        worker.claim()
        service._capture_apply_change_set(service.get_job(job['id']))
        (out / 'apply_manifest.jsonl').write_text(json.dumps({'job_type': 'research_outputs', 'item_key': '10.1000/a', 'record_uuid': 'created-record'}) + '\n')
        worker.recover()
        assert worker.claim() is None
        change_set = service.get_job_change_set(job['id'])
        assert change_set['applied_item_count'] == 1
        assert service.get_job(job['id'])['results']['updated_count'] == 1
        assert service.get_results_dashboard(days=30)['totals']['net_entities'] == 1
    finally:
        worker.close()

"""Upgrade rollback scenarios with the acknowledged evidence of modern applies."""
import json

from app.db import connect_db
from app.services.jobs import _pure_base_url
from safe_creation import tenant_key


def acknowledged_evidence(service, job_id, **metadata):
    with connect_db(service.db_path) as db:
        rows = db.execute('SELECT id,new_value_json FROM job_change_set_items WHERE change_set_id IN (SELECT id FROM job_change_sets WHERE job_id=?)', (job_id,)).fetchall()
        for row in rows:
            payload = json.loads(row['new_value_json'])
            payload.update(target_tenant=tenant_key(_pure_base_url()), **metadata)
            db.execute('UPDATE job_change_set_items SET new_value_json=? WHERE id=?', (json.dumps(payload), row['id']))

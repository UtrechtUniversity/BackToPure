"""Successful writes must remain visible when a later record crashes apply."""
import json
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from app import create_app
from app.db import init_db
from app.models import JobStatus
from app.services.jobs import JobService


@pytest.mark.parametrize('written', [True, False])
def test_failed_apply_preserves_successful_research_outputs(tmp_path, written):
    app = create_app()
    app.config['BTP_DATA_DIR'] = str(tmp_path / 'data')
    init_db(app)
    service = JobService(
        app.extensions['btp_db']['db_path'],
        project_root=Path(__file__).resolve().parents[1],
        runtime_root=tmp_path,
    )
    job = service.create_job(
        job_id='partial-output', job_type='research_outputs',
        status=JobStatus.NEEDS_REVIEW, created_at='2026-09-30T08:00:00Z',
    )
    directory = tmp_path / job['artifact_dir']
    directory.mkdir(parents=True)
    (directory / 'to_be_updated.csv').write_text(
        'to_be_updated,updated,doi,title\nX,,10.1000/a,First\nX,,10.1000/b,Second\n'
    )
    service.update_job(
        job['id'],
        artifacts={'canOpen': True, 'canApply': True, 'artifacts': {
            'directory': job['artifact_dir'], 'csv': ['to_be_updated.csv'], 'json': [],
        }},
        results={'entity_label': 'research outputs', 'ready_count': 2, 'updated_count': 0},
    )
    process = MagicMock(returncode=1)

    def finish():
        if written:
            (directory / 'apply_manifest.jsonl').write_text(json.dumps({
                'job_type': 'research_outputs', 'item_key': '10.1000/a',
                'record_uuid': 'created-record',
            }) + '\n')
        return '', 'Could not create the next external person'

    process.communicate.side_effect = finish
    with patch('app.services.jobs.subprocess.Popen', return_value=process):
        result = service.apply_job(job['id'])

    assert result['status'] == 'failed'
    assert 'Could not create the next external person' in result['error_message']
    assert result['results']['updated_count'] == (1 if written else 0)
    change_set = service.get_job_change_set(job['id'])
    assert change_set['applied_item_count'] == (1 if written else 0)
    assert change_set['status'] == ('applied' if written else 'failed')
    dashboard = service.get_results_dashboard(days=30)
    assert dashboard['totals']['net_entities'] == (1 if written else 0)
    if written:
        item = next(item for item in change_set['items'] if item['item_key'] == '10.1000/a')
        assert item['new_value']['record_uuid'] == 'created-record'

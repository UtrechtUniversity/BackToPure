from pathlib import Path
from uuid import uuid4

import requests
from flask import current_app, jsonify, redirect, request, send_from_directory

from app.services import JobService
from config import FACULTY_PREFIX, RIC_BASE_URL, is_primary_organization_key

# Paths served by the old Flask UI. The UI is gone, but people have these
# bookmarked, so they redirect to the React app rather than 404.
_LEGACY_REDIRECTS = (
    ('/', 'index'),
    ('/home', 'home'),
    ('/enrich_internal_persons_with_ids', 'enrich_internal_persons'),
    ('/enrich_external_persons', 'enrich_external_persons'),
    ('/enrich_external_orgs', 'enrich_external_orgs'),
    ('/import_research_outputs', 'import_research_outputs'),
    ('/import_datasets', 'import_datasets'),
)


def _redirect_to_frontend():
    return redirect('/app', code=302)


def _job_service() -> JobService:
    db_path = current_app.extensions["btp_db"]["db_path"]
    project_root = Path(current_app.config.get("BTP_PROJECT_ROOT", Path(current_app.root_path).parent))
    runtime_root = Path(current_app.config.get("BTP_RUNTIME_ROOT", project_root))
    logs_dir = Path(current_app.config.get("BTP_LOGS_DIR", runtime_root / "logs" / "jobs"))
    return JobService(
        db_path,
        project_root=project_root,
        runtime_root=runtime_root,
        logs_dir=logs_dir,
    )


def _runtime_root() -> Path:
    project_root = Path(current_app.config.get("BTP_PROJECT_ROOT", Path(current_app.root_path).parent))
    return Path(current_app.config.get("BTP_RUNTIME_ROOT", project_root))


def _frontend_dist_dir() -> Path:
    return Path(
        current_app.config.get(
            "BTP_FRONTEND_DIST",
            Path(current_app.root_path).parent / "frontend" / "dist",
        )
    )


def _fetch_faculties():
    params = {'value': FACULTY_PREFIX}
    url = RIC_BASE_URL + 'organization/search'
    response = requests.get(url, params=params)
    response.raise_for_status()
    data = response.json()
    faculties = data.get("results", [])
    return [
        {'value': f['_key'], 'label': f['value']}
        for f in faculties
        if _is_primary_faculty_key(f.get('_key'))
    ]


def _is_primary_faculty_key(key):
    return is_primary_organization_key(key)


def init_app(app):
    @app.route('/app')
    @app.route('/app/')
    @app.route('/app/<path:frontend_path>')
    def frontend_app(frontend_path: str = ''):
        dist_dir = _frontend_dist_dir()
        if not dist_dir.exists():
            return jsonify({
                'error': 'Frontend build not found',
                'message': 'Build the frontend into frontend/dist before serving it from Flask.',
            }), 503

        if frontend_path:
            requested_path = dist_dir / frontend_path
            if requested_path.is_file():
                return send_from_directory(dist_dir, frontend_path)

        return send_from_directory(dist_dir, 'index.html')

    # The legacy Flask UI was removed; these paths are kept as redirects so
    # existing bookmarks land on the React app instead of a 404.
    for legacy_path, legacy_endpoint in _LEGACY_REDIRECTS:
        app.add_url_rule(
            legacy_path,
            endpoint=legacy_endpoint,
            view_func=_redirect_to_frontend,
        )

    @app.route('/api/faculties')
    def api_get_faculties():
        try:
            faculty_options = _fetch_faculties()
        except requests.exceptions.RequestException:
            return jsonify({'error': 'Cannot connect to ricgraph'}), 500

        return jsonify({'items': faculty_options})

    @app.route('/api/jobs', methods=['GET'])
    def api_list_jobs():
        service = _job_service()
        limit = request.args.get('limit', type=int)
        jobs = service.list_jobs(limit=limit)
        return jsonify({'items': jobs})

    @app.route('/api/results-dashboard', methods=['GET'])
    def api_results_dashboard():
        service = _job_service()
        days = request.args.get('days', default=None, type=int)
        if days is not None and days <= 0:
            return jsonify({'error': 'days must be a positive integer'}), 400
        return jsonify(service.get_results_dashboard(days=days))

    @app.route('/api/jobs', methods=['POST'])
    def api_create_job():
        payload = request.get_json(silent=True) or {}
        job_type = payload.get('jobType')
        if not job_type:
            return jsonify({'error': 'jobType is required'}), 400
        params = payload.get('params')
        if params is None:
            params = {}
        if not isinstance(params, dict):
            return jsonify({'error': 'params must be an object'}), 400

        service = _job_service()
        try:
            job = service.create_job(
                job_id=f"job_{uuid4().hex[:12]}",
                job_type=job_type,
                params=params,
                created_at=service._utcnow(),
            )
        except ValueError as exc:
            return jsonify({'error': str(exc)}), 400

        return jsonify(job), 201

    @app.route('/api/jobs/<job_id>', methods=['GET'])
    def api_get_job(job_id):
        service = _job_service()
        job = service.get_job(job_id)
        if job is None:
            return jsonify({'error': f'Job not found: {job_id}'}), 404
        return jsonify(job)

    @app.route('/api/jobs/<job_id>', methods=['DELETE'])
    def api_delete_job(job_id):
        service = _job_service()
        try:
            deleted = service.delete_job(job_id)
        except ValueError as exc:
            return jsonify({'error': str(exc)}), 400

        if not deleted:
            return jsonify({'error': f'Job not found: {job_id}'}), 404
        return '', 204

    @app.route('/api/jobs/<job_id>/run', methods=['POST'])
    def api_run_job(job_id):
        service = _job_service()
        try:
            job = service.run_job(job_id)
        except ValueError as exc:
            return jsonify({'error': str(exc)}), 400

        if job is None:
            return jsonify({'error': f'Job not found: {job_id}'}), 404
        return jsonify(job)

    @app.route('/api/jobs/<job_id>/cancel', methods=['POST'])
    def api_cancel_job(job_id):
        service = _job_service()
        try:
            job = service.cancel_job(job_id)
        except ValueError as exc:
            return jsonify({'error': str(exc)}), 400

        if job is None:
            return jsonify({'error': f'Job not found: {job_id}'}), 404
        return jsonify(job)

    @app.route('/api/jobs/<job_id>/apply', methods=['POST'])
    def api_apply_job(job_id):
        service = _job_service()
        try:
            job = service.apply_job(job_id)
        except ValueError as exc:
            return jsonify({'error': str(exc)}), 400

        if job is None:
            return jsonify({'error': f'Job not found: {job_id}'}), 404
        return jsonify(job)

    @app.route('/api/jobs/<job_id>/rollback', methods=['POST'])
    def api_rollback_job(job_id):
        service = _job_service()
        try:
            rollback_job = service.rollback_job(job_id)
        except ValueError as exc:
            return jsonify({'error': str(exc)}), 400

        if rollback_job is None:
            return jsonify({'error': f'Job not found: {job_id}'}), 404
        return jsonify(rollback_job)

    @app.route('/api/jobs/<job_id>/change-set', methods=['GET'])
    def api_get_job_change_set(job_id):
        service = _job_service()
        job = service.get_job(job_id)
        if job is None:
            return jsonify({'error': f'Job not found: {job_id}'}), 404

        change_set = service.get_job_change_set(job_id)
        if change_set is None:
            return jsonify({'error': f'Change set not found for job: {job_id}'}), 404
        return jsonify(change_set)

    @app.route('/api/jobs/<job_id>/logs', methods=['GET'])
    def api_get_job_logs(job_id):
        service = _job_service()
        job = service.get_job(job_id)
        if job is None:
            return jsonify({'error': f'Job not found: {job_id}'}), 404

        if not job.get('log_path'):
            return jsonify({'jobId': job_id, 'logPath': None, 'content': ''})

        log_path = Path(service.project_root) / job['log_path']
        if not log_path.exists():
            return jsonify({'jobId': job_id, 'logPath': job['log_path'], 'content': ''})

        with open(log_path, 'r', encoding='utf-8') as handle:
            content = handle.read()
        return jsonify({'jobId': job_id, 'logPath': job['log_path'], 'content': content})

    @app.route('/api/jobs/<job_id>/artifacts', methods=['GET'])
    def api_get_job_artifacts(job_id):
        service = _job_service()
        artifacts = service.get_job_artifacts(job_id)
        if artifacts is None:
            return jsonify({'error': f'Job not found: {job_id}'}), 404
        return jsonify(artifacts)

    @app.route('/api/jobs/<job_id>/review-table', methods=['GET'])
    def api_get_job_review_table(job_id):
        service = _job_service()
        try:
            review_table = service.get_job_review_table(job_id)
        except FileNotFoundError as exc:
            return jsonify({'error': str(exc)}), 404

        if review_table is None:
            return jsonify({'error': f'Job not found: {job_id}'}), 404
        return jsonify(review_table)

    @app.route('/api/jobs/<job_id>/review-table', methods=['POST'])
    def api_update_job_review_table(job_id):
        payload = request.get_json(silent=True) or {}
        file_name = payload.get('fileName')
        updates = payload.get('updates')
        expected_row_count = payload.get('expectedRowCount')
        if not isinstance(file_name, str) or not file_name:
            return jsonify({'error': 'fileName is required'}), 400
        if not isinstance(updates, list):
            return jsonify({'error': 'updates must be a list'}), 400
        if not isinstance(expected_row_count, int):
            return jsonify({'error': 'expectedRowCount must be an integer'}), 400

        service = _job_service()
        try:
            review_table = service.update_job_review_table(
                job_id,
                file_name=file_name,
                expected_row_count=expected_row_count,
                updates=updates,
            )
        except FileNotFoundError as exc:
            return jsonify({'error': str(exc)}), 404
        except ValueError as exc:
            return jsonify({'error': str(exc)}), 400

        if review_table is None:
            return jsonify({'error': f'Job not found: {job_id}'}), 404
        return jsonify(review_table)

    @app.route('/api/jobs/<job_id>/artifacts/<path:artifact_name>', methods=['GET'])
    def api_download_job_artifact(job_id, artifact_name):
        service = _job_service()
        try:
            resolved = service.resolve_job_artifact(job_id, artifact_name)
        except ValueError as exc:
            return jsonify({'error': str(exc)}), 400
        except FileNotFoundError:
            return jsonify({'error': f'Artifact not found: {artifact_name}'}), 404

        if resolved is None:
            return jsonify({'error': f'Job not found: {job_id}'}), 404

        _job, artifact_path = resolved
        return send_from_directory(artifact_path.parent, artifact_path.name, as_attachment=True)

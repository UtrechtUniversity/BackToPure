import logging
import os
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path
from uuid import uuid4

import requests
from flask import Response, current_app, jsonify, render_template, request, send_from_directory

from app.models import JobType, get_job_type_definition
from app.services import JobService
from config import FACULTY_PREFIX, RIC_BASE_URL, is_primary_organization_key


@dataclass(frozen=True)
class LegacyWorkflow:
    job_type: JobType
    page_route: str
    page_endpoint: str
    template_name: str
    run_route: str
    run_endpoint: str
    source_tokens: tuple[str, ...]
    cli_arg_spec: tuple[object, ...]
    form_defaults: dict[str, str] | None = None
    template_context: dict[str, str] | None = None
    failure_label: str | None = None


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


LEGACY_WORKFLOWS = (
    LegacyWorkflow(
        job_type=JobType.INTERNAL_PERSONS,
        page_route="/enrich_internal_persons_with_ids",
        page_endpoint="enrich_internal_persons",
        template_name="enrich_internal_persons.html",
        run_route="/run_enrich_internal_persons",
        run_endpoint="run_enrich_internal_persons",
        source_tokens=("enrich_internal_persons_with_ids",),
        cli_arg_spec=(("faculty_choice",),),
        failure_label="internal persons",
    ),
    LegacyWorkflow(
        job_type=JobType.EXTERNAL_PERSONS,
        page_route="/enrich_external_persons",
        page_endpoint="enrich_external_persons",
        template_name="enrich_external_persons.html",
        run_route="/run_enrich_external_persons",
        run_endpoint="run_enrich_pure_external_persons",
        source_tokens=("enrich_external_persons",),
        cli_arg_spec=(("faculty_choice",), "yes", ("use_openalex_fallback",)),
        form_defaults={"use_openalex_fallback": "no"},
        failure_label="external persons",
    ),
    LegacyWorkflow(
        job_type=JobType.EXTERNAL_ORGS,
        page_route="/enrich_external_orgs",
        page_endpoint="enrich_external_orgs",
        template_name="enrich_external_orgs.html",
        run_route="/run_enrich_pure_external_orgs",
        run_endpoint="run_enrich_pure_external_orgs",
        source_tokens=("enrich_external_orgs",),
        cli_arg_spec=(("faculty_choice",),),
        template_context={"feature": "Enrich External Organisations"},
        failure_label="external orgs",
    ),
    LegacyWorkflow(
        job_type=JobType.RESEARCH_OUTPUTS,
        page_route="/import_research_outputs",
        page_endpoint="import_research_outputs",
        template_name="import_research_outputs.html",
        run_route="/run_import_research_outputs",
        run_endpoint="run_import_research_outputs",
        source_tokens=("import_research_output", "import_research_outputs"),
        cli_arg_spec=(("faculty_choice",),),
        failure_label="research outputs",
    ),
    LegacyWorkflow(
        job_type=JobType.DATASETS,
        page_route="/import_datasets",
        page_endpoint="import_datasets",
        template_name="import_datasets.html",
        run_route="/run_import_datasets",
        run_endpoint="run_import_datasets",
        source_tokens=("import_datasets",),
        cli_arg_spec=(("faculty_choice",),),
        failure_label="datasets",
    ),
)


def _workflow_definition(workflow: LegacyWorkflow):
    return get_job_type_definition(workflow.job_type)


def _workflow_requirements(workflow: LegacyWorkflow):
    definition = _workflow_definition(workflow)
    requirements = {}
    if definition.required_csv:
        requirements["csv"] = list(definition.required_csv)
    if definition.required_csv_prefixes:
        requirements["csv_prefix"] = list(definition.required_csv_prefixes)
    if definition.required_json:
        requirements["json"] = list(definition.required_json)
    return requirements


def _resolve_legacy_workflow(source: str) -> LegacyWorkflow | None:
    for workflow in LEGACY_WORKFLOWS:
        if any(token in source for token in workflow.source_tokens):
            return workflow
    return None


def _resolve_output_target(source: str):
    workflow = _resolve_legacy_workflow(source)
    if workflow is None:
        return None, None
    return _workflow_definition(workflow).artifact_dir, _workflow_requirements(workflow)


def _resolve_runtime_output_target(source: str):
    artifact_dir, requirements = _resolve_output_target(source)
    if artifact_dir is None:
        return None, requirements
    return _runtime_root() / artifact_dir, requirements


def _has_named_files(directory_path: str, filenames):
    return any(os.path.isfile(os.path.join(directory_path, filename)) for filename in filenames)


def _has_prefixed_files(directory_path: str, prefixes, suffix):
    try:
        entries = os.listdir(directory_path)
    except FileNotFoundError:
        return False
    return any(
        entry.endswith(suffix) and any(entry.startswith(prefix) for prefix in prefixes)
        for entry in entries
    )


def _script_python():
    venv_python = os.path.join(os.getcwd(), '.venv', 'bin', 'python')
    if os.path.isfile(venv_python) and os.access(venv_python, os.X_OK):
        return venv_python
    return sys.executable


def _python_command(*args):
    return [_script_python(), *args]


def _script_path(script_path: str) -> Path:
    return Path(current_app.config.get("BTP_PROJECT_ROOT", Path(current_app.root_path).parent)) / script_path


def _stream_process(command, *, env=None, failure_label: str):
    try:
        process = subprocess.Popen(
            command,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
            env=env,
        )
    except FileNotFoundError as exc:
        yield f"Error: could not start Python interpreter: {exc}\n"
        return

    assert process.stdout is not None
    for line in iter(process.stdout.readline, ''):
        logging.debug(line.rstrip())
        yield line

    process.stdout.close()
    process.wait()
    if process.returncode != 0:
        yield f"Error: {failure_label} script exited with code {process.returncode}\n"


def _workflow_cli_args(workflow: LegacyWorkflow, form_data):
    args = []
    defaults = workflow.form_defaults or {}
    for item in workflow.cli_arg_spec:
        if isinstance(item, str):
            args.append(item)
            continue
        value = None
        for key in item:
            value = form_data.get(key)
            if value is not None:
                break
        if value is None:
            for key in item:
                if key in defaults:
                    value = defaults[key]
                    break
        args.append("" if value is None else value)
    return args


def _render_workflow_page(workflow: LegacyWorkflow, **context):
    page_context = dict(workflow.template_context or {})
    page_context.update(context)
    return render_template(workflow.template_name, **page_context)


def _legacy_page_view(workflow: LegacyWorkflow):
    def view():
        return _render_workflow_page(workflow)

    return view


def _legacy_run_view(workflow: LegacyWorkflow):
    def view():
        script_path = _script_path(_workflow_definition(workflow).script_path)
        if not script_path.exists():
            return _render_workflow_page(workflow, message=f"Script path does not exist: {script_path}")

        command = _python_command("-u", str(script_path), *_workflow_cli_args(workflow, request.form))
        return Response(
            _stream_process(command, failure_label=workflow.failure_label or workflow.page_endpoint),
            mimetype='text/plain',
        )

    return view

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

    @app.route('/')
    def index():
        return render_template('home.html')  # Ensure this points to your home page template

    @app.route('/faculties')

    def get_faculties():
        try:
            faculty_options = _fetch_faculties()
        except requests.exceptions.RequestException:
            # Return a JSON response indicating an error, with status code 500
            return jsonify({'error': 'Cannot connect to ricgraph'}), 500

        return jsonify(faculty_options)

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

    for workflow in LEGACY_WORKFLOWS:
        app.add_url_rule(
            workflow.page_route,
            endpoint=workflow.page_endpoint,
            view_func=_legacy_page_view(workflow),
        )
        app.add_url_rule(
            workflow.run_route,
            endpoint=workflow.run_endpoint,
            view_func=_legacy_run_view(workflow),
            methods=['POST'],
        )

    @app.route('/home')
    def home():
        return render_template('home.html')


    @app.route('/open_directory', methods=['POST'])

    def open_directory():
        # Adjust this path to your target directory
        referer = request.headers.get('Referer', 'unknown')
        logging.debug(f"open_directory called from referer: {referer}")
        # directory_path = 'output'
        # Step 2: Execute specific logic based on the Referer
        directory_path, _ = _resolve_runtime_output_target(referer)

        try:
            if not directory_path:
                return jsonify({'status': 'error', 'message': f'Unknown source page: {referer}'}), 400
            directory_path.mkdir(parents=True, exist_ok=True)
            # Open the directory using the appropriate command for each OS
            if os.name == 'nt':  # Windows
                subprocess.Popen(['explorer', str(directory_path)])
            elif os.name == 'posix':  # macOS and Linux
                # Use xdg-open for Linux systems
                subprocess.Popen(['xdg-open', str(directory_path)])
            else:
                return jsonify({'status': 'error', 'message': 'Unsupported OS'}), 500

            return jsonify({'status': 'success'}), 200
        except Exception as e:
            return jsonify({'status': 'error', 'message': str(e)}), 500

    @app.route('/update_status', methods=['GET'])
    def update_status():
        source = request.args.get('source', '')
        directory_path, required_files = _resolve_runtime_output_target(source)

        if not directory_path:
            return jsonify({'status': 'error', 'message': f'Unknown source: {source}'}), 400

        if not directory_path.exists():
            return jsonify({
                'status': 'success',
                'can_open': False,
                'can_apply': False,
            })

        if required_files:
            csv_ok = False
            json_ok = False
            if 'csv' in required_files:
                csv_ok = _has_named_files(str(directory_path), required_files['csv'])
            if 'csv_prefix' in required_files:
                csv_ok = csv_ok or _has_prefixed_files(str(directory_path), required_files['csv_prefix'], '.csv')
            if 'json' in required_files:
                json_ok = _has_named_files(str(directory_path), required_files['json'])
            return jsonify({
                'status': 'success',
                'can_open': csv_ok or json_ok,
                'can_apply': csv_ok and json_ok,
            })

        files_present = any(directory_path.iterdir())
        return jsonify({
            'status': 'success',
            'can_open': files_present,
            'can_apply': files_present,
        })

    @app.route('/run_apply_updates_to_pure', methods=['POST'])
    def run_apply_updates_to_pure():
        referer = request.headers.get('Referer', 'unknown')
        script_path = _script_path('src/apply_updates_to_pure.py')
        logging.debug(f"Checking if script exists at path: {script_path}")
        if not script_path.exists():
            logging.error(f"Script path does not exist: {script_path}")
            return jsonify({'status': 'error', 'message': f'Script path does not exist: {script_path}'}), 404

        env = os.environ.copy()
        env['REFERER_PAGE'] = referer
        output_dir, _ = _resolve_runtime_output_target(referer)
        if output_dir is not None:
            env['BTP_OUTPUT_DIR'] = str(output_dir)
        command = _python_command('-u', str(script_path))
        return Response(
            _stream_process(command, env=env, failure_label='apply updates'),
            mimetype='text/plain',
        )

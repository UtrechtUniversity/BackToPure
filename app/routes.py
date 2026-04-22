from flask import render_template, request, jsonify, Response, current_app, send_from_directory
import requests
import subprocess
import os
import logging
import sys
from pathlib import Path
from uuid import uuid4
from config import RIC_BASE_URL, FACULTY_PREFIX
from app.services import JobService
# Configure logging


def _job_service() -> JobService:
    db_path = current_app.extensions["btp_db"]["db_path"]
    project_root = Path(current_app.config.get("BTP_PROJECT_ROOT", Path(current_app.root_path).parent))
    return JobService(db_path, project_root=project_root)


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
    return [{'value': f['_key'], 'label': f['value']} for f in faculties]


def _resolve_output_target(source: str):
    if 'enrich_external_persons' in source:
        return "output/external_persons", {
            "csv": ["ext_pers_update.csv"],
            "json": ["to_be_updated.json"],
        }
    if 'enrich_internal_persons_with_ids' in source:
        return "output/internal_persons", {
            "csv_prefix": ["personstobeupdated_"],
            "json": ["datatotal.json"],
        }
    if 'enrich_external_orgs' in source:
        return "output/external_orgs", {
            "csv": ["external_orgs_to_update.csv"],
            "json": ["external_orgs_updates.json"],
        }
    if 'import_datasets' in source:
        return "output/datasets", {
            "csv": ["to_be_updated.csv"],
            "json": ["datasets_to_be_updated.json"],
        }
    if 'import_research_output' in source or 'import_research_outputs' in source:
        return "output/research_output", {
            "csv": ["to_be_updated.csv"],
            "json": ["output_to_be_updated.json"],
        }
    return None, None


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

    @app.route('/enrich_internal_persons_with_ids')
    def enrich_internal_persons():
        return render_template('enrich_internal_persons.html')

    @app.route('/run_enrich_internal_persons', methods=['POST'])
    def run_enrich_internal_persons():
        faculty_choice = request.form.get('faculty_choice')
        # test_choice = request.form.get('test_choice')

        script_path = os.path.join('src', 'enrich_internal_persons_with_ids.py')
        if not os.path.exists(script_path):
            return render_template('enrich_internal_persons.html', message=f"Script path does not exist: {script_path}")

        def generate():
            script_path = os.path.join('src', 'enrich_internal_persons_with_ids.py')
            if not os.path.exists(script_path):
                yield f"Script path does not exist: {script_path}\n"
                return

            try:
                process = subprocess.Popen(
                    _python_command('-u', script_path, faculty_choice),
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    text=True,
                    bufsize=1
                )
            except FileNotFoundError as exc:
                yield f"Error: could not start Python interpreter: {exc}\n"
                return

            for line in iter(process.stdout.readline, ''):
                yield line

            for line in iter(process.stderr.readline, ''):
                logging.error(line.strip())
                yield f"Error: {line}"

            process.stdout.close()
            process.stderr.close()
            process.wait()
            if process.returncode != 0:
                yield f"Error: external orgs script exited with code {process.returncode}\n"

        return Response(generate(), mimetype='text/plain')

    @app.route('/enrich_external_persons')
    def enrich_external_persons():
        return render_template('enrich_external_persons.html')

    @app.route('/run_enrich_external_persons', methods=['POST'])
    def run_enrich_pure_external_persons():
        faculty_choice = request.form.get('faculty_choice')
        use_openalex_fallback = request.form.get('use_openalex_fallback', 'yes')

        def generate():
            script_path = os.path.join('src', 'enrich_pure_external_persons.py')
            if not os.path.exists(script_path):
                yield f"Script path does not exist: {script_path}\n"
                return

            try:
                process = subprocess.Popen(
                    _python_command('-u', script_path, faculty_choice, 'yes', use_openalex_fallback),
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    text=True
                )
            except FileNotFoundError as exc:
                yield f"Error: could not start Python interpreter: {exc}\n"
                return

            for line in iter(process.stdout.readline, ''):
                yield line

            for line in iter(process.stderr.readline, ''):
                logging.error(line.strip())
                yield f"Error: {line}"

            process.stdout.close()
            process.stderr.close()
            process.wait()
            if process.returncode != 0:
                yield f"Error: external persons script exited with code {process.returncode}\n"

        return Response(generate(), mimetype='text/plain')

    @app.route('/enrich_external_orgs')
    def enrich_external_orgs():
        return render_template('enrich_external_orgs.html', feature='Enrich External Organisations')

    @app.route('/run_enrich_pure_external_orgs', methods=['POST'])
    def run_enrich_pure_external_orgs():
        faculty_choice = request.form.get('faculty_choice')


        def generate():
            script_path = os.path.join('src', 'enrich_pure_external_orgs.py')
            if not os.path.exists(script_path):
                yield f"Script path does not exist: {script_path}\n"
                return

            try:
                process = subprocess.Popen(
                    _python_command('-u', script_path, faculty_choice),
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    text=True,
                    bufsize=1
                )
            except FileNotFoundError as exc:
                yield f"Error: could not start Python interpreter: {exc}\n"
                return

            for line in iter(process.stdout.readline, ''):
                yield line

            for line in iter(process.stderr.readline, ''):
                logging.error(line.strip())
                yield f"Error: {line}"

            process.stdout.close()
            process.stderr.close()
            process.wait()
            if process.returncode != 0:
                yield f"Error: external orgs script exited with code {process.returncode}\n"

        return Response(generate(), mimetype='text/plain')

    @app.route('/import_research_outputs')
    def import_research_outputs():
        return render_template('import_research_outputs.html')

    @app.route('/run_import_research_outputs', methods=['POST'])
    def run_import_research_outputs():
        faculty_choice = request.form.get('faculty_choice')
        # test_choice = request.form.get('test_choice')

        def generate():
            script_path = os.path.join('src', 'update_researchoutput_from_ricgraph.py')
            if not os.path.exists(script_path):
                yield f"Script path does not exist: {script_path}\n"
                return

            try:
                process = subprocess.Popen(
                    _python_command('-u', script_path, faculty_choice),
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    text=True,
                    bufsize=1  # Enable line buffering
                )
            except FileNotFoundError as exc:
                yield f"Error: could not start Python interpreter: {exc}\n"
                return

            for line in iter(process.stdout.readline, ''):
                yield line

            for line in iter(process.stderr.readline, ''):
                logging.error(line.strip())
                yield f"Error: {line}"

            process.stdout.close()
            process.stderr.close()
            process.wait()

        return Response(generate(), mimetype='text/plain')
    @app.route('/import_datasets')
    def import_datasets():
        return render_template('import_datasets.html')

    @app.route('/run_import_datasets', methods=['POST'])
    def run_import_datasets():
        faculty_choice = request.form.get('faculty_choice')
        # test_choice = request.form.get('test_choice')
        script_path = os.path.join('src', 'update_datasets_from_ricgraph.py')

        if not os.path.exists(script_path):
            return render_template('import_datasets.html', message=f"Script path does not exist: {script_path}")

        def generate():
            try:
                process = subprocess.Popen(
                    _python_command(script_path, faculty_choice),
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    text=True
                )
            except FileNotFoundError as exc:
                yield f"Error: could not start Python interpreter: {exc}\n"
                return

            # Stream the stdout
            for line in iter(process.stdout.readline, ''):
                logging.debug(line.strip())
                yield line

            # Stream the stderr
            for line in iter(process.stderr.readline, ''):
                logging.error(line.strip())
                yield f"Error: {line}"

            process.stdout.close()
            process.stderr.close()
            process.wait()

        return Response(generate(), mimetype='text/plain')

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
        directory_path, _ = _resolve_output_target(referer)

        try:
            if not directory_path:
                return jsonify({'status': 'error', 'message': f'Unknown source page: {referer}'}), 400
            os.makedirs(directory_path, exist_ok=True)
            # Open the directory using the appropriate command for each OS
            if os.name == 'nt':  # Windows
                subprocess.Popen(['explorer', directory_path])
            elif os.name == 'posix':  # macOS and Linux
                # Use xdg-open for Linux systems
                subprocess.Popen(['xdg-open', directory_path])
            else:
                return jsonify({'status': 'error', 'message': 'Unsupported OS'}), 500

            return jsonify({'status': 'success'}), 200
        except Exception as e:
            return jsonify({'status': 'error', 'message': str(e)}), 500

    @app.route('/update_status', methods=['GET'])
    def update_status():
        source = request.args.get('source', '')
        directory_path, required_files = _resolve_output_target(source)

        if not directory_path:
            return jsonify({'status': 'error', 'message': f'Unknown source: {source}'}), 400

        if not os.path.exists(directory_path):
            return jsonify({
                'status': 'success',
                'can_open': False,
                'can_apply': False,
            })

        if required_files:
            csv_ok = False
            json_ok = False
            if 'csv' in required_files:
                csv_ok = _has_named_files(directory_path, required_files['csv'])
            if 'csv_prefix' in required_files:
                csv_ok = csv_ok or _has_prefixed_files(directory_path, required_files['csv_prefix'], '.csv')
            if 'json' in required_files:
                json_ok = _has_named_files(directory_path, required_files['json'])
            return jsonify({
                'status': 'success',
                'can_open': csv_ok or json_ok,
                'can_apply': csv_ok and json_ok,
            })

        files_present = any(os.scandir(directory_path))
        return jsonify({
            'status': 'success',
            'can_open': files_present,
            'can_apply': files_present,
        })

    @app.route('/run_apply_updates_to_pure', methods=['POST'])
    def run_apply_updates_to_pure():

        referer = request.headers.get('Referer', 'unknown')
        script_path = os.path.join('src', 'apply_updates_to_pure.py')

        # Step 1: Check if script path exists and log
        logging.debug(f"Checking if script exists at path: {script_path}")
        if not os.path.exists(script_path):
            logging.error(f"Script path does not exist: {script_path}")
            return jsonify({'status': 'error', 'message': f'Script path does not exist: {script_path}'}), 404

        def generate():
            logging.debug("Script has started running...\n")
            logging.debug("Script execution has started...")

            try:
                # Step 2: Attempt to start the subprocess
                env = os.environ.copy()
                env['REFERER_PAGE'] = referer

                process = subprocess.Popen(
                    _python_command('-u', script_path),  # '-u' for unbuffered output
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    text=True,
                    env=env  # Pass the environment variables
                )
                logging.debug(f"Subprocess started with PID: {process.pid}")

                # Step 3: Stream stdout
                for line in iter(process.stdout.readline, ''):
                    logging.debug(f"stdout: {line.strip()}")
                    yield line

                # Step 4: Stream stderr
                for line in iter(process.stderr.readline, ''):
                    logging.error(f"stderr: {line.strip()}")
                    yield f"Error: {line}"

                # Step 5: Close streams and check return code
                process.stdout.close()
                process.stderr.close()
                return_code = process.wait()
                logging.debug(f"Process finished with return code: {return_code}")

                if return_code != 0:
                    logging.debug(f"Script finished with errors. Return code: {return_code}\n")
                else:
                    logging.debug("Script finished successfully.\n")

            except FileNotFoundError as fnf_error:
                logging.error(f"FileNotFoundError: {fnf_error}")
                yield "Error: Script file not found.\n"
            except Exception as e:
                logging.error(f"Exception occurred: {e}")
                yield f"Error: {str(e)}\n"

        return Response(generate(), mimetype='text/plain')

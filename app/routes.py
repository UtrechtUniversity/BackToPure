from flask import render_template, request, jsonify, Response
import requests
import subprocess
import os
import logging
from config import RIC_BASE_URL, FACULTY_PREFIX
# Configure logging

def init_app(app):
    @app.route('/')
    def index():
        return render_template('home.html')  # Ensure this points to your home page template

    @app.route('/faculties')

    def get_faculties():
        params = {'value': FACULTY_PREFIX}
        url = RIC_BASE_URL + 'organization/search'
        try:
            response = requests.get(url, params=params)
            response.raise_for_status()  # Raise an error for bad responses (4xx, 5xx)
        except requests.exceptions.RequestException:
            # Return a JSON response indicating an error, with status code 500
            return jsonify({'error': 'Cannot connect to ricgraph'}), 500

        data = response.json()
        faculties = data.get("results", [])
        faculty_options = [{'value': f['_key'], 'label': f['value']} for f in faculties]
        return jsonify(faculty_options)

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

            process = subprocess.Popen(
                ['python', script_path, faculty_choice],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True
            )

            for line in iter(process.stdout.readline, ''):
                yield line

            for line in iter(process.stderr.readline, ''):
                yield f"Error: {line}"

            process.stdout.close()
            process.stderr.close()
            process.wait()

        return Response(generate(), mimetype='text/plain')

    @app.route('/enrich_external_persons')
    def enrich_external_persons():
        return render_template('enrich_external_persons.html')

    @app.route('/run_enrich_external_persons', methods=['POST'])
    def run_enrich_pure_external_persons():
        faculty_choice = request.form.get('faculty_choice')

        def generate():
            script_path = os.path.join('src', 'enrich_pure_external_persons.py')
            if not os.path.exists(script_path):
                yield f"Script path does not exist: {script_path}\n"
                return

            process = subprocess.Popen(
                ['python', script_path, faculty_choice],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True
            )

            for line in iter(process.stdout.readline, ''):
                yield line

            # for line in iter(process.stderr.readline, ''):
            #     yield f"Error: {line}"

            process.stdout.close()
            process.stderr.close()
            process.wait()

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

            process = subprocess.Popen(
                ['python', script_path, faculty_choice],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True
            )

            for line in iter(process.stdout.readline, ''):
                yield line

            for line in iter(process.stderr.readline, ''):
                yield f"Error: {line}"

            process.stdout.close()
            process.stderr.close()
            process.wait()

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

            process = subprocess.Popen(
                ['python', script_path, faculty_choice],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                bufsize=1  # Enable line buffering
            )

            for line in iter(process.stdout.readline, ''):
                yield line

            for line in iter(process.stderr.readline, ''):
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
            process = subprocess.Popen(
                ['python', script_path, faculty_choice],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True
            )

            # Stream the stdout
            for line in iter(process.stdout.readline, ''):
                print(line, flush=True)  # Ensure output is flushed immediately
                yield line

            # Stream the stderr
            for line in iter(process.stderr.readline, ''):
                print(f"Error: {line}", flush=True)  # Ensure output is flushed immediately
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
        print('test', referer)
        # directory_path = 'output'
        # Step 2: Execute specific logic based on the Referer
        if 'enrich_external_persons' in referer:
            directory_path = "output/external_persons"
        elif 'enrich_internal_persons_with_ids' in referer:
            directory_path = "output/internal_persons"
        elif 'enrich_external_orgs' in referer:
            directory_path = "output/external_orgs"
        elif 'import_datasets' in referer:
            directory_path = "output/datasets"
        elif 'import_research_output' in referer:
            directory_path = "output/research_output"

        try:
            if not os.path.exists(directory_path):
                return jsonify({'status': 'error', 'message': 'Directory does not exist'}), 404
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
                    ['python', '-u', script_path],  # '-u' for unbuffered output
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


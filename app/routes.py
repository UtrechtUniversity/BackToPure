from flask import render_template, request, jsonify, Response
import requests
import subprocess
import os
from config import RIC_BASE_URL, FACULTY_PREFIX

def init_app(app):
    @app.route('/')
    def index():
        return render_template('home.html')  # Ensure this points to your home page template

    @app.route('/faculties')
    def get_faculties():
        params = {'value': FACULTY_PREFIX}
        url = RIC_BASE_URL + 'organization/search'
        response = requests.get(url, params=params)
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
        test_choice = request.form.get('test_choice')

        script_path = os.path.join('src', 'enrich_internal_persons_with_ids.py')
        if not os.path.exists(script_path):
            return render_template('enrich_internal_persons.html', message=f"Script path does not exist: {script_path}")

        def generate():
            process = subprocess.Popen(
                ['python', script_path, faculty_choice, test_choice],
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

    @app.route('/run_enrich_pure_external_persons', methods=['POST'])
    def run_enrich_pure_external_persons():
        faculty_choice = request.form.get('faculty_choice')
        test_choice = request.form.get('test_choice')

        def generate():
            script_path = os.path.join('src', 'enrich_pure_external_persons.py')
            if not os.path.exists(script_path):
                yield f"Script path does not exist: {script_path}\n"
                return

            process = subprocess.Popen(
                ['python', script_path, faculty_choice, test_choice],
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

    @app.route('/enrich_external_orgs')
    def enrich_external_orgs():
        return render_template('coming_soon.html', feature='Enrich External Organisations')

    @app.route('/import_research_outputs')
    def import_research_outputs():
        return render_template('import_research_outputs.html')

    @app.route('/run_import_research_outputs', methods=['POST'])
    def run_import_research_outputs():
        faculty_choice = request.form.get('faculty_choice')
        test_choice = request.form.get('test_choice')

        def generate():
            script_path = os.path.join('src', 'update_researchoutput_from_ricgraph.py')
            if not os.path.exists(script_path):
                yield f"Script path does not exist: {script_path}\n"
                return

            process = subprocess.Popen(
                ['python', script_path, faculty_choice, test_choice],
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
    @app.route('/import_datasets')
    def import_datasets():
        return render_template('import_datasets.html')

    @app.route('/run_import_datasets', methods=['POST'])
    def run_import_datasets():
        faculty_choice = request.form.get('faculty_choice')
        test_choice = request.form.get('test_choice')
        script_path = os.path.join('src', 'update_datasets_from_ricgraph.py')

        if not os.path.exists(script_path):
            return render_template('import_datasets.html', message=f"Script path does not exist: {script_path}")

        def generate():
            process = subprocess.Popen(
                ['python', script_path, faculty_choice, test_choice],
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

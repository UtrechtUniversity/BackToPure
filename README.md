# BackToPure

## Overview
BackToPure is a Flask-based web application that simplifies the process of enriching and updating research-related records in Pure using external data sources such as Ricgraph and OpenAlex. This tool provides an interactive interface to handle the enrichment of internal and external person records, external organizations, research outputs, and datasets.

The application orchestrates a series of Python scripts to fetch, process, and upload data seamlessly via a user-friendly interface.

---

## Key Features
- **Home Dashboard:** Central hub for navigating different operations.
- **Enrich Internal Persons:** Enriches internal person profiles with identifiers such as ORCID.
- **Enrich External Persons:** Matches and updates external researchers with ORCID and OpenAlex IDs.
- **Enrich External Organizations:** Enriches external organizations in Pure with ROR IDs.
- **Import Research Outputs:** Imports research outputs from Ricgraph into Pure.
- **Import Datasets:** Imports datasets from Ricgraph into Pure.
- **Apply Updates:** Processes and uploads the updates to the Pure system.
- **Directory Access:** Opens directories with generated output files for review.

---

## Prerequisites

### System Requirements
- **OS:** Windows, macOS, or Linux
- **Python:** Version 3.10 or higher
- **Node.js:** Version 20 or higher for the React frontend
- **Virtual Environment:** Recommended to use `venv`

### Dependencies
To install the backend runtime packages, run:

```bash
pip install -r requirements.txt
```

For backend development and tests, use:

```bash
pip install -r requirements-dev.txt
```

For the frontend, install packages from `frontend/`:

```bash
cd frontend
npm ci
```

### Access to Ricgraph
To use BackToPure, **access to Ricgraph** is mandatory. Ricgraph is a data storage and query system used to manage research-related data and link it to external systems. The application fetches data about faculties, researchers, and outputs directly from Ricgraph's API. Ensure that you have API access for querying Ricgraph.

---

## Project Structure

```
BackToPure/
├── BackToPure.py (Flask app entry point)
├── app/ (Flask app package, routes, services, templates, static assets)
├── src/ (workflow scripts and Pure/Ricgraph/OpenAlex helpers)
│   ├── enrich_internal_persons_with_ids.py
│   ├── enrich_pure_external_persons.py
│   ├── enrich_pure_external_orgs.py
│   ├── update_researchoutput_from_ricgraph.py
│   ├── update_datasets_from_ricgraph.py
│   ├── apply_updates_to_pure.py
│   ├── config.py
│   └── config.example.ini
├── frontend/ (React/Vite UI)
├── tests/ (backend tests)
├── docs/ (plans and implementation notes)
├── requirements.txt (backend runtime dependencies)
└── requirements-dev.txt (backend test dependencies)
```

---

## Installation Guide

### 1. Clone the Repository
```bash
git clone https://github.com/username/BackToPure.git
cd BackToPure
```

### 2. Set Up a Virtual Environment
```bash
python -m venv .venv
source .venv/bin/activate  # Linux/macOS
.venv\Scripts\activate  # Windows
```

### 3. Install Dependencies
```bash
pip install -e ".[dev]"
cd frontend
npm ci
cd ..
```

If you prefer requirements files, this still works:

```bash
pip install -r requirements-dev.txt
```

### 4. Configure the Application
Create a private local config file from the example:

```bash
cp src/config.example.ini src/config.ini
```

Then edit `src/config.ini` with your Pure, Ricgraph, and OpenAlex settings. This file is intentionally ignored by git because it contains local URLs and API keys.

You can also point the app at a private config file elsewhere:

```bash
export BTP_CONFIG_PATH=/path/to/config.ini
```

For production, set a private Flask secret:

```bash
export BTP_SECRET_KEY='replace-with-a-long-random-value'
```

---

## Usage Guide

### 1. Start the Flask Application Locally
```bash
python3 BackToPure.py
```
The backend runs on `http://127.0.0.1:5002` by default.

You can override the local host, port, and debug mode:

```bash
BTP_HOST=0.0.0.0 BTP_PORT=5002 FLASK_DEBUG=1 python3 BackToPure.py
```

The same app can also be started with Flask's CLI:

```bash
flask --app BackToPure run --port 5002
```

### 2. Navigate to the Dashboard
- Legacy Flask UI: open `http://127.0.0.1:5002`
- React UI: build the frontend first, then open `http://127.0.0.1:5002/app`

```bash
cd frontend
npm run build
cd ..
python3 BackToPure.py
```

### 3. Enrich and Update Records
- **Internal Persons:** Select the "Enrich Internal Persons" option and choose the desired faculty.
- **External Persons:** Choose "Enrich External Persons" to update records with OpenAlex and ORCID IDs.
- **External Organizations:** Enrich organizations with missing ROR IDs.
- **Import Research Outputs/Datasets:** Import data from Ricgraph into Pure.

### 4. Review and Apply Updates
- After each import or enrichment, access the relevant output files.
- Modify the CSV files to remove unwanted updates.
- Click "Apply Updates" to send changes to Pure.

---

## Tests

Run backend tests from the repository root:

```bash
python3 -m pytest -q
```

Run Python formatting and lint checks:

```bash
python3 -m ruff check --select I,F app tests BackToPure.py
```

Run frontend tests from `frontend/`:

```bash
cd frontend
npm test
```

Build the frontend:

```bash
cd frontend
npm run build
```

The CI workflow runs:

- backend editable install with `.[dev]`
- `ruff check --select I,F app tests BackToPure.py`
- backend pytest
- frontend `npm test`
- frontend `npm run build`

Generated outputs such as `data/`, `logs/`, `output/`, and `frontend/dist/` are ignored by git.

For local development, the editable install keeps the `app/` package and the flat workflow modules from `src/` importable without manually setting `PYTHONPATH`.

---

## Troubleshooting
- **Error: "configuration file ... does not exist"**: Copy `src/config.example.ini` to `src/config.ini` or set `BTP_CONFIG_PATH`.
- **Error: "Script path does not exist"**: Ensure all scripts are located in the `src/` directory and start the backend from the repository root.
- **React app returns "Frontend build not found"**: Run `npm run build` inside `frontend/`.
- **Connection Issues:** Check if Ricgraph and Pure APIs are accessible.
- **Permission Denied:** Run the application with elevated permissions if required.

---

## Security and Licensing
- **License:** MIT License (see below).
- **Author:** David Grote Beverborg

```text
MIT License

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
```

---

## Contribution
Feel free to open issues and submit pull requests to improve the project.

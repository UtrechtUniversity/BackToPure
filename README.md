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
- **Python:** Version 3.7 or higher
- **Virtual Environment:** Recommended to use `venv`

### Dependencies
To install the required Python packages, run:

```bash
pip install -r requirements.txt
```

Ensure the following dependencies are listed in `requirements.txt`:
- Flask
- pandas
- requests
- configparser
- logging
- tenacity (for retry logic)
- urllib3

### Access to Ricgraph
To use BackToPure, **access to Ricgraph** is mandatory. Ricgraph is a data storage and query system used to manage research-related data and link it to external systems. The application fetches data about faculties, researchers, and outputs directly from Ricgraph's API. Ensure that you have API access for querying Ricgraph.

---

## Project Structure

```
BackToPure/
├── app.py (creates Flask app)
├── BackToPure.py (main entry point)
├── src/ (contains scripts for enrichment and updates)
│   ├── enrich_internal_persons_with_ids.py
│   ├── enrich_pure_external_persons.py
│   ├── enrich_pure_external_orgs.py
│   ├── update_researchoutput_from_ricgraph.py
│   ├── update_datasets_from_ricgraph.py
│   └── apply_updates_to_pure.py
├── templates/ (HTML templates for pages)
├── static/ (static files such as CSS and JavaScript)
├── config.py (configuration variables)
├── routes.py (Flask routes)
└── requirements.txt (list of dependencies)
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
pip install -r requirements.txt
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

### 1. Start the Flask Application
```bash
python BackToPure.py
```
The application will run on `http://localhost:5001`.

### 2. Navigate to the Dashboard
- Open your web browser and go to `http://localhost:5001`

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

## Troubleshooting
- **Error: "Script path does not exist"**: Ensure all scripts are located in the `src/` directory.
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

# ########################################################################
# Script: BackToPure.py
#
# Description:
# This script is the entry point for the BackToPure Flask web application,
# which provides a user-friendly interface for interacting with research
# data in Pure and Ricgraph. The application enables the enrichment and
# updating of internal and external records in Pure using external data sources
# such as OpenAlex.
#
# Web Application Features:
# --------------------------
# 1. **Home Page (`/`)**:
#    - Displays the home page template.
#
# 2. **List Faculties (`/faculties`)**:
#    - Fetches and returns a list of faculties from Ricgraph in JSON format.
#
# 3. **Enrich Internal Persons with IDs (`/enrich_internal_persons`)**:
#    - Displays the page for enriching internal persons' records.
#    - Subprocess: Runs `enrich_internal_persons_with_ids.py` to enrich internal person profiles with identifiers.
#
# 4. **Enrich External Persons (`/enrich_external_persons`)**:
#    - Displays the page for enriching external persons.
#    - Subprocess: Runs `enrich_pure_external_persons.py` to update external person records with additional identifiers.
#
# 5. **Enrich External Organizations (`/enrich_external_orgs`)**:
#    - Displays the page for enriching external organizations.
#    - Subprocess: Runs `enrich_pure_external_orgs.py` to update external organizations with ROR IDs.
#
# 6. **Import Research Outputs (`/import_research_outputs`)**:
#    - Displays the page for importing research outputs.
#    - Subprocess: Runs `update_researchoutput_from_ricgraph.py` to import research outputs from Ricgraph into Pure.
#
# 7. **Import Datasets (`/import_datasets`)**:
#    - Displays the page for importing datasets.
#    - Subprocess: Runs `update_datasets_from_ricgraph.py` to import datasets from Ricgraph into Pure.
#
# 8. **Open Output Directory (`/open_directory`)**:
#    - Opens the relevant output directory based on the type of data being processed.
#
# 9. **Apply Updates to Pure (`/run_apply_updates_to_pure`)**:
#    - Runs `apply_updates_to_pure.py` to apply updates for research outputs, datasets, or persons to the Pure system.
#
# Key Notes:
# -----------
# - The application streams real-time logs from subprocesses directly to the web interface.
# - It supports test mode to simulate actions without making changes in Pure.
# - The application requires the presence of various Python scripts in the `src` directory.
#
# Dependencies:
# --------------
# - Flask, requests, subprocess, logging, config, etc.
#
# Author: David Grote Beverborg
# Created: 2024
#
# License:
# MIT License
#
# Copyright (c) 2024 David Grote Beverborg
# ########################################################################


import sys
import os

# Add the src directory to the Python path
sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))

from app import create_app

app = create_app()

if __name__ == '__main__':
    debug = os.environ.get("FLASK_DEBUG", "0").lower() in {"1", "true", "yes", "on"}
    host = os.environ.get("BTP_HOST", "127.0.0.1")
    port = int(os.environ.get("BTP_PORT", "5002"))
    app.run(host=host, port=port, debug=debug)

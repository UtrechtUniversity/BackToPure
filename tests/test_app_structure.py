import configparser
import json
import os
import signal
import sqlite3
import subprocess
import tempfile
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd

import apply_updates_to_pure
import config as btp_config
import doctor
import pure_datasets
import pure_researchoutputs
from app import create_app
from app.db import connect_db, init_db
from app.models import (
    JOB_TYPE_REGISTRY,
    JobStatus,
    JobType,
    get_job_type_definition,
    is_valid_transition,
)
from app.services import JobService
from config import FACULTY_PREFIX, OPENALEXEX_ID_URI, ORCID_ID_URI, PURE_BASE_URL, RIC_BASE_URL, ROR_ID_URI


class FakeCompletedProcess:
    def __init__(self, returncode=0, stdout="", stderr=""):
        self.returncode = returncode
        self._stdout = stdout
        self._stderr = stderr

    def communicate(self):
        return self._stdout, self._stderr


class FakeReadableStream:
    def __init__(self, lines):
        self._lines = list(lines)

    def readline(self):
        if self._lines:
            return self._lines.pop(0)
        return ""


class FakeStreamingProcess:
    def __init__(self, stdout_lines=None, stderr="", returncode=0):
        self.stdout = FakeReadableStream(stdout_lines or [])
        self.returncode = returncode
        self._stderr = stderr

    def communicate(self):
        return "", self._stderr


class AppStructureTests(unittest.TestCase):
    def test_workflow_code_does_not_hardcode_local_ricgraph_api(self):
        project_root = Path(__file__).resolve().parents[1]
        forbidden = "127.0.0.1:3030/api"
        offenders = []

        for base in (project_root / "src", project_root / "app"):
            for path in base.rglob("*.py"):
                if "__pycache__" in path.parts:
                    continue
                text = path.read_text(encoding="utf-8")
                if forbidden in text:
                    offenders.append(str(path.relative_to(project_root)))

        self.assertEqual([], offenders)

    def test_primary_organization_filter_uses_configured_prefixes(self):
        with patch.object(btp_config, "PRIMARY_ORGANIZATION_PREFIXES", ("school:",)), patch.object(
            btp_config, "EXCLUDED_ORGANIZATION_PREFIXES", ("school research:", "archive:")
        ):
            self.assertTrue(btp_config.is_primary_organization_key("school: medicine|organization_name"))
            self.assertFalse(
                btp_config.is_primary_organization_key("school research: medicine|organization_name")
            )
            self.assertFalse(btp_config.is_primary_organization_key("department: medicine|organization_name"))
            self.assertTrue(btp_config.is_excluded_organization_key("archive: old|organization_name"))

    def test_pure_uri_config_validation_reports_placeholders(self):
        parser = configparser.ConfigParser()
        parser.read_dict(
            {
                "ID_URI": {
                    "OPENALEX": "/dk/atira/pure/person/personsources/open_alex_id",
                    "OPENALEXEX": "replace-with-external-openalex-source",
                    "ORCIDEXT": "/dk/atira/pure/externalperson/externalpersonsources/orcid",
                    "ROR_ID_URI": "/dk/atira/pure/ueoexternalorganisation/ueoexternalorganisationsources/ror_id",
                },
                "URI": {
                    "contributor": "/dk/atira/pure/dataset/roles/dataset/contributor",
                    "creator": "/dk/atira/pure/dataset/roles/dataset/creator",
                    "type_dataset": "/dk/atira/pure/dataset/datasettypes/dataset/dataset",
                    "supervisor": "/dk/atira/pure/researchoutput/roles/internalexternal/thesis/supervisor",
                    "cosupervisor": "/dk/atira/pure/researchoutput/roles/internalexternal/thesis/cosupervisor",
                },
                "DEFAULTS": {
                    "publisher": "replace-with-default-publisher-uuid",
                    "university": "organization-uuid",
                    "visibility_key": "FREE",
                    "workflow_step": "forApproval",
                    "language_uri": "/dk/atira/pure/core/languages/und",
                },
            }
        )

        issues = btp_config.validate_pure_uri_config(parser)

        self.assertIn("Placeholder or empty value for external person OpenAlex source URI", issues[0])
        self.assertTrue(any("[DEFAULTS] publisher" in issue for issue in issues))

    def test_doctor_skip_network_runs_local_checks(self):
        results = doctor.run_environment_checks(skip_network=True)

        self.assertIn("imports", [result.name for result in results])
        self.assertNotIn("Pure API", [result.name for result in results])
        self.assertTrue(all(result.ok or not result.required for result in results))

    def test_create_app_reads_runtime_paths_from_environment(self):
        with tempfile.TemporaryDirectory() as tmpdir, patch.dict(
            os.environ,
            {
                "BTP_RUNTIME_ROOT": os.path.join(tmpdir, "runtime"),
                "BTP_DATA_DIR": os.path.join(tmpdir, "state", "data"),
                "BTP_LOGS_DIR": os.path.join(tmpdir, "state", "logs", "jobs"),
                "BTP_FRONTEND_DIST": os.path.join(tmpdir, "frontend-build"),
            },
            clear=False,
        ):
            app = create_app()

        self.assertEqual(os.path.join(tmpdir, "runtime"), app.config["BTP_RUNTIME_ROOT"])
        self.assertEqual(os.path.join(tmpdir, "state", "data"), app.config["BTP_DATA_DIR"])
        self.assertEqual(os.path.join(tmpdir, "state", "logs", "jobs"), app.config["BTP_LOGS_DIR"])
        self.assertEqual(os.path.join(tmpdir, "frontend-build"), app.config["BTP_FRONTEND_DIST"])

    def test_create_app_initializes_backend_storage_extension(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            app = create_app()
            app.config["BTP_DATA_DIR"] = tmpdir

            init_db(app)

            self.assertIn("btp_db", app.extensions)
            self.assertEqual(tmpdir, str(app.extensions["btp_db"]["data_dir"]))
            self.assertEqual(os.path.join(tmpdir, "jobs.sqlite"), str(app.extensions["btp_db"]["db_path"]))
            self.assertTrue(os.path.exists(os.path.join(tmpdir, "jobs.sqlite")))

            with sqlite3.connect(os.path.join(tmpdir, "jobs.sqlite")) as connection:
                table = connection.execute(
                    "SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'jobs'"
                ).fetchone()

            self.assertEqual(("jobs",), table)

    def test_job_service_includes_source_config_summary(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            app = create_app()
            app.config["BTP_DATA_DIR"] = tmpdir
            init_db(app)

            service = JobService(app.extensions["btp_db"]["db_path"])
            created = service.create_job(
                job_id="job-source-config",
                job_type=JobType.INTERNAL_PERSONS.value,
                status=JobStatus.QUEUED,
                params={"facultyChoice": "uu faculty: faculteit test|organization_name"},
                created_at="2026-04-20T13:00:00Z",
            )

            self.assertEqual(PURE_BASE_URL, created["sourceConfig"]["pureBaseUrl"])
            self.assertEqual(RIC_BASE_URL, created["sourceConfig"]["ricgraphBaseUrl"])
            self.assertEqual(FACULTY_PREFIX, created["sourceConfig"]["facultyPrefix"])
            self.assertEqual(
                "uu faculty: faculteit test|organization_name",
                created["sourceConfig"]["facultyChoice"],
            )

    def test_job_model_enums_expose_phase_one_types_and_statuses(self):
        self.assertEqual("internal_persons", JobType.INTERNAL_PERSONS.value)
        self.assertEqual("applying", JobStatus.APPLYING.value)
        self.assertEqual("completed", JobStatus.COMPLETED.value)

    def test_job_registry_exposes_expected_script_and_artifact_rules(self):
        definition = get_job_type_definition(JobType.INTERNAL_PERSONS)

        self.assertEqual("src/enrich_internal_persons_with_ids.py", definition.script_path)
        self.assertEqual("output/internal_persons", definition.artifact_dir)
        self.assertEqual(("personstobeupdated_",), definition.required_csv_prefixes)
        self.assertEqual(("datatotal.json",), definition.required_json)
        self.assertEqual(("faculty_choice", "facultyChoice"), definition.allowed_params)
        self.assertEqual((("faculty_choice", "facultyChoice"),), definition.cli_param_aliases)
        self.assertEqual(set(JobType), set(JOB_TYPE_REGISTRY))

    def test_status_transition_rules_match_phase_one_state_machine(self):
        self.assertTrue(is_valid_transition(JobStatus.QUEUED, JobStatus.RUNNING))
        self.assertTrue(is_valid_transition(JobStatus.RUNNING, JobStatus.NEEDS_REVIEW))
        self.assertTrue(is_valid_transition(JobStatus.NEEDS_REVIEW, JobStatus.APPLYING))
        self.assertTrue(is_valid_transition(JobStatus.APPLYING, JobStatus.COMPLETED))
        self.assertFalse(is_valid_transition(JobStatus.COMPLETED, JobStatus.RUNNING))
        self.assertFalse(is_valid_transition(JobStatus.FAILED, JobStatus.COMPLETED))

    def test_job_service_healthcheck_reports_db_path(self):
        service = JobService("/tmp/jobs.sqlite", runtime_root="/tmp/runtime")

        result = service.healthcheck()

        self.assertEqual("ok", result["status"])
        self.assertEqual("/tmp/jobs.sqlite", result["db_path"])
        self.assertEqual("/tmp/runtime", result["runtime_root"])
        self.assertIn("logs_dir", result)

    def test_job_service_can_create_fetch_list_and_update_jobs(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            app = create_app()
            app.config["BTP_DATA_DIR"] = tmpdir
            init_db(app)

            service = JobService(app.extensions["btp_db"]["db_path"])
            created = service.create_job(
                job_id="job-001",
                job_type=JobType.INTERNAL_PERSONS.value,
                status=JobStatus.QUEUED,
                params={"facultyChoice": "all"},
                created_at="2026-04-20T13:00:00Z",
            )

            fetched = service.get_job("job-001")
            updated = service.update_job(
                "job-001",
                status=JobStatus.RUNNING,
                started_at="2026-04-20T13:01:00Z",
                log_path="logs/jobs/job-001.log",
            )
            listed = service.list_jobs()

            self.assertEqual("job-001", created["id"])
            self.assertEqual({"facultyChoice": "all"}, fetched["params"])
            self.assertEqual("running", updated["status"])
            self.assertEqual("logs/jobs/job-001.log", updated["log_path"])
            self.assertEqual(["job-001"], [job["id"] for job in listed])
            self.assertEqual("output/internal_persons/job-001", created["artifact_dir"])
            self.assertFalse(created["canOpen"])
            self.assertFalse(created["canApply"])

    def test_job_service_uses_job_scoped_artifact_dir_for_external_persons(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            app = create_app()
            app.config["BTP_DATA_DIR"] = tmpdir
            init_db(app)

            service = JobService(app.extensions["btp_db"]["db_path"])
            created = service.create_job(
                job_id="job-002",
                job_type=JobType.EXTERNAL_PERSONS.value,
                status=JobStatus.QUEUED,
                params={"facultyChoice": "all", "useOpenAlexFallback": "yes"},
                created_at="2026-04-20T13:00:00Z",
            )

            self.assertEqual("output/external_persons/job-002", created["artifact_dir"])
            self.assertFalse(created["canOpen"])
            self.assertFalse(created["canApply"])

    def test_job_service_rejects_faculty_research_organisation_scope(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            app = create_app()
            app.config["BTP_DATA_DIR"] = tmpdir
            init_db(app)

            service = JobService(app.extensions["btp_db"]["db_path"])

            with self.assertRaisesRegex(ValueError, "Excluded organisations are not supported"):
                service.create_job(
                    job_id="job-research-scope",
                    job_type=JobType.EXTERNAL_PERSONS.value,
                    status=JobStatus.QUEUED,
                    params={"facultyChoice": "uu faculty research: faculteit test|organization_name"},
                    created_at="2026-04-20T13:00:00Z",
                )

    def test_job_service_uses_job_scoped_artifact_dir_for_research_outputs(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            app = create_app()
            app.config["BTP_DATA_DIR"] = tmpdir
            init_db(app)

            service = JobService(app.extensions["btp_db"]["db_path"])
            created = service.create_job(
                job_id="job-003",
                job_type=JobType.RESEARCH_OUTPUTS.value,
                status=JobStatus.QUEUED,
                params={"facultyChoice": "all"},
                created_at="2026-04-20T13:00:00Z",
            )

            self.assertEqual("output/research_output/job-003", created["artifact_dir"])
            self.assertFalse(created["canOpen"])
            self.assertFalse(created["canApply"])

    def test_job_service_uses_job_scoped_artifact_dir_for_datasets(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            app = create_app()
            app.config["BTP_DATA_DIR"] = tmpdir
            init_db(app)

            service = JobService(app.extensions["btp_db"]["db_path"])
            created = service.create_job(
                job_id="job-004",
                job_type=JobType.DATASETS.value,
                status=JobStatus.QUEUED,
                params={"facultyChoice": "all"},
                created_at="2026-04-20T13:00:00Z",
            )

            self.assertEqual("output/datasets/job-004", created["artifact_dir"])
            self.assertFalse(created["canOpen"])
            self.assertFalse(created["canApply"])

    def test_job_service_results_dashboard_summarizes_net_counts_and_rollbacks(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            app = create_app()
            app.config["BTP_DATA_DIR"] = tmpdir
            init_db(app)

            service = JobService(app.extensions["btp_db"]["db_path"])
            service.create_job(
                job_id="job-int-001",
                job_type=JobType.INTERNAL_PERSONS.value,
                status=JobStatus.COMPLETED,
                created_at="2026-04-20T13:00:00Z",
            )
            service.create_job(
                job_id="job-ds-001",
                job_type=JobType.DATASETS.value,
                status=JobStatus.COMPLETED,
                created_at="2026-04-20T13:10:00Z",
            )

            with connect_db(app.extensions["btp_db"]["db_path"]) as connection:
                connection.execute(
                    """
                    INSERT INTO job_change_sets (id, job_id, job_type, status, created_at, applied_at, rolled_back_at, item_count, applied_item_count)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        "changeset-int-001",
                        "job-int-001",
                        JobType.INTERNAL_PERSONS.value,
                        "rolled_back",
                        "2026-04-20T13:00:00Z",
                        "2026-04-20T13:01:00Z",
                        "2026-04-20T13:02:00Z",
                        2,
                        2,
                    ),
                )
                connection.execute(
                    """
                    INSERT INTO job_change_set_items (change_set_id, item_key, entity_uuid, entity_label, field_name, identifier_type, old_value_json, new_value_json, apply_status, rollback_status, conflict_reason)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        "changeset-int-001",
                        "uuid-1|orcid|0000-1",
                        "uuid-1",
                        "Alpha",
                        "identifier",
                        "orcid",
                        "null",
                        '{"value": "0000-1"}',
                        "applied",
                        "pending",
                        None,
                    ),
                )
                connection.execute(
                    """
                    INSERT INTO job_change_set_items (change_set_id, item_key, entity_uuid, entity_label, field_name, identifier_type, old_value_json, new_value_json, apply_status, rollback_status, conflict_reason)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        "changeset-int-001",
                        "uuid-2|openalex|A123",
                        "uuid-2",
                        "Beta",
                        "identifier",
                        "openalex",
                        "null",
                        '{"value": "A123"}',
                        "applied",
                        "rolled_back",
                        None,
                    ),
                )
                connection.execute(
                    """
                    INSERT INTO job_change_sets (id, job_id, job_type, status, created_at, applied_at, rolled_back_at, item_count, applied_item_count)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        "changeset-ds-001",
                        "job-ds-001",
                        JobType.DATASETS.value,
                        "applied",
                        "2026-04-20T13:10:00Z",
                        "2026-04-20T13:11:00Z",
                        None,
                        1,
                        1,
                    ),
                )
                connection.execute(
                    """
                    INSERT INTO job_change_set_items (change_set_id, item_key, entity_uuid, entity_label, field_name, identifier_type, old_value_json, new_value_json, apply_status, rollback_status, conflict_reason)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        "changeset-ds-001",
                        "10.1/test",
                        "10.1/test",
                        "Dataset",
                        "record",
                        "dataset",
                        "null",
                        '{"doi": "10.1/test"}',
                        "applied",
                        "pending",
                        None,
                    ),
                )
                connection.commit()

            dashboard = service.get_results_dashboard()

            self.assertEqual(3, dashboard["totals"]["applied_items"])
            self.assertEqual(1, dashboard["totals"]["rolled_back_items"])
            self.assertEqual(2, dashboard["totals"]["net_items"])
            internal = next(item for item in dashboard["workflows"] if item["job_type"] == JobType.INTERNAL_PERSONS.value)
            self.assertEqual(2, internal["applied_entities"])
            self.assertEqual(1, internal["rolled_back_entities"])
            self.assertEqual(1, internal["net_entities"])
            self.assertEqual("ORCID", internal["breakdown"][0]["label"])
            self.assertEqual(1, internal["breakdown"][0]["net_items"])
            datasets = next(item for item in dashboard["workflows"] if item["job_type"] == JobType.DATASETS.value)
            self.assertEqual(1, datasets["net_entities"])
            self.assertEqual("New Datasets", datasets["breakdown"][0]["label"])

    def test_job_service_results_dashboard_can_filter_to_recent_days(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            app = create_app()
            app.config["BTP_DATA_DIR"] = tmpdir
            init_db(app)

            service = JobService(app.extensions["btp_db"]["db_path"])
            service.create_job(
                job_id="job-int-001",
                job_type=JobType.INTERNAL_PERSONS.value,
                status=JobStatus.COMPLETED,
                created_at="2026-04-20T13:00:00Z",
            )

            with connect_db(app.extensions["btp_db"]["db_path"]) as connection:
                connection.execute(
                    """
                    INSERT INTO job_change_sets (id, job_id, job_type, status, created_at, applied_at, item_count, applied_item_count)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        "changeset-old",
                        "job-int-001",
                        JobType.INTERNAL_PERSONS.value,
                        "applied",
                        "2026-01-01T13:00:00Z",
                        "2026-01-01T13:00:00Z",
                        1,
                        1,
                    ),
                )
                connection.execute(
                    """
                    INSERT INTO job_change_set_items (change_set_id, item_key, entity_uuid, entity_label, field_name, identifier_type, old_value_json, new_value_json, apply_status, rollback_status, conflict_reason)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        "changeset-old",
                        "uuid-1|orcid|old",
                        "uuid-1",
                        "Alpha",
                        "identifier",
                        "orcid",
                        "null",
                        '{"value":"old"}',
                        "applied",
                        "pending",
                        None,
                    ),
                )
                connection.execute(
                    """
                    INSERT INTO job_change_sets (id, job_id, job_type, status, created_at, applied_at, item_count, applied_item_count)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        "changeset-new",
                        "job-int-001",
                        JobType.INTERNAL_PERSONS.value,
                        "applied",
                        "2099-01-01T13:00:00Z",
                        "2099-01-01T13:00:00Z",
                        1,
                        1,
                    ),
                )
                connection.execute(
                    """
                    INSERT INTO job_change_set_items (change_set_id, item_key, entity_uuid, entity_label, field_name, identifier_type, old_value_json, new_value_json, apply_status, rollback_status, conflict_reason)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        "changeset-new",
                        "uuid-2|orcid|new",
                        "uuid-2",
                        "Beta",
                        "identifier",
                        "orcid",
                        "null",
                        '{"value":"new"}',
                        "applied",
                        "pending",
                        None,
                    ),
                )
                connection.commit()

            dashboard = service.get_results_dashboard(days=30)
            internal = next(item for item in dashboard["workflows"] if item["job_type"] == JobType.INTERNAL_PERSONS.value)

            self.assertEqual(1, internal["net_entities"])
            self.assertEqual(1, internal["applied_items"])

    @patch("pure_researchoutputs.create_external_person")
    @patch("pure_researchoutputs.find_external_person")
    @patch("pure_researchoutputs.pure_persons.find_person")
    def test_research_outputs_run_does_not_create_external_persons_during_review(
        self,
        mock_find_person,
        mock_find_external_person,
        mock_create_external_person,
    ):
        mock_find_person.side_effect = [
            {"uuid": "pers-1", "firstName": "Internal", "lastName": "Person", "associationsUUIDs": []},
            None,
        ]
        mock_find_external_person.return_value = (None, "0000-0001-0002-0003", "A123")

        contributors = [
            {"name": "Internal Person", "first_name": "Internal", "last_name": "Person", "ids": {}, "affiliations": {}},
            {"name": "External Person", "first_name": "External", "last_name": "Person", "ids": {}, "affiliations": {}},
        ]

        details = pure_researchoutputs.get_contributors_details(contributors, "2026-01-01")
        formatted = pure_researchoutputs.format_contributors(details)

        mock_create_external_person.assert_not_called()
        external = next(item for item in formatted if item["typeDiscriminator"] == "ExternalContributorAssociation")
        self.assertNotIn("externalPerson", external)
        self.assertEqual("External", external["_btp_external_person"]["first_name"])

    @patch("pure_datasets.create_external_person")
    @patch("pure_datasets.pure_persons.find_external_person")
    @patch("pure_datasets.pure_persons.find_person")
    def test_datasets_run_does_not_create_external_persons_during_review(
        self,
        mock_find_person,
        mock_find_external_person,
        mock_create_external_person,
    ):
        mock_find_person.side_effect = [
            {"uuid": "pers-1", "firstName": "Internal", "lastName": "Person", "associationsUUIDs": [], "type": "creator"},
            None,
        ]
        mock_find_external_person.return_value = (None, "0000-0001-0002-0003", "A123")

        contributors = [
            {"first_name": "Internal", "last_name": "Person", "person_ids": {}, "type": "creator"},
            {"first_name": "External", "last_name": "Person", "person_ids": {}, "type": "creator"},
        ]

        details = pure_datasets.get_contributors_details(contributors, "2026-01-01")
        formatted = pure_datasets.format_contributors(details)

        mock_create_external_person.assert_not_called()
        external = next(item for item in formatted if item["typeDiscriminator"] == "ExternalDataSetPersonAssociation")
        self.assertNotIn("externalPerson", external)
        self.assertEqual("External", external["_btp_external_person"]["first_name"])

    def test_research_output_apply_manifest_is_written(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            previous_manifest = os.environ.get("BTP_APPLY_MANIFEST_FILE")
            os.environ["BTP_APPLY_MANIFEST_FILE"] = os.path.join(tmpdir, "apply_manifest.jsonl")
            try:
                csv_file = pd.DataFrame([{"to_be_updated": "x", "updated": "", "doi": "10.1/test", "title": "Test"}])
                big_json = [{"electronicVersions": [{"doi": "10.1/test"}], "title": {"value": "Test"}}]
                with patch("apply_updates_to_pure.pure_researchoutputs.create_research_output") as mock_create:
                    mock_create.return_value = {
                        "success": True,
                        "uuid": "ro-1",
                        "created_external_persons": [{"uuid": "ext-1"}],
                    }
                    apply_updates_to_pure.process_research_output("to_be_updated.csv", csv_file, big_json)

                with open(os.environ["BTP_APPLY_MANIFEST_FILE"], "r", encoding="utf-8") as handle:
                    lines = [json.loads(line) for line in handle if line.strip()]
                self.assertEqual("10.1/test", lines[0]["item_key"])
                self.assertEqual("ro-1", lines[0]["record_uuid"])
                self.assertEqual("ext-1", lines[0]["created_external_persons"][0]["uuid"])
            finally:
                if previous_manifest is None:
                    os.environ.pop("BTP_APPLY_MANIFEST_FILE", None)
                else:
                    os.environ["BTP_APPLY_MANIFEST_FILE"] = previous_manifest

    def test_job_service_returns_primary_review_table_for_internal_persons(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            app = create_app()
            app.config["BTP_DATA_DIR"] = tmpdir
            init_db(app)

            service = JobService(app.extensions["btp_db"]["db_path"], project_root=tmpdir)
            artifact_dir = os.path.join("output", "internal_persons", "job-001")
            os.makedirs(os.path.join(tmpdir, artifact_dir), exist_ok=True)
            csv_path = os.path.join(tmpdir, artifact_dir, "personstobeupdated_20260421.csv")
            with open(csv_path, "w", encoding="utf-8") as handle:
                handle.write("to_be_updated,updated,PURE_UUID_PERS,FULL_NAME\nX, ,uuid-1,Alpha\n, X,uuid-2,Beta\n")

            service.create_job(
                job_id="job-001",
                job_type=JobType.INTERNAL_PERSONS.value,
                status=JobStatus.NEEDS_REVIEW,
                artifact_dir=artifact_dir,
                created_at="2026-04-20T13:00:00Z",
            )

            table = service.get_job_review_table("job-001")

            self.assertEqual("personstobeupdated_20260421.csv", table["fileName"])
            self.assertEqual(["to_be_updated", "updated", "PURE_UUID_PERS", "FULL_NAME"], table["columns"])
            self.assertEqual(2, table["rowCount"])
            self.assertEqual("to_be_updated", table["selectionColumn"])
            self.assertEqual(["to_be_updated"], table["editableColumns"])
            self.assertEqual("Alpha", table["rows"][0]["FULL_NAME"])
            self.assertEqual(0, table["rows"][0]["_rowIndex"])

    def test_job_service_returns_default_columns_for_empty_dataset_review_table(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            app = create_app()
            app.config["BTP_DATA_DIR"] = tmpdir
            init_db(app)

            service = JobService(app.extensions["btp_db"]["db_path"], project_root=tmpdir)
            artifact_dir = os.path.join("output", "datasets", "job-001")
            os.makedirs(os.path.join(tmpdir, artifact_dir), exist_ok=True)
            csv_path = os.path.join(tmpdir, artifact_dir, "to_be_updated.csv")
            with open(csv_path, "w", encoding="utf-8") as handle:
                handle.write("\n")
            json_path = os.path.join(tmpdir, artifact_dir, "datasets_to_be_updated.json")
            with open(json_path, "w", encoding="utf-8") as handle:
                handle.write("[]")

            service.create_job(
                job_id="job-001",
                job_type=JobType.DATASETS.value,
                status=JobStatus.NEEDS_REVIEW,
                artifact_dir=artifact_dir,
                created_at="2026-04-20T13:00:00Z",
            )

            table = service.get_job_review_table("job-001")

            self.assertEqual("to_be_updated.csv", table["fileName"])
            self.assertEqual(["to_be_updated", "updated", "doi", "title"], table["columns"])
            self.assertEqual(0, table["rowCount"])
            self.assertEqual([], table["rows"])
            self.assertEqual("to_be_updated", table["selectionColumn"])
            self.assertEqual(["to_be_updated"], table["editableColumns"])

    def test_job_service_updates_review_table_selection_and_refreshes_results(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            app = create_app()
            app.config["BTP_DATA_DIR"] = tmpdir
            init_db(app)

            service = JobService(app.extensions["btp_db"]["db_path"], project_root=tmpdir)
            artifact_dir = os.path.join("output", "internal_persons", "job-001")
            os.makedirs(os.path.join(tmpdir, artifact_dir), exist_ok=True)
            csv_path = os.path.join(tmpdir, artifact_dir, "personstobeupdated_20260421.csv")
            with open(csv_path, "w", encoding="utf-8") as handle:
                handle.write("to_be_updated,updated,PURE_UUID_PERS,FULL_NAME\nX, ,uuid-1,Alpha\n, ,uuid-2,Beta\n")

            service.create_job(
                job_id="job-001",
                job_type=JobType.INTERNAL_PERSONS.value,
                status=JobStatus.NEEDS_REVIEW,
                artifact_dir=artifact_dir,
                created_at="2026-04-20T13:00:00Z",
            )

            table = service.update_job_review_table(
                "job-001",
                file_name="personstobeupdated_20260421.csv",
                expected_row_count=2,
                updates=[{"rowIndex": 0, "to_be_updated": ""}, {"rowIndex": 1, "to_be_updated": "X"}],
            )
            job = service.get_job("job-001")

            self.assertEqual("", table["rows"][0]["to_be_updated"])
            self.assertEqual("X", table["rows"][1]["to_be_updated"])
            self.assertEqual(1, job["results"]["ready_count"])

    def test_job_service_returns_none_for_missing_job(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            app = create_app()
            app.config["BTP_DATA_DIR"] = tmpdir
            init_db(app)

            service = JobService(app.extensions["btp_db"]["db_path"])

            self.assertIsNone(service.get_job("missing-job"))
            self.assertIsNone(service.update_job("missing-job", status=JobStatus.FAILED))

    def test_job_service_rejects_unknown_update_fields(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            app = create_app()
            app.config["BTP_DATA_DIR"] = tmpdir
            init_db(app)

            service = JobService(app.extensions["btp_db"]["db_path"])
            service.create_job(
                job_id="job-001",
                job_type=JobType.INTERNAL_PERSONS.value,
                created_at="2026-04-20T13:00:00Z",
            )

            with self.assertRaises(ValueError):
                service.update_job("job-001", unknown_field="value")

    def test_job_service_rejects_invalid_job_type(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            app = create_app()
            app.config["BTP_DATA_DIR"] = tmpdir
            init_db(app)

            service = JobService(app.extensions["btp_db"]["db_path"])

            with self.assertRaises(ValueError):
                service.create_job(
                    job_id="job-001",
                    job_type="bad-type",
                    created_at="2026-04-20T13:00:00Z",
                )

    def test_job_service_rejects_invalid_job_params(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            app = create_app()
            app.config["BTP_DATA_DIR"] = tmpdir
            init_db(app)

            service = JobService(app.extensions["btp_db"]["db_path"])

            with self.assertRaises(ValueError):
                service.create_job(
                    job_id="job-001",
                    job_type=JobType.INTERNAL_PERSONS.value,
                    params={"not_allowed": True},
                    created_at="2026-04-20T13:00:00Z",
                )

    def test_job_service_rejects_invalid_status_transition(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            app = create_app()
            app.config["BTP_DATA_DIR"] = tmpdir
            init_db(app)

            service = JobService(app.extensions["btp_db"]["db_path"])
            service.create_job(
                job_id="job-001",
                job_type=JobType.INTERNAL_PERSONS.value,
                created_at="2026-04-20T13:00:00Z",
            )

            with self.assertRaises(ValueError):
                service.update_job("job-001", status=JobStatus.COMPLETED)

    def test_job_service_allows_multiple_active_internal_persons_jobs_with_isolated_artifact_dirs(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            app = create_app()
            app.config["BTP_DATA_DIR"] = tmpdir
            init_db(app)

            service = JobService(app.extensions["btp_db"]["db_path"])
            first = service.create_job(
                job_id="job-001",
                job_type=JobType.INTERNAL_PERSONS.value,
                created_at="2026-04-20T13:00:00Z",
            )
            second = service.create_job(
                job_id="job-002",
                job_type=JobType.INTERNAL_PERSONS.value,
                created_at="2026-04-20T13:01:00Z",
            )

            self.assertEqual("output/internal_persons/job-001", first["artifact_dir"])
            self.assertEqual("output/internal_persons/job-002", second["artifact_dir"])
            self.assertEqual("job-002", second["id"])

    @patch("app.services.jobs.subprocess.Popen")
    def test_job_service_run_job_marks_success_and_writes_logs(self, mock_popen):
        with tempfile.TemporaryDirectory() as tmpdir:
            app = create_app()
            app.config["BTP_DATA_DIR"] = os.path.join(tmpdir, "data")
            init_db(app)

            service = JobService(
                app.extensions["btp_db"]["db_path"],
                project_root=tmpdir,
            )
            os.makedirs(os.path.join(tmpdir, "src"), exist_ok=True)
            with open(os.path.join(tmpdir, "src", "enrich_internal_persons_with_ids.py"), "w", encoding="utf-8") as handle:
                handle.write("print('placeholder')\n")

            service.create_job(
                job_id="job-001",
                job_type=JobType.INTERNAL_PERSONS.value,
                params={"facultyChoice": "all"},
                created_at="2026-04-20T13:00:00Z",
            )
            mock_popen.return_value = FakeCompletedProcess(returncode=0, stdout="line 1\n", stderr="")

            result = service.run_job("job-001")

            self.assertEqual("completed", result["status"])
            self.assertEqual(0, result["exit_code"])
            self.assertEqual("logs/jobs/job-001.log", result["log_path"])
            self.assertFalse(result["canOpen"])
            self.assertFalse(result["canApply"])
            with open(os.path.join(tmpdir, "logs", "jobs", "job-001.log"), "r", encoding="utf-8") as handle:
                contents = handle.read()
            self.assertIn("line 1", contents)
            command = mock_popen.call_args.args[0]
            self.assertIn("enrich_internal_persons_with_ids.py", command[2])
            self.assertEqual("all", command[3])
            self.assertEqual(
                os.path.join(tmpdir, "output", "internal_persons", "job-001"),
                mock_popen.call_args.kwargs["env"]["BTP_OUTPUT_DIR"],
            )

    @patch("app.services.jobs.subprocess.Popen")
    def test_job_service_run_job_streams_stdout_to_job_log(self, mock_popen):
        with tempfile.TemporaryDirectory() as tmpdir:
            app = create_app()
            app.config["BTP_DATA_DIR"] = os.path.join(tmpdir, "data")
            init_db(app)

            service = JobService(
                app.extensions["btp_db"]["db_path"],
                project_root=tmpdir,
            )
            os.makedirs(os.path.join(tmpdir, "src"), exist_ok=True)
            with open(os.path.join(tmpdir, "src", "enrich_internal_persons_with_ids.py"), "w", encoding="utf-8") as handle:
                handle.write("print('placeholder')\n")

            service.create_job(
                job_id="job-001",
                job_type=JobType.INTERNAL_PERSONS.value,
                params={"facultyChoice": "all"},
                created_at="2026-04-20T13:00:00Z",
            )
            mock_popen.return_value = FakeStreamingProcess(stdout_lines=["line 1\n", "line 2\n"])

            result = service.run_job("job-001")

            self.assertEqual("completed", result["status"])
            with open(os.path.join(tmpdir, "logs", "jobs", "job-001.log"), "r", encoding="utf-8") as handle:
                contents = handle.read()
            self.assertIn("line 1", contents)
            self.assertIn("line 2", contents)
            self.assertEqual(subprocess.STDOUT, mock_popen.call_args.kwargs["stderr"])

    @patch("app.services.jobs.subprocess.Popen")
    def test_job_service_run_job_reports_killed_process_signal(self, mock_popen):
        with tempfile.TemporaryDirectory() as tmpdir:
            app = create_app()
            app.config["BTP_DATA_DIR"] = os.path.join(tmpdir, "data")
            init_db(app)

            service = JobService(
                app.extensions["btp_db"]["db_path"],
                project_root=tmpdir,
            )
            os.makedirs(os.path.join(tmpdir, "src"), exist_ok=True)
            with open(os.path.join(tmpdir, "src", "enrich_internal_persons_with_ids.py"), "w", encoding="utf-8") as handle:
                handle.write("print('placeholder')\n")

            service.create_job(
                job_id="job-001",
                job_type=JobType.INTERNAL_PERSONS.value,
                params={"facultyChoice": "all"},
                created_at="2026-04-20T13:00:00Z",
            )
            mock_popen.return_value = FakeCompletedProcess(returncode=-9, stdout="", stderr="")

            result = service.run_job("job-001")

            self.assertEqual("failed", result["status"])
            self.assertEqual(-9, result["exit_code"])
            self.assertEqual("Process was killed by SIGKILL (9)", result["error_message"])

    @patch("app.services.jobs.subprocess.Popen")
    def test_job_service_run_job_sets_research_output_job_directory_in_env(self, mock_popen):
        with tempfile.TemporaryDirectory() as tmpdir:
            app = create_app()
            app.config["BTP_DATA_DIR"] = os.path.join(tmpdir, "data")
            init_db(app)

            service = JobService(
                app.extensions["btp_db"]["db_path"],
                project_root=tmpdir,
            )
            os.makedirs(os.path.join(tmpdir, "src"), exist_ok=True)
            with open(os.path.join(tmpdir, "src", "update_researchoutput_from_ricgraph.py"), "w", encoding="utf-8") as handle:
                handle.write("print('placeholder')\n")

            service.create_job(
                job_id="job-ro-001",
                job_type=JobType.RESEARCH_OUTPUTS.value,
                params={"facultyChoice": "all"},
                created_at="2026-04-20T13:00:00Z",
            )
            mock_popen.return_value = FakeCompletedProcess(returncode=0, stdout="research outputs done\n", stderr="")

            result = service.run_job("job-ro-001")

            self.assertEqual("completed", result["status"])
            command = mock_popen.call_args.args[0]
            self.assertIn("update_researchoutput_from_ricgraph.py", command[2])
            self.assertEqual("all", command[3])
            self.assertEqual(
                os.path.join(tmpdir, "output", "research_output", "job-ro-001"),
                mock_popen.call_args.kwargs["env"]["BTP_OUTPUT_DIR"],
            )

    @patch("app.services.jobs.subprocess.Popen")
    def test_job_service_run_job_sets_dataset_job_directory_in_env(self, mock_popen):
        with tempfile.TemporaryDirectory() as tmpdir:
            app = create_app()
            app.config["BTP_DATA_DIR"] = os.path.join(tmpdir, "data")
            init_db(app)

            service = JobService(
                app.extensions["btp_db"]["db_path"],
                project_root=tmpdir,
            )
            os.makedirs(os.path.join(tmpdir, "src"), exist_ok=True)
            with open(os.path.join(tmpdir, "src", "update_datasets_from_ricgraph.py"), "w", encoding="utf-8") as handle:
                handle.write("print('placeholder')\n")

            service.create_job(
                job_id="job-ds-001",
                job_type=JobType.DATASETS.value,
                params={"facultyChoice": "all"},
                created_at="2026-04-20T13:00:00Z",
            )
            mock_popen.return_value = FakeCompletedProcess(returncode=0, stdout="datasets done\n", stderr="")

            result = service.run_job("job-ds-001")

            self.assertEqual("completed", result["status"])
            command = mock_popen.call_args.args[0]
            self.assertIn("update_datasets_from_ricgraph.py", command[2])
            self.assertEqual("all", command[3])
            self.assertEqual(
                os.path.join(tmpdir, "output", "datasets", "job-ds-001"),
                mock_popen.call_args.kwargs["env"]["BTP_OUTPUT_DIR"],
            )

    @patch("app.services.jobs.subprocess.Popen")
    def test_job_service_run_job_sets_needs_review_when_internal_person_artifacts_exist(self, mock_popen):
        with tempfile.TemporaryDirectory() as tmpdir:
            app = create_app()
            app.config["BTP_DATA_DIR"] = os.path.join(tmpdir, "data")
            init_db(app)

            service = JobService(
                app.extensions["btp_db"]["db_path"],
                project_root=tmpdir,
            )
            os.makedirs(os.path.join(tmpdir, "src"), exist_ok=True)
            job_output_dir = os.path.join(tmpdir, "output", "internal_persons", "job-001")
            os.makedirs(job_output_dir, exist_ok=True)
            with open(os.path.join(tmpdir, "src", "enrich_internal_persons_with_ids.py"), "w", encoding="utf-8") as handle:
                handle.write("print('placeholder')\n")
            with open(
                os.path.join(tmpdir, "output", "internal_persons", "personstobeupdated_20260312.csv"),
                "w",
                encoding="utf-8",
            ) as handle:
                handle.write("to_be_updated,updated,PURE_UUID_PERS\n,X,pers-old\n")
            with open(
                os.path.join(tmpdir, "output", "internal_persons", "datatotal.json"),
                "w",
                encoding="utf-8",
            ) as handle:
                handle.write('[{"uuid":"pers-old"}]')

            service.create_job(
                job_id="job-001",
                job_type=JobType.INTERNAL_PERSONS.value,
                created_at="2026-04-20T13:00:00Z",
            )

            def write_new_artifacts():
                with open(
                    os.path.join(job_output_dir, "personstobeupdated_20260420.csv"),
                    "w",
                    encoding="utf-8",
                ) as handle:
                    handle.write("to_be_updated,updated,PURE_UUID_PERS\nX,,pers-1\n")
                with open(
                    os.path.join(job_output_dir, "datatotal.json"),
                    "w",
                    encoding="utf-8",
                ) as handle:
                    handle.write('[{"uuid":"pers-1"},{"uuid":"pers-2"}]')
                return "done\n", ""

            mock_process = MagicMock()
            mock_process.returncode = 0
            mock_process.communicate.side_effect = write_new_artifacts
            mock_popen.return_value = mock_process

            result = service.run_job("job-001")

            self.assertEqual("needs_review", result["status"])
            self.assertTrue(result["canOpen"])
            self.assertTrue(result["canApply"])
            self.assertEqual(["personstobeupdated_20260420.csv"], result["artifacts"]["csv"])
            self.assertEqual(["datatotal.json"], result["artifacts"]["json"])
            self.assertEqual(2, result["results"]["found_count"])
            self.assertEqual(1, result["results"]["ready_count"])
            self.assertEqual(0, result["results"]["updated_count"])

    @patch("app.services.jobs.subprocess.Popen")
    def test_job_service_run_job_marks_failure_on_nonzero_exit(self, mock_popen):
        with tempfile.TemporaryDirectory() as tmpdir:
            app = create_app()
            app.config["BTP_DATA_DIR"] = os.path.join(tmpdir, "data")
            init_db(app)

            service = JobService(
                app.extensions["btp_db"]["db_path"],
                project_root=tmpdir,
            )
            os.makedirs(os.path.join(tmpdir, "src"), exist_ok=True)
            with open(os.path.join(tmpdir, "src", "enrich_internal_persons_with_ids.py"), "w", encoding="utf-8") as handle:
                handle.write("print('placeholder')\n")

            service.create_job(
                job_id="job-001",
                job_type=JobType.INTERNAL_PERSONS.value,
                created_at="2026-04-20T13:00:00Z",
            )
            mock_popen.return_value = FakeCompletedProcess(returncode=2, stdout="", stderr="boom\n")

            result = service.run_job("job-001")

            self.assertEqual("failed", result["status"])
            self.assertEqual(2, result["exit_code"])
            self.assertEqual("boom", result["error_message"])

    def test_job_service_run_job_marks_failure_when_script_missing(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            app = create_app()
            app.config["BTP_DATA_DIR"] = os.path.join(tmpdir, "data")
            init_db(app)

            service = JobService(
                app.extensions["btp_db"]["db_path"],
                project_root=tmpdir,
            )
            service.create_job(
                job_id="job-001",
                job_type=JobType.INTERNAL_PERSONS.value,
                created_at="2026-04-20T13:00:00Z",
            )

            result = service.run_job("job-001")

            self.assertEqual("failed", result["status"])
            self.assertEqual(127, result["exit_code"])
            self.assertIn("Script path does not exist", result["error_message"])

    def test_job_service_run_job_rejects_non_queued_jobs(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            app = create_app()
            app.config["BTP_DATA_DIR"] = os.path.join(tmpdir, "data")
            init_db(app)

            service = JobService(
                app.extensions["btp_db"]["db_path"],
                project_root=tmpdir,
            )
            service.create_job(
                job_id="job-001",
                job_type=JobType.INTERNAL_PERSONS.value,
                status=JobStatus.FAILED,
                created_at="2026-04-20T13:00:00Z",
            )

            with self.assertRaises(ValueError):
                service.run_job("job-001")

    @patch("app.services.jobs.subprocess.Popen")
    def test_job_service_apply_job_completes_reviewable_internal_persons_job(self, mock_popen):
        with tempfile.TemporaryDirectory() as tmpdir:
            app = create_app()
            app.config["BTP_DATA_DIR"] = os.path.join(tmpdir, "data")
            init_db(app)

            service = JobService(
                app.extensions["btp_db"]["db_path"],
                project_root=tmpdir,
            )
            os.makedirs(os.path.join(tmpdir, "src"), exist_ok=True)
            os.makedirs(os.path.join(tmpdir, "output", "internal_persons"), exist_ok=True)
            with open(os.path.join(tmpdir, "src", "apply_updates_to_pure.py"), "w", encoding="utf-8") as handle:
                handle.write("print('placeholder')\n")
            with open(
                os.path.join(tmpdir, "output", "internal_persons", "personstobeupdated_20260420.csv"),
                "w",
                encoding="utf-8",
            ) as handle:
                handle.write(
                    "to_be_updated,updated,FULL_NAME,person_id,PURE_UUID_PERS,new_id,new_value,uri\n"
                    "X,,Alpha,person-1,pers-1,orcid,0000-0001,\n"
                )
            with open(
                os.path.join(tmpdir, "output", "internal_persons", "datatotal.json"),
                "w",
                encoding="utf-8",
            ) as handle:
                handle.write('[{"uuid":"pers-1","orcid":"0000-0000","identifiers":[]}]')

            service.create_job(
                job_id="job-001",
                job_type=JobType.INTERNAL_PERSONS.value,
                status=JobStatus.NEEDS_REVIEW,
                created_at="2026-04-20T13:00:00Z",
                log_path="logs/jobs/job-001.log",
            )
            service.update_job(
                "job-001",
                artifacts={
                    "canOpen": True,
                    "canApply": True,
                    "artifacts": {
                        "directory": "output/internal_persons",
                        "csv": ["personstobeupdated_20260420.csv"],
                        "json": ["datatotal.json"],
                    },
                },
                results={
                    "entity_label": "persons",
                    "found_label": "Persons found",
                    "ready_label": "Persons ready to update",
                    "updated_label": "Persons updated",
                    "found_count": 1,
                    "ready_count": 1,
                    "updated_count": 0,
                },
            )
            def apply_and_update_csv():
                with open(
                    os.path.join(tmpdir, "output", "internal_persons", "personstobeupdated_20260420.csv"),
                    "w",
                    encoding="utf-8",
                ) as handle:
                    handle.write(
                        "to_be_updated,updated,FULL_NAME,person_id,PURE_UUID_PERS,new_id,new_value,uri\n"
                        ",X,Alpha,person-1,pers-1,orcid,0000-0001,\n"
                    )
                return "apply ok\n", ""

            mock_process = MagicMock()
            mock_process.returncode = 0
            mock_process.communicate.side_effect = apply_and_update_csv
            mock_popen.return_value = mock_process

            result = service.apply_job("job-001")

            self.assertEqual("completed", result["status"])
            self.assertEqual(0, result["exit_code"])
            with open(os.path.join(tmpdir, "logs", "jobs", "job-001.log"), "r", encoding="utf-8") as handle:
                contents = handle.read()
            self.assertIn("=== APPLY START", contents)
            self.assertIn("apply ok", contents)
            self.assertIn("=== APPLY FINISHED", contents)
            command = mock_popen.call_args.args[0]
            self.assertIn("apply_updates_to_pure.py", command[2])
            self.assertEqual(
                "enrich_internal_persons_with_ids",
                mock_popen.call_args.kwargs["env"]["REFERER_PAGE"],
            )
            self.assertEqual(
                "personstobeupdated_20260420.csv",
                mock_popen.call_args.kwargs["env"]["BTP_TARGET_CSV_FILES"],
            )
            self.assertEqual(
                "datatotal.json",
                mock_popen.call_args.kwargs["env"]["BTP_TARGET_JSON_FILES"],
            )
            self.assertEqual(
                os.path.join(tmpdir, "output", "internal_persons"),
                mock_popen.call_args.kwargs["env"]["BTP_OUTPUT_DIR"],
            )
            change_set = service.get_job_change_set("job-001")
            self.assertIsNotNone(change_set)
            self.assertEqual("applied", change_set["status"])
            self.assertEqual(1, change_set["item_count"])
            self.assertEqual(1, change_set["applied_item_count"])
            self.assertEqual("orcid", change_set["items"][0]["field_name"])
            self.assertEqual("0000-0000", change_set["items"][0]["old_value"])
            self.assertEqual({"value": "0000-0001"}, change_set["items"][0]["new_value"])
            refreshed = service.get_job("job-001")
            self.assertEqual(0, refreshed["results"]["ready_count"])
            self.assertEqual(1, refreshed["results"]["updated_count"])

    def test_job_service_apply_job_requires_needs_review_state(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            app = create_app()
            app.config["BTP_DATA_DIR"] = os.path.join(tmpdir, "data")
            init_db(app)

            service = JobService(
                app.extensions["btp_db"]["db_path"],
                project_root=tmpdir,
            )
            service.create_job(
                job_id="job-001",
                job_type=JobType.INTERNAL_PERSONS.value,
                status=JobStatus.COMPLETED,
                created_at="2026-04-20T13:00:00Z",
            )

            with self.assertRaises(ValueError):
                service.apply_job("job-001")

    def test_job_service_apply_job_requires_ready_artifacts(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            app = create_app()
            app.config["BTP_DATA_DIR"] = os.path.join(tmpdir, "data")
            init_db(app)

            service = JobService(
                app.extensions["btp_db"]["db_path"],
                project_root=tmpdir,
            )
            service.create_job(
                job_id="job-001",
                job_type=JobType.INTERNAL_PERSONS.value,
                status=JobStatus.NEEDS_REVIEW,
                created_at="2026-04-20T13:00:00Z",
            )

            with self.assertRaises(ValueError):
                service.apply_job("job-001")

    def test_job_service_apply_job_rejects_zero_selected_updates(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            app = create_app()
            app.config["BTP_DATA_DIR"] = os.path.join(tmpdir, "data")
            init_db(app)

            service = JobService(
                app.extensions["btp_db"]["db_path"],
                project_root=tmpdir,
            )
            service.create_job(
                job_id="job-001",
                job_type=JobType.INTERNAL_PERSONS.value,
                status=JobStatus.NEEDS_REVIEW,
                created_at="2026-04-20T13:00:00Z",
            )
            service.update_job(
                "job-001",
                artifacts={
                    "canOpen": True,
                    "canApply": True,
                    "artifacts": {
                        "directory": "output/internal_persons",
                        "csv": ["personstobeupdated_20260421.csv"],
                        "json": ["datatotal.json"],
                    },
                },
                results={
                    "entity_label": "persons",
                    "found_label": "Persons found",
                    "ready_label": "Persons ready to update",
                    "updated_label": "Persons updated",
                    "found_count": 10,
                    "ready_count": 0,
                    "updated_count": 0,
                },
            )
            refreshed = service.get_job("job-001")

            self.assertFalse(refreshed["canApply"])

            with self.assertRaises(ValueError):
                service.apply_job("job-001")

    def test_job_service_lists_job_artifacts_and_resolves_download_path(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            app = create_app()
            app.config["BTP_DATA_DIR"] = os.path.join(tmpdir, "data")
            init_db(app)

            service = JobService(
                app.extensions["btp_db"]["db_path"],
                project_root=tmpdir,
            )
            output_dir = os.path.join(tmpdir, "output", "internal_persons")
            os.makedirs(output_dir, exist_ok=True)
            with open(os.path.join(output_dir, "personstobeupdated_20260420.csv"), "w", encoding="utf-8") as handle:
                handle.write("id\n1\n")
            with open(os.path.join(output_dir, "datatotal.json"), "w", encoding="utf-8") as handle:
                handle.write("[]")
            service.create_job(
                job_id="job-001",
                job_type=JobType.INTERNAL_PERSONS.value,
                created_at="2026-04-20T13:00:00Z",
            )

            artifacts = service.get_job_artifacts("job-001")
            self.assertEqual("job-001", artifacts["jobId"])
            self.assertEqual("output/internal_persons/job-001", artifacts["directory"])
            self.assertEqual([], artifacts["items"])

    def test_job_service_lists_snapshotted_artifacts_for_reviewable_job(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            app = create_app()
            app.config["BTP_DATA_DIR"] = os.path.join(tmpdir, "data")
            init_db(app)

            service = JobService(
                app.extensions["btp_db"]["db_path"],
                project_root=tmpdir,
            )
            output_dir = os.path.join(tmpdir, "output", "internal_persons")
            os.makedirs(output_dir, exist_ok=True)
            with open(os.path.join(output_dir, "personstobeupdated_20260420.csv"), "w", encoding="utf-8") as handle:
                handle.write("to_be_updated,updated,PURE_UUID_PERS,new_id,new_value\nX,,pers-1,orcid,0000-0001\n")
            with open(os.path.join(output_dir, "datatotal.json"), "w", encoding="utf-8") as handle:
                handle.write('[{"uuid":"pers-1"}]')
            service.create_job(
                job_id="job-001",
                job_type=JobType.INTERNAL_PERSONS.value,
                status=JobStatus.NEEDS_REVIEW,
                created_at="2026-04-20T13:00:00Z",
            )

            artifacts = service.get_job_artifacts("job-001")
            resolved = service.resolve_job_artifact("job-001", "datatotal.json")

            self.assertEqual(
                ["datatotal.json", "personstobeupdated_20260420.csv"],
                sorted(item["name"] for item in artifacts["items"]),
            )
            datatotal_item = next(item for item in artifacts["items"] if item["name"] == "datatotal.json")
            self.assertEqual("json", datatotal_item["kind"])
            self.assertEqual("/api/jobs/job-001/artifacts/datatotal.json", datatotal_item["download_path"])
            self.assertEqual("job-001", resolved[0]["id"])
            self.assertTrue(str(resolved[1]).endswith(os.path.join("output", "internal_persons", "datatotal.json")))

    def test_job_service_summarizes_internal_person_results(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            app = create_app()
            app.config["BTP_DATA_DIR"] = os.path.join(tmpdir, "data")
            init_db(app)

            service = JobService(
                app.extensions["btp_db"]["db_path"],
                project_root=tmpdir,
            )
            output_dir = os.path.join(tmpdir, "output", "internal_persons")
            os.makedirs(output_dir, exist_ok=True)
            with open(os.path.join(output_dir, "personstobeupdated_20260420.csv"), "w", encoding="utf-8") as handle:
                handle.write(
                    "to_be_updated,updated,PURE_UUID_PERS,new_id,new_value\n"
                    "X,,pers-1,orcid,0000-0001\n"
                    "X,,pers-1,scopus,123\n"
                    ",X,pers-2,orcid,0000-0002\n"
                )
            with open(os.path.join(output_dir, "datatotal.json"), "w", encoding="utf-8") as handle:
                handle.write('[{"uuid":"pers-1"},{"uuid":"pers-2"},{"uuid":"pers-3"}]')
            service.create_job(
                job_id="job-001",
                job_type=JobType.INTERNAL_PERSONS.value,
                status=JobStatus.NEEDS_REVIEW,
                created_at="2026-04-20T13:00:00Z",
            )

            job = service.get_job("job-001")

            self.assertEqual("persons", job["results"]["entity_label"])
            self.assertEqual(3, job["results"]["found_count"])
            self.assertEqual(1, job["results"]["ready_count"])
            self.assertEqual(1, job["results"]["updated_count"])

    def test_job_service_summarizes_only_snapshotted_internal_person_artifacts(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            app = create_app()
            app.config["BTP_DATA_DIR"] = os.path.join(tmpdir, "data")
            init_db(app)

            service = JobService(
                app.extensions["btp_db"]["db_path"],
                project_root=tmpdir,
            )
            output_dir = os.path.join(tmpdir, "output", "internal_persons")
            os.makedirs(output_dir, exist_ok=True)
            with open(os.path.join(output_dir, "personstobeupdated_20260312.csv"), "w", encoding="utf-8") as handle:
                handle.write("to_be_updated,updated,PURE_UUID_PERS\n,X,pers-old\n")
            with open(os.path.join(output_dir, "personstobeupdated_20260421.csv"), "w", encoding="utf-8") as handle:
                handle.write("to_be_updated,updated,PURE_UUID_PERS\nX,,pers-new\n")
            with open(os.path.join(output_dir, "datatotal.json"), "w", encoding="utf-8") as handle:
                handle.write('[{"uuid":"pers-new"}]')

            service.create_job(
                job_id="job-001",
                job_type=JobType.INTERNAL_PERSONS.value,
                status=JobStatus.NEEDS_REVIEW,
                created_at="2026-04-20T13:00:00Z",
            )
            service.update_job(
                "job-001",
                artifacts={
                    "canOpen": True,
                    "canApply": True,
                    "artifacts": {
                        "directory": "output/internal_persons",
                        "csv": ["personstobeupdated_20260421.csv"],
                        "json": ["datatotal.json"],
                    },
                },
                results={
                    "entity_label": "persons",
                    "found_label": "Persons found",
                    "ready_label": "Persons ready to update",
                    "updated_label": "Persons updated",
                    "found_count": 1,
                    "ready_count": 1,
                    "updated_count": 0,
                },
            )

            job = service.get_job("job-001")

            self.assertEqual(["personstobeupdated_20260421.csv"], job["artifacts"]["csv"])
            self.assertEqual(1, job["results"]["found_count"])
            self.assertEqual(1, job["results"]["ready_count"])
            self.assertEqual(0, job["results"]["updated_count"])

    def test_job_service_summarizes_external_person_results(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            app = create_app()
            app.config["BTP_DATA_DIR"] = os.path.join(tmpdir, "data")
            init_db(app)

            service = JobService(
                app.extensions["btp_db"]["db_path"],
                project_root=tmpdir,
            )
            output_dir = os.path.join(tmpdir, "output", "external_persons", "job-002")
            os.makedirs(output_dir, exist_ok=True)
            with open(os.path.join(output_dir, "ext_pers_update.csv"), "w", encoding="utf-8") as handle:
                handle.write(
                    "to_be_updated,updated,Name,Alex_ID,Pure_UUID,ORCID,Source\n"
                    "X,,Alpha,A1,pers-1,0000-0001,openalex\n"
                    "X,,Alpha duplicate,A2,pers-1,0000-0002,openalex\n"
                    ",X,Beta,A3,pers-2,0000-0003,openalex\n"
                )
            with open(os.path.join(output_dir, "to_be_updated.json"), "w", encoding="utf-8") as handle:
                handle.write('[{"UUID":"pers-1"},{"UUID":"pers-2"},{"UUID":"pers-3"}]')

            service.create_job(
                job_id="job-002",
                job_type=JobType.EXTERNAL_PERSONS.value,
                status=JobStatus.NEEDS_REVIEW,
                created_at="2026-04-20T13:00:00Z",
            )
            service.update_job(
                "job-002",
                artifacts={
                    "canOpen": True,
                    "canApply": True,
                    "artifacts": {
                        "directory": "output/external_persons/job-002",
                        "csv": ["ext_pers_update.csv"],
                        "json": ["to_be_updated.json"],
                    },
                },
                results={
                    "entity_label": "persons",
                    "found_label": "Persons found",
                    "ready_label": "Persons ready to update",
                    "updated_label": "Persons updated",
                    "found_count": 3,
                    "ready_count": 1,
                    "updated_count": 1,
                },
            )

            job = service.get_job("job-002")

            self.assertEqual(3, job["results"]["found_count"])
            self.assertEqual(1, job["results"]["ready_count"])
            self.assertEqual(1, job["results"]["updated_count"])
            self.assertTrue(job["canApply"])

    def test_job_service_external_persons_can_apply_false_when_no_selected_rows(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            app = create_app()
            app.config["BTP_DATA_DIR"] = os.path.join(tmpdir, "data")
            init_db(app)

            service = JobService(
                app.extensions["btp_db"]["db_path"],
                project_root=tmpdir,
            )
            output_dir = os.path.join(tmpdir, "output", "external_persons", "job-003")
            os.makedirs(output_dir, exist_ok=True)
            with open(os.path.join(output_dir, "ext_pers_update.csv"), "w", encoding="utf-8") as handle:
                handle.write(
                    "to_be_updated,updated,Name,Alex_ID,Pure_UUID,ORCID,Source\n"
                    ",X,Beta,A3,pers-2,0000-0003,openalex\n"
                )
            with open(os.path.join(output_dir, "to_be_updated.json"), "w", encoding="utf-8") as handle:
                handle.write('[{"UUID":"pers-2"}]')

            service.create_job(
                job_id="job-003",
                job_type=JobType.EXTERNAL_PERSONS.value,
                status=JobStatus.NEEDS_REVIEW,
                created_at="2026-04-20T13:00:00Z",
            )
            service.update_job(
                "job-003",
                artifacts={
                    "canOpen": True,
                    "canApply": True,
                    "artifacts": {
                        "directory": "output/external_persons/job-003",
                        "csv": ["ext_pers_update.csv"],
                        "json": ["to_be_updated.json"],
                    },
                },
                results={
                    "entity_label": "persons",
                    "found_label": "Persons found",
                    "ready_label": "Persons ready to update",
                    "updated_label": "Persons updated",
                    "found_count": 1,
                    "ready_count": 0,
                    "updated_count": 1,
                },
            )

            job = service.get_job("job-003")

            self.assertFalse(job["canApply"])

    def test_job_service_builds_external_persons_command_with_faculty_before_test_flag(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            app = create_app()
            app.config["BTP_DATA_DIR"] = os.path.join(tmpdir, "data")
            init_db(app)

            service = JobService(
                app.extensions["btp_db"]["db_path"],
                project_root=tmpdir,
            )
            definition = get_job_type_definition(JobType.EXTERNAL_PERSONS.value)

            command = service._build_command(
                definition,
                {
                    "facultyChoice": "uu faculty: faculteit rebo|organization_name",
                    "useOpenAlexFallback": "yes",
                },
                Path(tmpdir) / "src" / "enrich_pure_external_persons.py",
            )

            self.assertEqual(
                [
                    "uu faculty: faculteit rebo|organization_name",
                    "yes",
                    "yes",
                ],
                command[-3:],
            )

    def test_job_service_builds_external_persons_command_with_ricgraph_only_default(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            app = create_app()
            app.config["BTP_DATA_DIR"] = os.path.join(tmpdir, "data")
            init_db(app)

            service = JobService(
                app.extensions["btp_db"]["db_path"],
                project_root=tmpdir,
            )
            definition = get_job_type_definition(JobType.EXTERNAL_PERSONS.value)

            command = service._build_command(
                definition,
                {"facultyChoice": "uu faculty: faculteit rebo|organization_name"},
                Path(tmpdir) / "src" / "enrich_pure_external_persons.py",
            )

            self.assertEqual(
                [
                    "uu faculty: faculteit rebo|organization_name",
                    "yes",
                    "no",
                ],
                command[-3:],
            )

    def test_job_service_creates_external_orgs_in_job_scoped_artifact_dir(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            app = create_app()
            app.config["BTP_DATA_DIR"] = os.path.join(tmpdir, "data")
            init_db(app)

            service = JobService(
                app.extensions["btp_db"]["db_path"],
                project_root=tmpdir,
            )

            job = service.create_job(
                job_id="job-org-001",
                job_type=JobType.EXTERNAL_ORGS.value,
                created_at="2026-04-20T13:00:00Z",
            )

            self.assertEqual("output/external_orgs/job-org-001", job["artifact_dir"])

    def test_job_service_rejects_invalid_artifact_name(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            app = create_app()
            app.config["BTP_DATA_DIR"] = os.path.join(tmpdir, "data")
            init_db(app)

            service = JobService(
                app.extensions["btp_db"]["db_path"],
                project_root=tmpdir,
            )
            service.create_job(
                job_id="job-001",
                job_type=JobType.INTERNAL_PERSONS.value,
                created_at="2026-04-20T13:00:00Z",
            )

            with self.assertRaises(ValueError):
                service.resolve_job_artifact("job-001", "../secrets.txt")

    def test_job_service_deletes_inactive_job_and_log(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            app = create_app()
            app.config["BTP_DATA_DIR"] = os.path.join(tmpdir, "data")
            init_db(app)

            service = JobService(
                app.extensions["btp_db"]["db_path"],
                project_root=tmpdir,
            )
            logs_dir = os.path.join(tmpdir, "logs", "jobs")
            os.makedirs(logs_dir, exist_ok=True)
            with open(os.path.join(logs_dir, "job-001.log"), "w", encoding="utf-8") as handle:
                handle.write("hello\n")
            service.create_job(
                job_id="job-001",
                job_type=JobType.INTERNAL_PERSONS.value,
                status=JobStatus.FAILED,
                created_at="2026-04-20T13:00:00Z",
                log_path="logs/jobs/job-001.log",
            )

            deleted = service.delete_job("job-001")

            self.assertTrue(deleted)
            self.assertIsNone(service.get_job("job-001"))
            self.assertFalse(os.path.exists(os.path.join(logs_dir, "job-001.log")))

    def test_job_service_delete_job_removes_change_sets(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            app = create_app()
            app.config["BTP_DATA_DIR"] = os.path.join(tmpdir, "data")
            init_db(app)

            service = JobService(
                app.extensions["btp_db"]["db_path"],
                project_root=tmpdir,
            )
            output_dir = os.path.join(tmpdir, "output", "internal_persons")
            os.makedirs(output_dir, exist_ok=True)
            with open(os.path.join(output_dir, "personstobeupdated_20260420.csv"), "w", encoding="utf-8") as handle:
                handle.write(
                    "to_be_updated,updated,FULL_NAME,person_id,PURE_UUID_PERS,new_id,new_value,uri\n"
                    "X,,Alpha,person-1,pers-1,orcid,0000-0001,\n"
                )
            with open(os.path.join(output_dir, "datatotal.json"), "w", encoding="utf-8") as handle:
                handle.write('[{"uuid":"pers-1","orcid":"0000-0000","identifiers":[]}]')

            service.create_job(
                job_id="job-010",
                job_type=JobType.INTERNAL_PERSONS.value,
                status=JobStatus.NEEDS_REVIEW,
                created_at="2026-04-20T13:00:00Z",
            )
            service.update_job(
                "job-010",
                artifacts={
                    "canOpen": True,
                    "canApply": True,
                    "artifacts": {
                        "directory": "output/internal_persons",
                        "csv": ["personstobeupdated_20260420.csv"],
                        "json": ["datatotal.json"],
                    },
                },
                results={
                    "entity_label": "persons",
                    "found_label": "Persons found",
                    "ready_label": "Persons ready to update",
                    "updated_label": "Persons updated",
                    "found_count": 1,
                    "ready_count": 1,
                    "updated_count": 0,
                },
            )

            change_set_id = service._capture_apply_change_set(service.get_job("job-010"))

            self.assertIsNotNone(change_set_id)
            self.assertIsNotNone(service.get_job_change_set("job-010"))

            service.delete_job("job-010")

            self.assertIsNone(service.get_job_change_set("job-010"))

    def test_job_service_delete_job_allows_queued_jobs(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            app = create_app()
            app.config["BTP_DATA_DIR"] = os.path.join(tmpdir, "data")
            init_db(app)

            service = JobService(
                app.extensions["btp_db"]["db_path"],
                project_root=tmpdir,
            )
            service.create_job(
                job_id="job-queued",
                job_type=JobType.INTERNAL_PERSONS.value,
                status=JobStatus.QUEUED,
                created_at="2026-04-20T13:00:00Z",
            )

            deleted = service.delete_job("job-queued")

            self.assertTrue(deleted)
            self.assertIsNone(service.get_job("job-queued"))

    def test_job_service_cancel_job_marks_active_job_failed(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            app = create_app()
            app.config["BTP_DATA_DIR"] = os.path.join(tmpdir, "data")
            init_db(app)

            service = JobService(
                app.extensions["btp_db"]["db_path"],
                project_root=tmpdir,
            )
            service.create_job(
                job_id="job-running",
                job_type=JobType.INTERNAL_PERSONS.value,
                status=JobStatus.RUNNING,
                created_at="2026-04-20T13:00:00Z",
                started_at="2026-04-20T13:01:00Z",
                log_path="logs/jobs/job-running.log",
            )

            with patch.object(service, "_find_active_job_pids", return_value=[1234]), patch.object(
                service,
                "_terminate_pid",
            ) as terminate_pid:
                cancelled = service.cancel_job("job-running")

            self.assertEqual("failed", cancelled["status"])
            self.assertEqual(-signal.SIGTERM, cancelled["exit_code"])
            self.assertEqual("Job was cancelled by user", cancelled["error_message"])
            terminate_pid.assert_called_once_with(1234)
            with open(os.path.join(tmpdir, "logs", "jobs", "job-running.log"), "r", encoding="utf-8") as handle:
                self.assertIn("Job was cancelled by user", handle.read())

    @patch("apply_updates_to_pure.session.put")
    def test_apply_process_internal_persons_normalizes_orcid_before_put(self, mock_put):
        import apply_updates_to_pure

        csv_file = pd.DataFrame(
            [
                {
                    "to_be_updated": "X",
                    "updated": "",
                    "PURE_UUID_PERS": "pers-1",
                    "new_id": "orcid",
                    "new_value": "0000-0003-2472-6589?LANG=EN",
                }
            ]
        )
        json_data = [{"uuid": "pers-1", "identifiers": []}]
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_put.return_value = mock_response

        with tempfile.TemporaryDirectory() as tmpdir:
            previous_output_dir = os.environ.get("BTP_OUTPUT_DIR")
            os.environ["BTP_OUTPUT_DIR"] = tmpdir
            try:
                apply_updates_to_pure.process_internal_persons("personstobeupdated_20260421.csv", csv_file, json_data)
            finally:
                if previous_output_dir is None:
                    os.environ.pop("BTP_OUTPUT_DIR", None)
                else:
                    os.environ["BTP_OUTPUT_DIR"] = previous_output_dir

        self.assertEqual("0000-0003-2472-6589", mock_put.call_args.kwargs["json"]["orcid"])

    @patch("apply_updates_to_pure.session.put")
    def test_apply_process_external_persons_updates_duplicate_rows_once_per_uuid(self, mock_put):
        import apply_updates_to_pure

        csv_file = pd.DataFrame(
            [
                {
                    "to_be_updated": "X",
                    "updated": "",
                    "Pure_UUID": "ext-1",
                    "Name": "Alpha",
                    "Alex_ID": "A1",
                    "ORCID": "0000-0001",
                },
                {
                    "to_be_updated": "X",
                    "updated": "",
                    "Pure_UUID": "ext-1",
                    "Name": "Alpha duplicate",
                    "Alex_ID": "A2",
                    "ORCID": "0000-0002",
                },
            ]
        )
        json_data = [{"UUID": "ext-1", "identifiers": []}]
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_put.return_value = mock_response

        with tempfile.TemporaryDirectory() as tmpdir:
            previous_output_dir = os.environ.get("BTP_OUTPUT_DIR")
            os.environ["BTP_OUTPUT_DIR"] = tmpdir
            try:
                apply_updates_to_pure.process_external_persons("ext_pers_update.csv", csv_file, json_data)
            finally:
                if previous_output_dir is None:
                    os.environ.pop("BTP_OUTPUT_DIR", None)
                else:
                    os.environ["BTP_OUTPUT_DIR"] = previous_output_dir

        self.assertEqual(1, mock_put.call_count)
        self.assertEqual(["X"], csv_file["updated"].dropna().unique().tolist())
        self.assertEqual([], [value for value in csv_file["to_be_updated"].fillna("").tolist() if value])

    @patch("apply_updates_to_pure.session.put")
    def test_apply_process_external_orgs_updates_duplicate_rows_once_per_uuid(self, mock_put):
        import apply_updates_to_pure

        csv_file = pd.DataFrame(
            [
                {
                    "to_be_updated": "X",
                    "updated": "",
                    "uuid": "org-1",
                    "pure_name": "Alpha",
                },
                {
                    "to_be_updated": "X",
                    "updated": "",
                    "uuid": "org-1",
                    "pure_name": "Alpha duplicate",
                },
            ]
        )
        json_data = [{"UUID": "org-1", "identifiers": [], "name": "Alpha"}]
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_put.return_value = mock_response

        with tempfile.TemporaryDirectory() as tmpdir:
            previous_output_dir = os.environ.get("BTP_OUTPUT_DIR")
            os.environ["BTP_OUTPUT_DIR"] = tmpdir
            try:
                apply_updates_to_pure.process_external_orgs("external_orgs_to_update.csv", csv_file, json_data)
            finally:
                if previous_output_dir is None:
                    os.environ.pop("BTP_OUTPUT_DIR", None)
                else:
                    os.environ["BTP_OUTPUT_DIR"] = previous_output_dir

        self.assertEqual(1, mock_put.call_count)
        self.assertEqual(["X"], csv_file["updated"].dropna().unique().tolist())
        self.assertEqual([], [value for value in csv_file["to_be_updated"].fillna("").tolist() if value])

    @patch("app.services.jobs.requests.put")
    @patch("app.services.jobs.requests.get")
    def test_job_service_rollback_job_reverts_internal_person_orcid_change(self, mock_get, mock_put):
        with tempfile.TemporaryDirectory() as tmpdir:
            app = create_app()
            app.config["BTP_DATA_DIR"] = os.path.join(tmpdir, "data")
            init_db(app)

            service = JobService(
                app.extensions["btp_db"]["db_path"],
                project_root=tmpdir,
            )
            output_dir = os.path.join(tmpdir, "output", "internal_persons")
            os.makedirs(output_dir, exist_ok=True)
            csv_path = os.path.join(output_dir, "personstobeupdated_20260420.csv")
            json_path = os.path.join(output_dir, "datatotal.json")
            with open(csv_path, "w", encoding="utf-8") as handle:
                handle.write(
                    "to_be_updated,updated,FULL_NAME,person_id,PURE_UUID_PERS,new_id,new_value,uri\n"
                    "X,,Alpha,person-1,pers-1,orcid,0000-0001,\n"
                )
            with open(json_path, "w", encoding="utf-8") as handle:
                handle.write('[{"uuid":"pers-1","orcid":"0000-0000","identifiers":[]}]')

            service.create_job(
                job_id="job-020",
                job_type=JobType.INTERNAL_PERSONS.value,
                status=JobStatus.COMPLETED,
                created_at="2026-04-20T13:00:00Z",
                log_path="logs/jobs/job-020.log",
            )
            service.update_job(
                "job-020",
                artifacts={
                    "canOpen": True,
                    "canApply": True,
                    "artifacts": {
                        "directory": "output/internal_persons",
                        "csv": ["personstobeupdated_20260420.csv"],
                        "json": ["datatotal.json"],
                    },
                },
                results={
                    "entity_label": "persons",
                    "found_label": "Persons found",
                    "ready_label": "Persons ready to update",
                    "updated_label": "Persons updated",
                    "found_count": 1,
                    "ready_count": 0,
                    "updated_count": 1,
                },
            )
            service._capture_apply_change_set(service.get_job("job-020"))
            with open(csv_path, "w", encoding="utf-8") as handle:
                handle.write(
                    "to_be_updated,updated,FULL_NAME,person_id,PURE_UUID_PERS,new_id,new_value,uri\n"
                    ",X,Alpha,person-1,pers-1,orcid,0000-0001,\n"
                )
            service._finalize_apply_change_set(service.get_job("job-020"), service.get_job_change_set("job-020")["id"])

            get_response = MagicMock()
            get_response.json.return_value = {"uuid": "pers-1", "orcid": "0000-0001", "identifiers": []}
            mock_get.return_value = get_response
            put_response = MagicMock()
            mock_put.return_value = put_response

            rollback_job = service.rollback_job("job-020")
            refreshed = service.get_job("job-020")
            change_set = service.get_job_change_set("job-020")

            self.assertEqual("completed", rollback_job["status"])
            self.assertIsNotNone(refreshed["rollback_job_id"])
            self.assertEqual("rolled_back", change_set["status"])
            self.assertEqual("rolled_back", change_set["items"][0]["rollback_status"])
            self.assertEqual("0000-0000", mock_put.call_args.kwargs["json"]["orcid"])

    @patch("app.services.jobs.requests.get")
    def test_job_service_captures_and_finalizes_external_person_change_set(self, mock_get):
        with tempfile.TemporaryDirectory() as tmpdir:
            app = create_app()
            app.config["BTP_DATA_DIR"] = os.path.join(tmpdir, "data")
            init_db(app)

            service = JobService(
                app.extensions["btp_db"]["db_path"],
                project_root=tmpdir,
            )
            output_dir = os.path.join(tmpdir, "output", "external_persons", "job-030")
            os.makedirs(output_dir, exist_ok=True)
            csv_path = os.path.join(output_dir, "ext_pers_update.csv")
            json_path = os.path.join(output_dir, "to_be_updated.json")
            with open(csv_path, "w", encoding="utf-8") as handle:
                handle.write(
                    "to_be_updated,updated,Name,Alex_ID,Pure_UUID,ORCID,Source\n"
                    "X,,Alpha,A1,ext-1,0000-0001,ricgraph\n"
                )
            with open(json_path, "w", encoding="utf-8") as handle:
                handle.write(
                    json.dumps(
                        [
                            {
                                "UUID": "ext-1",
                                "identifiers": [
                                    {"id": "0000-0001", "type": {"uri": ORCID_ID_URI}},
                                    {"id": "A1", "type": {"uri": OPENALEXEX_ID_URI}},
                                ],
                            }
                        ]
                    )
                )

            get_response = MagicMock()
            get_response.json.return_value = {"uuid": "ext-1", "identifiers": []}
            mock_get.return_value = get_response

            service.create_job(
                job_id="job-030",
                job_type=JobType.EXTERNAL_PERSONS.value,
                status=JobStatus.COMPLETED,
                created_at="2026-04-20T13:00:00Z",
                log_path="logs/jobs/job-030.log",
            )
            service.update_job(
                "job-030",
                artifact_dir="output/external_persons/job-030",
                artifacts={
                    "canOpen": True,
                    "canApply": True,
                    "artifacts": {
                        "directory": "output/external_persons/job-030",
                        "csv": ["ext_pers_update.csv"],
                        "json": ["to_be_updated.json"],
                    },
                },
                results={
                    "entity_label": "persons",
                    "found_label": "Persons found",
                    "ready_label": "Persons ready to update",
                    "updated_label": "Persons updated",
                    "found_count": 1,
                    "ready_count": 0,
                    "updated_count": 1,
                },
            )

            change_set_id = service._capture_apply_change_set(service.get_job("job-030"))
            self.assertIsNotNone(change_set_id)

            with open(csv_path, "w", encoding="utf-8") as handle:
                handle.write(
                    "to_be_updated,updated,Name,Alex_ID,Pure_UUID,ORCID,Source\n"
                    ",X,Alpha,A1,ext-1,0000-0001,ricgraph\n"
                )

            service._finalize_apply_change_set(service.get_job("job-030"), change_set_id)
            change_set = service.get_job_change_set("job-030")

            self.assertEqual("applied", change_set["status"])
            self.assertEqual(2, change_set["item_count"])
            self.assertEqual(2, change_set["applied_item_count"])
            self.assertEqual({"applied"}, {item["apply_status"] for item in change_set["items"]})

    @patch("app.services.jobs.requests.put")
    @patch("app.services.jobs.requests.get")
    def test_job_service_rollback_job_reverts_external_person_identifier_changes(self, mock_get, mock_put):
        with tempfile.TemporaryDirectory() as tmpdir:
            app = create_app()
            app.config["BTP_DATA_DIR"] = os.path.join(tmpdir, "data")
            init_db(app)

            service = JobService(
                app.extensions["btp_db"]["db_path"],
                project_root=tmpdir,
            )
            output_dir = os.path.join(tmpdir, "output", "external_persons", "job-031")
            os.makedirs(output_dir, exist_ok=True)
            csv_path = os.path.join(output_dir, "ext_pers_update.csv")
            json_path = os.path.join(output_dir, "to_be_updated.json")
            with open(csv_path, "w", encoding="utf-8") as handle:
                handle.write(
                    "to_be_updated,updated,Name,Alex_ID,Pure_UUID,ORCID,Source\n"
                    "X,,Alpha,A1,ext-1,0000-0001,ricgraph\n"
                )
            with open(json_path, "w", encoding="utf-8") as handle:
                handle.write(
                    json.dumps(
                        [
                            {
                                "UUID": "ext-1",
                                "identifiers": [
                                    {"id": "0000-0001", "type": {"uri": ORCID_ID_URI}},
                                    {"id": "A1", "type": {"uri": OPENALEXEX_ID_URI}},
                                ],
                            }
                        ]
                    )
                )

            first_get_response = MagicMock()
            first_get_response.json.return_value = {"uuid": "ext-1", "identifiers": []}
            second_get_response = MagicMock()
            second_get_response.json.return_value = {
                "uuid": "ext-1",
                "identifiers": [
                    {"id": "0000-0001", "type": {"uri": ORCID_ID_URI}},
                    {"id": "A1", "type": {"uri": OPENALEXEX_ID_URI}},
                ],
            }
            mock_get.side_effect = [first_get_response, second_get_response, second_get_response]
            put_response = MagicMock()
            mock_put.return_value = put_response

            service.create_job(
                job_id="job-031",
                job_type=JobType.EXTERNAL_PERSONS.value,
                status=JobStatus.COMPLETED,
                created_at="2026-04-20T13:00:00Z",
                log_path="logs/jobs/job-031.log",
            )
            service.update_job(
                "job-031",
                artifact_dir="output/external_persons/job-031",
                artifacts={
                    "canOpen": True,
                    "canApply": True,
                    "artifacts": {
                        "directory": "output/external_persons/job-031",
                        "csv": ["ext_pers_update.csv"],
                        "json": ["to_be_updated.json"],
                    },
                },
                results={
                    "entity_label": "persons",
                    "found_label": "Persons found",
                    "ready_label": "Persons ready to update",
                    "updated_label": "Persons updated",
                    "found_count": 1,
                    "ready_count": 0,
                    "updated_count": 1,
                },
            )

            change_set_id = service._capture_apply_change_set(service.get_job("job-031"))
            with open(csv_path, "w", encoding="utf-8") as handle:
                handle.write(
                    "to_be_updated,updated,Name,Alex_ID,Pure_UUID,ORCID,Source\n"
                    ",X,Alpha,A1,ext-1,0000-0001,ricgraph\n"
                )
            service._finalize_apply_change_set(service.get_job("job-031"), change_set_id)

            rollback_job = service.rollback_job("job-031")
            change_set = service.get_job_change_set("job-031")

            self.assertEqual("completed", rollback_job["status"])
            self.assertEqual("rolled_back", change_set["status"])
            self.assertEqual({"rolled_back"}, {item["rollback_status"] for item in change_set["items"]})
            sent_identifiers = mock_put.call_args.kwargs["json"]["identifiers"]
            self.assertEqual([], sent_identifiers)

    def test_job_service_captures_and_finalizes_external_org_change_set(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            app = create_app()
            app.config["BTP_DATA_DIR"] = os.path.join(tmpdir, "data")
            init_db(app)

            service = JobService(
                app.extensions["btp_db"]["db_path"],
                project_root=tmpdir,
            )
            output_dir = os.path.join(tmpdir, "output", "external_orgs", "job-032")
            os.makedirs(output_dir, exist_ok=True)
            csv_path = os.path.join(output_dir, "external_orgs_to_update.csv")
            json_path = os.path.join(output_dir, "external_orgs_updates.json")
            with open(csv_path, "w", encoding="utf-8") as handle:
                handle.write(
                    "to_be_updated,updated,uuid,pure_name,needs_ror_update,needs_geo_update,ror,pure_address_field\n"
                    "X,,org-1,Alpha,true,true,01abc,address\n"
                )
            with open(json_path, "w", encoding="utf-8") as handle:
                handle.write(
                    json.dumps(
                        [
                            {
                                "uuid": "org-1",
                                "identifiers": [
                                    {"id": "01abc", "type": {"uri": ROR_ID_URI}},
                                ],
                                "address": {"city": "Utrecht"},
                                "_btp_match_metadata": {
                                    "pure_address_field": "address",
                                    "previous_address": {"city": "Amsterdam"},
                                    "pure_address_payload": {"city": "Utrecht"},
                                },
                            }
                        ]
                    )
                )

            service.create_job(
                job_id="job-032",
                job_type=JobType.EXTERNAL_ORGS.value,
                status=JobStatus.COMPLETED,
                created_at="2026-04-20T13:00:00Z",
                log_path="logs/jobs/job-032.log",
            )
            service.update_job(
                "job-032",
                artifact_dir="output/external_orgs/job-032",
                artifacts={
                    "canOpen": True,
                    "canApply": True,
                    "artifacts": {
                        "directory": "output/external_orgs/job-032",
                        "csv": ["external_orgs_to_update.csv"],
                        "json": ["external_orgs_updates.json"],
                    },
                },
                results={
                    "entity_label": "organisations",
                    "found_label": "Organisations found",
                    "ready_label": "Organisations ready to update",
                    "updated_label": "Organisations updated",
                    "rolled_back_label": "Organisations rolled back",
                    "found_count": 1,
                    "ready_count": 0,
                    "updated_count": 1,
                    "rolled_back_count": 0,
                },
            )

            change_set_id = service._capture_apply_change_set(service.get_job("job-032"))
            self.assertIsNotNone(change_set_id)

            with open(csv_path, "w", encoding="utf-8") as handle:
                handle.write(
                    "to_be_updated,updated,uuid,pure_name,needs_ror_update,needs_geo_update,ror,pure_address_field\n"
                    ",X,org-1,Alpha,true,true,01abc,address\n"
                )

            service._finalize_apply_change_set(service.get_job("job-032"), change_set_id)
            change_set = service.get_job_change_set("job-032")

            self.assertEqual("applied", change_set["status"])
            self.assertEqual(2, change_set["item_count"])
            self.assertEqual(2, change_set["applied_item_count"])
            self.assertEqual({"applied"}, {item["apply_status"] for item in change_set["items"]})

    @patch("app.services.jobs.requests.put")
    @patch("app.services.jobs.requests.get")
    def test_job_service_rollback_job_reverts_external_org_changes(self, mock_get, mock_put):
        with tempfile.TemporaryDirectory() as tmpdir:
            app = create_app()
            app.config["BTP_DATA_DIR"] = os.path.join(tmpdir, "data")
            init_db(app)

            service = JobService(
                app.extensions["btp_db"]["db_path"],
                project_root=tmpdir,
            )
            output_dir = os.path.join(tmpdir, "output", "external_orgs", "job-033")
            os.makedirs(output_dir, exist_ok=True)
            csv_path = os.path.join(output_dir, "external_orgs_to_update.csv")
            json_path = os.path.join(output_dir, "external_orgs_updates.json")
            with open(csv_path, "w", encoding="utf-8") as handle:
                handle.write(
                    "to_be_updated,updated,uuid,pure_name,needs_ror_update,needs_geo_update,ror,pure_address_field\n"
                    "X,,org-1,Alpha,true,true,01abc,address\n"
                )
            with open(json_path, "w", encoding="utf-8") as handle:
                handle.write(
                    json.dumps(
                        [
                            {
                                "uuid": "org-1",
                                "identifiers": [
                                    {"id": "01abc", "type": {"uri": ROR_ID_URI}},
                                ],
                                "address": {"city": "Utrecht"},
                                "_btp_match_metadata": {
                                    "pure_address_field": "address",
                                    "previous_address": {"city": "Amsterdam"},
                                    "pure_address_payload": {"city": "Utrecht"},
                                },
                            }
                        ]
                    )
                )

            get_response = MagicMock()
            get_response.json.return_value = {
                "uuid": "org-1",
                "identifiers": [{"id": "01abc", "type": {"uri": ROR_ID_URI}}],
                "address": {"city": "Utrecht"},
            }
            mock_get.return_value = get_response
            mock_put.return_value = MagicMock()

            service.create_job(
                job_id="job-033",
                job_type=JobType.EXTERNAL_ORGS.value,
                status=JobStatus.COMPLETED,
                created_at="2026-04-20T13:00:00Z",
                log_path="logs/jobs/job-033.log",
            )
            service.update_job(
                "job-033",
                artifact_dir="output/external_orgs/job-033",
                artifacts={
                    "canOpen": True,
                    "canApply": True,
                    "artifacts": {
                        "directory": "output/external_orgs/job-033",
                        "csv": ["external_orgs_to_update.csv"],
                        "json": ["external_orgs_updates.json"],
                    },
                },
                results={
                    "entity_label": "organisations",
                    "found_label": "Organisations found",
                    "ready_label": "Organisations ready to update",
                    "updated_label": "Organisations updated",
                    "rolled_back_label": "Organisations rolled back",
                    "found_count": 1,
                    "ready_count": 0,
                    "updated_count": 1,
                    "rolled_back_count": 0,
                },
            )

            change_set_id = service._capture_apply_change_set(service.get_job("job-033"))
            with open(csv_path, "w", encoding="utf-8") as handle:
                handle.write(
                    "to_be_updated,updated,uuid,pure_name,needs_ror_update,needs_geo_update,ror,pure_address_field\n"
                    ",X,org-1,Alpha,true,true,01abc,address\n"
                )
            service._finalize_apply_change_set(service.get_job("job-033"), change_set_id)

            rollback_job = service.rollback_job("job-033")
            refreshed = service.get_job("job-033")
            change_set = service.get_job_change_set("job-033")

            self.assertEqual("completed", rollback_job["status"])
            self.assertEqual("rolled_back", change_set["status"])
            self.assertEqual({"rolled_back"}, {item["rollback_status"] for item in change_set["items"]})
            self.assertEqual(0, refreshed["results"]["updated_count"])
            self.assertEqual(1, refreshed["results"]["rolled_back_count"])
            payload = mock_put.call_args.kwargs["json"]
            self.assertEqual([], payload["identifiers"])
            self.assertEqual({"city": "Amsterdam"}, payload["address"])

    @patch("app.services.jobs.requests.put")
    @patch("app.services.jobs.requests.get")
    def test_job_service_rollback_job_retries_failed_external_person_rollback(self, mock_get, mock_put):
        with tempfile.TemporaryDirectory() as tmpdir:
            app = create_app()
            app.config["BTP_DATA_DIR"] = os.path.join(tmpdir, "data")
            init_db(app)

            service = JobService(
                app.extensions["btp_db"]["db_path"],
                project_root=tmpdir,
            )
            service.create_job(
                job_id="job-031b",
                job_type=JobType.EXTERNAL_PERSONS.value,
                status=JobStatus.COMPLETED,
                created_at="2026-04-20T13:00:00Z",
                log_path="logs/jobs/job-031b.log",
                rollback_job_id="rollback-old",
            )
            service.create_job(
                job_id="rollback-old",
                job_type=JobType.EXTERNAL_PERSONS.value,
                status=JobStatus.FAILED,
                created_at="2026-04-20T13:10:00Z",
                started_at="2026-04-20T13:10:00Z",
                finished_at="2026-04-20T13:10:01Z",
                log_path="logs/jobs/rollback-old.log",
                artifact_dir="output/external_persons/job-031b",
            )

            with connect_db(service.db_path) as connection:
                connection.execute(
                    """
                    INSERT INTO job_change_sets (
                        id, job_id, job_type, status, created_at, applied_at, rolled_back_at, item_count, applied_item_count
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        "changeset-031b",
                        "job-031b",
                        JobType.EXTERNAL_PERSONS.value,
                        "rolled_back_with_conflicts",
                        "2026-04-20T13:00:00Z",
                        "2026-04-20T13:05:00Z",
                        "2026-04-20T13:10:01Z",
                        2,
                        2,
                    ),
                )
                connection.execute(
                    """
                    INSERT INTO job_change_set_items (
                        change_set_id, item_key, entity_uuid, entity_label, field_name, identifier_type,
                        old_value_json, new_value_json, apply_status, rollback_status, conflict_reason
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        "changeset-031b",
                        "ext-1|orcid|0000-0001",
                        "ext-1",
                        "Alpha",
                        "identifiers",
                        "orcid",
                        "null",
                        json.dumps({"id": "0000-0001", "uri": ORCID_ID_URI}),
                        "applied",
                        "rolled_back",
                        None,
                    ),
                )
                connection.execute(
                    """
                    INSERT INTO job_change_set_items (
                        change_set_id, item_key, entity_uuid, entity_label, field_name, identifier_type,
                        old_value_json, new_value_json, apply_status, rollback_status, conflict_reason
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        "changeset-031b",
                        "ext-1|openalex|A1",
                        "ext-1",
                        "Alpha",
                        "identifiers",
                        "openalex",
                        "null",
                        json.dumps({"id": "A1", "uri": OPENALEXEX_ID_URI}),
                        "applied",
                        "failed",
                        "409 conflict",
                    ),
                )
                connection.commit()

            get_response = MagicMock()
            get_response.json.return_value = {
                "uuid": "ext-1",
                "identifiers": [{"id": "A1", "type": {"uri": OPENALEXEX_ID_URI}}],
            }
            mock_get.return_value = get_response
            mock_put.return_value = MagicMock()

            rollback_job = service.rollback_job("job-031b")
            refreshed = service.get_job("job-031b")
            change_set = service.get_job_change_set("job-031b")

            self.assertEqual("completed", rollback_job["status"])
            self.assertNotEqual("rollback-old", refreshed["rollback_job_id"])
            self.assertEqual("rolled_back", change_set["status"])
            self.assertEqual({"rolled_back"}, {item["rollback_status"] for item in change_set["items"]})
            sent_identifiers = mock_put.call_args.kwargs["json"]["identifiers"]
            self.assertEqual([], sent_identifiers)

    def test_job_service_enriches_results_with_rolled_back_counts(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            app = create_app()
            app.config["BTP_DATA_DIR"] = os.path.join(tmpdir, "data")
            init_db(app)

            service = JobService(
                app.extensions["btp_db"]["db_path"],
                project_root=tmpdir,
            )
            service.create_job(
                job_id="job-rolled",
                job_type=JobType.EXTERNAL_PERSONS.value,
                status=JobStatus.COMPLETED,
                created_at="2026-04-20T13:00:00Z",
                log_path="logs/jobs/job-rolled.log",
                rollback_job_id="rollback-rolled",
            )
            service.update_job(
                "job-rolled",
                results={
                    "entity_label": "persons",
                    "found_label": "Persons found",
                    "ready_label": "Persons ready to update",
                    "updated_label": "Persons updated",
                    "rolled_back_label": "Persons rolled back",
                    "found_count": 1,
                    "ready_count": 0,
                    "updated_count": 1,
                    "rolled_back_count": 0,
                },
            )

            with connect_db(service.db_path) as connection:
                connection.execute(
                    """
                    INSERT INTO job_change_sets (
                        id, job_id, job_type, status, created_at, applied_at, rolled_back_at, item_count, applied_item_count
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        "changeset-rolled",
                        "job-rolled",
                        JobType.EXTERNAL_PERSONS.value,
                        "rolled_back",
                        "2026-04-20T13:00:00Z",
                        "2026-04-20T13:05:00Z",
                        "2026-04-20T13:10:00Z",
                        1,
                        1,
                    ),
                )
                connection.execute(
                    """
                    INSERT INTO job_change_set_items (
                        change_set_id, item_key, entity_uuid, entity_label, field_name, identifier_type,
                        old_value_json, new_value_json, apply_status, rollback_status, conflict_reason
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        "changeset-rolled",
                        "ext-1|openalex|A1",
                        "ext-1",
                        "Alpha",
                        "identifiers",
                        "openalex",
                        "null",
                        json.dumps({"id": "A1", "uri": OPENALEXEX_ID_URI}),
                        "applied",
                        "rolled_back",
                        None,
                    ),
                )
                connection.commit()

            job = service.get_job("job-rolled")

            self.assertEqual(0, job["results"]["updated_count"])
            self.assertEqual(1, job["results"]["rolled_back_count"])

    @patch("app.services.jobs.requests.put")
    @patch("app.services.jobs.requests.get")
    def test_job_service_rollback_job_clears_internal_person_orcid_with_null_when_old_value_missing(self, mock_get, mock_put):
        with tempfile.TemporaryDirectory() as tmpdir:
            app = create_app()
            app.config["BTP_DATA_DIR"] = os.path.join(tmpdir, "data")
            init_db(app)

            service = JobService(
                app.extensions["btp_db"]["db_path"],
                project_root=tmpdir,
            )
            output_dir = os.path.join(tmpdir, "output", "internal_persons")
            os.makedirs(output_dir, exist_ok=True)
            csv_path = os.path.join(output_dir, "personstobeupdated_20260420.csv")
            json_path = os.path.join(output_dir, "datatotal.json")
            with open(csv_path, "w", encoding="utf-8") as handle:
                handle.write(
                    "to_be_updated,updated,FULL_NAME,person_id,PURE_UUID_PERS,new_id,new_value,uri\n"
                    "X,,Alpha,person-1,pers-1,orcid,0000-0001,\n"
                )
            with open(json_path, "w", encoding="utf-8") as handle:
                handle.write('[{"uuid":"pers-1","identifiers":[]}]')

            service.create_job(
                job_id="job-020b",
                job_type=JobType.INTERNAL_PERSONS.value,
                status=JobStatus.COMPLETED,
                created_at="2026-04-20T13:00:00Z",
                log_path="logs/jobs/job-020b.log",
            )
            service.update_job(
                "job-020b",
                artifacts={
                    "canOpen": True,
                    "canApply": True,
                    "artifacts": {
                        "directory": "output/internal_persons",
                        "csv": ["personstobeupdated_20260420.csv"],
                        "json": ["datatotal.json"],
                    },
                },
                results={
                    "entity_label": "persons",
                    "found_label": "Persons found",
                    "ready_label": "Persons ready to update",
                    "updated_label": "Persons updated",
                    "found_count": 1,
                    "ready_count": 0,
                    "updated_count": 1,
                },
            )
            service._capture_apply_change_set(service.get_job("job-020b"))
            with open(csv_path, "w", encoding="utf-8") as handle:
                handle.write(
                    "to_be_updated,updated,FULL_NAME,person_id,PURE_UUID_PERS,new_id,new_value,uri\n"
                    ",X,Alpha,person-1,pers-1,orcid,0000-0001,\n"
                )
            service._finalize_apply_change_set(service.get_job("job-020b"), service.get_job_change_set("job-020b")["id"])

            get_response = MagicMock()
            get_response.json.return_value = {"uuid": "pers-1", "orcid": "0000-0001", "identifiers": []}
            mock_get.return_value = get_response
            put_response = MagicMock()
            mock_put.return_value = put_response

            rollback_job = service.rollback_job("job-020b")
            change_set = service.get_job_change_set("job-020b")

            self.assertEqual("completed", rollback_job["status"])
            self.assertEqual("rolled_back", change_set["status"])
            self.assertEqual("rolled_back", change_set["items"][0]["rollback_status"])
            self.assertIn("orcid", mock_put.call_args.kwargs["json"])
            self.assertIsNone(mock_put.call_args.kwargs["json"]["orcid"])

    @patch("app.services.jobs.requests.put")
    @patch("app.services.jobs.requests.get")
    def test_job_service_rollback_job_skips_conflict_when_current_value_changed(self, mock_get, mock_put):
        with tempfile.TemporaryDirectory() as tmpdir:
            app = create_app()
            app.config["BTP_DATA_DIR"] = os.path.join(tmpdir, "data")
            init_db(app)

            service = JobService(
                app.extensions["btp_db"]["db_path"],
                project_root=tmpdir,
            )
            output_dir = os.path.join(tmpdir, "output", "internal_persons")
            os.makedirs(output_dir, exist_ok=True)
            csv_path = os.path.join(output_dir, "personstobeupdated_20260420.csv")
            json_path = os.path.join(output_dir, "datatotal.json")
            with open(csv_path, "w", encoding="utf-8") as handle:
                handle.write(
                    "to_be_updated,updated,FULL_NAME,person_id,PURE_UUID_PERS,new_id,new_value,uri\n"
                    "X,,Alpha,person-1,pers-1,orcid,0000-0001,\n"
                )
            with open(json_path, "w", encoding="utf-8") as handle:
                handle.write('[{"uuid":"pers-1","orcid":"0000-0000","identifiers":[]}]')

            service.create_job(
                job_id="job-021",
                job_type=JobType.INTERNAL_PERSONS.value,
                status=JobStatus.COMPLETED,
                created_at="2026-04-20T13:00:00Z",
                log_path="logs/jobs/job-021.log",
            )
            service.update_job(
                "job-021",
                artifacts={
                    "canOpen": True,
                    "canApply": True,
                    "artifacts": {
                        "directory": "output/internal_persons",
                        "csv": ["personstobeupdated_20260420.csv"],
                        "json": ["datatotal.json"],
                    },
                },
                results={
                    "entity_label": "persons",
                    "found_label": "Persons found",
                    "ready_label": "Persons ready to update",
                    "updated_label": "Persons updated",
                    "found_count": 1,
                    "ready_count": 0,
                    "updated_count": 1,
                },
            )
            service._capture_apply_change_set(service.get_job("job-021"))
            with open(csv_path, "w", encoding="utf-8") as handle:
                handle.write(
                    "to_be_updated,updated,FULL_NAME,person_id,PURE_UUID_PERS,new_id,new_value,uri\n"
                    ",X,Alpha,person-1,pers-1,orcid,0000-0001,\n"
                )
            service._finalize_apply_change_set(service.get_job("job-021"), service.get_job_change_set("job-021")["id"])

            get_response = MagicMock()
            get_response.json.return_value = {"uuid": "pers-1", "orcid": "9999-9999", "identifiers": []}
            mock_get.return_value = get_response

            rollback_job = service.rollback_job("job-021")
            change_set = service.get_job_change_set("job-021")

            self.assertEqual("completed", rollback_job["status"])
            self.assertEqual("rolled_back_with_conflicts", change_set["status"])
            self.assertEqual("conflict", change_set["items"][0]["rollback_status"])
            mock_put.assert_not_called()

    def test_job_service_rollback_job_rejects_change_set_with_zero_applied_items(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            app = create_app()
            app.config["BTP_DATA_DIR"] = os.path.join(tmpdir, "data")
            init_db(app)

            service = JobService(
                app.extensions["btp_db"]["db_path"],
                project_root=tmpdir,
            )
            output_dir = os.path.join(tmpdir, "output", "internal_persons")
            os.makedirs(output_dir, exist_ok=True)
            with open(os.path.join(output_dir, "personstobeupdated_20260420.csv"), "w", encoding="utf-8") as handle:
                handle.write(
                    "to_be_updated,updated,FULL_NAME,person_id,PURE_UUID_PERS,new_id,new_value,uri\n"
                    "X,,Alpha,person-1,pers-1,orcid,0000-0001,\n"
                )
            with open(os.path.join(output_dir, "datatotal.json"), "w", encoding="utf-8") as handle:
                handle.write('[{"uuid":"pers-1","orcid":"0000-0000","identifiers":[]}]')

            service.create_job(
                job_id="job-022",
                job_type=JobType.INTERNAL_PERSONS.value,
                status=JobStatus.COMPLETED,
                created_at="2026-04-20T13:00:00Z",
                log_path="logs/jobs/job-022.log",
            )
            service.update_job(
                "job-022",
                artifacts={
                    "canOpen": True,
                    "canApply": True,
                    "artifacts": {
                        "directory": "output/internal_persons",
                        "csv": ["personstobeupdated_20260420.csv"],
                        "json": ["datatotal.json"],
                    },
                },
                results={
                    "entity_label": "persons",
                    "found_label": "Persons found",
                    "ready_label": "Persons ready to update",
                    "updated_label": "Persons updated",
                    "found_count": 1,
                    "ready_count": 1,
                    "updated_count": 0,
                },
            )
            service._capture_apply_change_set(service.get_job("job-022"))
            service._finalize_apply_change_set(service.get_job("job-022"), service.get_job_change_set("job-022")["id"])

            change_set = service.get_job_change_set("job-022")

            self.assertEqual("no_applied_changes", change_set["status"])
            self.assertEqual(0, change_set["applied_item_count"])
            with self.assertRaises(ValueError):
                service.rollback_job("job-022")

    def test_job_service_rejects_deleting_active_job(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            app = create_app()
            app.config["BTP_DATA_DIR"] = os.path.join(tmpdir, "data")
            init_db(app)

            service = JobService(
                app.extensions["btp_db"]["db_path"],
                project_root=tmpdir,
            )
            service.create_job(
                job_id="job-001",
                job_type=JobType.INTERNAL_PERSONS.value,
                status=JobStatus.RUNNING,
                created_at="2026-04-20T13:00:00Z",
            )

            with self.assertRaises(ValueError):
                service.delete_job("job-001")

    @patch("app.services.jobs.requests.delete")
    @patch("app.services.jobs.requests.get")
    def test_job_service_rolls_back_research_output_and_created_external_person(self, mock_get, mock_delete):
        with tempfile.TemporaryDirectory() as tmpdir:
            app = create_app()
            app.config["BTP_DATA_DIR"] = os.path.join(tmpdir, "data")
            init_db(app)

            service = JobService(app.extensions["btp_db"]["db_path"], project_root=tmpdir)
            service.create_job(
                job_id="job-ro-rb",
                job_type=JobType.RESEARCH_OUTPUTS.value,
                status=JobStatus.COMPLETED,
                created_at="2026-04-20T13:00:00Z",
                log_path="logs/jobs/job-ro-rb.log",
                artifact_dir="output/research_output/job-ro-rb",
            )

            with connect_db(app.extensions["btp_db"]["db_path"]) as connection:
                connection.execute(
                    """
                    INSERT INTO job_change_sets (
                        id, job_id, job_type, status, created_at, applied_at, item_count, applied_item_count
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        "changeset-ro-rb",
                        "job-ro-rb",
                        JobType.RESEARCH_OUTPUTS.value,
                        "applied",
                        "2026-04-20T13:01:00Z",
                        "2026-04-20T13:02:00Z",
                        1,
                        1,
                    ),
                )
                connection.execute(
                    """
                    INSERT INTO job_change_set_items (
                        change_set_id, item_key, entity_uuid, entity_label, field_name, identifier_type,
                        old_value_json, new_value_json, apply_status, rollback_status, conflict_reason
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        "changeset-ro-rb",
                        "10.1/test",
                        "10.1/test",
                        "Test output",
                        "record",
                        "research_output",
                        "null",
                        json.dumps(
                            {
                                "doi": "10.1/test",
                                "record_type": "research_outputs",
                                "record_uuid": "ro-uuid-1",
                                "created_external_persons": [
                                    {"uuid": "ext-uuid-1", "first_name": "Ext", "last_name": "Person"}
                                ],
                            },
                            sort_keys=True,
                        ),
                        "applied",
                        "pending",
                        None,
                    ),
                )
                connection.commit()

            mock_get.side_effect = [
                MagicMock(json=lambda: {"electronicVersions": [{"doi": "10.1/test"}]}, raise_for_status=MagicMock()),
                MagicMock(json=lambda: {"name": {"firstName": "Ext", "lastName": "Person"}}, raise_for_status=MagicMock()),
            ]
            mock_delete.side_effect = [
                MagicMock(raise_for_status=MagicMock()),
                MagicMock(raise_for_status=MagicMock()),
            ]

            rollback_job = service.rollback_job("job-ro-rb")
            change_set = service.get_job_change_set("job-ro-rb")

            self.assertEqual("completed", rollback_job["status"])
            self.assertEqual("rolled_back", change_set["status"])
            self.assertEqual("rolled_back", change_set["items"][0]["rollback_status"])
            deleted_urls = [call.args[0] for call in mock_delete.call_args_list]
            self.assertIn(f"{PURE_BASE_URL}research-outputs/ro-uuid-1", deleted_urls)
            self.assertIn(f"{PURE_BASE_URL}external-persons/ext-uuid-1", deleted_urls)

    @patch("app.services.jobs.requests.delete")
    @patch("app.services.jobs.requests.get")
    def test_job_service_rolls_back_dataset_creation(self, mock_get, mock_delete):
        with tempfile.TemporaryDirectory() as tmpdir:
            app = create_app()
            app.config["BTP_DATA_DIR"] = os.path.join(tmpdir, "data")
            init_db(app)

            service = JobService(app.extensions["btp_db"]["db_path"], project_root=tmpdir)
            service.create_job(
                job_id="job-ds-rb",
                job_type=JobType.DATASETS.value,
                status=JobStatus.COMPLETED,
                created_at="2026-04-20T13:00:00Z",
                log_path="logs/jobs/job-ds-rb.log",
                artifact_dir="output/datasets/job-ds-rb",
            )

            with connect_db(app.extensions["btp_db"]["db_path"]) as connection:
                connection.execute(
                    """
                    INSERT INTO job_change_sets (
                        id, job_id, job_type, status, created_at, applied_at, item_count, applied_item_count
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        "changeset-ds-rb",
                        "job-ds-rb",
                        JobType.DATASETS.value,
                        "applied",
                        "2026-04-20T13:01:00Z",
                        "2026-04-20T13:02:00Z",
                        1,
                        1,
                    ),
                )
                connection.execute(
                    """
                    INSERT INTO job_change_set_items (
                        change_set_id, item_key, entity_uuid, entity_label, field_name, identifier_type,
                        old_value_json, new_value_json, apply_status, rollback_status, conflict_reason
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        "changeset-ds-rb",
                        "10.2/test",
                        "10.2/test",
                        "Test dataset",
                        "record",
                        "dataset",
                        "null",
                        json.dumps(
                            {
                                "doi": "10.2/test",
                                "record_type": "datasets",
                                "record_uuid": "ds-uuid-1",
                                "created_external_persons": [],
                            },
                            sort_keys=True,
                        ),
                        "applied",
                        "pending",
                        None,
                    ),
                )
                connection.commit()

            mock_get.return_value = MagicMock(
                json=lambda: {"doi": {"doi": "10.2/test"}},
                raise_for_status=MagicMock(),
            )
            mock_delete.return_value = MagicMock(raise_for_status=MagicMock())

            rollback_job = service.rollback_job("job-ds-rb")
            change_set = service.get_job_change_set("job-ds-rb")

            self.assertEqual("completed", rollback_job["status"])
            self.assertEqual("rolled_back", change_set["status"])
            self.assertEqual("rolled_back", change_set["items"][0]["rollback_status"])
            self.assertEqual(f"{PURE_BASE_URL}data-sets/ds-uuid-1", mock_delete.call_args.args[0])

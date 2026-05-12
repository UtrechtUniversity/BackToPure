import os
import tempfile
import unittest
from unittest.mock import MagicMock, patch

from requests.exceptions import RequestException

from app import create_app
from app import routes
from app.db import connect_db, init_db
from app.services import JobService


class FakeStream:
    def __init__(self, lines):
        self._lines = list(lines)

    def readline(self):
        if self._lines:
            return self._lines.pop(0)
        return ""

    def close(self):
        return None


class FakeProcess:
    def __init__(self, stdout_lines=None, stderr_lines=None, returncode=0):
        self.stdout = FakeStream(stdout_lines or [])
        self.stderr = FakeStream(stderr_lines or [])
        self.returncode = returncode
        self.pid = 12345

    def wait(self):
        return self.returncode


class RouteHelperTests(unittest.TestCase):
    def test_resolve_output_target_maps_known_sources(self):
        directory, required = routes._resolve_output_target("import_research_outputs")
        self.assertEqual("output/research_output", directory)
        self.assertEqual(["to_be_updated.csv"], required["csv"])
        self.assertEqual(["output_to_be_updated.json"], required["json"])

    def test_resolve_output_target_rejects_unknown_source(self):
        directory, required = routes._resolve_output_target("unknown")
        self.assertIsNone(directory)
        self.assertIsNone(required)

    def test_has_prefixed_files_detects_matching_entries(self):
        with patch("app.routes.os.listdir", return_value=["personstobeupdated_20260420.csv", "other.txt"]):
            self.assertTrue(routes._has_prefixed_files("/tmp/ignored", ["personstobeupdated_"], ".csv"))

    def test_has_named_files_detects_expected_file(self):
        with patch("app.routes.os.path.isfile", side_effect=lambda path: path.endswith("a.json")):
            self.assertTrue(routes._has_named_files("/tmp/ignored", ["a.json", "b.json"]))


class FlaskRouteTests(unittest.TestCase):
    def setUp(self):
        self.tempdir = tempfile.TemporaryDirectory()
        self.app = create_app()
        self.app.config["BTP_DATA_DIR"] = os.path.join(self.tempdir.name, "data")
        self.app.config["BTP_PROJECT_ROOT"] = self.tempdir.name
        self.app.config["BTP_FRONTEND_DIST"] = os.path.join(self.tempdir.name, "frontend-dist")
        init_db(self.app)
        self.client = self.app.test_client()

    def tearDown(self):
        self.tempdir.cleanup()

    def test_frontend_app_returns_503_when_dist_missing(self):
        result = self.client.get("/app")

        self.assertEqual(503, result.status_code)
        self.assertEqual("Frontend build not found", result.get_json()["error"])

    def test_frontend_app_serves_index_spa_routes_and_assets(self):
        frontend_dist = os.path.join(self.tempdir.name, "frontend-dist")
        os.makedirs(os.path.join(frontend_dist, "assets"), exist_ok=True)
        with open(os.path.join(frontend_dist, "index.html"), "w", encoding="utf-8") as handle:
            handle.write("<html><body>frontend shell</body></html>")
        with open(os.path.join(frontend_dist, "assets", "app.js"), "w", encoding="utf-8") as handle:
            handle.write("console.log('asset');")
        self.app.config["BTP_FRONTEND_DIST"] = frontend_dist

        with self.client.get("/app") as index_result:
            self.assertEqual(200, index_result.status_code)
            self.assertIn("frontend shell", index_result.get_data(as_text=True))

        with self.client.get("/app/jobs/job-001") as nested_result:
            self.assertEqual(200, nested_result.status_code)
            self.assertIn("frontend shell", nested_result.get_data(as_text=True))

        with self.client.get("/app/assets/app.js") as asset_result:
            self.assertEqual(200, asset_result.status_code)
            self.assertIn("console.log('asset');", asset_result.get_data(as_text=True))

    @patch("app.routes.requests.get")
    def test_faculties_returns_options(self, mock_get):
        response = MagicMock()
        response.json.return_value = {"results": [{"_key": "fac-1", "value": "Science"}]}
        response.raise_for_status.return_value = None
        mock_get.return_value = response

        result = self.client.get("/faculties")

        self.assertEqual(200, result.status_code)
        self.assertEqual([{"value": "fac-1", "label": "Science"}], result.get_json())

    @patch("app.routes.requests.get", side_effect=RequestException("down"))
    def test_faculties_returns_error_when_ricgraph_unavailable(self, _mock_get):
        result = self.client.get("/faculties")

        self.assertEqual(500, result.status_code)
        self.assertEqual({"error": "Cannot connect to ricgraph"}, result.get_json())

    @patch("app.routes.requests.get")
    def test_api_faculties_returns_items(self, mock_get):
        response = MagicMock()
        response.json.return_value = {"results": [{"_key": "fac-1", "value": "Science"}]}
        response.raise_for_status.return_value = None
        mock_get.return_value = response

        result = self.client.get("/api/faculties")

        self.assertEqual(200, result.status_code)
        self.assertEqual({"items": [{"value": "fac-1", "label": "Science"}]}, result.get_json())

    @patch("app.routes.requests.get", side_effect=RequestException("down"))
    def test_api_faculties_returns_error_when_ricgraph_unavailable(self, _mock_get):
        result = self.client.get("/api/faculties")

        self.assertEqual(500, result.status_code)
        self.assertEqual({"error": "Cannot connect to ricgraph"}, result.get_json())

    def test_api_create_job_requires_job_type(self):
        result = self.client.post("/api/jobs", json={"params": {}})

        self.assertEqual(400, result.status_code)
        self.assertEqual({"error": "jobType is required"}, result.get_json())

    def test_api_create_job_rejects_non_object_params(self):
        result = self.client.post("/api/jobs", json={"jobType": "internal_persons", "params": []})

        self.assertEqual(400, result.status_code)
        self.assertEqual({"error": "params must be an object"}, result.get_json())

    def test_api_create_job_returns_created_job(self):
        result = self.client.post(
            "/api/jobs",
            json={"jobType": "internal_persons", "params": {"facultyChoice": "all"}},
        )

        body = result.get_json()

        self.assertEqual(201, result.status_code)
        self.assertEqual("internal_persons", body["job_type"])
        self.assertEqual("queued", body["status"])
        self.assertEqual({"facultyChoice": "all"}, body["params"])
        self.assertFalse(body["canOpen"])
        self.assertFalse(body["canApply"])
        self.assertEqual({"directory": body["artifact_dir"], "csv": [], "json": []}, body["artifacts"])
        self.assertTrue(body["artifact_dir"].startswith("output/internal_persons/job_"))
        self.assertTrue(body["id"].startswith("job_"))

    def test_api_create_external_persons_job_returns_job_scoped_artifact_dir(self):
        result = self.client.post(
            "/api/jobs",
            json={
                "jobType": "external_persons",
                "params": {"facultyChoice": "all", "useOpenAlexFallback": "yes"},
            },
        )

        body = result.get_json()

        self.assertEqual(201, result.status_code)
        self.assertEqual("external_persons", body["job_type"])
        self.assertTrue(body["artifact_dir"].startswith("output/external_persons/job_"))
        self.assertEqual({"directory": body["artifact_dir"], "csv": [], "json": []}, body["artifacts"])

    def test_api_create_research_outputs_job_returns_job_scoped_artifact_dir(self):
        result = self.client.post(
            "/api/jobs",
            json={
                "jobType": "research_outputs",
                "params": {"facultyChoice": "all"},
            },
        )

        body = result.get_json()

        self.assertEqual(201, result.status_code)
        self.assertEqual("research_outputs", body["job_type"])
        self.assertTrue(body["artifact_dir"].startswith("output/research_output/job_"))
        self.assertEqual({"directory": body["artifact_dir"], "csv": [], "json": []}, body["artifacts"])

    def test_api_create_datasets_job_returns_job_scoped_artifact_dir(self):
        result = self.client.post(
            "/api/jobs",
            json={
                "jobType": "datasets",
                "params": {"facultyChoice": "all"},
            },
        )

        body = result.get_json()

        self.assertEqual(201, result.status_code)
        self.assertEqual("datasets", body["job_type"])
        self.assertTrue(body["artifact_dir"].startswith("output/datasets/job_"))
        self.assertEqual({"directory": body["artifact_dir"], "csv": [], "json": []}, body["artifacts"])

    def test_api_create_job_rejects_invalid_job_type(self):
        result = self.client.post("/api/jobs", json={"jobType": "bad-type"})

        self.assertEqual(400, result.status_code)
        self.assertIn("Unsupported job type", result.get_json()["error"])

    def test_api_create_job_allows_second_active_internal_persons_job_with_isolated_artifacts(self):
        first = self.client.post("/api/jobs", json={"jobType": "internal_persons"}).get_json()

        result = self.client.post("/api/jobs", json={"jobType": "internal_persons"})

        self.assertEqual(201, result.status_code)
        second = result.get_json()
        self.assertNotEqual(first["artifact_dir"], second["artifact_dir"])

    def test_api_list_jobs_returns_created_jobs(self):
        service = JobService(
            self.app.extensions["btp_db"]["db_path"],
            project_root=self.tempdir.name,
        )
        created = service.create_job(
            job_id="job-001",
            job_type="internal_persons",
            created_at="2026-04-20T13:00:00Z",
        )

        result = self.client.get("/api/jobs")

        self.assertEqual(200, result.status_code)
        body = result.get_json()
        self.assertEqual(1, len(body["items"]))
        self.assertEqual(created, body["items"][0])

    def test_api_results_dashboard_returns_workflow_totals(self):
        service = JobService(
            self.app.extensions["btp_db"]["db_path"],
            project_root=self.tempdir.name,
        )
        service.create_job(
            job_id="job-001",
            job_type="internal_persons",
            status="completed",
            created_at="2026-04-20T13:00:00Z",
        )
        with connect_db(self.app.extensions["btp_db"]["db_path"]) as connection:
            connection.execute(
                """
                INSERT INTO job_change_sets (id, job_id, job_type, status, created_at, applied_at, item_count, applied_item_count)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    "changeset-001",
                    "job-001",
                    "internal_persons",
                    "applied",
                    "2026-04-20T13:00:00Z",
                    "2026-04-20T13:01:00Z",
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
                    "changeset-001",
                    "uuid-1|orcid|0000-1",
                    "uuid-1",
                    "Alpha",
                    "identifier",
                    "orcid",
                    "null",
                    '{"value":"0000-1"}',
                    "applied",
                    "pending",
                    None,
                ),
            )
            connection.commit()

        result = self.client.get("/api/results-dashboard")

        self.assertEqual(200, result.status_code)
        body = result.get_json()
        self.assertEqual(1, body["totals"]["net_items"])
        internal = next(item for item in body["workflows"] if item["job_type"] == "internal_persons")
        self.assertEqual("Internal Persons", internal["label"])
        self.assertEqual(1, internal["net_entities"])

    def test_api_results_dashboard_rejects_non_positive_days(self):
        result = self.client.get("/api/results-dashboard?days=0")

        self.assertEqual(400, result.status_code)
        self.assertEqual({"error": "days must be a positive integer"}, result.get_json())

    def test_api_get_job_returns_not_found_for_missing_job(self):
        result = self.client.get("/api/jobs/missing-job")

        self.assertEqual(404, result.status_code)
        self.assertEqual({"error": "Job not found: missing-job"}, result.get_json())

    def test_api_get_job_returns_existing_job(self):
        service = JobService(
            self.app.extensions["btp_db"]["db_path"],
            project_root=self.tempdir.name,
        )
        created = service.create_job(
            job_id="job-001",
            job_type="internal_persons",
            created_at="2026-04-20T13:00:00Z",
        )

        result = self.client.get("/api/jobs/job-001")

        self.assertEqual(200, result.status_code)
        self.assertEqual(created, result.get_json())

    def test_api_get_job_review_table_returns_primary_csv_rows(self):
        service = JobService(
            self.app.extensions["btp_db"]["db_path"],
            project_root=self.tempdir.name,
        )
        artifact_dir = os.path.join("output", "internal_persons", "job-001")
        os.makedirs(os.path.join(self.tempdir.name, artifact_dir), exist_ok=True)
        with open(
            os.path.join(self.tempdir.name, artifact_dir, "personstobeupdated_20260421.csv"),
            "w",
            encoding="utf-8",
        ) as handle:
            handle.write("to_be_updated,updated,PURE_UUID_PERS,FULL_NAME\nX, ,uuid-1,Alpha\n")

        service.create_job(
            job_id="job-001",
            job_type="internal_persons",
            status="needs_review",
            artifact_dir=artifact_dir,
            created_at="2026-04-20T13:00:00Z",
        )

        result = self.client.get("/api/jobs/job-001/review-table")

        self.assertEqual(200, result.status_code)
        body = result.get_json()
        self.assertEqual("personstobeupdated_20260421.csv", body["fileName"])
        self.assertEqual(1, body["rowCount"])
        self.assertEqual("Alpha", body["rows"][0]["FULL_NAME"])

    def test_api_get_job_review_table_returns_empty_dataset_table_with_default_columns(self):
        service = JobService(
            self.app.extensions["btp_db"]["db_path"],
            project_root=self.tempdir.name,
        )
        artifact_dir = os.path.join("output", "datasets", "job-001")
        os.makedirs(os.path.join(self.tempdir.name, artifact_dir), exist_ok=True)
        with open(
            os.path.join(self.tempdir.name, artifact_dir, "to_be_updated.csv"),
            "w",
            encoding="utf-8",
        ) as handle:
            handle.write("\n")
        with open(
            os.path.join(self.tempdir.name, artifact_dir, "datasets_to_be_updated.json"),
            "w",
            encoding="utf-8",
        ) as handle:
            handle.write("[]")

        service.create_job(
            job_id="job-001",
            job_type="datasets",
            status="needs_review",
            artifact_dir=artifact_dir,
            created_at="2026-04-20T13:00:00Z",
        )

        result = self.client.get("/api/jobs/job-001/review-table")

        self.assertEqual(200, result.status_code)
        body = result.get_json()
        self.assertEqual("to_be_updated.csv", body["fileName"])
        self.assertEqual(["to_be_updated", "updated", "doi", "title"], body["columns"])
        self.assertEqual(0, body["rowCount"])
        self.assertEqual([], body["rows"])

    def test_api_get_job_review_table_returns_not_found_when_csv_missing(self):
        service = JobService(
            self.app.extensions["btp_db"]["db_path"],
            project_root=self.tempdir.name,
        )
        service.create_job(
            job_id="job-001",
            job_type="internal_persons",
            status="needs_review",
            artifact_dir=os.path.join("output", "internal_persons", "job-001"),
            created_at="2026-04-20T13:00:00Z",
        )

        result = self.client.get("/api/jobs/job-001/review-table")

        self.assertEqual(404, result.status_code)

    def test_api_update_job_review_table_saves_selection_changes(self):
        service = JobService(
            self.app.extensions["btp_db"]["db_path"],
            project_root=self.tempdir.name,
        )
        artifact_dir = os.path.join("output", "internal_persons", "job-001")
        os.makedirs(os.path.join(self.tempdir.name, artifact_dir), exist_ok=True)
        with open(
            os.path.join(self.tempdir.name, artifact_dir, "personstobeupdated_20260421.csv"),
            "w",
            encoding="utf-8",
        ) as handle:
            handle.write("to_be_updated,updated,PURE_UUID_PERS,FULL_NAME\nX, ,uuid-1,Alpha\n, ,uuid-2,Beta\n")

        service.create_job(
            job_id="job-001",
            job_type="internal_persons",
            status="needs_review",
            artifact_dir=artifact_dir,
            created_at="2026-04-20T13:00:00Z",
        )

        result = self.client.post(
            "/api/jobs/job-001/review-table",
            json={
                "fileName": "personstobeupdated_20260421.csv",
                "expectedRowCount": 2,
                "updates": [
                    {"rowIndex": 0, "to_be_updated": ""},
                    {"rowIndex": 1, "to_be_updated": "X"},
                ],
            },
        )

        self.assertEqual(200, result.status_code)
        body = result.get_json()
        self.assertEqual("", body["rows"][0]["to_be_updated"])
        self.assertEqual("X", body["rows"][1]["to_be_updated"])

    def test_api_update_job_review_table_rejects_bad_payload(self):
        result = self.client.post(
            "/api/jobs/job-001/review-table",
            json={"fileName": "", "expectedRowCount": "2", "updates": {}},
        )

        self.assertEqual(400, result.status_code)

    def test_api_delete_job_returns_not_found_for_missing_job(self):
        result = self.client.delete("/api/jobs/missing-job")

        self.assertEqual(404, result.status_code)
        self.assertEqual({"error": "Job not found: missing-job"}, result.get_json())

    def test_api_delete_job_rejects_active_job(self):
        service = JobService(
            self.app.extensions["btp_db"]["db_path"],
            project_root=self.tempdir.name,
        )
        service.create_job(
            job_id="job-001",
            job_type="internal_persons",
            status="running",
            created_at="2026-04-20T13:00:00Z",
        )

        result = self.client.delete("/api/jobs/job-001")

        self.assertEqual(400, result.status_code)
        self.assertIn("cannot be deleted while it is active", result.get_json()["error"])

    def test_api_delete_job_removes_inactive_job(self):
        service = JobService(
            self.app.extensions["btp_db"]["db_path"],
            project_root=self.tempdir.name,
        )
        logs_dir = os.path.join(self.tempdir.name, "logs", "jobs")
        os.makedirs(logs_dir, exist_ok=True)
        with open(os.path.join(logs_dir, "job-001.log"), "w", encoding="utf-8") as handle:
            handle.write("hello\n")
        service.create_job(
            job_id="job-001",
            job_type="internal_persons",
            status="failed",
            created_at="2026-04-20T13:00:00Z",
            log_path="logs/jobs/job-001.log",
        )

        result = self.client.delete("/api/jobs/job-001")

        self.assertEqual(204, result.status_code)
        self.assertEqual(b"", result.data)
        self.assertIsNone(service.get_job("job-001"))

    def test_api_get_job_does_not_inherit_shared_artifacts_before_run(self):
        service = JobService(
            self.app.extensions["btp_db"]["db_path"],
            project_root=self.tempdir.name,
        )
        output_dir = os.path.join(self.tempdir.name, "output", "internal_persons")
        os.makedirs(output_dir, exist_ok=True)
        with open(os.path.join(output_dir, "personstobeupdated_20260420.csv"), "w", encoding="utf-8") as handle:
            handle.write("id\n1\n")
        with open(os.path.join(output_dir, "datatotal.json"), "w", encoding="utf-8") as handle:
            handle.write("[]")
        service.create_job(
            job_id="job-001",
            job_type="internal_persons",
            created_at="2026-04-20T13:00:00Z",
        )

        result = self.client.get("/api/jobs/job-001")

        body = result.get_json()
        self.assertEqual(200, result.status_code)
        self.assertFalse(body["canOpen"])
        self.assertFalse(body["canApply"])
        self.assertEqual([], body["artifacts"]["csv"])
        self.assertEqual([], body["artifacts"]["json"])

    def test_api_get_job_reports_cannot_apply_when_no_rows_are_selected(self):
        service = JobService(
            self.app.extensions["btp_db"]["db_path"],
            project_root=self.tempdir.name,
        )
        service.create_job(
            job_id="job-001",
            job_type="internal_persons",
            status="needs_review",
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
                "found_count": 2,
                "ready_count": 0,
                "updated_count": 0,
            },
        )

        result = self.client.get("/api/jobs/job-001")

        self.assertEqual(200, result.status_code)
        self.assertFalse(result.get_json()["canApply"])

    def test_api_get_job_logs_returns_empty_content_when_log_missing(self):
        service = JobService(
            self.app.extensions["btp_db"]["db_path"],
            project_root=self.tempdir.name,
        )
        service.create_job(
            job_id="job-001",
            job_type="internal_persons",
            created_at="2026-04-20T13:00:00Z",
        )

        result = self.client.get("/api/jobs/job-001/logs")

        self.assertEqual(200, result.status_code)
        self.assertEqual({"jobId": "job-001", "logPath": None, "content": ""}, result.get_json())

    def test_api_get_job_logs_returns_file_content(self):
        service = JobService(
            self.app.extensions["btp_db"]["db_path"],
            project_root=self.tempdir.name,
        )
        logs_dir = os.path.join(self.tempdir.name, "logs", "jobs")
        os.makedirs(logs_dir, exist_ok=True)
        log_path = os.path.join(logs_dir, "job-001.log")
        with open(log_path, "w", encoding="utf-8") as handle:
            handle.write("hello log\n")
        service.create_job(
            job_id="job-001",
            job_type="internal_persons",
            created_at="2026-04-20T13:00:00Z",
            log_path="logs/jobs/job-001.log",
        )

        result = self.client.get("/api/jobs/job-001/logs")

        self.assertEqual(200, result.status_code)
        self.assertEqual(
            {"jobId": "job-001", "logPath": "logs/jobs/job-001.log", "content": "hello log\n"},
            result.get_json(),
        )

    def test_api_get_job_artifacts_returns_items(self):
        service = JobService(
            self.app.extensions["btp_db"]["db_path"],
            project_root=self.tempdir.name,
        )
        artifact_dir = os.path.join(self.tempdir.name, "output", "internal_persons")
        os.makedirs(artifact_dir, exist_ok=True)
        with open(os.path.join(artifact_dir, "personstobeupdated_20260420.csv"), "w", encoding="utf-8") as handle:
            handle.write("id\n1\n")
        with open(os.path.join(artifact_dir, "datatotal.json"), "w", encoding="utf-8") as handle:
            handle.write("[]")
        service.create_job(
            job_id="job-001",
            job_type="internal_persons",
            created_at="2026-04-20T13:00:00Z",
        )

        result = self.client.get("/api/jobs/job-001/artifacts")

        self.assertEqual(200, result.status_code)
        body = result.get_json()
        self.assertEqual("job-001", body["jobId"])
        self.assertEqual("output/internal_persons/job-001", body["directory"])
        self.assertEqual([], body["items"])

    def test_api_download_job_artifact_returns_file(self):
        service = JobService(
            self.app.extensions["btp_db"]["db_path"],
            project_root=self.tempdir.name,
        )
        artifact_dir = os.path.join(self.tempdir.name, "output", "internal_persons")
        os.makedirs(artifact_dir, exist_ok=True)
        with open(os.path.join(artifact_dir, "datatotal.json"), "w", encoding="utf-8") as handle:
            handle.write("[]")
        service.create_job(
            job_id="job-001",
            job_type="internal_persons",
            status="needs_review",
            created_at="2026-04-20T13:00:00Z",
        )

        with self.client.get("/api/jobs/job-001/artifacts/datatotal.json") as result:
            self.assertEqual(200, result.status_code)
            self.assertEqual("attachment; filename=datatotal.json", result.headers["Content-Disposition"])
            self.assertEqual("[]", result.get_data(as_text=True))

    def test_api_download_job_artifact_rejects_invalid_name(self):
        service = JobService(
            self.app.extensions["btp_db"]["db_path"],
            project_root=self.tempdir.name,
        )
        service.create_job(
            job_id="job-001",
            job_type="internal_persons",
            status="needs_review",
            created_at="2026-04-20T13:00:00Z",
        )

        result = self.client.get("/api/jobs/job-001/artifacts/../bad")

        self.assertEqual(400, result.status_code)
        self.assertEqual({"error": "Invalid artifact name"}, result.get_json())

    def test_api_run_job_returns_not_found_for_missing_job(self):
        result = self.client.post("/api/jobs/missing-job/run")

        self.assertEqual(404, result.status_code)
        self.assertEqual({"error": "Job not found: missing-job"}, result.get_json())

    def test_api_run_job_rejects_non_queued_job(self):
        service = JobService(
            self.app.extensions["btp_db"]["db_path"],
            project_root=self.tempdir.name,
        )
        service.create_job(
            job_id="job-001",
            job_type="internal_persons",
            status="failed",
            created_at="2026-04-20T13:00:00Z",
        )

        result = self.client.post("/api/jobs/job-001/run")

        self.assertEqual(400, result.status_code)
        self.assertIn("must be queued", result.get_json()["error"])

    def test_api_apply_job_returns_not_found_for_missing_job(self):
        result = self.client.post("/api/jobs/missing-job/apply")

        self.assertEqual(404, result.status_code)
        self.assertEqual({"error": "Job not found: missing-job"}, result.get_json())

    def test_api_apply_job_rejects_job_that_is_not_ready(self):
        service = JobService(
            self.app.extensions["btp_db"]["db_path"],
            project_root=self.tempdir.name,
        )
        service.create_job(
            job_id="job-001",
            job_type="internal_persons",
            status="completed",
            created_at="2026-04-20T13:00:00Z",
        )

        result = self.client.post("/api/jobs/job-001/apply")

        self.assertEqual(400, result.status_code)
        self.assertIn("must be in needs_review", result.get_json()["error"])

    @patch("app.services.jobs.subprocess.Popen")
    def test_api_run_job_executes_internal_persons_and_exposes_logs(self, mock_popen):
        os.makedirs(os.path.join(self.tempdir.name, "src"), exist_ok=True)
        os.makedirs(os.path.join(self.tempdir.name, "output", "internal_persons"), exist_ok=True)
        with open(
            os.path.join(self.tempdir.name, "src", "enrich_internal_persons_with_ids.py"),
            "w",
            encoding="utf-8",
        ) as handle:
            handle.write("print('placeholder')\n")
        with open(
            os.path.join(self.tempdir.name, "output", "internal_persons", "personstobeupdated_20260312.csv"),
            "w",
            encoding="utf-8",
        ) as handle:
            handle.write("to_be_updated,updated,PURE_UUID_PERS\n,X,pers-old\n")
        with open(
            os.path.join(self.tempdir.name, "output", "internal_persons", "datatotal.json"),
            "w",
            encoding="utf-8",
        ) as handle:
            handle.write('[{"uuid":"pers-old"}]')

        mock_process = MagicMock()
        mock_process.returncode = 0

        created = self.client.post(
            "/api/jobs",
            json={"jobType": "internal_persons", "params": {"facultyChoice": "all"}},
        ).get_json()
        job_output_dir = os.path.join(self.tempdir.name, created["artifact_dir"])
        os.makedirs(job_output_dir, exist_ok=True)

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
            return "collected\n", ""

        mock_process.communicate.side_effect = write_new_artifacts
        mock_popen.return_value = mock_process

        run_result = self.client.post(f"/api/jobs/{created['id']}/run")
        run_body = run_result.get_json()
        logs_result = self.client.get(f"/api/jobs/{created['id']}/logs")

        self.assertEqual(200, run_result.status_code)
        self.assertEqual("needs_review", run_body["status"])
        self.assertTrue(run_body["canOpen"])
        self.assertTrue(run_body["canApply"])
        self.assertEqual(["personstobeupdated_20260420.csv"], run_body["artifacts"]["csv"])
        self.assertEqual(["datatotal.json"], run_body["artifacts"]["json"])
        self.assertEqual(created["artifact_dir"], run_body["artifact_dir"])
        self.assertEqual(2, run_body["results"]["found_count"])
        self.assertEqual(1, run_body["results"]["ready_count"])
        self.assertEqual(0, run_body["results"]["updated_count"])
        self.assertEqual("logs/jobs/" + created["id"] + ".log", run_body["log_path"])
        self.assertEqual(200, logs_result.status_code)
        self.assertIn("collected", logs_result.get_json()["content"])
        command = mock_popen.call_args.args[0]
        self.assertIn("enrich_internal_persons_with_ids.py", command[2])
        self.assertEqual("all", command[3])
        self.assertEqual(
            os.path.join(self.tempdir.name, created["artifact_dir"]),
            mock_popen.call_args.kwargs["env"]["BTP_OUTPUT_DIR"],
        )

    @patch("app.services.jobs.subprocess.Popen")
    def test_api_apply_job_executes_apply_flow_and_exposes_logs(self, mock_popen):
        os.makedirs(os.path.join(self.tempdir.name, "src"), exist_ok=True)
        os.makedirs(os.path.join(self.tempdir.name, "output", "internal_persons"), exist_ok=True)
        with open(
            os.path.join(self.tempdir.name, "src", "apply_updates_to_pure.py"),
            "w",
            encoding="utf-8",
        ) as handle:
            handle.write("print('placeholder')\n")
        with open(
            os.path.join(self.tempdir.name, "output", "internal_persons", "personstobeupdated_20260420.csv"),
            "w",
            encoding="utf-8",
        ) as handle:
            handle.write("id\n1\n")
        with open(
            os.path.join(self.tempdir.name, "output", "internal_persons", "datatotal.json"),
            "w",
            encoding="utf-8",
        ) as handle:
            handle.write("[]")

        mock_process = MagicMock()
        mock_process.communicate.return_value = ("apply ok\n", "")
        mock_process.returncode = 0
        mock_popen.return_value = mock_process

        service = JobService(
            self.app.extensions["btp_db"]["db_path"],
            project_root=self.tempdir.name,
        )
        service.create_job(
            job_id="job-001",
            job_type="internal_persons",
            status="needs_review",
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

        apply_result = self.client.post("/api/jobs/job-001/apply")
        apply_body = apply_result.get_json()
        logs_result = self.client.get("/api/jobs/job-001/logs")

        self.assertEqual(200, apply_result.status_code)
        self.assertEqual("completed", apply_body["status"])
        self.assertEqual(200, logs_result.status_code)
        self.assertIn("apply ok", logs_result.get_json()["content"])
        self.assertEqual(
            "enrich_internal_persons_with_ids",
            mock_popen.call_args.kwargs["env"]["REFERER_PAGE"],
        )
        self.assertEqual(
            os.path.join(self.tempdir.name, "output", "internal_persons"),
            mock_popen.call_args.kwargs["env"]["BTP_OUTPUT_DIR"],
        )

    @patch("app.services.jobs.requests.put")
    @patch("app.services.jobs.requests.get")
    def test_api_rollback_job_executes_safe_internal_person_rollback(self, mock_get, mock_put):
        service = JobService(
            self.app.extensions["btp_db"]["db_path"],
            project_root=self.tempdir.name,
        )
        os.makedirs(os.path.join(self.tempdir.name, "output", "internal_persons"), exist_ok=True)
        csv_path = os.path.join(self.tempdir.name, "output", "internal_persons", "personstobeupdated_20260420.csv")
        json_path = os.path.join(self.tempdir.name, "output", "internal_persons", "datatotal.json")
        with open(csv_path, "w", encoding="utf-8") as handle:
            handle.write(
                "to_be_updated,updated,FULL_NAME,person_id,PURE_UUID_PERS,new_id,new_value,uri\n"
                "X,,Alpha,person-1,pers-1,orcid,0000-0001,\n"
            )
        with open(json_path, "w", encoding="utf-8") as handle:
            handle.write('[{"uuid":"pers-1","orcid":"0000-0000","identifiers":[]}]')

        service.create_job(
            job_id="job-rollback-source",
            job_type="internal_persons",
            status="completed",
            created_at="2026-04-20T13:00:00Z",
            log_path="logs/jobs/job-rollback-source.log",
        )
        service.update_job(
            "job-rollback-source",
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
        service._capture_apply_change_set(service.get_job("job-rollback-source"))
        with open(csv_path, "w", encoding="utf-8") as handle:
            handle.write(
                "to_be_updated,updated,FULL_NAME,person_id,PURE_UUID_PERS,new_id,new_value,uri\n"
                ",X,Alpha,person-1,pers-1,orcid,0000-0001,\n"
            )
        service._finalize_apply_change_set(
            service.get_job("job-rollback-source"),
            service.get_job_change_set("job-rollback-source")["id"],
        )

        get_response = MagicMock()
        get_response.json.return_value = {"uuid": "pers-1", "orcid": "0000-0001", "identifiers": []}
        mock_get.return_value = get_response
        mock_put.return_value = MagicMock()

        result = self.client.post("/api/jobs/job-rollback-source/rollback")

        body = result.get_json()
        self.assertEqual(200, result.status_code)
        self.assertTrue(body["id"].startswith("rollback_"))
        self.assertEqual("completed", body["status"])

        change_set_result = self.client.get("/api/jobs/job-rollback-source/change-set")
        self.assertEqual(200, change_set_result.status_code)
        self.assertEqual("rolled_back", change_set_result.get_json()["status"])

    def test_update_status_rejects_unknown_source(self):
        result = self.client.get("/update_status?source=unknown")

        self.assertEqual(400, result.status_code)
        self.assertEqual("error", result.get_json()["status"])

    @patch("app.routes.os.path.exists", return_value=False)
    def test_update_status_returns_false_when_output_missing(self, _exists):
        result = self.client.get("/update_status?source=import_datasets")

        self.assertEqual(200, result.status_code)
        self.assertEqual(
            {"status": "success", "can_open": False, "can_apply": False},
            result.get_json(),
        )

    @patch("app.routes._has_named_files")
    @patch("app.routes.os.path.exists", return_value=True)
    def test_update_status_reports_ready_apply_state(self, _exists, mock_has_named_files):
        mock_has_named_files.side_effect = [True, True]

        result = self.client.get("/update_status?source=import_datasets")

        self.assertEqual(200, result.status_code)
        self.assertEqual(
            {"status": "success", "can_open": True, "can_apply": True},
            result.get_json(),
        )

    @patch("app.routes.Path.exists", return_value=True)
    @patch("app.routes.subprocess.Popen")
    def test_run_endpoints_stream_subprocess_output(self, mock_popen, _exists):
        mock_popen.side_effect = lambda *args, **kwargs: FakeProcess(stdout_lines=["line 1\n", "line 2\n"])

        cases = [
            ("/run_enrich_internal_persons", {"faculty_choice": "all"}, "src/enrich_internal_persons_with_ids.py"),
            ("/run_enrich_external_persons", {"faculty_choice": "all", "use_openalex_fallback": "yes"}, "src/enrich_pure_external_persons.py"),
            ("/run_enrich_pure_external_orgs", {"faculty_choice": "all"}, "src/enrich_pure_external_orgs.py"),
            ("/run_import_research_outputs", {"faculty_choice": "all"}, "src/update_researchoutput_from_ricgraph.py"),
            ("/run_import_datasets", {"faculty_choice": "all"}, "src/update_datasets_from_ricgraph.py"),
        ]

        for url, data, script_path in cases:
            with self.subTest(url=url):
                result = self.client.post(url, data=data)
                body = result.get_data(as_text=True)

                self.assertEqual(200, result.status_code)
                self.assertIn("line 1", body)
                self.assertIn("line 2", body)
                command = mock_popen.call_args[0][0]
                self.assertTrue(any(str(item).endswith(script_path) for item in command))

    @patch("app.routes.Path.exists", return_value=True)
    @patch("app.routes.subprocess.Popen")
    def test_run_apply_updates_streams_output(self, mock_popen, _exists):
        mock_popen.return_value = FakeProcess(stdout_lines=["apply ok\n"])

        result = self.client.post(
            "/run_apply_updates_to_pure",
            headers={"Referer": "http://localhost/import_datasets"},
        )

        body = result.get_data(as_text=True)

        self.assertEqual(200, result.status_code)
        self.assertIn("apply ok", body)
        self.assertEqual("http://localhost/import_datasets", mock_popen.call_args.kwargs["env"]["REFERER_PAGE"])

    @patch("app.routes.Path.exists", return_value=False)
    def test_run_apply_updates_returns_404_when_script_missing(self, _exists):
        result = self.client.post("/run_apply_updates_to_pure")

        self.assertEqual(404, result.status_code)
        self.assertEqual("error", result.get_json()["status"])

    def test_open_directory_rejects_unknown_source(self):
        result = self.client.post("/open_directory", headers={"Referer": "http://localhost/unknown"})

        self.assertEqual(400, result.status_code)
        self.assertEqual("error", result.get_json()["status"])

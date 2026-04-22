from __future__ import annotations

import csv
import json
import os
import subprocess
import sys
import uuid
from pathlib import Path
from typing import Any
from datetime import datetime, timezone

from app.db import connect_db
from app.models import JobStatus, JobType, get_job_type_definition, is_valid_transition, parse_job_status
from config import OPENALEXEX_ID_URI, ORCID_ID_URI, PURE_BASE_URL, PURE_HEADERS, ROR_ID_URI
import requests


class JobService:
    """Phase-1 scaffold for background job orchestration."""

    def __init__(
        self,
        db_path: Path,
        *,
        project_root: Path | None = None,
        logs_dir: Path | None = None,
        python_executable: str | None = None,
    ):
        self.db_path = Path(db_path)
        self.project_root = Path(project_root) if project_root is not None else self.db_path.parent.parent
        self.logs_dir = Path(logs_dir) if logs_dir is not None else self.project_root / "logs" / "jobs"
        self.python_executable = python_executable or self._default_python_executable()

    def healthcheck(self) -> dict:
        return {
            "status": "ok",
            "db_path": str(self.db_path),
            "logs_dir": str(self.logs_dir),
        }

    def create_job(
        self,
        *,
        job_id: str,
        job_type: str,
        status: str | JobStatus = JobStatus.QUEUED,
        params: dict[str, Any] | None = None,
        created_at: str,
        started_at: str | None = None,
        finished_at: str | None = None,
        exit_code: int | None = None,
        log_path: str | None = None,
        artifact_dir: str | None = None,
        apply_job_id: str | None = None,
        rollback_job_id: str | None = None,
        error_message: str | None = None,
    ) -> dict:
        definition = get_job_type_definition(job_type)
        parsed_status = parse_job_status(status)
        self._validate_params(definition.allowed_params, params or {})
        self._ensure_job_creation_allowed(definition.job_type.value, parsed_status.value)
        resolved_artifact_dir = artifact_dir or self._default_artifact_dir(
            definition,
            parsed_status,
            job_id,
        )
        initial_artifacts = (
            self._empty_artifact_state(resolved_artifact_dir)
            if parsed_status == JobStatus.QUEUED
            else self._detect_artifacts(definition.job_type.value, resolved_artifact_dir)
        )
        initial_results = (
            self._empty_results_summary(definition)
            if parsed_status == JobStatus.QUEUED
            else self._summarize_results(definition.job_type.value, resolved_artifact_dir)
        )

        payload = {
            "id": job_id,
            "job_type": definition.job_type.value,
            "status": parsed_status.value,
            "params_json": json.dumps(params or {}, sort_keys=True),
            "created_at": created_at,
            "started_at": started_at,
            "finished_at": finished_at,
            "exit_code": exit_code,
            "log_path": log_path,
            "artifact_dir": resolved_artifact_dir,
            "artifacts_json": json.dumps(initial_artifacts, sort_keys=True),
            "results_json": json.dumps(initial_results, sort_keys=True),
            "apply_job_id": apply_job_id,
            "rollback_job_id": rollback_job_id,
            "error_message": error_message,
        }
        with connect_db(self.db_path) as connection:
            connection.execute(
                """
                INSERT INTO jobs (
                    id, job_type, status, params_json, created_at, started_at, finished_at,
                    exit_code, log_path, artifact_dir, artifacts_json, results_json, apply_job_id, rollback_job_id, error_message
                ) VALUES (
                    :id, :job_type, :status, :params_json, :created_at, :started_at, :finished_at,
                    :exit_code, :log_path, :artifact_dir, :artifacts_json, :results_json, :apply_job_id, :rollback_job_id, :error_message
                )
                """,
                payload,
            )
            connection.commit()
        return self.get_job(job_id)

    def get_job(self, job_id: str) -> dict | None:
        with connect_db(self.db_path) as connection:
            row = connection.execute(
                "SELECT * FROM jobs WHERE id = ?",
                (job_id,),
            ).fetchone()
        return self._enrich_job(self._row_to_job(row))

    def list_jobs(self, *, limit: int | None = None) -> list[dict]:
        query = "SELECT * FROM jobs ORDER BY created_at DESC, id DESC"
        params: tuple[Any, ...] = ()
        if limit is not None:
            query += " LIMIT ?"
            params = (limit,)
        with connect_db(self.db_path) as connection:
            rows = connection.execute(query, params).fetchall()
        return [self._enrich_job(self._row_to_job(row)) for row in rows]

    def get_results_dashboard(self, *, days: int | None = None) -> dict[str, Any]:
        workflow_labels = {
            JobType.INTERNAL_PERSONS.value: "Internal Persons",
            JobType.EXTERNAL_PERSONS.value: "External Persons",
            JobType.EXTERNAL_ORGS.value: "External Organisations",
            JobType.RESEARCH_OUTPUTS.value: "Research Outputs",
            JobType.DATASETS.value: "Datasets",
        }
        breakdown_labels = {
            JobType.INTERNAL_PERSONS.value: {
                "orcid": "ORCID",
                "scopus author id": "Scopus Author ID",
                "scopus": "Scopus Author ID",
                "openalex": "OpenAlex ID",
            },
            JobType.EXTERNAL_PERSONS.value: {
                "orcid": "ORCID",
                "alex_id": "OpenAlex ID",
            },
            JobType.EXTERNAL_ORGS.value: {
                "ror": "ROR",
                "address": "Address/Geo",
            },
            JobType.RESEARCH_OUTPUTS.value: {
                "research_output": "New Research Outputs",
            },
            JobType.DATASETS.value: {
                "dataset": "New Datasets",
            },
        }

        workflows: dict[str, dict[str, Any]] = {}
        for job_type, label in workflow_labels.items():
            workflows[job_type] = {
                "job_type": job_type,
                "label": label,
                "applied_items": 0,
                "rolled_back_items": 0,
                "net_items": 0,
                "applied_entities": 0,
                "rolled_back_entities": 0,
                "net_entities": 0,
                "breakdown": [],
            }

        query = """
            SELECT
                cs.job_type AS job_type,
                item.entity_uuid AS entity_uuid,
                item.identifier_type AS identifier_type,
                item.apply_status AS apply_status,
                item.rollback_status AS rollback_status
            FROM job_change_set_items item
            JOIN job_change_sets cs ON cs.id = item.change_set_id
            WHERE item.apply_status = 'applied'
        """
        params: tuple[Any, ...] = ()
        if days is not None:
            cutoff = datetime.now(timezone.utc).replace(microsecond=0)
            cutoff_iso = (cutoff.timestamp() - (days * 24 * 60 * 60))
            cutoff_text = datetime.fromtimestamp(cutoff_iso, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
            query += " AND cs.applied_at IS NOT NULL AND cs.applied_at >= ?"
            params = (cutoff_text,)

        with connect_db(self.db_path) as connection:
            rows = connection.execute(query, params).fetchall()

        entity_sets: dict[str, dict[str, set[str]]] = {
            job_type: {"applied": set(), "rolled_back": set(), "net": set()}
            for job_type in workflow_labels
        }
        breakdown_totals: dict[str, dict[str, dict[str, int]]] = {
            job_type: {}
            for job_type in workflow_labels
        }

        for row in rows:
            job_type = str(row["job_type"])
            if job_type not in workflows:
                continue
            entity_uuid = str(row["entity_uuid"] or "").strip()
            identifier_type = str(row["identifier_type"] or "").strip().lower() or "other"
            rollback_status = str(row["rollback_status"] or "").strip().lower()

            workflows[job_type]["applied_items"] += 1
            if entity_uuid:
                entity_sets[job_type]["applied"].add(entity_uuid)

            breakdown = breakdown_totals[job_type].setdefault(
                identifier_type,
                {"applied": 0, "rolled_back": 0, "net": 0},
            )
            breakdown["applied"] += 1

            if rollback_status == "rolled_back":
                workflows[job_type]["rolled_back_items"] += 1
                breakdown["rolled_back"] += 1
                if entity_uuid:
                    entity_sets[job_type]["rolled_back"].add(entity_uuid)
            else:
                workflows[job_type]["net_items"] += 1
                breakdown["net"] += 1
                if entity_uuid:
                    entity_sets[job_type]["net"].add(entity_uuid)

        totals = {
            "applied_items": 0,
            "rolled_back_items": 0,
            "net_items": 0,
            "applied_entities": 0,
            "rolled_back_entities": 0,
            "net_entities": 0,
        }

        for job_type, workflow in workflows.items():
            workflow["applied_entities"] = len(entity_sets[job_type]["applied"])
            workflow["rolled_back_entities"] = len(entity_sets[job_type]["rolled_back"])
            workflow["net_entities"] = len(entity_sets[job_type]["net"])
            workflow["breakdown"] = [
                {
                    "key": key,
                    "label": breakdown_labels.get(job_type, {}).get(key, key.replace("_", " ").title()),
                    "applied_items": values["applied"],
                    "rolled_back_items": values["rolled_back"],
                    "net_items": values["net"],
                }
                for key, values in sorted(
                    breakdown_totals[job_type].items(),
                    key=lambda item: (-item[1]["net"], item[0]),
                )
            ]
            totals["applied_items"] += workflow["applied_items"]
            totals["rolled_back_items"] += workflow["rolled_back_items"]
            totals["net_items"] += workflow["net_items"]
            totals["applied_entities"] += workflow["applied_entities"]
            totals["rolled_back_entities"] += workflow["rolled_back_entities"]
            totals["net_entities"] += workflow["net_entities"]

        return {
            "totals": totals,
            "workflows": [workflows[job_type] for job_type in workflow_labels],
        }

    def delete_job(self, job_id: str) -> bool:
        job = self.get_job(job_id)
        if job is None:
            return False
        if job["status"] in {
            JobStatus.QUEUED.value,
            JobStatus.RUNNING.value,
            JobStatus.APPLYING.value,
        }:
            raise ValueError(f"Job {job_id} cannot be deleted while it is active")

        log_path = self.project_root / job["log_path"] if job.get("log_path") else None
        with connect_db(self.db_path) as connection:
            connection.execute(
                "DELETE FROM job_change_set_items WHERE change_set_id IN (SELECT id FROM job_change_sets WHERE job_id = ?)",
                (job_id,),
            )
            connection.execute("DELETE FROM job_change_sets WHERE job_id = ?", (job_id,))
            connection.execute("DELETE FROM jobs WHERE id = ?", (job_id,))
            connection.commit()

        if log_path and log_path.exists():
            log_path.unlink()
        return True

    def update_job(self, job_id: str, **fields: Any) -> dict | None:
        if not fields:
            return self.get_job(job_id)

        allowed_fields = {
            "job_type",
            "status",
            "params",
            "created_at",
            "started_at",
            "finished_at",
            "exit_code",
            "log_path",
            "artifact_dir",
            "artifacts",
            "results",
            "apply_job_id",
            "rollback_job_id",
            "error_message",
        }
        unknown_fields = set(fields) - allowed_fields
        if unknown_fields:
            raise ValueError(f"Unknown job fields: {sorted(unknown_fields)}")

        db_fields: dict[str, Any] = {}
        current_job = self.get_job(job_id)
        if current_job is None:
            return None

        for key, value in fields.items():
            if key == "params":
                definition = get_job_type_definition(fields.get("job_type", current_job["job_type"]))
                self._validate_params(definition.allowed_params, value or {})
                db_fields["params_json"] = json.dumps(value or {}, sort_keys=True)
            elif key == "job_type":
                definition = get_job_type_definition(value)
                self._validate_params(definition.allowed_params, current_job["params"])
                db_fields["job_type"] = definition.job_type.value
                if not fields.get("artifact_dir"):
                    db_fields["artifact_dir"] = definition.artifact_dir
                if not fields.get("artifacts"):
                    db_fields["artifacts_json"] = json.dumps(self._empty_artifact_state(definition.artifact_dir), sort_keys=True)
                if not fields.get("results"):
                    db_fields["results_json"] = json.dumps(self._empty_results_summary(definition), sort_keys=True)
            elif key == "status":
                next_status = parse_job_status(value)
                if current_job["status"] != next_status.value and not is_valid_transition(current_job["status"], next_status):
                    raise ValueError(
                        f"Invalid status transition: {current_job['status']} -> {next_status.value}"
                    )
                db_fields["status"] = next_status.value
            elif key == "artifacts":
                db_fields["artifacts_json"] = json.dumps(value, sort_keys=True)
            elif key == "results":
                db_fields["results_json"] = json.dumps(value, sort_keys=True)
            else:
                db_fields[key] = value

        assignments = ", ".join(f"{column} = :{column}" for column in db_fields)
        db_fields["id"] = job_id

        with connect_db(self.db_path) as connection:
            cursor = connection.execute(
                f"UPDATE jobs SET {assignments} WHERE id = :id",
                db_fields,
            )
            connection.commit()
        return self.get_job(job_id)

    def run_job(self, job_id: str) -> dict | None:
        job = self.get_job(job_id)
        if job is None:
            return None

        if job["status"] != JobStatus.QUEUED.value:
            raise ValueError(f"Job {job_id} must be queued before it can run")

        definition = get_job_type_definition(job["job_type"])
        script_path = self.project_root / definition.script_path
        stored_log_path = Path("logs") / "jobs" / f"{job_id}.log"
        log_path = self.project_root / stored_log_path
        self.logs_dir.mkdir(parents=True, exist_ok=True)

        started_at = self._utcnow()
        self.update_job(
            job_id,
            status=JobStatus.RUNNING,
            started_at=started_at,
            log_path=str(stored_log_path),
        )

        if not script_path.exists():
            message = f"Script path does not exist: {script_path}"
            self._append_log(log_path, f"ERROR {message}\n")
            return self.update_job(
                job_id,
                status=JobStatus.FAILED,
                finished_at=self._utcnow(),
                exit_code=127,
                error_message=message,
            )

        command = self._build_command(definition, job["params"], script_path)
        artifact_baseline = self._snapshot_artifact_state(job["job_type"], job.get("artifact_dir"))
        env = dict(os.environ)
        env["BTP_OUTPUT_DIR"] = str(self._artifact_directory(job.get("artifact_dir")))
        try:
            process = subprocess.Popen(
                command,
                cwd=self.project_root,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                bufsize=1,
                env=env,
            )
            stdout, stderr = process.communicate()
        except FileNotFoundError as exc:
            message = f"Could not start process: {exc}"
            self._append_log(log_path, f"ERROR {message}\n")
            return self.update_job(
                job_id,
                status=JobStatus.FAILED,
                finished_at=self._utcnow(),
                exit_code=127,
                error_message=message,
            )

        if stdout:
            self._append_log(log_path, stdout)
        if stderr:
            self._append_log(log_path, stderr)

        finished_at = self._utcnow()
        if process.returncode == 0:
            artifact_summary = self._detect_run_artifacts(
                job["job_type"],
                job.get("artifact_dir"),
                artifact_baseline,
            )
            results_summary = self._summarize_results(
                job["job_type"],
                job.get("artifact_dir"),
                artifact_summary=artifact_summary,
            )
            success_status = JobStatus.NEEDS_REVIEW if artifact_summary["canApply"] else JobStatus.COMPLETED
            return self.update_job(
                job_id,
                status=success_status,
                finished_at=finished_at,
                exit_code=process.returncode,
                artifacts=artifact_summary,
                results=results_summary,
                error_message=None,
            )

        error_message = stderr.strip() or f"Process exited with code {process.returncode}"
        return self.update_job(
            job_id,
            status=JobStatus.FAILED,
            finished_at=finished_at,
            exit_code=process.returncode,
            error_message=error_message,
        )

    def apply_job(self, job_id: str) -> dict | None:
        job = self.get_job(job_id)
        if job is None:
            return None
        if job["status"] != JobStatus.NEEDS_REVIEW.value:
            raise ValueError(f"Job {job_id} must be in needs_review before apply can run")
        if not job["canApply"]:
            raise ValueError(f"Job {job_id} is not ready to apply")
        if job["results"]["ready_count"] == 0:
            raise ValueError(f"Job {job_id} has no selected updates to apply")

        script_path = self.project_root / "src" / "apply_updates_to_pure.py"
        log_path = self.project_root / (job["log_path"] or Path("logs") / "jobs" / f"{job_id}.log")
        self.logs_dir.mkdir(parents=True, exist_ok=True)
        self.update_job(job_id, status=JobStatus.APPLYING)

        if not script_path.exists():
            message = f"Script path does not exist: {script_path}"
            self._append_log(log_path, f"\n=== APPLY START ===\nERROR {message}\n")
            return self.update_job(
                job_id,
                status=JobStatus.FAILED,
                finished_at=self._utcnow(),
                exit_code=127,
                error_message=message,
            )

        env = dict(os.environ)
        env["REFERER_PAGE"] = self._referer_page_for_job_type(job["job_type"])
        env["BTP_OUTPUT_DIR"] = str(self._artifact_directory(job.get("artifact_dir")))
        artifact_summary = job["artifact_state"]
        env["BTP_TARGET_CSV_FILES"] = ",".join(artifact_summary["artifacts"]["csv"])
        env["BTP_TARGET_JSON_FILES"] = ",".join(artifact_summary["artifacts"]["json"])
        apply_manifest_path = self._artifact_directory(job.get("artifact_dir")) / "apply_manifest.jsonl"
        if apply_manifest_path.exists():
            apply_manifest_path.unlink()
        env["BTP_APPLY_MANIFEST_FILE"] = str(apply_manifest_path)
        change_set_id = self._capture_apply_change_set(job)
        self._append_log(log_path, f"\n=== APPLY START {self._utcnow()} ===\n")

        try:
            process = subprocess.Popen(
                [self.python_executable, "-u", str(script_path)],
                cwd=self.project_root,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                bufsize=1,
                env=env,
            )
            stdout, stderr = process.communicate()
        except FileNotFoundError as exc:
            message = f"Could not start process: {exc}"
            self._append_log(log_path, f"ERROR {message}\n")
            if change_set_id is not None:
                self._mark_change_set_failed(change_set_id)
            return self.update_job(
                job_id,
                status=JobStatus.FAILED,
                finished_at=self._utcnow(),
                exit_code=127,
                error_message=message,
            )

        if stdout:
            self._append_log(log_path, stdout)
        if stderr:
            self._append_log(log_path, stderr)

        finished_at = self._utcnow()
        if process.returncode == 0:
            self._append_log(log_path, f"=== APPLY FINISHED {finished_at} ===\n")
            if change_set_id is not None:
                self._finalize_apply_change_set(job, change_set_id)
            refreshed_results = self._summarize_results(
                job["job_type"],
                job.get("artifact_dir"),
                artifact_summary=job["artifact_state"],
            )
            return self.update_job(
                job_id,
                status=JobStatus.COMPLETED,
                finished_at=finished_at,
                exit_code=process.returncode,
                artifacts=job["artifact_state"],
                results=refreshed_results,
                error_message=None,
            )

        error_message = stderr.strip() or f"Process exited with code {process.returncode}"
        if change_set_id is not None:
            self._mark_change_set_failed(change_set_id)
        return self.update_job(
            job_id,
            status=JobStatus.FAILED,
            finished_at=finished_at,
            exit_code=process.returncode,
            error_message=error_message,
        )

    def rollback_job(self, job_id: str) -> dict | None:
        job = self.get_job(job_id)
        if job is None:
            return None
        if job["job_type"] not in {
            JobType.INTERNAL_PERSONS.value,
            JobType.EXTERNAL_PERSONS.value,
            JobType.EXTERNAL_ORGS.value,
            JobType.RESEARCH_OUTPUTS.value,
            JobType.DATASETS.value,
        }:
            raise ValueError(f"Rollback is not supported for job type {job['job_type']}")
        if job["status"] != JobStatus.COMPLETED.value:
            raise ValueError(f"Job {job_id} must be completed before rollback can run")
        existing_rollback_job = None
        if job.get("rollback_job_id"):
            existing_rollback_job = self.get_job(job["rollback_job_id"])
            if existing_rollback_job is None:
                raise ValueError(f"Job {job_id} references a missing rollback run")
            if existing_rollback_job["status"] != JobStatus.FAILED.value:
                raise ValueError(f"Job {job_id} already has a rollback run")

        change_set = self.get_job_change_set(job_id)
        if change_set is None:
            raise ValueError(f"Job {job_id} has no rollbackable change set")
        retryable_items = [
            item
            for item in change_set["items"]
            if item["apply_status"] == "applied" and item["rollback_status"] not in {"rolled_back", "conflict", "skipped"}
        ]
        if change_set["status"] not in {"applied", "rolled_back_with_conflicts"}:
            raise ValueError(f"Job {job_id} does not have an applied change set ready for rollback")
        if change_set["applied_item_count"] == 0 or not retryable_items:
            raise ValueError(f"Job {job_id} has no successfully applied changes to roll back")

        rollback_job_id = f"rollback_{uuid.uuid4().hex[:12]}"
        rollback_log_path = Path("logs") / "jobs" / f"{rollback_job_id}.log"
        rollback_job = self.create_job(
            job_id=rollback_job_id,
            job_type=job["job_type"],
            status=JobStatus.APPLYING,
            params={},
            created_at=self._utcnow(),
            started_at=self._utcnow(),
            log_path=str(rollback_log_path),
            artifact_dir=job.get("artifact_dir"),
        )
        self.update_job(job_id, rollback_job_id=rollback_job_id)

        log_path = self.project_root / rollback_log_path
        self.logs_dir.mkdir(parents=True, exist_ok=True)
        self._append_log(log_path, f"=== ROLLBACK START {self._utcnow()} ===\n")

        if job["job_type"] == JobType.INTERNAL_PERSONS.value:
            stats = self._execute_internal_persons_rollback(change_set, log_path)
        elif job["job_type"] == JobType.EXTERNAL_PERSONS.value:
            stats = self._execute_external_persons_rollback(change_set, log_path)
        elif job["job_type"] == JobType.EXTERNAL_ORGS.value:
            stats = self._execute_external_orgs_rollback(change_set, log_path)
        elif job["job_type"] == JobType.RESEARCH_OUTPUTS.value:
            stats = self._execute_research_outputs_rollback(change_set, log_path)
        else:
            stats = self._execute_datasets_rollback(change_set, log_path)
        finished_at = self._utcnow()
        self._append_log(log_path, f"=== ROLLBACK FINISHED {finished_at} ===\n")

        error_message = None
        rollback_status = JobStatus.COMPLETED
        if stats["failed"] > 0:
            rollback_status = JobStatus.FAILED
            error_message = "One or more rollback updates failed"

        updated_change_set_status = "rolled_back"
        if stats["failed"] > 0 or stats["conflicts"] > 0:
            updated_change_set_status = "rolled_back_with_conflicts"

        with connect_db(self.db_path) as connection:
            connection.execute(
                "UPDATE job_change_sets SET status = ?, rolled_back_at = ? WHERE id = ?",
                (updated_change_set_status, finished_at, change_set["id"]),
            )
            connection.commit()

        return self.update_job(
            rollback_job_id,
            status=rollback_status,
            finished_at=finished_at,
            exit_code=0 if rollback_status == JobStatus.COMPLETED else 1,
            error_message=error_message,
        )

    def get_job_artifacts(self, job_id: str) -> dict | None:
        job = self.get_job(job_id)
        if job is None:
            return None

        directory = self._artifact_directory(job.get("artifact_dir"))
        items: list[dict[str, Any]] = []
        tracked_names = [*job["artifact_state"]["artifacts"]["csv"], *job["artifact_state"]["artifacts"]["json"]]
        for filename in tracked_names:
            entry = directory / filename
            if not entry.exists() or not entry.is_file():
                continue
            items.append(
                {
                    "name": entry.name,
                    "kind": self._artifact_kind(entry.name),
                    "size_bytes": entry.stat().st_size,
                    "download_path": f"/api/jobs/{job_id}/artifacts/{entry.name}",
                }
            )

        return {
            "jobId": job_id,
            "directory": job["artifact_dir"],
            "items": items,
        }

    def get_job_review_table(self, job_id: str) -> dict | None:
        job = self.get_job(job_id)
        if job is None:
            return None

        definition = get_job_type_definition(job["job_type"])
        primary_csv_name = self._primary_review_csv_name(definition, job["artifact_state"])
        if primary_csv_name is None:
            raise FileNotFoundError("No tracked review CSV is available for this job")

        csv_path = self._artifact_directory(job.get("artifact_dir")) / primary_csv_name
        if not csv_path.exists() or not csv_path.is_file():
            raise FileNotFoundError(primary_csv_name)

        with open(csv_path, "r", encoding="utf-8", newline="") as handle:
            reader = csv.DictReader(handle)
            columns = list(reader.fieldnames or [])
            rows = []
            for row_index, row in enumerate(reader):
                normalized_row = {column: row.get(column, "") or "" for column in columns}
                normalized_row["_rowIndex"] = row_index
                rows.append(normalized_row)
        if not columns and not rows:
            columns = self._default_review_table_columns(job["job_type"])

        editable_columns = ["to_be_updated"] if "to_be_updated" in columns else []
        return {
            "jobId": job_id,
            "fileName": primary_csv_name,
            "columns": columns,
            "rows": rows,
            "rowCount": len(rows),
            "selectionColumn": "to_be_updated" if "to_be_updated" in columns else None,
            "editableColumns": editable_columns,
        }

    def update_job_review_table(
        self,
        job_id: str,
        *,
        file_name: str,
        expected_row_count: int,
        updates: list[dict[str, Any]],
    ) -> dict | None:
        job = self.get_job(job_id)
        if job is None:
            return None

        definition = get_job_type_definition(job["job_type"])
        primary_csv_name = self._primary_review_csv_name(definition, job["artifact_state"])
        if primary_csv_name is None:
            raise FileNotFoundError("No review CSV is available for this job")
        if file_name != primary_csv_name:
            raise ValueError(f"Only the primary review CSV can be updated for this job: {primary_csv_name}")

        csv_path = self._artifact_directory(job.get("artifact_dir")) / primary_csv_name
        if not csv_path.exists() or not csv_path.is_file():
            raise FileNotFoundError(primary_csv_name)

        with open(csv_path, "r", encoding="utf-8", newline="") as handle:
            reader = csv.DictReader(handle)
            columns = list(reader.fieldnames or [])
            rows = list(reader)

        if "to_be_updated" not in columns:
            raise ValueError("This review CSV does not support inline selection editing")
        if len(rows) != expected_row_count:
            raise ValueError("The review CSV changed while you were editing it. Refresh the table and try again.")

        normalized_updates: dict[int, str] = {}
        for update in updates:
            if not isinstance(update, dict):
                raise ValueError("Each review-table update must be an object")
            unknown_fields = set(update) - {"rowIndex", "to_be_updated"}
            if unknown_fields:
                raise ValueError(f"Unsupported review-table fields: {sorted(unknown_fields)}")
            row_index = update.get("rowIndex")
            if not isinstance(row_index, int):
                raise ValueError("Each review-table update must include an integer rowIndex")
            if row_index < 0 or row_index >= len(rows):
                raise ValueError(f"Row index out of range: {row_index}")
            marker = "X" if self._marker_is_selected(update.get("to_be_updated")) else ""
            normalized_updates[row_index] = marker

        if normalized_updates:
            for row_index, marker in normalized_updates.items():
                rows[row_index]["to_be_updated"] = marker

            with open(csv_path, "w", encoding="utf-8", newline="") as handle:
                writer = csv.DictWriter(handle, fieldnames=columns)
                writer.writeheader()
                writer.writerows(rows)

            refreshed_results = self._summarize_results(
                job["job_type"],
                job.get("artifact_dir"),
                artifact_summary=job["artifact_state"],
            )
            self.update_job(job_id, results=refreshed_results)

        return self.get_job_review_table(job_id)

    def get_job_change_set(self, job_id: str) -> dict | None:
        with connect_db(self.db_path) as connection:
            change_set_row = connection.execute(
                """
                SELECT *
                FROM job_change_sets
                WHERE job_id = ?
                ORDER BY created_at DESC, id DESC
                LIMIT 1
                """,
                (job_id,),
            ).fetchone()
            if change_set_row is None:
                return None
            item_rows = connection.execute(
                """
                SELECT *
                FROM job_change_set_items
                WHERE change_set_id = ?
                ORDER BY id ASC
                """,
                (change_set_row["id"],),
            ).fetchall()

        return {
            "id": change_set_row["id"],
            "job_id": change_set_row["job_id"],
            "job_type": change_set_row["job_type"],
            "status": change_set_row["status"],
            "created_at": change_set_row["created_at"],
            "applied_at": change_set_row["applied_at"],
            "item_count": change_set_row["item_count"],
            "applied_item_count": change_set_row["applied_item_count"],
            "items": [self._change_set_item_to_dict(row) for row in item_rows],
        }

    def resolve_job_artifact(self, job_id: str, artifact_name: str) -> tuple[dict, Path] | None:
        job = self.get_job(job_id)
        if job is None:
            return None
        if artifact_name in {"", ".", ".."} or "/" in artifact_name or "\\" in artifact_name:
            raise ValueError("Invalid artifact name")

        artifact_path = self._artifact_directory(job.get("artifact_dir")) / artifact_name
        tracked_names = set(job["artifact_state"]["artifacts"]["csv"]) | set(job["artifact_state"]["artifacts"]["json"])
        if artifact_name not in tracked_names:
            raise FileNotFoundError(artifact_name)
        if not artifact_path.exists() or not artifact_path.is_file():
            raise FileNotFoundError(artifact_name)
        return job, artifact_path

    @staticmethod
    def _validate_params(allowed_params: tuple[str, ...], params: dict[str, Any]) -> None:
        invalid_params = set(params) - set(allowed_params)
        if invalid_params:
            raise ValueError(f"Unsupported job params: {sorted(invalid_params)}")

    def _ensure_job_creation_allowed(self, job_type: str, status: str) -> None:
        return

    def find_active_job(self, job_type: str) -> dict | None:
        with connect_db(self.db_path) as connection:
            row = connection.execute(
                """
                SELECT * FROM jobs
                WHERE job_type = ? AND status IN (?, ?)
                ORDER BY created_at ASC, id ASC
                LIMIT 1
                """,
                (job_type, JobStatus.QUEUED.value, JobStatus.RUNNING.value),
            ).fetchone()
        return self._enrich_job(self._row_to_job(row))

    def _build_command(self, definition, params: dict[str, Any], script_path: Path) -> list[str]:
        command = [self.python_executable, "-u", str(script_path)]
        if definition.job_type == JobType.EXTERNAL_PERSONS:
            for alias in ("faculty_choice", "facultyChoice"):
                if alias in params:
                    command.append(str(params[alias]))
                    break
            command.extend(definition.fixed_args)
            for alias in ("use_openalex_fallback", "useOpenAlexFallback"):
                if alias in params:
                    command.append(str(params[alias]))
                    break
            return command
        for aliases in definition.cli_param_aliases:
            for alias in aliases:
                if alias in params:
                    command.append(str(params[alias]))
                    break
        command.extend(definition.fixed_args)
        return command

    def _default_python_executable(self) -> str:
        venv_python = self.project_root / ".venv" / "bin" / "python"
        if venv_python.is_file():
            return str(venv_python)
        return sys.executable

    @staticmethod
    def _default_artifact_dir(definition, status: JobStatus, job_id: str) -> str:
        if definition.job_type in {
            JobType.INTERNAL_PERSONS,
            JobType.EXTERNAL_PERSONS,
            JobType.EXTERNAL_ORGS,
            JobType.RESEARCH_OUTPUTS,
            JobType.DATASETS,
        } and status in {JobStatus.QUEUED, JobStatus.RUNNING}:
            return str(Path(definition.artifact_dir) / job_id)
        return definition.artifact_dir

    @staticmethod
    def _referer_page_for_job_type(job_type: str) -> str:
        referers = {
            JobType.INTERNAL_PERSONS.value: "enrich_internal_persons_with_ids",
            JobType.EXTERNAL_PERSONS.value: "enrich_external_persons",
            JobType.EXTERNAL_ORGS.value: "enrich_external_orgs",
            JobType.RESEARCH_OUTPUTS.value: "import_research_outputs",
            JobType.DATASETS.value: "import_datasets",
        }
        if job_type not in referers:
            raise ValueError(f"Unsupported job type for apply: {job_type}")
        return referers[job_type]

    def _artifact_directory(self, artifact_dir: str | None) -> Path:
        return self.project_root / (artifact_dir or "")

    @staticmethod
    def _artifact_kind(filename: str) -> str:
        suffix = Path(filename).suffix.lower()
        if suffix == ".csv":
            return "csv"
        if suffix == ".json":
            return "json"
        return "other"

    @staticmethod
    def _primary_review_csv_name(definition, artifact_state: dict[str, Any] | None) -> str | None:
        tracked_csv = tuple(((artifact_state or {}).get("artifacts") or {}).get("csv") or ())
        for filename in definition.required_csv:
            if filename in tracked_csv:
                return filename
        for prefix in definition.required_csv_prefixes:
            for filename in tracked_csv:
                if filename.startswith(prefix) and filename.lower().endswith(".csv"):
                    return filename
        return tracked_csv[0] if tracked_csv else None

    @staticmethod
    def _default_review_table_columns(job_type: str) -> list[str]:
        default_columns = {
            JobType.RESEARCH_OUTPUTS.value: ["to_be_updated", "updated", "doi", "title"],
            JobType.DATASETS.value: ["to_be_updated", "updated", "doi", "title"],
        }
        return list(default_columns.get(job_type, []))

    @staticmethod
    def _append_log(log_path: Path, content: str) -> None:
        with open(log_path, "a", encoding="utf-8") as handle:
            handle.write(content)

    @staticmethod
    def _utcnow() -> str:
        return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")

    @staticmethod
    def _row_to_job(row) -> dict | None:
        if row is None:
            return None
        return {
            "id": row["id"],
            "job_type": row["job_type"],
            "status": row["status"],
            "params": json.loads(row["params_json"]),
            "created_at": row["created_at"],
            "started_at": row["started_at"],
            "finished_at": row["finished_at"],
            "exit_code": row["exit_code"],
            "log_path": row["log_path"],
            "artifact_dir": row["artifact_dir"],
            "artifact_state": json.loads(row["artifacts_json"]) if row["artifacts_json"] else None,
            "results": json.loads(row["results_json"]) if row["results_json"] else None,
            "apply_job_id": row["apply_job_id"],
            "rollback_job_id": row["rollback_job_id"],
            "error_message": row["error_message"],
        }

    def _enrich_job(self, job: dict | None) -> dict | None:
        if job is None:
            return None
        definition = get_job_type_definition(job["job_type"])
        artifact_summary = job.get("artifact_state") or self._empty_artifact_state(job.get("artifact_dir") or definition.artifact_dir)
        results_summary = job.get("results") or self._empty_results_summary(definition)
        change_set = self.get_job_change_set(job["id"])
        if change_set is not None:
            results_summary = self._apply_change_set_results(results_summary, change_set)
        effective_can_apply = artifact_summary["canApply"] and results_summary["ready_count"] > 0
        return {
            **job,
            "canOpen": artifact_summary["canOpen"],
            "canApply": effective_can_apply,
            "artifacts": artifact_summary["artifacts"],
            "results": results_summary,
        }

    def _detect_artifacts(self, job_type: str, artifact_dir: str | None) -> dict:
        definition = get_job_type_definition(job_type)
        directory = self.project_root / (artifact_dir or definition.artifact_dir)
        artifacts = {
            "directory": str(Path(artifact_dir or definition.artifact_dir)),
            "csv": [],
            "json": [],
        }
        if not directory.exists():
            return {
                "canOpen": False,
                "canApply": False,
                "artifacts": artifacts,
            }

        entries = [entry.name for entry in directory.iterdir() if entry.is_file()]
        csv_matches = set()
        json_matches = set()

        for filename in definition.required_csv:
            if filename in entries:
                csv_matches.add(filename)
        for prefix in definition.required_csv_prefixes:
            for entry in entries:
                if entry.startswith(prefix) and entry.endswith(".csv"):
                    csv_matches.add(entry)
        for filename in definition.required_json:
            if filename in entries:
                json_matches.add(filename)

        artifacts["csv"] = sorted(csv_matches)
        artifacts["json"] = sorted(json_matches)

        has_any_artifact = bool(csv_matches or json_matches)
        required_named_csv_ok = all(filename in csv_matches for filename in definition.required_csv)
        required_prefixed_csv_ok = (
            not definition.required_csv_prefixes
            or any(
                any(match.startswith(prefix) for prefix in definition.required_csv_prefixes)
                for match in csv_matches
            )
        )
        required_csv_ok = (
            not definition.required_csv
            and not definition.required_csv_prefixes
        ) or (required_named_csv_ok and required_prefixed_csv_ok)
        required_json_ok = (
            not definition.required_json
            or all(filename in json_matches for filename in definition.required_json)
        )

        return {
            "canOpen": has_any_artifact,
            "canApply": required_csv_ok and required_json_ok,
            "artifacts": artifacts,
        }

    def _detect_run_artifacts(
        self,
        job_type: str,
        artifact_dir: str | None,
        baseline: dict[str, tuple[int, int]],
    ) -> dict:
        definition = get_job_type_definition(job_type)
        directory = self.project_root / (artifact_dir or definition.artifact_dir)
        artifacts = {
            "directory": str(Path(artifact_dir or definition.artifact_dir)),
            "csv": [],
            "json": [],
        }
        if not directory.exists():
            return {
                "canOpen": False,
                "canApply": False,
                "artifacts": artifacts,
            }

        changed_names: set[str] = set()
        for entry in self._matching_artifact_paths(directory, definition):
            signature = self._artifact_signature(entry)
            if baseline.get(entry.name) != signature:
                changed_names.add(entry.name)

        artifacts["csv"] = sorted(name for name in changed_names if name.lower().endswith(".csv"))
        artifacts["json"] = sorted(name for name in changed_names if name.lower().endswith(".json"))

        has_any_artifact = bool(changed_names)
        required_named_csv_ok = all(filename in artifacts["csv"] for filename in definition.required_csv)
        required_prefixed_csv_ok = (
            not definition.required_csv_prefixes
            or any(
                any(match.startswith(prefix) for prefix in definition.required_csv_prefixes)
                for match in artifacts["csv"]
            )
        )
        required_csv_ok = (
            not definition.required_csv
            and not definition.required_csv_prefixes
        ) or (required_named_csv_ok and required_prefixed_csv_ok)
        required_json_ok = (
            not definition.required_json
            or all(filename in artifacts["json"] for filename in definition.required_json)
        )

        return {
            "canOpen": has_any_artifact,
            "canApply": required_csv_ok and required_json_ok,
            "artifacts": artifacts,
        }

    def _summarize_results(
        self,
        job_type: str,
        artifact_dir: str | None,
        *,
        artifact_summary: dict[str, Any] | None = None,
    ) -> dict:
        definition = get_job_type_definition(job_type)
        directory = self._artifact_directory(artifact_dir or definition.artifact_dir)

        tracked_json = None
        tracked_csv = None
        if artifact_summary is not None:
            tracked_json = tuple(artifact_summary["artifacts"]["json"])
            tracked_csv = tuple(artifact_summary["artifacts"]["csv"])

        found_count = self._count_json_entities(directory, tracked_json or definition.required_json)
        ready_ids: set[str] = set()
        updated_ids: set[str] = set()
        review_rows = 0

        for csv_path in self._matching_csv_paths(directory, definition, tracked_names=tracked_csv):
            with open(csv_path, "r", encoding="utf-8", newline="") as handle:
                reader = csv.DictReader(handle)
                for row_index, row in enumerate(reader, start=1):
                    review_rows += 1
                    identity = self._row_identity(row, definition.identity_columns, csv_path.name, row_index)
                    if self._marker_is_selected(row.get("to_be_updated")):
                        ready_ids.add(identity)
                    if self._marker_is_selected(row.get("updated")):
                        updated_ids.add(identity)

        if found_count is None and review_rows:
            found_count = len(ready_ids or updated_ids) or review_rows

        return {
            "entity_label": definition.entity_label_plural,
            "found_label": f"{definition.entity_label_plural.capitalize()} found",
            "ready_label": f"{definition.entity_label_plural.capitalize()} ready to update",
            "updated_label": f"{definition.entity_label_plural.capitalize()} updated",
            "found_count": found_count,
            "ready_count": len(ready_ids),
            "updated_count": len(updated_ids),
        }

    @staticmethod
    def _empty_artifact_state(artifact_dir: str) -> dict:
        return {
            "canOpen": False,
            "canApply": False,
            "artifacts": {
                "directory": str(Path(artifact_dir)),
                "csv": [],
                "json": [],
            },
        }

    @staticmethod
    def _empty_results_summary(definition) -> dict:
        return {
            "entity_label": definition.entity_label_plural,
            "found_label": f"{definition.entity_label_plural.capitalize()} found",
            "ready_label": f"{definition.entity_label_plural.capitalize()} ready to update",
            "updated_label": f"{definition.entity_label_plural.capitalize()} updated",
            "rolled_back_label": f"{definition.entity_label_plural.capitalize()} rolled back",
            "found_count": None,
            "ready_count": 0,
            "updated_count": 0,
            "rolled_back_count": 0,
        }

    @staticmethod
    def _apply_change_set_results(results_summary: dict[str, Any], change_set: dict[str, Any]) -> dict[str, Any]:
        adjusted = dict(results_summary)
        applied_items = [item for item in change_set["items"] if item["apply_status"] == "applied"]
        remaining_updated_entities = {
            item["entity_uuid"]
            for item in applied_items
            if item["rollback_status"] not in {"rolled_back", "conflict", "skipped"}
        }
        rolled_back_entities = {
            item["entity_uuid"] for item in applied_items if item["rollback_status"] == "rolled_back"
        }
        adjusted.setdefault("rolled_back_label", f"{results_summary['entity_label'].capitalize()} rolled back")
        adjusted["updated_count"] = len(remaining_updated_entities)
        adjusted["rolled_back_count"] = len(rolled_back_entities)
        return adjusted

    def _matching_artifact_paths(self, directory: Path, definition) -> list[Path]:
        if not directory.exists():
            return []

        paths: list[Path] = []
        for entry in directory.iterdir():
            if not entry.is_file() or entry.suffix.lower() != ".csv":
                if entry.suffix.lower() != ".json" or entry.name not in definition.required_json:
                    continue
                paths.append(entry)
                continue
            if entry.name in definition.required_csv or any(
                entry.name.startswith(prefix) for prefix in definition.required_csv_prefixes
            ):
                paths.append(entry)
        return sorted(paths, key=lambda path: path.name.lower())

    def _matching_csv_paths(
        self,
        directory: Path,
        definition,
        *,
        tracked_names: tuple[str, ...] | None = None,
    ) -> list[Path]:
        tracked_name_set = set(tracked_names or ())
        return [
            path
            for path in self._matching_artifact_paths(directory, definition)
            if path.suffix.lower() == ".csv" and (not tracked_name_set or path.name in tracked_name_set)
        ]

    def _capture_apply_change_set(self, job: dict) -> str | None:
        if job["job_type"] == JobType.INTERNAL_PERSONS.value:
            changes = self._collect_internal_persons_apply_changes(job)
        elif job["job_type"] == JobType.EXTERNAL_PERSONS.value:
            changes = self._collect_external_persons_apply_changes(job)
        elif job["job_type"] == JobType.EXTERNAL_ORGS.value:
            changes = self._collect_external_orgs_apply_changes(job)
        elif job["job_type"] == JobType.RESEARCH_OUTPUTS.value:
            changes = self._collect_record_creation_apply_changes(job, "research_output")
        elif job["job_type"] == JobType.DATASETS.value:
            changes = self._collect_record_creation_apply_changes(job, "dataset")
        else:
            return None
        if not changes:
            return None

        change_set_id = f"changeset_{uuid.uuid4().hex[:12]}"
        with connect_db(self.db_path) as connection:
            connection.execute(
                """
                INSERT INTO job_change_sets (
                    id, job_id, job_type, status, created_at, item_count, applied_item_count
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    change_set_id,
                    job["id"],
                    job["job_type"],
                    "pending",
                    self._utcnow(),
                    len(changes),
                    0,
                ),
            )
            connection.executemany(
                """
                INSERT INTO job_change_set_items (
                    change_set_id, item_key, entity_uuid, entity_label, field_name,
                    identifier_type, old_value_json, new_value_json, apply_status, conflict_reason
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    (
                        change_set_id,
                        change["item_key"],
                        change["entity_uuid"],
                        change["entity_label"],
                        change["field_name"],
                        change["identifier_type"],
                        json.dumps(change["old_value"], sort_keys=True),
                        json.dumps(change["new_value"], sort_keys=True),
                        "pending",
                        None,
                    )
                    for change in changes
                ],
            )
            connection.commit()
        return change_set_id

    def _collect_internal_persons_apply_changes(self, job: dict) -> list[dict[str, Any]]:
        artifact_state = job["artifact_state"] or {}
        tracked_csv = tuple((artifact_state.get("artifacts") or {}).get("csv") or ())
        tracked_json = tuple((artifact_state.get("artifacts") or {}).get("json") or ())
        if not tracked_csv or not tracked_json:
            return []

        definition = get_job_type_definition(job["job_type"])
        artifact_dir = self._artifact_directory(job.get("artifact_dir"))
        json_by_uuid: dict[str, dict[str, Any]] = {}
        for filename in tracked_json:
            json_path = artifact_dir / filename
            if not json_path.exists():
                continue
            with open(json_path, "r", encoding="utf-8") as handle:
                try:
                    payload = json.load(handle)
                except json.JSONDecodeError:
                    continue
            if not isinstance(payload, list):
                continue
            for item in payload:
                if isinstance(item, dict) and item.get("uuid"):
                    json_by_uuid[item["uuid"]] = item

        changes: list[dict[str, Any]] = []
        for csv_path in self._matching_csv_paths(artifact_dir, definition, tracked_names=tracked_csv):
            with open(csv_path, "r", encoding="utf-8", newline="") as handle:
                reader = csv.DictReader(handle)
                for row in reader:
                    if not self._marker_is_selected(row.get("to_be_updated")):
                        continue
                    entity_uuid = str(row.get("PURE_UUID_PERS") or "").strip()
                    identifier_type = str(row.get("new_id") or "").strip()
                    new_value_raw = str(row.get("new_value") or "").strip()
                    if not entity_uuid or not identifier_type or not new_value_raw:
                        continue
                    old_value, new_value, field_name = self._internal_person_change_values(
                        json_by_uuid.get(entity_uuid, {}),
                        row,
                    )
                    changes.append(
                        {
                            "item_key": self._internal_person_change_key(entity_uuid, identifier_type, new_value_raw),
                            "entity_uuid": entity_uuid,
                            "entity_label": str(row.get("FULL_NAME") or "").strip() or None,
                            "field_name": field_name,
                            "identifier_type": identifier_type,
                            "old_value": old_value,
                            "new_value": new_value,
                        }
                    )
        return changes

    def _collect_external_persons_apply_changes(self, job: dict) -> list[dict[str, Any]]:
        artifact_state = job["artifact_state"] or {}
        tracked_csv = tuple((artifact_state.get("artifacts") or {}).get("csv") or ())
        tracked_json = tuple((artifact_state.get("artifacts") or {}).get("json") or ())
        if not tracked_csv or not tracked_json:
            return []

        definition = get_job_type_definition(job["job_type"])
        artifact_dir = self._artifact_directory(job.get("artifact_dir"))
        json_by_uuid: dict[str, dict[str, Any]] = {}
        for filename in tracked_json:
            json_path = artifact_dir / filename
            if not json_path.exists():
                continue
            with open(json_path, "r", encoding="utf-8") as handle:
                try:
                    payload = json.load(handle)
                except json.JSONDecodeError:
                    continue
            if not isinstance(payload, list):
                continue
            for item in payload:
                if not isinstance(item, dict):
                    continue
                item_uuid = item.get("UUID") or item.get("uuid")
                if item_uuid:
                    json_by_uuid[str(item_uuid)] = item

        current_person_cache: dict[str, dict[str, Any]] = {}
        changes: list[dict[str, Any]] = []
        for csv_path in self._matching_csv_paths(artifact_dir, definition, tracked_names=tracked_csv):
            with open(csv_path, "r", encoding="utf-8", newline="") as handle:
                reader = csv.DictReader(handle)
                for row in reader:
                    if not self._marker_is_selected(row.get("to_be_updated")):
                        continue
                    entity_uuid = str(row.get("Pure_UUID") or "").strip()
                    if not entity_uuid:
                        continue
                    source_person = json_by_uuid.get(entity_uuid, {})
                    current_person = current_person_cache.get(entity_uuid)
                    if current_person is None:
                        try:
                            response = requests.get(
                                f"{PURE_BASE_URL}external-persons/{entity_uuid}",
                                headers=PURE_HEADERS,
                                timeout=30,
                            )
                            response.raise_for_status()
                            current_person = response.json()
                        except requests.RequestException:
                            current_person = {}
                        current_person_cache[entity_uuid] = current_person

                    for column_name, identifier_uri in (("ORCID", ORCID_ID_URI), ("Alex_ID", OPENALEXEX_ID_URI)):
                        identifier_value = self._normalized_csv_value(row.get(column_name))
                        if not identifier_value:
                            continue
                        if self._identifier_exists(
                            current_person.get("identifiers", []),
                            identifier_value,
                            identifier_uri,
                        ):
                            continue
                        if not self._identifier_exists(
                            source_person.get("identifiers", []),
                            identifier_value,
                            identifier_uri,
                        ):
                            continue
                        changes.append(
                            {
                                "item_key": self._external_person_change_key(entity_uuid, identifier_uri, identifier_value),
                                "entity_uuid": entity_uuid,
                                "entity_label": str(row.get("Name") or "").strip() or None,
                                "field_name": "identifier",
                                "identifier_type": column_name.lower(),
                                "old_value": None,
                                "new_value": {"id": identifier_value, "uri": identifier_uri},
                            }
                        )
        return changes

    def _collect_external_orgs_apply_changes(self, job: dict) -> list[dict[str, Any]]:
        artifact_state = job["artifact_state"] or {}
        tracked_csv = tuple((artifact_state.get("artifacts") or {}).get("csv") or ())
        tracked_json = tuple((artifact_state.get("artifacts") or {}).get("json") or ())
        if not tracked_csv or not tracked_json:
            return []

        definition = get_job_type_definition(job["job_type"])
        artifact_dir = self._artifact_directory(job.get("artifact_dir"))
        json_by_uuid: dict[str, dict[str, Any]] = {}
        for filename in tracked_json:
            json_path = artifact_dir / filename
            if not json_path.exists():
                continue
            with open(json_path, "r", encoding="utf-8") as handle:
                try:
                    payload = json.load(handle)
                except json.JSONDecodeError:
                    continue
            if not isinstance(payload, list):
                continue
            for item in payload:
                if not isinstance(item, dict):
                    continue
                item_uuid = item.get("UUID") or item.get("uuid")
                if item_uuid:
                    json_by_uuid[str(item_uuid)] = item

        changes: list[dict[str, Any]] = []
        for csv_path in self._matching_csv_paths(artifact_dir, definition, tracked_names=tracked_csv):
            with open(csv_path, "r", encoding="utf-8", newline="") as handle:
                reader = csv.DictReader(handle)
                for row in reader:
                    if not self._marker_is_selected(row.get("to_be_updated")):
                        continue
                    entity_uuid = str(row.get("uuid") or "").strip()
                    if not entity_uuid:
                        continue
                    source_org = json_by_uuid.get(entity_uuid, {})
                    match_metadata = source_org.get("_btp_match_metadata") or {}

                    if str(row.get("needs_ror_update") or "").strip().lower() == "true":
                        ror_id = self._normalized_csv_value(row.get("ror"))
                        if ror_id and self._identifier_exists(source_org.get("identifiers", []), ror_id, ROR_ID_URI):
                            changes.append(
                                {
                                    "item_key": f"{entity_uuid}|{ROR_ID_URI}|{ror_id}",
                                    "entity_uuid": entity_uuid,
                                    "entity_label": str(row.get("pure_name") or "").strip() or None,
                                    "field_name": "identifier",
                                    "identifier_type": "ror",
                                    "old_value": None,
                                    "new_value": {"id": ror_id, "uri": ROR_ID_URI},
                                }
                            )

                    if str(row.get("needs_geo_update") or "").strip().lower() == "true":
                        address_field = str(match_metadata.get("pure_address_field") or row.get("pure_address_field") or "address")
                        previous_address = match_metadata.get("previous_address")
                        desired_address = match_metadata.get("pure_address_payload")
                        if desired_address:
                            changes.append(
                                {
                                    "item_key": f"{entity_uuid}|{address_field}|address",
                                    "entity_uuid": entity_uuid,
                                    "entity_label": str(row.get("pure_name") or "").strip() or None,
                                    "field_name": address_field,
                                    "identifier_type": "address",
                                    "old_value": {"field": address_field, "value": previous_address},
                                    "new_value": {"field": address_field, "value": desired_address},
                                }
                            )
        return changes

    def _collect_record_creation_apply_changes(self, job: dict, record_type: str) -> list[dict[str, Any]]:
        artifact_state = job["artifact_state"] or {}
        tracked_csv = tuple((artifact_state.get("artifacts") or {}).get("csv") or ())
        if not tracked_csv:
            return []

        definition = get_job_type_definition(job["job_type"])
        artifact_dir = self._artifact_directory(job.get("artifact_dir"))
        changes: list[dict[str, Any]] = []
        for csv_path in self._matching_csv_paths(artifact_dir, definition, tracked_names=tracked_csv):
            with open(csv_path, "r", encoding="utf-8", newline="") as handle:
                reader = csv.DictReader(handle)
                for row in reader:
                    if not self._marker_is_selected(row.get("to_be_updated")):
                        continue
                    doi = self._normalize_doi(str(row.get("doi") or ""))
                    if not doi:
                        continue
                    changes.append(
                        {
                            "item_key": doi,
                            "entity_uuid": doi,
                            "entity_label": str(row.get("title") or "").strip() or None,
                            "field_name": "record",
                            "identifier_type": record_type,
                            "old_value": None,
                            "new_value": {"doi": doi, "record_type": record_type},
                        }
                    )
        return changes

    @staticmethod
    def _internal_person_change_key(entity_uuid: str, identifier_type: str, new_value: str) -> str:
        return f"{entity_uuid}|{identifier_type}|{new_value}"

    @staticmethod
    def _internal_person_change_values(source_person: dict[str, Any], row: dict[str, Any]) -> tuple[Any, dict[str, Any], str]:
        identifier_type = str(row.get("new_id") or "").strip()
        new_value = str(row.get("new_value") or "").strip()
        if identifier_type == "orcid":
            return source_person.get("orcid"), {"value": new_value}, "orcid"

        target_uri = str(row.get("uri") or "").strip()
        old_identifier = None
        for entry in source_person.get("identifiers", []):
            if not isinstance(entry, dict):
                continue
            entry_uri = ((entry.get("type") or {}).get("uri") or "").strip()
            if target_uri and entry_uri == target_uri:
                old_identifier = {
                    "id": entry.get("id"),
                    "uri": entry_uri,
                }
                break
        return old_identifier, {"id": new_value, "uri": target_uri}, "identifier"

    def _finalize_apply_change_set(self, job: dict, change_set_id: str) -> None:
        definition = get_job_type_definition(job["job_type"])
        artifact_state = job["artifact_state"] or self._empty_artifact_state(job.get("artifact_dir") or definition.artifact_dir)
        tracked_csv = tuple(artifact_state["artifacts"]["csv"])
        applied_keys: set[str] = set()
        applied_entities: set[str] = set()
        if job["job_type"] == JobType.INTERNAL_PERSONS.value:
            applied_keys = self._collect_applied_internal_person_keys(
                self._artifact_directory(job.get("artifact_dir")),
                definition,
                tracked_csv,
            )
        elif job["job_type"] == JobType.EXTERNAL_PERSONS.value:
            applied_entities = self._collect_applied_external_person_uuids(
                self._artifact_directory(job.get("artifact_dir")),
                definition,
                tracked_csv,
            )
        elif job["job_type"] == JobType.EXTERNAL_ORGS.value:
            applied_entities = self._collect_applied_external_org_uuids(
                self._artifact_directory(job.get("artifact_dir")),
                definition,
                tracked_csv,
            )
        elif job["job_type"] in {JobType.RESEARCH_OUTPUTS.value, JobType.DATASETS.value}:
            applied_records = self._load_apply_manifest(job)
        else:
            applied_records = {}

        with connect_db(self.db_path) as connection:
            item_rows = connection.execute(
                "SELECT id, item_key, entity_uuid FROM job_change_set_items WHERE change_set_id = ?",
                (change_set_id,),
            ).fetchall()
            applied_count = 0
            for row in item_rows:
                if job["job_type"] in {JobType.EXTERNAL_PERSONS.value, JobType.EXTERNAL_ORGS.value}:
                    status = "applied" if row["entity_uuid"] in applied_entities else "not_applied"
                    new_value_json = None
                elif job["job_type"] in {JobType.RESEARCH_OUTPUTS.value, JobType.DATASETS.value}:
                    applied_record = applied_records.get(row["item_key"])
                    status = "applied" if applied_record else "not_applied"
                    new_value_json = json.dumps(applied_record, sort_keys=True) if applied_record else None
                else:
                    status = "applied" if row["item_key"] in applied_keys else "not_applied"
                    new_value_json = None
                if status == "applied":
                    applied_count += 1
                if new_value_json is not None:
                    connection.execute(
                        "UPDATE job_change_set_items SET apply_status = ?, new_value_json = ?, conflict_reason = NULL WHERE id = ?",
                        (status, new_value_json, row["id"]),
                    )
                else:
                    connection.execute(
                        "UPDATE job_change_set_items SET apply_status = ?, conflict_reason = NULL WHERE id = ?",
                        (status, row["id"]),
                    )
            connection.execute(
                """
                UPDATE job_change_sets
                SET status = ?, applied_at = ?, applied_item_count = ?
                WHERE id = ?
                """,
                ("applied" if applied_count > 0 else "no_applied_changes", self._utcnow(), applied_count, change_set_id),
            )
            connection.commit()

    def _mark_change_set_failed(self, change_set_id: str) -> None:
        with connect_db(self.db_path) as connection:
            connection.execute(
                "UPDATE job_change_sets SET status = ? WHERE id = ?",
                ("failed", change_set_id),
            )
            connection.commit()

    def _collect_applied_internal_person_keys(
        self,
        artifact_dir: Path,
        definition,
        tracked_csv: tuple[str, ...],
    ) -> set[str]:
        applied_keys: set[str] = set()
        for csv_path in self._matching_csv_paths(artifact_dir, definition, tracked_names=tracked_csv):
            with open(csv_path, "r", encoding="utf-8", newline="") as handle:
                reader = csv.DictReader(handle)
                for row in reader:
                    if not self._marker_is_selected(row.get("updated")):
                        continue
                    entity_uuid = str(row.get("PURE_UUID_PERS") or "").strip()
                    identifier_type = str(row.get("new_id") or "").strip()
                    new_value = str(row.get("new_value") or "").strip()
                    if not entity_uuid or not identifier_type or not new_value:
                        continue
                    applied_keys.add(self._internal_person_change_key(entity_uuid, identifier_type, new_value))
        return applied_keys

    def _collect_applied_external_person_uuids(
        self,
        artifact_dir: Path,
        definition,
        tracked_csv: tuple[str, ...],
    ) -> set[str]:
        applied_entities: set[str] = set()
        for csv_path in self._matching_csv_paths(artifact_dir, definition, tracked_names=tracked_csv):
            with open(csv_path, "r", encoding="utf-8", newline="") as handle:
                reader = csv.DictReader(handle)
                for row in reader:
                    if not self._marker_is_selected(row.get("updated")):
                        continue
                    entity_uuid = str(row.get("Pure_UUID") or "").strip()
                    if entity_uuid:
                        applied_entities.add(entity_uuid)
        return applied_entities

    def _collect_applied_external_org_uuids(
        self,
        artifact_dir: Path,
        definition,
        tracked_csv: tuple[str, ...],
    ) -> set[str]:
        applied_entities: set[str] = set()
        for csv_path in self._matching_csv_paths(artifact_dir, definition, tracked_names=tracked_csv):
            with open(csv_path, "r", encoding="utf-8", newline="") as handle:
                reader = csv.DictReader(handle)
                for row in reader:
                    if not self._marker_is_selected(row.get("updated")):
                        continue
                    entity_uuid = str(row.get("uuid") or "").strip()
                    if entity_uuid:
                        applied_entities.add(entity_uuid)
        return applied_entities

    def _load_apply_manifest(self, job: dict) -> dict[str, dict[str, Any]]:
        manifest_path = self._artifact_directory(job.get("artifact_dir")) / "apply_manifest.jsonl"
        if not manifest_path.exists():
            return {}

        applied_records: dict[str, dict[str, Any]] = {}
        with open(manifest_path, "r", encoding="utf-8") as handle:
            for line in handle:
                line = line.strip()
                if not line:
                    continue
                try:
                    entry = json.loads(line)
                except json.JSONDecodeError:
                    continue
                item_key = self._normalize_doi(str(entry.get("item_key") or entry.get("doi") or ""))
                if not item_key:
                    continue
                applied_records[item_key] = {
                    "doi": item_key,
                    "record_type": entry.get("job_type"),
                    "record_uuid": entry.get("record_uuid"),
                    "created_external_persons": entry.get("created_external_persons", []),
                }
        return applied_records

    @staticmethod
    def _external_person_change_key(entity_uuid: str, identifier_uri: str, identifier_value: str) -> str:
        return f"{entity_uuid}|{identifier_uri}|{identifier_value}"

    @staticmethod
    def _normalize_doi(value: str) -> str:
        return value.replace("https://doi.org/", "").replace("http://doi.org/", "").strip().lower()

    @staticmethod
    def _normalized_csv_value(value: Any) -> str:
        if value is None:
            return ""
        text = str(value).strip()
        if not text or text.lower() == "nan":
            return ""
        return text

    @staticmethod
    def _identifier_exists(identifiers: list[dict[str, Any]], identifier_value: str, identifier_uri: str) -> bool:
        for entry in identifiers or []:
            if not isinstance(entry, dict):
                continue
            if entry.get("id") == identifier_value and ((entry.get("type") or {}).get("uri")) == identifier_uri:
                return True
        return False

    @staticmethod
    def _change_set_item_to_dict(row) -> dict[str, Any]:
        return {
            "id": row["id"],
            "change_set_id": row["change_set_id"],
            "item_key": row["item_key"],
            "entity_uuid": row["entity_uuid"],
            "entity_label": row["entity_label"],
            "field_name": row["field_name"],
            "identifier_type": row["identifier_type"],
            "old_value": json.loads(row["old_value_json"]) if row["old_value_json"] else None,
            "new_value": json.loads(row["new_value_json"]) if row["new_value_json"] else None,
            "apply_status": row["apply_status"],
            "rollback_status": row["rollback_status"],
            "conflict_reason": row["conflict_reason"],
        }

    def _execute_internal_persons_rollback(self, change_set: dict[str, Any], log_path: Path) -> dict[str, int]:
        persons_cache: dict[str, dict[str, Any]] = {}
        stats = {"checked": 0, "rolled_back": 0, "conflicts": 0, "failed": 0}

        with connect_db(self.db_path) as connection:
            for item in change_set["items"]:
                if item["apply_status"] != "applied":
                    connection.execute(
                        "UPDATE job_change_set_items SET rollback_status = ?, conflict_reason = ? WHERE id = ?",
                        ("skipped", "apply_status_not_applied", item["id"]),
                    )
                    continue

                stats["checked"] += 1
                entity_uuid = item["entity_uuid"]
                person = persons_cache.get(entity_uuid)
                if person is None:
                    try:
                        response = requests.get(
                            f"{PURE_BASE_URL}persons/{entity_uuid}",
                            headers=PURE_HEADERS,
                            timeout=30,
                        )
                        response.raise_for_status()
                        person = response.json()
                        persons_cache[entity_uuid] = person
                    except requests.RequestException as exc:
                        stats["failed"] += 1
                        connection.execute(
                            "UPDATE job_change_set_items SET rollback_status = ?, conflict_reason = ? WHERE id = ?",
                            ("failed", str(exc), item["id"]),
                        )
                        self._append_log(log_path, f"ERROR Could not load person {entity_uuid}: {exc}\n")
                        continue

                if item["field_name"] == "orcid":
                    current_value = person.get("orcid")
                    expected_value = (item["new_value"] or {}).get("value")
                    if current_value != expected_value:
                        stats["conflicts"] += 1
                        reason = f"Current ORCID does not match job-applied value for {entity_uuid}"
                        connection.execute(
                            "UPDATE job_change_set_items SET rollback_status = ?, conflict_reason = ? WHERE id = ?",
                            ("conflict", reason, item["id"]),
                        )
                        self._append_log(log_path, f"SKIP {reason}\n")
                        continue
                    if item["old_value"] in {None, ""}:
                        person["orcid"] = None
                    else:
                        person["orcid"] = item["old_value"]
                else:
                    identifiers = person.get("identifiers", [])
                    new_identifier = item["new_value"] or {}
                    new_id = new_identifier.get("id")
                    new_uri = new_identifier.get("uri")
                    match_index = next(
                        (
                            index
                            for index, entry in enumerate(identifiers)
                            if isinstance(entry, dict)
                            and entry.get("id") == new_id
                            and ((entry.get("type") or {}).get("uri")) == new_uri
                        ),
                        None,
                    )
                    if match_index is None:
                        stats["conflicts"] += 1
                        reason = f"Current identifier does not match job-applied value for {entity_uuid}"
                        connection.execute(
                            "UPDATE job_change_set_items SET rollback_status = ?, conflict_reason = ? WHERE id = ?",
                            ("conflict", reason, item["id"]),
                        )
                        self._append_log(log_path, f"SKIP {reason}\n")
                        continue

                    old_identifier = item["old_value"]
                    if old_identifier:
                        identifiers[match_index]["id"] = old_identifier.get("id")
                        identifiers[match_index].setdefault("type", {})["uri"] = old_identifier.get("uri")
                    else:
                        identifiers.pop(match_index)
                    person["identifiers"] = identifiers

                try:
                    response = requests.put(
                        f"{PURE_BASE_URL}persons/{entity_uuid}",
                        headers=PURE_HEADERS,
                        json=person,
                        timeout=30,
                    )
                    response.raise_for_status()
                    stats["rolled_back"] += 1
                    connection.execute(
                        "UPDATE job_change_set_items SET rollback_status = ?, conflict_reason = NULL WHERE id = ?",
                        ("rolled_back", item["id"]),
                    )
                    self._append_log(log_path, f"ROLLED BACK {entity_uuid} {item['identifier_type']}\n")
                except requests.RequestException as exc:
                    stats["failed"] += 1
                    connection.execute(
                        "UPDATE job_change_set_items SET rollback_status = ?, conflict_reason = ? WHERE id = ?",
                        ("failed", str(exc), item["id"]),
                    )
                    self._append_log(log_path, f"ERROR Could not rollback {entity_uuid}: {exc}\n")
                    continue

            connection.commit()

        self._append_log(
            log_path,
            (
                f"Rollback checked {stats['checked']} items, "
                f"rolled back {stats['rolled_back']}, "
                f"skipped {stats['conflicts']} conflicts, "
                f"failed {stats['failed']}.\n"
            ),
        )
        return stats

    def _execute_external_persons_rollback(self, change_set: dict[str, Any], log_path: Path) -> dict[str, int]:
        persons_cache: dict[str, dict[str, Any]] = {}
        stats = {"checked": 0, "rolled_back": 0, "conflicts": 0, "failed": 0}

        with connect_db(self.db_path) as connection:
            grouped_items: dict[str, list[dict[str, Any]]] = {}
            for item in change_set["items"]:
                if item["apply_status"] != "applied":
                    connection.execute(
                        "UPDATE job_change_set_items SET rollback_status = ?, conflict_reason = ? WHERE id = ?",
                        ("skipped", "apply_status_not_applied", item["id"]),
                    )
                    continue
                if item["rollback_status"] in {"rolled_back", "conflict", "skipped"}:
                    continue
                grouped_items.setdefault(item["entity_uuid"], []).append(item)

            for entity_uuid, entity_items in grouped_items.items():
                stats["checked"] += len(entity_items)
                person = persons_cache.get(entity_uuid)
                if person is None:
                    try:
                        response = requests.get(
                            f"{PURE_BASE_URL}external-persons/{entity_uuid}",
                            headers=PURE_HEADERS,
                            timeout=30,
                        )
                        response.raise_for_status()
                        person = response.json()
                        persons_cache[entity_uuid] = person
                    except requests.RequestException as exc:
                        stats["failed"] += 1
                        connection.execute(
                            "UPDATE job_change_set_items SET rollback_status = ?, conflict_reason = ? WHERE id = ?",
                            ("failed", str(exc), item["id"]),
                        )
                        self._append_log(log_path, f"ERROR Could not load external person {entity_uuid}: {exc}\n")
                        continue

                identifiers = person.get("identifiers", [])
                removable_indexes: list[tuple[int, dict[str, Any]]] = []
                conflict_found = False
                for item in entity_items:
                    new_identifier = item["new_value"] or {}
                    new_id = new_identifier.get("id")
                    new_uri = new_identifier.get("uri")
                    match_index = next(
                        (
                            index
                            for index, entry in enumerate(identifiers)
                            if isinstance(entry, dict)
                            and entry.get("id") == new_id
                            and ((entry.get("type") or {}).get("uri")) == new_uri
                        ),
                        None,
                    )
                    if match_index is None:
                        stats["conflicts"] += 1
                        reason = f"Current external-person identifier does not match job-applied value for {entity_uuid}"
                        connection.execute(
                            "UPDATE job_change_set_items SET rollback_status = ?, conflict_reason = ? WHERE id = ?",
                            ("conflict", reason, item["id"]),
                        )
                        self._append_log(log_path, f"SKIP {reason}\n")
                        conflict_found = True
                        continue
                    removable_indexes.append((match_index, item))

                if not removable_indexes:
                    continue

                updated_identifiers = [
                    entry
                    for index, entry in enumerate(identifiers)
                    if index not in {match_index for match_index, _item in removable_indexes}
                ]
                person["identifiers"] = updated_identifiers

                try:
                    response = requests.put(
                        f"{PURE_BASE_URL}external-persons/{entity_uuid}",
                        headers=PURE_HEADERS,
                        json=person,
                        timeout=30,
                    )
                    response.raise_for_status()
                    stats["rolled_back"] += len(removable_indexes)
                    for _match_index, item in removable_indexes:
                        connection.execute(
                            "UPDATE job_change_set_items SET rollback_status = ?, conflict_reason = NULL WHERE id = ?",
                            ("rolled_back", item["id"]),
                        )
                        self._append_log(log_path, f"ROLLED BACK {entity_uuid} {item['identifier_type']}\n")
                except requests.RequestException as exc:
                    stats["failed"] += len(removable_indexes)
                    for _match_index, item in removable_indexes:
                        connection.execute(
                            "UPDATE job_change_set_items SET rollback_status = ?, conflict_reason = ? WHERE id = ?",
                            ("failed", str(exc), item["id"]),
                        )
                    self._append_log(log_path, f"ERROR Could not rollback external person {entity_uuid}: {exc}\n")
                    continue

            connection.commit()

        self._append_log(
            log_path,
            (
                f"Rollback checked {stats['checked']} items, "
                f"rolled back {stats['rolled_back']}, "
                f"skipped {stats['conflicts']} conflicts, "
                f"failed {stats['failed']}.\n"
            ),
        )
        return stats

    def _execute_external_orgs_rollback(self, change_set: dict[str, Any], log_path: Path) -> dict[str, int]:
        org_cache: dict[str, dict[str, Any]] = {}
        stats = {"checked": 0, "rolled_back": 0, "conflicts": 0, "failed": 0}

        with connect_db(self.db_path) as connection:
            grouped_items: dict[str, list[dict[str, Any]]] = {}
            for item in change_set["items"]:
                if item["apply_status"] != "applied":
                    connection.execute(
                        "UPDATE job_change_set_items SET rollback_status = ?, conflict_reason = ? WHERE id = ?",
                        ("skipped", "apply_status_not_applied", item["id"]),
                    )
                    continue
                if item["rollback_status"] in {"rolled_back", "conflict", "skipped"}:
                    continue
                grouped_items.setdefault(item["entity_uuid"], []).append(item)

            for entity_uuid, entity_items in grouped_items.items():
                stats["checked"] += len(entity_items)
                org = org_cache.get(entity_uuid)
                if org is None:
                    try:
                        response = requests.get(
                            f"{PURE_BASE_URL}external-organizations/{entity_uuid}",
                            headers=PURE_HEADERS,
                            timeout=30,
                        )
                        response.raise_for_status()
                        org = response.json()
                        org_cache[entity_uuid] = org
                    except requests.RequestException as exc:
                        stats["failed"] += len(entity_items)
                        for item in entity_items:
                            connection.execute(
                                "UPDATE job_change_set_items SET rollback_status = ?, conflict_reason = ? WHERE id = ?",
                                ("failed", str(exc), item["id"]),
                            )
                        self._append_log(log_path, f"ERROR Could not load external organization {entity_uuid}: {exc}\n")
                        continue

                conflict_items: list[dict[str, Any]] = []
                rollback_items: list[dict[str, Any]] = []
                for item in entity_items:
                    if item["identifier_type"] == "ror":
                        new_identifier = item["new_value"] or {}
                        if not self._identifier_exists(org.get("identifiers", []), new_identifier.get("id"), new_identifier.get("uri")):
                            conflict_items.append(item)
                            continue
                    elif item["identifier_type"] == "address":
                        field_name = (item["new_value"] or {}).get("field") or item["field_name"]
                        current_value = org.get(field_name)
                        expected_value = (item["new_value"] or {}).get("value")
                        if current_value != expected_value:
                            conflict_items.append(item)
                            continue
                    rollback_items.append(item)

                for item in conflict_items:
                    stats["conflicts"] += 1
                    reason = f"Current external-organization value does not match job-applied value for {entity_uuid}"
                    connection.execute(
                        "UPDATE job_change_set_items SET rollback_status = ?, conflict_reason = ? WHERE id = ?",
                        ("conflict", reason, item["id"]),
                    )
                    self._append_log(log_path, f"SKIP {reason}\n")

                if not rollback_items:
                    continue

                updated_org = dict(org)
                identifiers = list(updated_org.get("identifiers", []) or [])
                for item in rollback_items:
                    if item["identifier_type"] == "ror":
                        new_identifier = item["new_value"] or {}
                        identifiers = [
                            entry for entry in identifiers
                            if not (
                                isinstance(entry, dict)
                                and entry.get("id") == new_identifier.get("id")
                                and ((entry.get("type") or {}).get("uri")) == new_identifier.get("uri")
                            )
                        ]
                    elif item["identifier_type"] == "address":
                        old_value = item["old_value"] or {}
                        field_name = old_value.get("field") or (item["new_value"] or {}).get("field") or item["field_name"]
                        if old_value.get("value"):
                            updated_org[field_name] = old_value.get("value")
                        else:
                            updated_org[field_name] = None
                updated_org["identifiers"] = identifiers

                try:
                    response = requests.put(
                        f"{PURE_BASE_URL}external-organizations/{entity_uuid}",
                        headers=PURE_HEADERS,
                        json=updated_org,
                        timeout=30,
                    )
                    response.raise_for_status()
                    org_cache[entity_uuid] = updated_org
                    stats["rolled_back"] += len(rollback_items)
                    for item in rollback_items:
                        connection.execute(
                            "UPDATE job_change_set_items SET rollback_status = ?, conflict_reason = NULL WHERE id = ?",
                            ("rolled_back", item["id"]),
                        )
                        self._append_log(log_path, f"ROLLED BACK {entity_uuid} {item['identifier_type']}\n")
                except requests.RequestException as exc:
                    stats["failed"] += len(rollback_items)
                    for item in rollback_items:
                        connection.execute(
                            "UPDATE job_change_set_items SET rollback_status = ?, conflict_reason = ? WHERE id = ?",
                            ("failed", str(exc), item["id"]),
                        )
                    self._append_log(log_path, f"ERROR Could not rollback external organization {entity_uuid}: {exc}\n")

            connection.commit()

        self._append_log(
            log_path,
            (
                f"Rollback checked {stats['checked']} items, "
                f"rolled back {stats['rolled_back']}, "
                f"skipped {stats['conflicts']} conflicts, "
                f"failed {stats['failed']}.\n"
            ),
        )
        return stats

    def _execute_research_outputs_rollback(self, change_set: dict[str, Any], log_path: Path) -> dict[str, int]:
        return self._execute_created_record_rollback(
            change_set,
            log_path,
            endpoint="research-outputs",
            doi_getter=self._research_output_dois,
            label="research output",
        )

    def _execute_datasets_rollback(self, change_set: dict[str, Any], log_path: Path) -> dict[str, int]:
        return self._execute_created_record_rollback(
            change_set,
            log_path,
            endpoint="data-sets",
            doi_getter=self._dataset_dois,
            label="dataset",
        )

    def _execute_created_record_rollback(
        self,
        change_set: dict[str, Any],
        log_path: Path,
        *,
        endpoint: str,
        doi_getter,
        label: str,
    ) -> dict[str, int]:
        stats = {"checked": 0, "rolled_back": 0, "conflicts": 0, "failed": 0}

        with connect_db(self.db_path) as connection:
            for item in change_set["items"]:
                if item["apply_status"] != "applied":
                    connection.execute(
                        "UPDATE job_change_set_items SET rollback_status = ?, conflict_reason = ? WHERE id = ?",
                        ("skipped", "apply_status_not_applied", item["id"]),
                    )
                    continue
                if item["rollback_status"] in {"rolled_back", "conflict", "skipped"}:
                    continue

                stats["checked"] += 1
                item_payload = item.get("new_value") or {}
                record_uuid = str(item_payload.get("record_uuid") or "").strip()
                expected_doi = self._normalize_doi(str(item_payload.get("doi") or item["entity_uuid"] or ""))
                if not record_uuid:
                    stats["conflicts"] += 1
                    reason = f"No created {label} UUID was recorded for {item['entity_uuid']}"
                    connection.execute(
                        "UPDATE job_change_set_items SET rollback_status = ?, conflict_reason = ? WHERE id = ?",
                        ("conflict", reason, item["id"]),
                    )
                    continue

                try:
                    response = requests.get(
                        f"{PURE_BASE_URL}{endpoint}/{record_uuid}",
                        headers=PURE_HEADERS,
                        timeout=30,
                    )
                    response.raise_for_status()
                    current_record = response.json()
                except requests.RequestException as exc:
                    stats["failed"] += 1
                    connection.execute(
                        "UPDATE job_change_set_items SET rollback_status = ?, conflict_reason = ? WHERE id = ?",
                        ("failed", str(exc), item["id"]),
                    )
                    self._append_log(log_path, f"ERROR Could not load {label} {record_uuid}: {exc}\n")
                    continue

                current_dois = {self._normalize_doi(doi) for doi in doi_getter(current_record)}
                if expected_doi and expected_doi not in current_dois:
                    stats["conflicts"] += 1
                    reason = f"Current {label} DOI does not match job-applied DOI for {record_uuid}"
                    connection.execute(
                        "UPDATE job_change_set_items SET rollback_status = ?, conflict_reason = ? WHERE id = ?",
                        ("conflict", reason, item["id"]),
                    )
                    continue

                try:
                    delete_response = requests.delete(
                        f"{PURE_BASE_URL}{endpoint}/{record_uuid}",
                        headers=PURE_HEADERS,
                        timeout=30,
                    )
                    delete_response.raise_for_status()
                except requests.RequestException as exc:
                    stats["failed"] += 1
                    connection.execute(
                        "UPDATE job_change_set_items SET rollback_status = ?, conflict_reason = ? WHERE id = ?",
                        ("failed", str(exc), item["id"]),
                    )
                    self._append_log(log_path, f"ERROR Could not delete {label} {record_uuid}: {exc}\n")
                    continue

                for person in item_payload.get("created_external_persons", []) or []:
                    self._delete_job_created_external_person(person, log_path)

                stats["rolled_back"] += 1
                connection.execute(
                    "UPDATE job_change_set_items SET rollback_status = ?, conflict_reason = NULL WHERE id = ?",
                    ("rolled_back", item["id"]),
                )
                self._append_log(log_path, f"ROLLED BACK {label} {record_uuid} ({expected_doi})\n")

            connection.commit()

        self._append_log(
            log_path,
            (
                f"Rollback checked {stats['checked']} items, "
                f"rolled back {stats['rolled_back']}, "
                f"skipped {stats['conflicts']} conflicts, "
                f"failed {stats['failed']}.\n"
            ),
        )
        return stats

    def _delete_job_created_external_person(self, person: dict[str, Any], log_path: Path) -> None:
        person_uuid = str(person.get("uuid") or "").strip()
        if not person_uuid:
            return
        try:
            response = requests.get(
                f"{PURE_BASE_URL}external-persons/{person_uuid}",
                headers=PURE_HEADERS,
                timeout=30,
            )
            response.raise_for_status()
            current_person = response.json()
        except requests.RequestException as exc:
            self._append_log(log_path, f"WARNING Could not load created external person {person_uuid}: {exc}\n")
            return

        current_name = current_person.get("name") or {}
        expected_first = str(person.get("first_name") or "")
        expected_last = str(person.get("last_name") or "")
        if expected_first and current_name.get("firstName") != expected_first:
            self._append_log(log_path, f"WARNING Skipped external person rollback for {person_uuid}: first name mismatch\n")
            return
        if expected_last and current_name.get("lastName") != expected_last:
            self._append_log(log_path, f"WARNING Skipped external person rollback for {person_uuid}: last name mismatch\n")
            return

        try:
            delete_response = requests.delete(
                f"{PURE_BASE_URL}external-persons/{person_uuid}",
                headers=PURE_HEADERS,
                timeout=30,
            )
            delete_response.raise_for_status()
            self._append_log(log_path, f"ROLLED BACK external person {person_uuid}\n")
        except requests.RequestException as exc:
            self._append_log(log_path, f"WARNING Could not delete created external person {person_uuid}: {exc}\n")

    @staticmethod
    def _research_output_dois(record: dict[str, Any]) -> list[str]:
        dois = []
        for version in record.get("electronicVersions", []) or []:
            doi = version.get("doi")
            if doi:
                dois.append(str(doi))
        return dois

    @staticmethod
    def _dataset_dois(record: dict[str, Any]) -> list[str]:
        doi = ((record.get("doi") or {}).get("doi") or "")
        return [str(doi)] if doi else []

    @staticmethod
    def _marker_is_selected(value: str | None) -> bool:
        return str(value or "").strip().lower() == "x"

    @staticmethod
    def _row_identity(row: dict[str, Any], identity_columns: tuple[str, ...], filename: str, row_index: int) -> str:
        for column in identity_columns:
            raw_value = row.get(column)
            if raw_value is not None and str(raw_value).strip():
                return str(raw_value).strip()
        return f"{filename}:{row_index}"

    def _snapshot_artifact_state(self, job_type: str, artifact_dir: str | None) -> dict[str, tuple[int, int]]:
        definition = get_job_type_definition(job_type)
        directory = self._artifact_directory(artifact_dir or definition.artifact_dir)
        return {
            path.name: self._artifact_signature(path)
            for path in self._matching_artifact_paths(directory, definition)
        }

    @staticmethod
    def _artifact_signature(path: Path) -> tuple[int, int]:
        stats = path.stat()
        return stats.st_mtime_ns, stats.st_size

    def _count_json_entities(self, directory: Path, filenames: tuple[str, ...]) -> int | None:
        if not directory.exists():
            return None

        total = 0
        found_any = False
        for filename in filenames:
            json_path = directory / filename
            if not json_path.exists() or not json_path.is_file():
                continue
            with open(json_path, "r", encoding="utf-8") as handle:
                try:
                    payload = json.load(handle)
                except json.JSONDecodeError:
                    continue
            found_any = True
            if isinstance(payload, list):
                total += len(payload)
            elif isinstance(payload, dict):
                total += len(payload)
        return total if found_any else None

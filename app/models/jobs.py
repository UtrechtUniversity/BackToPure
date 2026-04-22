from __future__ import annotations

from dataclasses import dataclass
from enum import Enum


class JobType(str, Enum):
    INTERNAL_PERSONS = "internal_persons"
    EXTERNAL_PERSONS = "external_persons"
    EXTERNAL_ORGS = "external_orgs"
    RESEARCH_OUTPUTS = "research_outputs"
    DATASETS = "datasets"


class JobStatus(str, Enum):
    QUEUED = "queued"
    RUNNING = "running"
    NEEDS_REVIEW = "needs_review"
    APPLYING = "applying"
    FAILED = "failed"
    COMPLETED = "completed"


@dataclass(frozen=True)
class JobTypeDefinition:
    job_type: JobType
    script_path: str
    artifact_dir: str
    entity_label_plural: str
    allowed_params: tuple[str, ...]
    cli_param_aliases: tuple[tuple[str, ...], ...]
    identity_columns: tuple[str, ...] = ()
    required_csv: tuple[str, ...] = ()
    required_csv_prefixes: tuple[str, ...] = ()
    required_json: tuple[str, ...] = ()
    fixed_args: tuple[str, ...] = ()


JOB_TYPE_REGISTRY: dict[JobType, JobTypeDefinition] = {
    JobType.INTERNAL_PERSONS: JobTypeDefinition(
        job_type=JobType.INTERNAL_PERSONS,
        script_path="src/enrich_internal_persons_with_ids.py",
        artifact_dir="output/internal_persons",
        entity_label_plural="persons",
        required_csv_prefixes=("personstobeupdated_",),
        required_json=("datatotal.json",),
        allowed_params=("faculty_choice", "facultyChoice"),
        cli_param_aliases=(("faculty_choice", "facultyChoice"),),
        identity_columns=("PURE_UUID_PERS", "person_id"),
    ),
    JobType.EXTERNAL_PERSONS: JobTypeDefinition(
        job_type=JobType.EXTERNAL_PERSONS,
        script_path="src/enrich_pure_external_persons.py",
        artifact_dir="output/external_persons",
        entity_label_plural="persons",
        required_csv=("ext_pers_update.csv",),
        required_json=("to_be_updated.json",),
        allowed_params=("faculty_choice", "facultyChoice", "use_openalex_fallback", "useOpenAlexFallback"),
        cli_param_aliases=(
            ("faculty_choice", "facultyChoice"),
            ("use_openalex_fallback", "useOpenAlexFallback"),
        ),
        identity_columns=("Pure_UUID", "Alex_ID", "ORCID", "Name"),
        fixed_args=("yes",),
    ),
    JobType.EXTERNAL_ORGS: JobTypeDefinition(
        job_type=JobType.EXTERNAL_ORGS,
        script_path="src/enrich_pure_external_orgs.py",
        artifact_dir="output/external_orgs",
        entity_label_plural="organisations",
        required_csv=("external_orgs_to_update.csv",),
        required_json=("external_orgs_updates.json",),
        allowed_params=("faculty_choice", "facultyChoice"),
        cli_param_aliases=(("faculty_choice", "facultyChoice"),),
        identity_columns=("uuid",),
    ),
    JobType.RESEARCH_OUTPUTS: JobTypeDefinition(
        job_type=JobType.RESEARCH_OUTPUTS,
        script_path="src/update_researchoutput_from_ricgraph.py",
        artifact_dir="output/research_output",
        entity_label_plural="research outputs",
        required_csv=("to_be_updated.csv",),
        required_json=("output_to_be_updated.json",),
        allowed_params=("faculty_choice", "facultyChoice"),
        cli_param_aliases=(("faculty_choice", "facultyChoice"),),
        identity_columns=("doi",),
    ),
    JobType.DATASETS: JobTypeDefinition(
        job_type=JobType.DATASETS,
        script_path="src/update_datasets_from_ricgraph.py",
        artifact_dir="output/datasets",
        entity_label_plural="datasets",
        required_csv=("to_be_updated.csv",),
        required_json=("datasets_to_be_updated.json",),
        allowed_params=("faculty_choice", "facultyChoice"),
        cli_param_aliases=(("faculty_choice", "facultyChoice"),),
        identity_columns=("doi",),
    ),
}


VALID_STATUS_TRANSITIONS: dict[JobStatus, set[JobStatus]] = {
    JobStatus.QUEUED: {JobStatus.RUNNING, JobStatus.FAILED},
    JobStatus.RUNNING: {JobStatus.NEEDS_REVIEW, JobStatus.COMPLETED, JobStatus.FAILED},
    JobStatus.NEEDS_REVIEW: {JobStatus.APPLYING, JobStatus.COMPLETED, JobStatus.FAILED},
    JobStatus.APPLYING: {JobStatus.COMPLETED, JobStatus.FAILED},
    JobStatus.FAILED: set(),
    JobStatus.COMPLETED: set(),
}


def parse_job_type(value: str | JobType) -> JobType:
    if isinstance(value, JobType):
        return value
    try:
        return JobType(value)
    except ValueError as exc:
        raise ValueError(f"Unsupported job type: {value}") from exc


def parse_job_status(value: str | JobStatus) -> JobStatus:
    if isinstance(value, JobStatus):
        return value
    try:
        return JobStatus(value)
    except ValueError as exc:
        raise ValueError(f"Unsupported job status: {value}") from exc


def get_job_type_definition(value: str | JobType) -> JobTypeDefinition:
    job_type = parse_job_type(value)
    return JOB_TYPE_REGISTRY[job_type]


def is_valid_transition(current: str | JobStatus, new: str | JobStatus) -> bool:
    current_status = parse_job_status(current)
    new_status = parse_job_status(new)
    return new_status in VALID_STATUS_TRANSITIONS[current_status]

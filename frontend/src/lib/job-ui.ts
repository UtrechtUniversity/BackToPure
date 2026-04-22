import type { JobRecord, JobStatus, JobType } from "./types";

const ACTIVE_STATUSES: JobStatus[] = ["queued", "running", "applying"];

export interface MetricDefinition {
  label: string;
  description: string;
}

export interface JobTypeGuide {
  jobType: JobType;
  title: string;
  availability: "live" | "legacy" | "planned";
  summary: string;
  currentScope: string;
  reviewFiles: string[];
  resultDefinitions: MetricDefinition[];
  userSteps: string[];
}

const JOB_TYPE_GUIDES: Record<JobType, JobTypeGuide> = {
  internal_persons: {
    jobType: "internal_persons",
    title: "Internal Persons",
    availability: "live",
    summary: "Enrich internal Pure person records with identifiers and prepare a reviewable update file.",
    currentScope: "Use this workflow to enrich internal Pure person records for review and update.",
    reviewFiles: ["personstobeupdated_<date>.csv", "datatotal.json"],
    resultDefinitions: [
      {
        label: "Persons found",
        description: "How many Pure person records from this faculty run were actually returned in the generated JSON snapshot.",
      },
      {
        label: "Persons ready to update",
        description: "How many unique persons in this run still have an X in to_be_updated after review.",
      },
      {
        label: "Persons updated",
        description: "How many unique persons from this run are already marked as updated in the review CSV file.",
      },
    ],
    userSteps: [
      "Start the run for the selected faculty.",
      "Wait until the CSV and JSON review files appear.",
      "Open the CSV and remove any rows you do not want to update.",
      "Apply updates only after the review file looks correct.",
    ],
  },
  external_persons: {
    jobType: "external_persons",
    title: "External Persons",
    availability: "live",
    summary: "Match external researchers and prepare ORCID or OpenAlex updates for review.",
    currentScope:
      "Use this workflow to review and update external person identifiers.",
    reviewFiles: ["ext_pers_update.csv", "to_be_updated.json"],
    resultDefinitions: [
      {
        label: "Persons found",
        description: "Expected number of external person candidates found for this run.",
      },
      {
        label: "Persons ready to update",
        description: "Expected number of reviewed external person rows still selected for update.",
      },
      {
        label: "Persons updated",
        description: "Expected number of external persons already written back during apply.",
      },
    ],
    userSteps: [
      "Run the external person enrichment.",
      "Review matching candidates carefully because identity ambiguity is higher.",
      "Apply only the rows that are still correct after manual checking.",
    ],
  },
  external_orgs: {
    jobType: "external_orgs",
    title: "External Organisations",
    availability: "live",
    summary: "Prepare organisation updates such as ROR enrichment for external Pure organisations.",
    currentScope:
      "Use this workflow to review and update external organisations, including ROR and address data.",
    reviewFiles: ["external_orgs_to_update.csv", "external_orgs_updates.json"],
    resultDefinitions: [
      {
        label: "Organisations found",
        description: "Expected number of external organisations considered in the run.",
      },
      {
        label: "Organisations ready to update",
        description: "Expected number of reviewed organisation rows still selected for update.",
      },
      {
        label: "Organisations updated",
        description: "Expected number of organisation records already applied.",
      },
      {
        label: "Organisations rolled back",
        description: "How many organisation records from this run have already been reverted through rollback.",
      },
    ],
    userSteps: [
      "Run the organisation enrichment.",
      "Review the proposed organisation mappings, ROR identifiers, and address changes.",
      "Apply only after validating the reviewed CSV and JSON output.",
    ],
  },
  research_outputs: {
    jobType: "research_outputs",
    title: "Research Outputs",
    availability: "live",
    summary: "Prepare research output records from Ricgraph for review and import into Pure.",
    currentScope: "Use this workflow to review and import research outputs into Pure.",
    reviewFiles: ["to_be_updated.csv", "output_to_be_updated.json"],
    resultDefinitions: [
      {
        label: "Research outputs found",
        description: "Expected number of output records included in the current run scope.",
      },
      {
        label: "Research outputs ready to update",
        description: "Expected number of output rows still selected after review.",
      },
      {
        label: "Research outputs updated",
        description: "Expected number of outputs already written to Pure.",
      },
    ],
    userSteps: [
      "Run the output import.",
      "Review the generated metadata carefully.",
      "Apply only the subset that is ready for publication in Pure.",
    ],
  },
  datasets: {
    jobType: "datasets",
    title: "Datasets",
    availability: "live",
    summary: "Prepare dataset records from Ricgraph for review and import into Pure.",
    currentScope: "Use this workflow to review and import datasets into Pure.",
    reviewFiles: ["to_be_updated.csv", "datasets_to_be_updated.json"],
    resultDefinitions: [
      {
        label: "Datasets found",
        description: "Expected number of dataset records covered by the run.",
      },
      {
        label: "Datasets ready to update",
        description: "Expected number of reviewed dataset rows still selected for update.",
      },
      {
        label: "Datasets updated",
        description: "Expected number of datasets already applied back to Pure.",
      },
    ],
    userSteps: [
      "Run the dataset import.",
      "Inspect the generated metadata and identifiers.",
      "Apply only the reviewed rows that should be sent to Pure.",
    ],
  },
};

export function isActiveJobStatus(status: JobStatus) {
  return ACTIVE_STATUSES.includes(status);
}

export function formatJobStatus(status: JobStatus | JobType) {
  return status.replace(/_/g, " ");
}

export function formatTimestamp(value: string | null) {
  if (!value) {
    return "Not available";
  }

  return new Intl.DateTimeFormat("en-GB", {
    dateStyle: "medium",
    timeStyle: "short",
  }).format(new Date(value));
}

export function describeReadiness(job: JobRecord) {
  if (job.canApply) {
    return "Ready to apply";
  }
  if (job.canOpen) {
    return "Review files available";
  }
  if (job.status === "running") {
    return "Generating review files";
  }
  if (job.status === "applying") {
    return "Applying updates";
  }
  if (job.status === "queued") {
    return "Not started yet";
  }
  if (job.status === "completed") {
    return "Finished";
  }
  if (job.status === "failed") {
    return "Needs attention";
  }
  return "No review files yet";
}

export function nextUserAction(job: JobRecord) {
  if (job.status === "queued") {
    return "Start the job to generate the review files.";
  }
  if (job.status === "running") {
    return "Wait for the run to finish. Review files will appear here automatically.";
  }
  if (job.status === "needs_review") {
    return "Download and inspect the CSV and JSON review files before applying updates.";
  }
  if (job.status === "applying") {
    return "Wait for the apply step to finish. The log below shows the live outcome.";
  }
  if (job.status === "completed") {
    return "The apply step finished. Use the review files and log below as your audit trail.";
  }
  if (job.status === "failed") {
    return "Read the error and log output, then decide whether the job should be rerun.";
  }
  return "No action available yet.";
}

export function formatBytes(value: number) {
  if (value < 1024) {
    return `${value} B`;
  }
  if (value < 1024 * 1024) {
    return `${(value / 1024).toFixed(1)} KB`;
  }
  return `${(value / (1024 * 1024)).toFixed(1)} MB`;
}

export function getJobTypeGuide(jobType: JobType) {
  return JOB_TYPE_GUIDES[jobType];
}

export function listJobTypeGuides() {
  return Object.values(JOB_TYPE_GUIDES);
}

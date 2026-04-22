import { useDeferredValue, useMemo, useState } from "react";
import { Link } from "react-router-dom";
import { useQuery } from "@tanstack/react-query";

import { api } from "../../lib/api";
import { describeReadiness, formatTimestamp } from "../../lib/job-ui";
import type { JobRecord, JobStatus, JobType } from "../../lib/types";

const JOB_TYPE_OPTIONS: Array<{ value: "all" | JobType; label: string }> = [
  { value: "all", label: "All Job Types" },
  { value: "internal_persons", label: "Internal Persons" },
  { value: "external_persons", label: "External Persons" },
  { value: "external_orgs", label: "External Organisations" },
  { value: "research_outputs", label: "Research Outputs" },
  { value: "datasets", label: "Datasets" },
];

const JOB_STATUS_OPTIONS: Array<{ value: "all" | JobStatus; label: string }> = [
  { value: "all", label: "All Statuses" },
  { value: "queued", label: "Queued" },
  { value: "running", label: "Running" },
  { value: "needs_review", label: "Needs Review" },
  { value: "applying", label: "Applying" },
  { value: "completed", label: "Completed" },
  { value: "failed", label: "Failed" },
];

function matchesJob(job: JobRecord, query: string) {
  if (!query) {
    return true;
  }
  const haystack = [
    job.id,
    job.job_type,
    job.status,
    job.error_message ?? "",
    describeReadiness(job),
    Object.values(job.params).join(" "),
  ]
    .join(" ")
    .toLowerCase();
  return haystack.includes(query);
}

export function HistoryPage() {
  const [jobTypeFilter, setJobTypeFilter] = useState<"all" | JobType>("all");
  const [statusFilter, setStatusFilter] = useState<"all" | JobStatus>("all");
  const [search, setSearch] = useState("");
  const deferredSearch = useDeferredValue(search.trim().toLowerCase());

  const jobsQuery = useQuery({
    queryKey: ["jobs", "history"],
    queryFn: () => api.getJobs(),
  });

  const jobs = jobsQuery.data?.items ?? [];
  const filteredJobs = useMemo(
    () =>
      jobs.filter((job) => {
        if (jobTypeFilter !== "all" && job.job_type !== jobTypeFilter) {
          return false;
        }
        if (statusFilter !== "all" && job.status !== statusFilter) {
          return false;
        }
        return matchesJob(job, deferredSearch);
      }),
    [deferredSearch, jobTypeFilter, jobs, statusFilter],
  );

  const completedJobs = filteredJobs.filter((job) => job.status === "completed").length;
  const reviewJobs = filteredJobs.filter((job) => job.status === "needs_review").length;
  const failedJobs = filteredJobs.filter((job) => job.status === "failed").length;

  return (
    <section className="page page-history">
      <header className="page-header">
        <p className="eyebrow">History</p>
        <h2>Job History</h2>
        <p>Browse all jobs, filter by workflow or status, and jump back into a specific run.</p>
      </header>

      <section className="panel">
        <div className="panel-header">
          <div>
            <p className="eyebrow">Filters</p>
            <h3>Find A Job</h3>
          </div>
          <button className="secondary-action" type="button" onClick={() => void jobsQuery.refetch()}>
            Refresh
          </button>
        </div>

        <div className="history-filter-grid">
          <label className="field">
            <span>Job Type</span>
            <select value={jobTypeFilter} onChange={(event) => setJobTypeFilter(event.target.value as "all" | JobType)}>
              {JOB_TYPE_OPTIONS.map((option) => (
                <option key={option.value} value={option.value}>
                  {option.label}
                </option>
              ))}
            </select>
          </label>
          <label className="field">
            <span>Status</span>
            <select value={statusFilter} onChange={(event) => setStatusFilter(event.target.value as "all" | JobStatus)}>
              {JOB_STATUS_OPTIONS.map((option) => (
                <option key={option.value} value={option.value}>
                  {option.label}
                </option>
              ))}
            </select>
          </label>
          <label className="field">
            <span>Search</span>
            <input
              type="search"
              value={search}
              onChange={(event) => setSearch(event.target.value)}
              placeholder="Job id, workflow, faculty, error..."
            />
          </label>
        </div>
      </section>

      <section className="metric-grid">
        <article className="metric-card">
          <p className="eyebrow">Visible Jobs</p>
          <strong>{filteredJobs.length}</strong>
          <span>Current filter result</span>
        </article>
        <article className="metric-card">
          <p className="eyebrow">Completed</p>
          <strong>{completedJobs}</strong>
          <span>Finished successfully</span>
        </article>
        <article className="metric-card">
          <p className="eyebrow">Needs Review</p>
          <strong>{reviewJobs}</strong>
          <span>Still waiting for action</span>
        </article>
        <article className="metric-card">
          <p className="eyebrow">Failed</p>
          <strong>{failedJobs}</strong>
          <span>Need attention</span>
        </article>
      </section>

      <section className="panel">
        <div className="panel-header">
          <div>
            <p className="eyebrow">All Jobs</p>
            <h3>Filtered Results</h3>
          </div>
        </div>

        {jobsQuery.isLoading ? <p className="empty-state">Loading job history...</p> : null}
        {jobsQuery.isError ? <p className="error-banner">Could not load job history from the backend API.</p> : null}
        {!jobsQuery.isLoading && !jobsQuery.isError && filteredJobs.length === 0 ? (
          <p className="empty-state">No jobs match the current filters.</p>
        ) : null}

        {!jobsQuery.isLoading && !jobsQuery.isError && filteredJobs.length ? (
          <div className="history-list">
            {filteredJobs.map((job) => (
              <Link key={job.id} to={`/jobs/${job.id}`} className="history-row">
                <div className="history-row-main">
                  <strong>{job.id}</strong>
                  <span>{job.job_type.replace(/_/g, " ")}</span>
                </div>
                <div className="history-row-meta">
                  <span>{job.status.replace(/_/g, " ")}</span>
                  <span>{describeReadiness(job)}</span>
                  <span>{formatTimestamp(job.created_at)}</span>
                </div>
              </Link>
            ))}
          </div>
        ) : null}
      </section>
    </section>
  );
}

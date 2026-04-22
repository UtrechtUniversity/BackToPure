import { useMutation, useQuery } from "@tanstack/react-query";
import { Link } from "react-router-dom";
import { useState } from "react";

import { ApiError, api } from "../../lib/api";
import { StatusPill } from "../../components/StatusPill";
import { describeReadiness, formatTimestamp, isActiveJobStatus } from "../../lib/job-ui";

export function DashboardPage() {
  const [resultsWindow, setResultsWindow] = useState<number | null>(30);
  const jobsQuery = useQuery({
    queryKey: ["jobs"],
    queryFn: () => api.getJobs(),
    refetchInterval: (query) =>
      query.state.data?.items.some((job) => isActiveJobStatus(job.status)) ? 5000 : false,
  });
  const resultsDashboardQuery = useQuery({
    queryKey: ["results-dashboard", resultsWindow],
    queryFn: () => api.getResultsDashboard(resultsWindow),
    refetchInterval: 15000,
  });

  const deleteJobMutation = useMutation({
    mutationFn: (jobId: string) => api.deleteJob(jobId),
    onSuccess: () => {
      void jobsQuery.refetch();
      void resultsDashboardQuery.refetch();
    },
  });

  const jobs = jobsQuery.data?.items ?? [];
  const activeJobs = jobs.filter((job) => isActiveJobStatus(job.status)).length;
  const reviewJobs = jobs.filter((job) => job.status === "needs_review").length;
  const failedJobs = jobs.filter((job) => job.status === "failed").length;
  const resultsDashboard = resultsDashboardQuery.data;

  return (
    <section className="page page-dashboard">
      <header className="hero-card">
        <div>
          <p className="eyebrow">BackToPure</p>
          <h2>Job Dashboard</h2>
          <p className="hero-copy">
            Start jobs, follow progress, review files, and apply approved updates.
          </p>
        </div>
        <div className="inline-actions">
          <Link className="secondary-action" to="/jobs/new/datasets">
            New Datasets Job
          </Link>
          <Link className="secondary-action" to="/jobs/new/research-outputs">
            New Research Outputs Job
          </Link>
          <Link className="secondary-action" to="/jobs/new/external-orgs">
            New External Organisations Job
          </Link>
          <Link className="secondary-action" to="/jobs/new/external-persons">
            New External Persons Job
          </Link>
          <Link className="primary-action" to="/jobs/new/internal-persons">
            New Internal Persons Job
          </Link>
        </div>
      </header>

      <section className="metric-grid">
        <article className="metric-card">
          <p className="eyebrow">Active</p>
          <strong>{activeJobs}</strong>
          <span>Queued or running jobs</span>
        </article>
        <article className="metric-card">
          <p className="eyebrow">Needs Review</p>
          <strong>{reviewJobs}</strong>
          <span>Review files ready to inspect</span>
        </article>
        <article className="metric-card">
          <p className="eyebrow">Failed</p>
          <strong>{failedJobs}</strong>
          <span>Runs needing attention</span>
        </article>
      </section>

      <section className="panel">
        <div className="panel-header">
          <div>
            <p className="eyebrow">Results Overview</p>
            <h3>Net Results Across All Jobs</h3>
          </div>
          <div className="results-toolbar">
            <div className="results-window-toggle" role="group" aria-label="Results Period">
              {[
                { label: "7d", value: 7 },
                { label: "30d", value: 30 },
                { label: "90d", value: 90 },
                { label: "All", value: null },
              ].map((option) => (
                <button
                  key={option.label}
                  className={resultsWindow === option.value ? "results-window-chip active" : "results-window-chip"}
                  type="button"
                  onClick={() => setResultsWindow(option.value)}
                >
                  {option.label}
                </button>
              ))}
            </div>
            <button className="secondary-action" type="button" onClick={() => void resultsDashboardQuery.refetch()}>
              Refresh
            </button>
          </div>
        </div>

        {resultsDashboardQuery.isLoading ? <p className="empty-state">Loading results overview...</p> : null}
        {resultsDashboardQuery.isError ? (
          <p className="error-banner">Could not load the results overview from the backend API.</p>
        ) : null}
        {resultsDashboard ? (
          <>
            <div className="results-overview-grid">
              <article className="metric-card results-total-card results-total-card-net">
                <p className="eyebrow">Net Effect</p>
                <strong>{resultsDashboard.totals.net_entities}</strong>
                <span>Entities still active after rollback</span>
              </article>
              <article className="metric-card results-total-card results-total-card-applied">
                <p className="eyebrow">Applied</p>
                <strong>{resultsDashboard.totals.applied_entities}</strong>
                <span>Entities updated or created in total</span>
              </article>
              <article className="metric-card results-total-card results-total-card-rollback">
                <p className="eyebrow">Rolled Back</p>
                <strong>{resultsDashboard.totals.rolled_back_entities}</strong>
                <span>Entities reversed in total</span>
              </article>
            </div>

            <div className="results-workflow-list">
              {resultsDashboard.workflows.map((workflow) => (
                <article key={workflow.job_type} className="results-workflow-card">
                  <div className="results-workflow-header">
                    <div>
                      <h4>{workflow.label}</h4>
                      <p className="job-card-line-secondary">
                        Net {workflow.net_entities}, applied {workflow.applied_entities}, rolled back {workflow.rolled_back_entities}
                      </p>
                    </div>
                    <div className="results-workflow-pills">
                      <span className="results-pill results-pill-net">Net {workflow.net_entities}</span>
                      <span className="results-pill results-pill-applied">Applied {workflow.applied_entities}</span>
                      <span className="results-pill results-pill-rollback">Rollback {workflow.rolled_back_entities}</span>
                    </div>
                  </div>
                  {workflow.breakdown.length ? (
                    <div className="results-breakdown-list">
                      {workflow.breakdown.map((item) => (
                        <div key={`${workflow.job_type}-${item.key}`} className="results-breakdown-row">
                          <span className="results-breakdown-label">{item.label}</span>
                          <span className="results-breakdown-values">
                            <span>Net {item.net_items}</span>
                            <span>Rollback {item.rolled_back_items}</span>
                          </span>
                        </div>
                      ))}
                    </div>
                  ) : (
                    <p className="job-card-line-secondary">
                      Net items {workflow.net_items} | rollback {workflow.rolled_back_items}
                    </p>
                  )}
                </article>
              ))}
            </div>
          </>
        ) : null}
      </section>

      <section className="panel panel-note">
        <p className="eyebrow">Review Flow</p>
        <h3>Review rows before applying updates</h3>
        <p className="hero-copy">
          Open each job to inspect the review files, adjust selected rows in the review table, and
          only apply after the proposed changes still look correct.
        </p>
      </section>

      <section className="panel">
        <div className="panel-header">
          <div>
            <p className="eyebrow">Recent Jobs</p>
            <h3>Tracked Runs</h3>
          </div>
          <button className="secondary-action" type="button" onClick={() => void jobsQuery.refetch()}>
            Refresh
          </button>
        </div>

        {jobsQuery.isLoading ? <p className="empty-state">Loading jobs...</p> : null}
        {jobsQuery.isError ? (
          <p className="error-banner">Could not load jobs from the backend API.</p>
        ) : null}
        {!jobsQuery.isLoading && !jobsQuery.isError && jobs.length === 0 ? (
          <p className="empty-state">
            No jobs yet. Start with internal persons, external persons, external organisations, research outputs, or datasets.
          </p>
        ) : null}

        {!jobsQuery.isLoading && !jobsQuery.isError && jobs.length ? (
          <div className="job-list">
            {jobs.map((job) => (
              <article key={job.id} className="job-card">
                <Link to={`/jobs/${job.id}`} className="job-card-link">
                  <div className="job-card-line">
                    <div className="job-card-inline-meta">
                      <StatusPill status={job.status} />
                      <span className="job-type">{job.job_type}</span>
                    </div>
                    <h4>{job.id}</h4>
                  </div>
                  <div className="job-card-line job-card-line-secondary">
                    <p>Created {formatTimestamp(job.created_at)}</p>
                    <p>Step: {describeReadiness(job)}</p>
                  </div>
                  {job.error_message ? <p className="job-error">{job.error_message}</p> : null}
                </Link>
                {!isActiveJobStatus(job.status) ? (
                  <div className="job-card-actions">
                    <button
                      className="danger-action"
                      type="button"
                      onClick={() => {
                        if (window.confirm(`Delete job ${job.id}?`)) {
                          deleteJobMutation.mutate(job.id);
                        }
                      }}
                      disabled={deleteJobMutation.isPending}
                    >
                      {deleteJobMutation.isPending && deleteJobMutation.variables === job.id
                        ? "Deleting..."
                        : "Delete"}
                    </button>
                  </div>
                ) : null}
              </article>
            ))}
          </div>
        ) : null}
        {deleteJobMutation.isError ? (
          <p className="error-banner">
            {deleteJobMutation.error instanceof ApiError
              ? deleteJobMutation.error.message
              : "Could not delete the selected job."}
          </p>
        ) : null}
      </section>
    </section>
  );
}
